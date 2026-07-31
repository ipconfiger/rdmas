//! Asynchronous Replication (T4-D)
//!
//! Design spec: Rust-RDMA.md §一.3, §八
//!
//! Async replication: the primary server RDMA-WRITEs updates to a backup node.
//! In production, this uses actual RDMA WRITE operations. For Wave 4, implement
//! the replication logic as a local simulation with an in-memory backup store.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A replicated backup of the primary server's state.
///
/// In production, this would be a remote RDMA node receiving WRITEs.
/// For Wave 4 simulation, it's an in-memory mirror.
pub struct BackupStore {
    /// Copy of the large object region data.
    objects: Arc<Mutex<Vec<u8>>>,
    /// Pending replication queue: (offset, data)
    pending: Arc<Mutex<VecDeque<(u64, Vec<u8>)>>>,
}

impl BackupStore {
    /// Create a new backup store with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            objects: Arc::new(Mutex::new(vec![0u8; capacity])),
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Enqueue a write for async replication.
    pub fn enqueue_write(&self, offset: u64, data: &[u8]) {
        self.pending.lock().unwrap().push_back((offset, data.to_vec()));
    }

    /// Replicate all pending writes to the backup.
    /// Returns the number of writes replicated.
    pub fn flush(&self) -> usize {
        let mut objects = self.objects.lock().unwrap();
        let mut pending = self.pending.lock().unwrap();
        let count = pending.len();

        while let Some((offset, data)) = pending.pop_front() {
            let end = (offset as usize + data.len()).min(objects.len());
            let start = offset as usize;
            objects[start..end].copy_from_slice(&data[..end - start]);
        }

        count
    }

    /// Read data from the backup at the given offset.
    pub fn read(&self, offset: u64, length: usize) -> Option<Vec<u8>> {
        let objects = self.objects.lock().unwrap();
        let start = offset as usize;
        let end = (start + length).min(objects.len());
        if start >= objects.len() {
            return None;
        }
        Some(objects[start..end].to_vec())
    }

    /// Number of pending replication entries.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

/// Replication manager: coordinates async replication from primary to backup.
pub struct ReplicationManager {
    backup: Arc<BackupStore>,
    /// Maximum replication lag (in number of pending entries) before blocking
    max_lag: usize,
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new(backup_capacity: usize, max_lag: usize) -> Self {
        Self {
            backup: Arc::new(BackupStore::new(backup_capacity)),
            max_lag,
        }
    }

    /// Write data and schedule async replication.
    pub fn write_with_replication(
        &self,
        offset: u64,
        data: &[u8],
        local_store: &mut [u8],
    ) {
        // Write locally first
        let end = (offset as usize + data.len()).min(local_store.len());
        local_store[offset as usize..end].copy_from_slice(&data[..end - offset as usize]);

        // Enqueue for async replication
        self.backup.enqueue_write(offset, data);
    }

    /// Flush replication queue. Called periodically or on demand.
    pub fn flush_replication(&self) -> usize {
        self.backup.flush()
    }

    /// Check if replication lag exceeds threshold.
    pub fn is_lagging(&self) -> bool {
        self.backup.pending_count() > self.max_lag
    }

    /// Get the backup store.
    pub fn backup(&self) -> &Arc<BackupStore> {
        &self.backup
    }

    /// Read from the backup (for verification).
    pub fn read_from_backup(&self, offset: u64, length: usize) -> Option<Vec<u8>> {
        self.backup.read(offset, length)
    }
}

/// Replication status for monitoring.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    /// Number of pending replication entries
    pub pending_count: usize,
    /// Whether replication is lagging
    pub lagging: bool,
    /// Total writes replicated
    pub total_replicated: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_store_enqueue_and_flush() {
        let store = BackupStore::new(1024);
        store.enqueue_write(0, &[1u8, 2, 3, 4]);
        store.enqueue_write(4, &[5u8, 6, 7, 8]);
        assert_eq!(store.pending_count(), 2);

        let flushed = store.flush();
        assert_eq!(flushed, 2);
        assert_eq!(store.pending_count(), 0);
    }

    #[test]
    fn test_backup_store_read() {
        let store = BackupStore::new(1024);
        store.enqueue_write(10, b"hello");
        store.flush();

        let data = store.read(10, 5).unwrap();
        assert_eq!(&data, b"hello");
    }

    #[test]
    fn test_backup_store_read_out_of_bounds() {
        let store = BackupStore::new(64);
        assert!(store.read(100, 10).is_none());
    }

    #[test]
    fn test_replication_manager_write_and_replicate() {
        let manager = ReplicationManager::new(1024, 10);
        let mut local = vec![0u8; 64];

        manager.write_with_replication(0, b"test_data", &mut local);

        assert_eq!(&local[0..9], b"test_data");
        assert!(manager.backup().pending_count() > 0);

        manager.flush_replication();
        assert_eq!(manager.backup().pending_count(), 0);

        let backup_data = manager.read_from_backup(0, 9).unwrap();
        assert_eq!(&backup_data, b"test_data");
    }

    #[test]
    fn test_replication_lag_detection() {
        let manager = ReplicationManager::new(1024, 2);
        let mut local = vec![0u8; 64];

        manager.write_with_replication(0, b"a", &mut local);
        assert!(!manager.is_lagging());

        manager.write_with_replication(1, b"b", &mut local);
        manager.write_with_replication(2, b"c", &mut local);
        assert!(manager.is_lagging()); // 3 pending > 2 max_lag
    }

    #[test]
    fn test_backup_store_empty() {
        let store = BackupStore::new(64);
        assert_eq!(store.pending_count(), 0);
        assert_eq!(store.flush(), 0);
    }
}
