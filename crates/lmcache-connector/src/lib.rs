//! LMCache L2 RDMA connector — PyO3 bindings for one-sided RDMA KV cache.
//!
//! ## Architecture
//!
//! This crate exposes a `RDMANativeConnector` Python class that provides a
//! 6-method async interface for LMCache batch operations:
//!
//! - `submit_batch_get` / `submit_batch_set` — async read/write
//! - `submit_batch_exists` / `submit_batch_delete` — existence check / delete
//! - `drain_completions` — poll for completed futures
//! - `event_fd` — completion notification fd for LMCache demux thread
//!
//! ## Implementation Status
//!
//! Currently operates in **local simulation** mode using in-process
//! `CuckooTable` + `LargeObjectRegion`. In production (Wave 5+), these
//! will be replaced with actual one-sided RDMA READ/WRITE/CAS operations
//! against a remote server's registered memory regions.
//!
//! Design doc: Rust-RDMA.md §五 "LMCache Integration"

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use pyo3::prelude::*;
use rdmas::engine::cuckoo::CuckooTable;
use rdmas::engine::extent::LargeObjectRegion;
use rdmas::engine::layout::{BucketMode, HashedKey};
use xxhash_rust::xxh64::xxh64;

// ---------------------------------------------------------------------------
// Key hashing — mirrors engine layout.rs semantics
// ---------------------------------------------------------------------------

/// Hash an LMCache ObjectKey string into a [`HashedKey`].
///
/// Uses XXH64 with seed=0 for the 64-bit hash and seed=1 for a secondary
/// 64-bit value stored as the first half of the 16-byte digest. This is
/// the same scheme used by the engine's client read/write paths.
fn hash_lmcache_key(key: &str) -> HashedKey {
    let hash = xxh64(key.as_bytes(), 0);
    let h2 = xxh64(key.as_bytes(), 1);
    let mut digest = [0u8; 16];
    digest[0..8].copy_from_slice(&hash.to_le_bytes());
    digest[8..16].copy_from_slice(&h2.to_le_bytes());
    HashedKey { hash, digest }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Type of async operation submitted to the connector.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum OpType {
    Get,
    Set,
    Exists,
    Delete,
}

/// A pending async operation tracked by future_id.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PendingOp {
    future_id: u64,
    op_type: OpType,
    keys: Vec<String>,
}

/// Completed batch operation result.
#[derive(Debug, Clone)]
struct Completion {
    future_id: u64,
    ok: bool,
    error: String,
    per_key_bools: Option<Vec<bool>>,
}

// ---------------------------------------------------------------------------
// RDMANativeConnector
// ---------------------------------------------------------------------------

/// Python-accessible RDMA connector for LMCache L2 storage.
///
/// Provides async batch get/set/exists/delete operations with completion
/// notification via an eventfd.  Currently runs in local simulation mode
/// using in-memory Cuckoo hash table + extent storage.
#[pyclass]
pub struct RDMANativeConnector {
    /// Local Cuckoo hash table (simulated; will be remote RDMA in prod).
    table: Arc<Mutex<CuckooTable>>,

    /// Large-object extent storage (simulated).
    large_objects: Arc<Mutex<LargeObjectRegion>>,

    /// Monotonically increasing future_id counter.
    next_id: AtomicU64,

    /// Pending operations keyed by future_id.
    #[allow(dead_code)]
    pending: Arc<Mutex<HashMap<u64, PendingOp>>>,

    /// Completed operation results waiting to be drained.
    completions: Arc<Mutex<Vec<Completion>>>,

    /// Background worker thread handles.
    workers: Vec<JoinHandle<()>>,

    /// Shutdown flag; set to `true` by `close()`.
    shutdown: Arc<AtomicBool>,

    /// Whether `close()` has already been called (prevents double-close of fd).
    closed: Arc<AtomicBool>,

    /// EventFD used to notify the LMCache demux thread that completions are
    /// ready to be drained via `drain_completions()`.  Stored as `i32` (the
    /// OS-level `c_int`); cast to `i64` when exposed to Python.
    event_fd: i32,

    /// Batch chunk threshold in bytes.
    _batch_chunk_num_bytes: u64,

    /// Number of background worker threads.
    _num_workers: usize,
}

#[pymethods]
impl RDMANativeConnector {
    /// Create a new RDMA connector.
    ///
    /// # Arguments
    ///
    /// - `device`: RDMA device name (unused in local simulation).
    /// - `server`: Server address (unused in local simulation).
    /// - `num_workers`: Number of background worker threads (default 4).
    /// - `batch_chunk_num_bytes`: Batch chunk threshold in bytes (default 16 MiB).
    #[new]
    #[pyo3(signature = (device="", server="", num_workers=4, batch_chunk_num_bytes=16777216))]
    fn new(
        device: &str,
        server: &str,
        num_workers: usize,
        batch_chunk_num_bytes: u64,
    ) -> PyResult<Self> {
        // device and server are unused in local simulation mode.
        let _ = (device, server);
        // Engine components: 1M-bucket Cuckoo table + 160 MiB extent region.
        let bucket_count: u64 = 1 << 20; // 1,048,576 buckets
        let table = CuckooTable::new(bucket_count, 16);
        let large_objects = LargeObjectRegion::new(batch_chunk_num_bytes as usize * 10);

        let shutdown = Arc::new(AtomicBool::new(false));
        let closed_flag = Arc::new(AtomicBool::new(false));
        let pending: Arc<Mutex<HashMap<u64, PendingOp>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let completions: Arc<Mutex<Vec<Completion>>> =
            Arc::new(Mutex::new(Vec::new()));

        // Create an eventfd for completion notification.
        // EFD_NONBLOCK so reads in the demux thread return EAGAIN when empty.
        let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        if event_fd < 0 {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "failed to create eventfd",
            ));
        }

        // Spawn background workers. In local simulation mode these are
        // placeholder — operations are processed synchronously in the
        // submit_* methods. In production they will poll the RDMA CQ
        // and push completions.
        let mut workers = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let shutdown = shutdown.clone();
            let handle = thread::Builder::new()
                .name(format!("lmcache-worker-{worker_id}"))
                .spawn(move || {
                    // Worker loop: sleep in short intervals until shutdown.
                    while !shutdown.load(Ordering::Relaxed) {
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                })
                .expect("failed to spawn worker thread");
            workers.push(handle);
        }

        Ok(RDMANativeConnector {
            table: Arc::new(Mutex::new(table)),
            large_objects: Arc::new(Mutex::new(large_objects)),
            next_id: AtomicU64::new(1),
            pending,
            completions,
            workers,
            shutdown,
            closed: closed_flag,
            event_fd,
            _batch_chunk_num_bytes: batch_chunk_num_bytes,
            _num_workers: num_workers,
        })
    }

    // -----------------------------------------------------------------------
    // event_fd
    // -----------------------------------------------------------------------

    /// Return the completion notification eventfd.
    ///
    /// The LMCache demux thread should `epoll`/`select` on this fd.  When
    /// the fd becomes readable, the caller should invoke `drain_completions()`
    /// to collect finished futures.
    fn event_fd(&self) -> i64 {
        self.event_fd as i64
    }

    // -----------------------------------------------------------------------
    // submit_batch_get
    // -----------------------------------------------------------------------

    /// Submit an async batch read operation.
    ///
    /// `keys` are LMCache ObjectKey strings.  `mvs` are pre-allocated
    /// `memoryview` objects that will be written into by the RDMA read
    /// (in production mode).  In local simulation we only record which
    /// keys exist.
    ///
    /// Returns a `future_id` that can be matched against completions
    /// returned by `drain_completions()`.
    fn submit_batch_get(&self, keys: Vec<String>, _mvs: Vec<PyObject>) -> u64 {
        let future_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let results: Vec<bool> = {
            let table = self.table.lock().unwrap();
            keys.iter()
                .map(|k| {
                    let hk = hash_lmcache_key(k);
                    table.lookup(&hk).is_some()
                })
                .collect()
        };

        self.push_completion(future_id, true, String::new(), Some(results));
        future_id
    }

    // -----------------------------------------------------------------------
    // submit_batch_set
    // -----------------------------------------------------------------------

    /// Submit an async batch write operation.
    ///
    /// Each `key` is paired with the corresponding `mv` (memoryview of the
    /// tensor data).  Values ≤ 32 bytes are stored inline in the Cuckoo
    /// bucket; larger values are stored as extents in the large-object region.
    ///
    /// Returns a `future_id` for matching against completions.
    fn submit_batch_set(&self, keys: Vec<String>, mvs: Vec<PyObject>) -> u64 {
        let future_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let results: Vec<bool> = {
            let mut table = self.table.lock().unwrap();
            let mut large_objects = self.large_objects.lock().unwrap();

            keys.iter()
                .zip(mvs.iter())
                .map(|(key, mv)| {
                    let hk = hash_lmcache_key(key);

                    // Extract raw bytes from the Python memoryview.
                    let data: Vec<u8> = Python::with_gil(|py| {
                        mv.call_method0(py, "tobytes")
                            .ok()
                            .and_then(|b| b.extract::<Vec<u8>>(py).ok())
                            .unwrap_or_default()
                    });

                    if data.is_empty() && !key.is_empty() {
                        // Zero-length values are legal (marker-only entries).
                        return table.insert(&hk, &data, BucketMode::Inline).is_ok();
                    }

                    let mode = if data.len() <= 32 {
                        BucketMode::Inline
                    } else {
                        BucketMode::Extent
                    };

                    match mode {
                        BucketMode::Inline => {
                            table.insert(&hk, &data, BucketMode::Inline).is_ok()
                        }
                        BucketMode::Extent => {
                            if let Some(offset) = large_objects.allocate(&data) {
                                table.insert_extent(&hk, offset, data.len() as u64).is_ok()
                            } else {
                                false
                            }
                        }
                    }
                })
                .collect()
        };

        self.push_completion(future_id, true, String::new(), Some(results));
        future_id
    }

    // -----------------------------------------------------------------------
    // submit_batch_exists
    // -----------------------------------------------------------------------

    /// Submit an async batch existence check.
    ///
    /// Returns a `future_id`; the completion's `per_key_bools` indicates
    /// which keys exist in the store.
    fn submit_batch_exists(&self, keys: Vec<String>) -> u64 {
        let future_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let results: Vec<bool> = {
            let table = self.table.lock().unwrap();
            keys.iter()
                .map(|k| {
                    let hk = hash_lmcache_key(k);
                    table.lookup(&hk).is_some()
                })
                .collect()
        };

        self.push_completion(future_id, true, String::new(), Some(results));
        future_id
    }

    // -----------------------------------------------------------------------
    // submit_batch_delete
    // -----------------------------------------------------------------------

    /// Submit an async batch delete operation.
    ///
    /// Returns a `future_id`; the completion's `per_key_bools` indicates
    /// which keys were successfully deleted (i.e., existed before the call).
    fn submit_batch_delete(&self, keys: Vec<String>) -> u64 {
        let future_id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let results: Vec<bool> = {
            let mut table = self.table.lock().unwrap();
            keys.iter()
                .map(|k| {
                    let hk = hash_lmcache_key(k);
                    table.delete(&hk)
                })
                .collect()
        };

        self.push_completion(future_id, true, String::new(), Some(results));
        future_id
    }

    // -----------------------------------------------------------------------
    // drain_completions
    // -----------------------------------------------------------------------

    /// Drain all completed futures.
    ///
    /// Each element is `(future_id, ok, error, per_key_bools)`:
    /// - `future_id`: matches the value returned by a `submit_*` call.
    /// - `ok`: whether the batch succeeded overall.
    /// - `error`: error message if `ok` is `False`.
    /// - `per_key_bools`: per-key results for set/delete/exists; also
    ///   populated for get (indicates key existence).
    ///
    /// The caller should invoke this whenever the `event_fd` becomes
    /// readable, and then call `eventfd_read` to clear the fd.
    fn drain_completions(&self) -> Vec<(u64, bool, String, Option<Vec<bool>>)> {
        let mut comps = self.completions.lock().unwrap();
        comps
            .drain(..)
            .map(|c| (c.future_id, c.ok, c.error, c.per_key_bools))
            .collect()
    }

    // -----------------------------------------------------------------------
    // close
    // -----------------------------------------------------------------------

    /// Gracefully shut down the connector.
    ///
    /// Signals background workers to stop, closes the eventfd, and joins
    /// worker threads.  Idempotent — safe to call multiple times.
    fn close(&self) {
        // Prevent double-close.
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        // Signal workers to exit.
        self.shutdown.store(true, Ordering::SeqCst);

        // Close the eventfd.
        unsafe {
            libc::close(self.event_fd);
        }
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

impl RDMANativeConnector {
    /// Push a completion and notify the eventfd.
    fn push_completion(
        &self,
        future_id: u64,
        ok: bool,
        error: String,
        per_key_bools: Option<Vec<bool>>,
    ) {
        self.completions.lock().unwrap().push(Completion {
            future_id,
            ok,
            error,
            per_key_bools,
        });

        // Write 8 bytes to eventfd to wake the polling demux thread.
        // Using a u64 counter value; the demux thread should call
        // eventfd_read to consume notifications.
        let buf: u64 = 1;
        let _ = unsafe {
            libc::write(
                self.event_fd,
                &buf as *const u64 as *const libc::c_void,
                8,
            )
        };
    }
}

// ---------------------------------------------------------------------------
// Drop
// ---------------------------------------------------------------------------

impl Drop for RDMANativeConnector {
    fn drop(&mut self) {
        // Signal shutdown if not already closed.
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.shutdown.store(true, Ordering::SeqCst);
            unsafe {
                libc::close(self.event_fd);
            }
        }

        // Join all worker threads. Drain the vec to take ownership.
        // We don't block indefinitely — workers sleep at 10ms intervals
        // and check the shutdown flag each iteration.
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Ensure the Python interpreter is initialized for tests that need GIL.
    fn ensure_python() {
        // prepare_freethreaded_python is idempotent — safe to call multiple times.
        pyo3::prepare_freethreaded_python();
    }

    // -----------------------------------------------------------------------
    // Key hashing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_hash_lmcache_key_nonzero() {
        let key = "llama-7b@0000000c@0@a1b2c3d4";
        let hk = hash_lmcache_key(key);
        assert_ne!(hk.hash, 0, "hash should be non-zero for non-empty key");
    }

    #[test]
    fn test_hash_deterministic() {
        let key = "test_key";
        let h1 = hash_lmcache_key(key);
        let h2 = hash_lmcache_key(key);
        assert_eq!(h1.hash, h2.hash, "hash must be deterministic");
        assert_eq!(h1.digest, h2.digest, "digest must be deterministic");
    }

    #[test]
    fn test_hash_different_keys_produce_different_hashes() {
        let h1 = hash_lmcache_key("key_a");
        let h2 = hash_lmcache_key("key_b");
        assert_ne!(h1.hash, h2.hash, "different keys should (almost always) hash differently");
    }

    #[test]
    fn test_hash_empty_key() {
        let hk = hash_lmcache_key("");
        // Empty key still produces a valid (non-zero) hash via XXH64.
        assert_ne!(hk.hash, 0);
    }

    #[test]
    fn test_hash_digest_structure() {
        let key = "my_model@00000001@0@abcdef01";
        let hk = hash_lmcache_key(key);
        // digest[0..8] = hash (we re-store it), digest[8..16] = secondary hash.
        // Verify digest[0..8] matches the primary hash.
        let primary_bytes = &hk.digest[0..8];
        let primary_from_digest = u64::from_le_bytes(primary_bytes.try_into().unwrap());
        assert_eq!(primary_from_digest, hk.hash);
    }

    // -----------------------------------------------------------------------
    // Integration tests (local simulation)
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_connector() {
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");
        assert!(connector.event_fd() >= 0, "eventfd must be a valid fd");
        connector.close();
    }

    #[test]
    fn test_batch_set_and_exists() {
        ensure_python();
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");

        let keys: Vec<String> = vec!["key_a".into(), "key_b".into(), "key_c".into()];

        // Build dummy memoryviews for set.
        Python::with_gil(|py| {
            let mvs: Vec<PyObject> = vec![
                pyo3::types::PyBytes::new(py, b"value_a").into(),
                pyo3::types::PyBytes::new(py, b"value_b").into(),
                pyo3::types::PyBytes::new(py, b"value_c").into(),
            ];

            let fid = connector.submit_batch_set(keys.clone(), mvs);
            assert!(fid > 0);
        });

        let fid_exists = connector.submit_batch_exists(keys);
        assert!(fid_exists > 0);

        let completions = connector.drain_completions();
        // We should have 2 completions: one for set, one for exists.
        assert_eq!(completions.len(), 2);

        // The exists completion should have all true.
        let exists_result = completions
            .iter()
            .find(|(fid, _, _, _)| *fid == fid_exists)
            .expect("exists completion not found");
        assert!(exists_result.1, "exists operation should succeed");
        assert_eq!(
            exists_result.3,
            Some(vec![true, true, true]),
            "all three keys should exist"
        );

        connector.close();
    }

    #[test]
    fn test_batch_set_and_get_existence() {
        ensure_python();
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");

        let keys: Vec<String> = vec!["get_test_key".into()];

        Python::with_gil(|py| {
            let mvs: Vec<PyObject> = vec![
                pyo3::types::PyBytes::new(py, b"get_test_value").into(),
            ];
            connector.submit_batch_set(keys.clone(), mvs);
        });

        let fid_get = connector.submit_batch_get(keys, vec![]);
        let completions = connector.drain_completions();
        let get_result = completions
            .iter()
            .find(|(fid, _, _, _)| *fid == fid_get)
            .expect("get completion not found");
        assert_eq!(get_result.3, Some(vec![true]), "key should exist");

        connector.close();
    }

    #[test]
    fn test_batch_delete() {
        ensure_python();
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");

        let keys: Vec<String> = vec!["del_key".into()];

        Python::with_gil(|py| {
            let mvs: Vec<PyObject> = vec![
                pyo3::types::PyBytes::new(py, b"value_to_delete").into(),
            ];
            connector.submit_batch_set(keys.clone(), mvs);
        });

        let fid_del = connector.submit_batch_delete(keys.clone());
        let fid_exists = connector.submit_batch_exists(keys);

        let completions = connector.drain_completions();
        let del_result = completions
            .iter()
            .find(|(fid, _, _, _)| *fid == fid_del)
            .expect("delete completion not found");
        assert_eq!(del_result.3, Some(vec![true]), "delete of existing key should succeed");

        let exists_result = completions
            .iter()
            .find(|(fid, _, _, _)| *fid == fid_exists)
            .expect("exists completion not found");
        assert_eq!(
            exists_result.3,
            Some(vec![false]),
            "key should no longer exist after delete"
        );

        connector.close();
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");

        let keys: Vec<String> = vec!["nonexistent".into()];
        let fid = connector.submit_batch_delete(keys);

        let completions = connector.drain_completions();
        let result = completions
            .iter()
            .find(|(f, _, _, _)| *f == fid)
            .expect("completion not found");

        assert_eq!(result.3, Some(vec![false]), "deleting nonexistent key should return false");
        connector.close();
    }

    #[test]
    fn test_event_fd() {
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");

        let fd = connector.event_fd();
        assert!(fd >= 0, "eventfd must be valid");

        // Submitting an operation should write to the eventfd.
        connector.submit_batch_exists(vec!["some_key".into()]);

        // Read from eventfd to verify it was written to.
        let mut val: u64 = 0;
        let ret = unsafe {
            libc::read(
                fd as i32,
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            )
        };
        assert!(ret >= 0, "reading eventfd should succeed");
        assert!(val > 0, "eventfd should have been written to");

        connector.close();
    }

    #[test]
    fn test_close_idempotent() {
        let connector = RDMANativeConnector::new("", "", 2, 16777216)
            .expect("creating connector should succeed");

        connector.close();
        // Second close should not panic.
        connector.close();
    }

    #[test]
    fn test_large_value_extent_path() {
        ensure_python();
        let connector = RDMANativeConnector::new("", "", 2, 1024 * 1024)
            .expect("creating connector should succeed");

        // Create a value larger than 32 bytes → should use Extent path.
        let large_value = vec![0xABu8; 128];
        let keys: Vec<String> = vec!["large_key".into()];

        Python::with_gil(|py| {
            let mvs: Vec<PyObject> = vec![
                pyo3::types::PyBytes::new(py, &large_value).into(),
            ];
            connector.submit_batch_set(keys.clone(), mvs);
        });

        // Verify the key exists.
        let fid = connector.submit_batch_exists(keys);
        let completions = connector.drain_completions();
        let result = completions
            .iter()
            .find(|(f, _, _, _)| *f == fid)
            .expect("completion not found");
        assert_eq!(result.3, Some(vec![true]), "large value key should exist");

        connector.close();
    }
}
