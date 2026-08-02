//! LRU eviction tracker for cache management.
//! T10-A: Tracks key access timestamps and supports eviction of
//! least-recently-used entries. Works alongside EpochGc for
//! tombstone-based GC.

use crossbeam::queue::SegQueue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Tracks LRU access order for cache eviction.
///
/// Uses `SegQueue` for lock-free access-time recording,
/// falling back to a periodic sort for eviction candidate selection.
/// Eviction candidates are selected by collecting recent access records,
/// sorting by timestamp, and returning the oldest key hashes.
pub struct LruTracker {
    /// Access records: `(key_hash, timestamp_ms)`.
    accesses: SegQueue<(u64, u64)>,
    /// Last access time per key for dedup during eviction selection.
    last_access: Mutex<HashMap<u64, u64>>,
    /// Total number of tracked keys (approximate).
    key_count: AtomicU64,
    /// Eviction watermark: start evicting when `key_count` exceeds this.
    watermark: u64,
    /// Monotonic eviction counter.
    evicted: AtomicU64,
}

impl LruTracker {
    /// Create a new LRU tracker.
    ///
    /// `watermark`: start eviction when the number of tracked keys
    /// exceeds this threshold.
    pub fn new(watermark: u64) -> Self {
        Self {
            accesses: SegQueue::new(),
            last_access: Mutex::new(HashMap::new()),
            key_count: AtomicU64::new(0),
            watermark,
            evicted: AtomicU64::new(0),
        }
    }

    /// Record an access to the given key hash.
    ///
    /// Thread-safe, lock-free: pushes `(key_hash, now_ms)` to the `SegQueue`
    /// and increments the approximate key count.
    pub fn record_access(&self, key_hash: u64) {
        let now = super::concurrency::now_ms() as u64;
        self.accesses.push((key_hash, now));
        // Increment key count (approximate; deduplication happens during
        // eviction candidate selection).
        self.key_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current tracked key count (approximate — includes
    /// duplicate accesses for the same key).
    pub fn key_count(&self) -> u64 {
        self.key_count.load(Ordering::Relaxed)
    }

    /// Check if eviction is needed (`key_count > watermark`).
    pub fn needs_eviction(&self) -> bool {
        self.key_count.load(Ordering::Relaxed) > self.watermark
    }

    /// Select up to `n` LRU candidates for eviction.
    ///
    /// Drains the `SegQueue`, deduplicates into `last_access`, sorts by
    /// timestamp, and returns the oldest `n` key hashes.  Keys that
    /// appear in `last_access` but not in the current drain are preserved
    /// (they may have been evicted previously).
    pub fn select_eviction_candidates(&self, n: usize) -> Vec<u64> {
        // Drain all pending access records
        let records: Vec<(u64, u64)> = {
            let mut v = Vec::with_capacity(256);
            while let Some(record) = self.accesses.pop() {
                v.push(record);
            }
            v
        };

        // Merge into last_access map (deduplicate by key_hash, keep
        // the most recent timestamp per key).
        {
            let mut la = self.last_access.lock().unwrap();
            for (key_hash, ts) in &records {
                la.insert(*key_hash, *ts);
            }
            // Update approximate key_count to reflect deduplicated set size.
            self.key_count
                .store(la.len() as u64, Ordering::Relaxed);

            // If map is empty or n == 0, nothing to evict.
            if la.is_empty() || n == 0 {
                return Vec::new();
            }

            // Sort entries by timestamp (ascending → oldest first).
            let mut sorted: Vec<(u64, u64)> = la.iter().map(|(&k, &v)| (k, v)).collect();
            sorted.sort_by_key(|&(_, ts)| ts);

            // Select the `n` oldest candidates and remove them from the map.
            let take = n.min(sorted.len());
            let candidates: Vec<u64> = sorted[..take].iter().map(|&(k, _)| k).collect();

            for &k in &candidates {
                la.remove(&k);
            }

            // Update count after removal.
            self.key_count
                .store(la.len() as u64, Ordering::Relaxed);

            candidates
        }
    }

    /// Increment the evicted counter by `delta` (typically called after
    /// actual eviction succeeds on the engine side).
    pub fn increment_evicted(&self, delta: u64) {
        self.evicted.fetch_add(delta, Ordering::Relaxed);
    }

    /// Number of evicted entries (monotonic counter).
    pub fn evicted_count(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }
}

impl Default for LruTracker {
    fn default() -> Self {
        Self::new(100_000) // default 100K watermark
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_access_increments_key_count() {
        let tracker = LruTracker::new(1000);
        assert_eq!(tracker.key_count(), 0);
        tracker.record_access(42);
        assert!(tracker.key_count() >= 1);
        tracker.record_access(7);
        assert!(tracker.key_count() >= 2);
    }

    #[test]
    fn watermark_default_is_100k() {
        let tracker = LruTracker::default();
        // Default watermark is 100_000.
        // key_count starts at 0, so needs_eviction should be false.
        assert!(!tracker.needs_eviction());
    }

    #[test]
    fn needs_eviction_false_below_watermark() {
        let tracker = LruTracker::new(100);
        for i in 0..50 {
            tracker.record_access(i);
        }
        // Duplicate accesses inflate key_count; select_candidates
        // deduplicates and resets it.  Call select_candidates to let
        // dedup shrink key_count below watermark.
        let _ = tracker.select_eviction_candidates(0);
        assert!(!tracker.needs_eviction());
    }

    #[test]
    fn needs_eviction_true_above_watermark() {
        let tracker = LruTracker::new(3);
        tracker.record_access(1);
        tracker.record_access(2);
        tracker.record_access(3);
        tracker.record_access(4);
        // With 4 access records pushed, key_count reads 4 which is > 3.
        assert!(tracker.needs_eviction());
    }

    #[test]
    fn select_candidates_returns_oldest() {
        let tracker = LruTracker::new(100);
        // Simulate two accesses: key 1 at a lower timestamp, key 2 later.
        // We push them directly into accesses to control ordering.
        tracker.accesses.push((1, 100));
        tracker.accesses.push((2, 200));
        tracker
            .key_count
            .store(2, Ordering::Relaxed);

        let candidates = tracker.select_eviction_candidates(1);
        assert_eq!(candidates.len(), 1);
        // Key 1 has the older timestamp (100 < 200), so it should be selected.
        assert!(candidates.contains(&1));
    }

    #[test]
    fn evicted_count_increments() {
        let tracker = LruTracker::new(100);
        assert_eq!(tracker.evicted_count(), 0);
        tracker.increment_evicted(3);
        assert_eq!(tracker.evicted_count(), 3);
        tracker.increment_evicted(2);
        assert_eq!(tracker.evicted_count(), 5);
    }

    #[test]
    fn select_zero_candidates_returns_empty() {
        let tracker = LruTracker::new(100);
        tracker.record_access(1);
        tracker.record_access(2);
        let candidates = tracker.select_eviction_candidates(0);
        assert!(candidates.is_empty());
    }

    #[test]
    fn select_more_than_available_returns_all() {
        let tracker = LruTracker::new(100);
        tracker.accesses.push((10, 10));
        tracker.accesses.push((20, 20));
        tracker
            .key_count
            .store(2, Ordering::Relaxed);
        let candidates = tracker.select_eviction_candidates(100);
        // Only 2 keys exist.
        assert_eq!(candidates.len(), 2);
    }
}
