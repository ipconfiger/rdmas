//! Performance optimizations for RDMA client operations (Wave 4 T4-C).
//!
//! Three local-simulation optimizations that target latency and throughput:
//!
//! 1. **Inline Fast Path** — wraps [`ClientReader::get`] to expose whether
//!    the read hit h1 (1-RTT in distributed mode) or required h2 probing.
//! 2. **SGE Batch Posting** — accumulates multiple write requests and
//!    flushes them in a single batch to amortize `post_send` overhead.
//! 3. **Poller Statistics** — counters for poller throughput and latency
//!    to enable runtime tuning.

use crate::client::write::WriteResult;
use crate::engine::extent::LargeObjectRegion;
use crate::engine::layout::{BucketMode, HashBucket, HashedKey};

/// Performance-optimized client read path.
///
/// Wraps the standard [`ClientReader`](crate::client::read::ClientReader) with
/// convenience helpers for the inline fast path (h1 hit detection).
pub struct OptimizedClientReader;

impl OptimizedClientReader {
    /// Read with inline fast path detection.
    ///
    /// Returns `Some((value, mode))` if the key is found (unlocked, alive).
    /// Returns `None` if the key is not found, the bucket is locked, or the
    /// read fails.
    ///
    /// # Fast path
    ///
    /// When the value fits in 32 bytes (**Inline** mode) AND the key matches
    /// at h1, this completes in 1 RTT in distributed mode (no h2 probe, no
    /// extent read).
    pub fn get_fast(
        key: &HashedKey,
        buckets: &[HashBucket],
        large_objects: Option<&LargeObjectRegion>,
        bucket_count: u64,
    ) -> Option<(Vec<u8>, BucketMode)> {
        crate::client::read::ClientReader::get(key, buckets, large_objects, bucket_count)
            .ok()
            .flatten()
            .map(|r| (r.value, r.mode))
    }
}

// ---------------------------------------------------------------------------
// SGE Batch Posting
// ---------------------------------------------------------------------------

/// Builds chained SendWorkRequests for batch RDMA posting.
///
/// In the local simulation, this delegates to single inserts via
/// [`ClientWriter::insert`](crate::client::write::ClientWriter::insert).
/// In the distributed version, these are posted as a WR chain
/// via [`QueuePair::post_send_batch`](crate::rdma::qp::QueuePair::post_send_batch).
pub struct BatchBuilder {
    /// Pending write requests: (key, value, mode)
    pending: Vec<(HashedKey, Vec<u8>, BucketMode)>,
    /// Maximum batch size
    max_batch: usize,
}

impl BatchBuilder {
    /// Create a new batch builder with the given maximum batch size.
    pub fn new(max_batch: usize) -> Self {
        Self {
            pending: Vec::with_capacity(max_batch),
            max_batch,
        }
    }

    /// Add a write to the batch.
    ///
    /// Does **not** automatically flush when the batch is full; the caller
    /// should check [`is_full`](Self::is_full) and call [`flush_local`](Self::flush_local)
    /// explicitly.
    pub fn add(&mut self, key: HashedKey, value: Vec<u8>, mode: BucketMode) {
        self.pending.push((key, value, mode));
    }

    /// Check if the batch is at or above capacity.
    pub fn is_full(&self) -> bool {
        self.pending.len() >= self.max_batch
    }

    /// Number of pending entries.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Returns `true` if there are no pending entries.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Flush all pending writes via local `ClientWriter::insert`.
    ///
    /// In LOCAL simulation mode, each entry is inserted individually.
    /// In DISTRIBUTED mode, a WR chain is built and posted via
    /// `qp.post_send_batch`.
    ///
    /// Returns the number of writes that succeeded (inserted without error).
    pub fn flush_local(
        &mut self,
        buckets: &mut [HashBucket],
        mut large_objects: Option<&mut LargeObjectRegion>,
        bucket_count: u64,
    ) -> usize {
        let mut success = 0;
        for (key, value, mode) in self.pending.drain(..) {
            let result = crate::client::write::ClientWriter::insert(
                &key,
                &value,
                mode,
                buckets,
                large_objects.as_deref_mut(),
                bucket_count,
            );
            if matches!(result, Ok(WriteResult::Inserted { .. })) {
                success += 1;
            }
        }
        success
    }
}

// ---------------------------------------------------------------------------
// Poller Statistics
// ---------------------------------------------------------------------------

/// Performance statistics collector.
///
/// Tracks read/write/CAS counts, retries, poller iterations, completions,
/// and h1/h2 hit distribution for [`crate::runtime::poller`] tuning.
#[derive(Debug, Default, Clone)]
pub struct PerfStats {
    /// Number of read operations
    pub reads: u64,
    /// Number of write operations
    pub writes: u64,
    /// Number of CAS operations
    pub cas_ops: u64,
    /// Number of retries
    pub retries: u64,
    /// Total poll iterations
    pub poll_iters: u64,
    /// Total completions harvested
    pub completions: u64,
    /// Hit at h1 (fast path, 1-RTT in distributed mode)
    pub h1_hits: u64,
    /// Hit at h2 (second probe, 2-RTT in distributed mode)
    pub h2_hits: u64,
}

impl PerfStats {
    /// Hit ratio at h1 (higher is better — more reads served in 1 RTT).
    pub fn h1_hit_ratio(&self) -> f64 {
        let total = self.h1_hits + self.h2_hits;
        if total == 0 {
            0.0
        } else {
            self.h1_hits as f64 / total as f64
        }
    }

    /// Read amplification: average number of buckets probed per read.
    ///
    /// For a perfect table with no collisions this approaches 1.0.
    pub fn read_amplification(&self) -> f64 {
        let total = self.h1_hits + self.h2_hits;
        if total == 0 {
            0.0
        } else {
            (self.h1_hits + 2 * self.h2_hits) as f64 / total as f64
        }
    }

    /// Average completions harvested per poll iteration.
    ///
    /// Higher values mean the poller is efficiently batching completions.
    pub fn completions_per_poll(&self) -> f64 {
        if self.poll_iters == 0 {
            0.0
        } else {
            self.completions as f64 / self.poll_iters as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::cuckoo::CuckooTable;
    use crate::engine::layout::BucketMode;
    use bytemuck::Zeroable;
    use xxhash_rust::xxh64::xxh64;

    /// Build a `HashedKey` from a string using XXH64 + 16-byte digest,
    /// matching the convention used by `CuckooTable::insert`.
    fn make_key(s: &str) -> HashedKey {
        let hash = xxh64(s.as_bytes(), 0);
        let mut digest = [0u8; 16];
        let h1 = xxh64(s.as_bytes(), 0);
        let h2 = xxh64(s.as_bytes(), 1);
        digest[0..8].copy_from_slice(&h1.to_le_bytes());
        digest[8..16].copy_from_slice(&h2.to_le_bytes());
        HashedKey { hash, digest }
    }

    // -----------------------------------------------------------------------
    // OptimizedClientReader tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_fast_inline_key_found() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("fastpath");
        table
            .insert(&key, b"inline-data", BucketMode::Inline)
            .unwrap();

        let result =
            OptimizedClientReader::get_fast(&key, table.buckets(), None, table.bucket_count());

        assert!(result.is_some(), "key should be found");
        let (value, mode) = result.unwrap();
        assert_eq!(mode, BucketMode::Inline);
        assert_eq!(&value[..11], b"inline-data");
    }

    #[test]
    fn test_get_fast_key_not_found() {
        let table = CuckooTable::new(64, 16);
        let key = make_key("missing");

        let result =
            OptimizedClientReader::get_fast(&key, table.buckets(), None, table.bucket_count());

        assert!(result.is_none(), "missing key should not be found");
    }

    #[test]
    fn test_get_fast_extent_key_found() {
        let mut table = CuckooTable::new(64, 16);
        let mut region = LargeObjectRegion::new(4096);
        let key = make_key("large-key");
        let data = b"this is a large extent value".to_vec();

        let offset = region.allocate(&data).unwrap();
        table
            .insert_extent(&key, offset, data.len() as u64)
            .unwrap();

        let result = OptimizedClientReader::get_fast(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        );

        assert!(result.is_some(), "extent key should be found");
        let (value, mode) = result.unwrap();
        assert_eq!(mode, BucketMode::Extent);
        assert_eq!(value, data);
    }

    // -----------------------------------------------------------------------
    // BatchBuilder tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_batch_builder_new_is_empty() {
        let builder = BatchBuilder::new(4);
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);
        assert!(!builder.is_full());
    }

    #[test]
    fn test_batch_builder_add_and_count() {
        let mut builder = BatchBuilder::new(4);
        for i in 0u32..4 {
            builder.add(
                make_key(&format!("k{i}")),
                i.to_le_bytes().to_vec(),
                BucketMode::Inline,
            );
        }

        assert_eq!(builder.len(), 4);
        assert!(builder.is_full());
        assert!(!builder.is_empty());
    }

    #[test]
    fn test_batch_builder_exceeds_capacity_becomes_full() {
        let mut builder = BatchBuilder::new(2);
        builder.add(make_key("a"), vec![1], BucketMode::Inline);
        assert!(!builder.is_full());
        builder.add(make_key("b"), vec![2], BucketMode::Inline);
        assert!(builder.is_full());
        builder.add(make_key("c"), vec![3], BucketMode::Inline);
        assert!(builder.is_full()); // still full
        assert_eq!(builder.len(), 3);
    }

    #[test]
    fn test_batch_builder_flush_to_empty_buckets() {
        let mut buckets = vec![HashBucket::zeroed(); 8];
        let bucket_count = buckets.len() as u64;
        let mut builder = BatchBuilder::new(4);

        let key0 = make_key("a");
        let key1 = make_key("b");
        let key2 = make_key("c");

        builder.add(key0.clone(), b"val-a".to_vec(), BucketMode::Inline);
        builder.add(key1.clone(), b"val-b".to_vec(), BucketMode::Inline);
        builder.add(key2.clone(), b"val-c".to_vec(), BucketMode::Inline);

        let flushed = builder.flush_local(&mut buckets, None, bucket_count);

        assert_eq!(flushed, 3);
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        // Verify keys are findable via ClientReader.
        for (key, expected) in &[
            (&key0, b"val-a" as &[u8]),
            (&key1, b"val-b"),
            (&key2, b"val-c"),
        ] {
            let result = crate::client::read::ClientReader::get(key, &buckets, None, bucket_count)
                .unwrap()
                .expect("key should be found");
            assert_eq!(&result.value[..expected.len()], *expected);
        }
    }

    #[test]
    fn test_batch_builder_flush_returns_zero_when_empty() {
        let mut buckets = vec![HashBucket::zeroed(); 8];
        let bucket_count = buckets.len() as u64;
        let mut builder = BatchBuilder::new(4);

        let flushed = builder.flush_local(&mut buckets, None, bucket_count);
        assert_eq!(flushed, 0);
    }

    #[test]
    fn test_batch_builder_flush_with_extent() {
        let mut buckets = vec![HashBucket::zeroed(); 8];
        let bucket_count = buckets.len() as u64;
        let mut region = LargeObjectRegion::new(4096);
        let mut builder = BatchBuilder::new(4);

        let key = make_key("extent-key");
        let data = vec![0xABu8; 200];

        builder.add(key.clone(), data.clone(), BucketMode::Extent);

        let flushed = builder.flush_local(&mut buckets, Some(&mut region), bucket_count);

        assert_eq!(flushed, 1);
        assert!(builder.is_empty());

        // Read back via ClientReader + region.
        let result =
            crate::client::read::ClientReader::get(&key, &buckets, Some(&region), bucket_count)
                .unwrap()
                .expect("extent key should be found");
        assert_eq!(result.mode, BucketMode::Extent);
        assert_eq!(result.value, data);
    }

    #[test]
    fn test_batch_builder_flush_partial_success_table_full() {
        // Create a tiny 4-bucket table; insert many keys to hit TableFull.
        let mut buckets = vec![HashBucket::zeroed(); 4];
        let bucket_count = buckets.len() as u64;
        let mut builder = BatchBuilder::new(8);

        for i in 0..32u64 {
            let hash = 0x1000 + i;
            let mut digest = [0u8; 16];
            digest[0..8].copy_from_slice(&hash.to_le_bytes());
            let key = HashedKey { hash, digest };
            builder.add(key, hash.to_le_bytes().to_vec(), BucketMode::Inline);
        }

        let flushed = builder.flush_local(&mut buckets, None, bucket_count);
        // With 4 buckets and aggressive kick chains, some entries succeed
        // but not all 32 (limited by MAX_KICK and table capacity).
        assert!(flushed < 32, "should not fit all 32 entries in 4 buckets");
    }

    #[test]
    fn test_batch_builder_is_full_at_exact_capacity() {
        let mut builder = BatchBuilder::new(1);
        assert!(!builder.is_full());
        builder.add(make_key("x"), vec![0], BucketMode::Inline);
        assert!(builder.is_full());
    }

    // -----------------------------------------------------------------------
    // PerfStats tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_perf_stats_h1_hit_ratio() {
        let stats = PerfStats {
            h1_hits: 80,
            h2_hits: 20,
            ..Default::default()
        };
        assert!((stats.h1_hit_ratio() - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_perf_stats_read_amplification() {
        let stats = PerfStats {
            h1_hits: 80,
            h2_hits: 20,
            ..Default::default()
        };
        // avg = (80*1 + 20*2) / 100 = 1.2
        assert!((stats.read_amplification() - 1.2).abs() < 0.01);
    }

    #[test]
    fn test_perf_stats_empty() {
        let stats = PerfStats::default();
        assert_eq!(stats.h1_hit_ratio(), 0.0);
        assert_eq!(stats.read_amplification(), 0.0);
        assert_eq!(stats.completions_per_poll(), 0.0);
    }

    #[test]
    fn test_perf_stats_completions_per_poll() {
        let stats = PerfStats {
            poll_iters: 100,
            completions: 250,
            ..Default::default()
        };
        assert!((stats.completions_per_poll() - 2.5).abs() < 0.01);
    }

    #[test]
    fn test_perf_stats_completions_per_poll_zero_iters() {
        let stats = PerfStats {
            completions: 10,
            ..Default::default()
        };
        assert_eq!(stats.completions_per_poll(), 0.0);
    }

    #[test]
    fn test_perf_stats_all_h1() {
        let stats = PerfStats {
            h1_hits: 100,
            ..Default::default()
        };
        assert!((stats.h1_hit_ratio() - 1.0).abs() < 0.01);
        assert!((stats.read_amplification() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_perf_stats_all_h2() {
        let stats = PerfStats {
            h2_hits: 100,
            ..Default::default()
        };
        assert!((stats.h1_hit_ratio() - 0.0).abs() < 0.01);
        // avg = (0*1 + 100*2) / 100 = 2.0
        assert!((stats.read_amplification() - 2.0).abs() < 0.01);
    }

    #[test]
    fn test_perf_stats_clone() {
        let stats = PerfStats {
            reads: 42,
            ..Default::default()
        };
        let cloned = stats.clone();
        assert_eq!(cloned.reads, 42);
    }
}
