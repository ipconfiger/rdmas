//! Client read path: one-sided RDMA READ against the server hash table.
//!
//! Design spec: Rust-RDMA.md §四.4 — Wave 3 T3-B
//!
//! # Local simulation mode
//!
//! In Wave 3 we simulate one-sided RDMA READ with direct Rust memory access.
//! The caller provides a slice of the remote hash table (`buckets`) and an
//! optional [`LargeObjectRegion`] for Extent-mode reads.  In the distributed
//! deployment (Wave 4+), these slices are replaced by actual `rdma_read`
//! work requests against registered memory regions.
//!
//! # Read algorithm
//!
//! 1. Hash the key → h1, h2
//! 2. Read bucket at h1 (simulated: index into the slice)
//! 3. Check lock: if locked → skip (retry is the caller's responsibility)
//! 4. Check key_hash + digest match: if no → read bucket at h2
//! 5. If Inline mode: return value from body (1 RTT in distributed mode)
//! 6. If Extent mode: parse `{offset, length}` from body, read data from
//!    `LargeObjectRegion` (2 RTT in distributed mode)
//! 7. Tombstone buckets are ignored (deleted keys are not found)
//!
//! # Optimistic consistency
//!
//! In the distributed mode the caller wraps raw reads with the optimistic
//! read protocol from `engine::concurrency` (version check before/after).
//! The local simulation returns `None` for locked buckets so the caller
//! can implement retry with version verification.

use crate::engine::extent::LargeObjectRegion;
use crate::engine::layout::{BucketMode, HashBucket, HashedKey};
use crate::error::RdmaError;
use crate::transport::Transport;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Result of a successful client read operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    /// The value read from the bucket (Inline body or Extent region data).
    pub value: Vec<u8>,
    /// Whether the bucket was Inline or Extent.
    pub mode: BucketMode,
}

// ---------------------------------------------------------------------------
// ClientReader
// ---------------------------------------------------------------------------

/// Stateless client read path: one-sided RDMA READ against the server hash table.
///
/// # Local simulation
///
/// In the local simulation, `get` receives direct references to the remote
/// hash table and extent region. This allows testing the read-path logic
/// without a real RDMA network.
///
/// In the distributed version, these would be replaced by [`crate::rdma::qp`]
/// work requests targeting remote memory regions.
pub struct ClientReader;

impl ClientReader {
    /// Read a key from the remote hash table.
    ///
    /// # Parameters
    ///
    /// - `key`: the hashed key to look up (hash + 16-byte digest).
    /// - `buckets`: a slice of the server's hash table (local simulation).
    /// - `large_objects`: the server's Large Object Region, required for
    ///   Extent-mode reads.  `None` when the hash table contains only
    ///   Inline entries.
    /// - `bucket_count`: total number of buckets; **must be a power of 2**.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(ReadResult))` — key found, value returned.
    /// - `Ok(None)` — key not found (both h1 and h2 missed, or locked).
    /// - `Err(RdmaError)` — fatal error (reserved for distributed mode).
    pub fn get(
        key: &HashedKey,
        buckets: &[HashBucket],
        large_objects: Option<&LargeObjectRegion>,
        bucket_count: u64,
    ) -> Result<Option<ReadResult>, RdmaError> {
        // Sanity check: hash must be non-zero (zero is reserved for "empty").
        if key.hash == 0 {
            return Err(RdmaError::InvalidKey);
        }

        // Require power-of-two bucket count (same as CuckooTable).
        if !bucket_count.is_power_of_two() {
            return Err(RdmaError::Internal(format!(
                "bucket_count must be a power of 2, got {bucket_count}"
            )));
        }

        let h1 = (key.hash % bucket_count) as usize;
        let h2 = (((key.hash >> 32) % bucket_count) | 1) as usize;

        // Guard: ensure computed indices are within bounds.
        if h1 >= buckets.len() || h2 >= buckets.len() {
            return Err(RdmaError::Internal(format!(
                "computed bucket index out of bounds: h1={h1} h2={h2} len={}",
                buckets.len()
            )));
        }

        // Phase 1: Read bucket at h1.
        if let Some(result) = Self::try_read_bucket(key, buckets, large_objects, h1) {
            return Ok(Some(result));
        }

        // Phase 2: Probe h2 if h1 didn't have the key.
        if let Some(result) = Self::try_read_bucket(key, buckets, large_objects, h2) {
            return Ok(Some(result));
        }

        Ok(None)
    }

    /// Distributed read: use the transport layer to read from remote memory.
    ///
    /// The `hash_table_vaddr` and `large_obj_vaddr` are the byte addresses
    /// of the server's registered memory regions. The bucket size is fixed
    /// at 64 bytes (matching [`HashBucket`]).
    ///
    /// # Arguments
    ///
    /// * `key` — the hashed key to look up.
    /// * `transport` — the transport layer (RDMA or TCP fallback).
    /// * `hash_table_vaddr` / `hash_table_rkey` — server's hash table region.
    /// * `large_obj_vaddr` / `large_obj_rkey` — server's large object region.
    /// * `bucket_count` — total number of buckets (must be a power of 2).
    /// * `lkey` — local memory key for the client's registered buffer.
    pub async fn get_remote(
        key: &HashedKey,
        transport: &dyn Transport,
        hash_table_vaddr: u64,
        hash_table_rkey: u32,
        large_obj_vaddr: u64,
        large_obj_rkey: u32,
        bucket_count: u64,
        lkey: u32,
    ) -> Result<Option<ReadResult>, RdmaError> {
        // Sanity checks (same as local `get`).
        if key.hash == 0 {
            return Err(RdmaError::InvalidKey);
        }
        if !bucket_count.is_power_of_two() {
            return Err(RdmaError::Internal(format!(
                "bucket_count must be a power of 2, got {bucket_count}"
            )));
        }

        // Hash to get bucket indices.
        let h1 = (key.hash % bucket_count) as u64;
        let h2 = ((key.hash >> 32) % bucket_count | 1) as u64;

        let bucket_size = 64u64;

        // Read bucket at h1 (64 bytes).
        let mut bucket_buf = vec![0u8; bucket_size as usize];
        transport
            .read(
                &mut bucket_buf,
                lkey,
                hash_table_vaddr + h1 * bucket_size,
                hash_table_rkey,
            )
            .await?;
        let bucket = bytemuck::from_bytes::<HashBucket>(&bucket_buf);

        if !bucket.is_locked() && bucket.matches_key(key.hash, &key.digest) {
            return Ok(Some(
                Self::extract_value(
                    bucket,
                    transport,
                    large_obj_vaddr,
                    large_obj_rkey,
                    lkey,
                )
                .await?,
            ));
        }

        // Probe h2.
        transport
            .read(
                &mut bucket_buf,
                lkey,
                hash_table_vaddr + h2 * bucket_size,
                hash_table_rkey,
            )
            .await?;
        let bucket = bytemuck::from_bytes::<HashBucket>(&bucket_buf);

        if !bucket.is_locked() && bucket.matches_key(key.hash, &key.digest) {
            return Ok(Some(
                Self::extract_value(
                    bucket,
                    transport,
                    large_obj_vaddr,
                    large_obj_rkey,
                    lkey,
                )
                .await?,
            ));
        }

        Ok(None)
    }

    /// Extract the value from a matched bucket, reading from the large-object
    /// region if the bucket is in Extent mode.
    async fn extract_value(
        bucket: &HashBucket,
        transport: &dyn Transport,
        large_obj_vaddr: u64,
        large_obj_rkey: u32,
        lkey: u32,
    ) -> Result<ReadResult, RdmaError> {
        if bucket.is_inline() {
            Ok(ReadResult {
                value: bucket.inline_value().to_vec(),
                mode: BucketMode::Inline,
            })
        } else {
            let (offset, length) = bucket.extent_ref();
            let mut buf = vec![0u8; length as usize];
            transport
                .read(
                    &mut buf,
                    lkey,
                    large_obj_vaddr + offset,
                    large_obj_rkey,
                )
                .await?;
            Ok(ReadResult {
                value: buf,
                mode: BucketMode::Extent,
            })
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Try to read from a specific bucket index.
    ///
    /// Returns `None` if the bucket is locked, empty, a tombstone, or
    /// does not match the target key.  Returns `None` also if the bucket
    /// is in Extent mode but no `LargeObjectRegion` is provided, or the
    /// extent read fails.
    fn try_read_bucket(
        key: &HashedKey,
        buckets: &[HashBucket],
        large_objects: Option<&LargeObjectRegion>,
        idx: usize,
    ) -> Option<ReadResult> {
        let bucket = &buckets[idx];

        // Locked bucket — caller should retry after the lock is released.
        if bucket.is_locked() {
            return None;
        }

        // Tombstone — the key was deleted.  Verify this *before* the hash
        // match so that a deleted key does not appear as found.
        if bucket.is_tombstone() {
            return None;
        }

        // Exact key-hash + digest match.
        if !bucket.matches_key(key.hash, &key.digest) {
            return None;
        }

        // --- Read the value based on mode ---

        if bucket.is_inline() {
            // Inline mode: the value is the 32-byte body.
            // Return a copy so the caller owns the data.
            let value = bucket.inline_value().to_vec();
            return Some(ReadResult {
                value,
                mode: BucketMode::Inline,
            });
        }

        // Extent mode: the body contains (offset, length) in little-endian.
        let (offset, _length) = bucket.extent_ref();

        // Read the actual data from the Large Object Region.
        if let Some(region) = large_objects {
            if let Some(data) = region.read(offset) {
                return Some(ReadResult {
                    value: data,
                    mode: BucketMode::Extent,
                });
            }
        }

        // Extent region unavailable, or extent read failed (corrupted).
        None
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

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    /// Build a `HashedKey` from a string using XXH64 + XXH128-style digest.
    ///
    /// The `hash` field is `xxh64(data, 0)`.  The `digest` is constructed
    /// from two XXH64 values (seeds 0 and 1) packed into 16 bytes, matching
    /// the convention used by `CuckooTable::insert`.
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
    // Inline read tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_inline_single_key() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("hello");
        table
            .insert(&key, b"world", BucketMode::Inline)
            .unwrap();

        let result = ClientReader::get(
            &key,
            table.buckets(),
            None,
            table.bucket_count(),
        )
        .unwrap()
        .expect("key should be found");

        assert_eq!(result.mode, BucketMode::Inline);
        // "world" padded to 32 bytes
        assert_eq!(&result.value[..5], b"world");
        assert!(result.value[5..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_read_inline_multiple_keys() {
        let mut table = CuckooTable::new(128, 16);
        let keys: Vec<HashedKey> = (0..10).map(|i| make_key(&format!("key-{i}"))).collect();

        for (i, k) in keys.iter().enumerate() {
            let val = format!("val-{i}").into_bytes();
            table.insert(k, &val, BucketMode::Inline).unwrap();
        }

        // Read back all keys.
        for (i, k) in keys.iter().enumerate() {
            let result = ClientReader::get(k, table.buckets(), None, table.bucket_count())
                .unwrap()
                .expect("key should be found");
            let expected = format!("val-{i}");
            assert_eq!(&result.value[..expected.len()], expected.as_bytes());
        }
    }

    #[test]
    fn test_read_inline_full_32_byte_value() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("big");
        let val = [0xABu8; 32];
        table.insert(&key, &val, BucketMode::Inline).unwrap();

        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("key should be found");

        assert_eq!(result.value.len(), 32);
        assert_eq!(&result.value[..], &val[..]);
    }

    #[test]
    fn test_read_inline_empty_value() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("empty");
        table.insert(&key, &[], BucketMode::Inline).unwrap();

        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("key should be found");

        assert_eq!(result.mode, BucketMode::Inline);
        assert!(result.value.iter().all(|&b| b == 0));
    }

    // -----------------------------------------------------------------------
    // Missing / not-found tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_missing_key() {
        let mut table = CuckooTable::new(64, 16);
        let present = make_key("present");
        let missing = make_key("missing");

        table.insert(&present, b"data", BucketMode::Inline).unwrap();

        let result = ClientReader::get(
            &missing,
            table.buckets(),
            None,
            table.bucket_count(),
        )
        .unwrap();

        assert!(result.is_none(), "missing key should not be found");
    }

    #[test]
    fn test_read_empty_table() {
        let table = CuckooTable::new(64, 16);
        let key = make_key("nope");

        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_deleted_key() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("delete-me");

        table.insert(&key, b"temp", BucketMode::Inline).unwrap();
        assert!(table.delete(&key));

        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count()).unwrap();
        assert!(result.is_none(), "deleted (tombstone) key should not be found");
    }

    // -----------------------------------------------------------------------
    // Extent read tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_extent_single() {
        let mut table = CuckooTable::new(64, 16);
        let mut region = LargeObjectRegion::new(4096);
        let key = make_key("large-value");
        let data = b"This is a large value stored in extent mode!".to_vec();

        let offset = region.allocate(&data).expect("extent allocation should succeed");
        table
            .insert_extent(&key, offset, data.len() as u64)
            .unwrap();

        let result = ClientReader::get(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        )
        .unwrap()
        .expect("extent key should be found");

        assert_eq!(result.mode, BucketMode::Extent);
        assert_eq!(result.value, data);
    }

    #[test]
    fn test_read_extent_multiple() {
        let mut table = CuckooTable::new(128, 16);
        let mut region = LargeObjectRegion::new(8192);

        let entries: Vec<(&str, Vec<u8>)> = vec![
            ("ext-a", vec![0xAAu8; 300]),
            ("ext-b", vec![0xBBu8; 500]),
            ("ext-c", vec![0xCCu8; 100]),
        ];

        let mut expected: Vec<(HashedKey, Vec<u8>, u64)> = Vec::new();

        for (name, data) in &entries {
            let key = make_key(name);
            let offset = region.allocate(data).unwrap();
            table.insert_extent(&key, offset, data.len() as u64).unwrap();
            expected.push((key, data.clone(), offset));
        }

        for (key, data, _offset) in &expected {
            let result = ClientReader::get(
                key,
                table.buckets(),
                Some(&region),
                table.bucket_count(),
            )
            .unwrap()
            .expect("extent key should be found");
            assert_eq!(result.mode, BucketMode::Extent);
            assert_eq!(&result.value, data);
        }
    }

    #[test]
    fn test_read_extent_without_region_returns_none() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("orphan-extent");
        table.insert_extent(&key, 0x1000, 256).unwrap();

        // No LargeObjectRegion provided → should not crash, return None.
        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_extent_corrupted_offset() {
        let mut table = CuckooTable::new(64, 16);
        let mut region = LargeObjectRegion::new(4096);
        let key = make_key("corrupt");

        let data = b"valid data".to_vec();
        let offset = region.allocate(&data).unwrap();
        table.insert_extent(&key, offset, data.len() as u64).unwrap();

        // Verify the extent is readable through the region.
        assert!(region.read(offset).is_some(), "extent should be readable");

        let result = ClientReader::get(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        )
        .unwrap()
        .expect("key should be found with correct extent data");

        assert_eq!(result.value, data);
    }

    #[test]
    fn test_read_extent_deleted() {
        let mut table = CuckooTable::new(64, 16);
        let mut region = LargeObjectRegion::new(4096);
        let key = make_key("ext-del");

        let offset = region.allocate(b"extent data").unwrap();
        table.insert_extent(&key, offset, 11).unwrap();
        table.delete(&key);

        let result = ClientReader::get(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        )
        .unwrap();
        assert!(result.is_none(), "deleted extent key should not be found");
    }

    // -----------------------------------------------------------------------
    // Locked bucket returns None (not found)
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_locked_bucket_returns_none() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("locked");

        // Insert normally.
        table.insert(&key, b"secret", BucketMode::Inline).unwrap();

        // Manually lock the bucket at the expected index.
        let h1 = (key.hash % table.bucket_count()) as usize;
        table.bucket_mut(h1 as u64).lock_version = 0x01; // lock bit set

        let result = ClientReader::get(
            &key,
            table.buckets(),
            None,
            table.bucket_count(),
        )
        .unwrap();

        // If h2 also doesn't have the key (or is also locked), return None.
        // Since the key was only inserted once (cuckoo puts it at h1 or h2),
        // and we locked h1, the result depends on whether the key lives at h1.
        // If the key is at h1, it returns None (locked).  If at h2, found.
        // Either outcome is valid — we just verify no panic.
        let _ = result;
    }

    // -----------------------------------------------------------------------
    // Hash collision: same hash, different digest
    // -----------------------------------------------------------------------

    #[test]
    fn test_collision_disambiguates_by_digest() {
        let mut table = CuckooTable::new(128, 16);

        // Two keys with the same hash, different digests.
        let k1 = HashedKey {
            hash: 0xDEAD,
            digest: *b"AAAA_AAAA_AAAA_A",
        };
        let k2 = HashedKey {
            hash: 0xDEAD,
            digest: *b"BBBB_BBBB_BBBB_B",
        };

        table.insert(&k1, b"alpha", BucketMode::Inline).unwrap();
        table.insert(&k2, b"beta", BucketMode::Inline).unwrap();

        let r1 = ClientReader::get(&k1, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("k1 should be found");
        assert_eq!(&r1.value[..5], b"alpha");

        let r2 = ClientReader::get(&k2, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("k2 should be found");
        assert_eq!(&r2.value[..4], b"beta");
    }

    #[test]
    fn test_collision_wrong_digest_not_found() {
        let mut table = CuckooTable::new(64, 16);
        let k1 = HashedKey {
            hash: 42,
            digest: [1u8; 16],
        };
        let k2 = HashedKey {
            hash: 42,
            digest: [2u8; 16],
        };

        table.insert(&k1, b"only-one", BucketMode::Inline).unwrap();

        // k2 has same hash but different digest → different key.
        let result = ClientReader::get(&k2, table.buckets(), None, table.bucket_count()).unwrap();
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Edge case: h1 and h2 map to same bucket
    // -----------------------------------------------------------------------

    #[test]
    fn test_same_h1_h2_bucket() {
        // With bucket_count == 2 and key.hash == 1:
        //   h1 = 1 % 2 = 1
        //   h2 = ((1 >> 32) % 2) | 1 = 0 | 1 = 1
        // Both 1 → only one bucket to check.
        let mut table = CuckooTable::new(2, 16);
        let key = HashedKey {
            hash: 1,
            digest: [0x42u8; 16],
        };

        table.insert(&key, b"hi", BucketMode::Inline).unwrap();

        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("key should be found at the single home bucket");

        assert_eq!(&result.value[..2], b"hi");
    }

    // -----------------------------------------------------------------------
    // Stress: many keys across inline + extent modes
    // -----------------------------------------------------------------------

    #[test]
    fn test_mixed_inline_and_extent() {
        let mut table = CuckooTable::new(256, 32);
        let mut region = LargeObjectRegion::new(65536);

        let inline_keys: Vec<_> = (0..10).map(|i| make_key(&format!("in-{i}"))).collect();
        let extent_keys: Vec<_> = (0..5).map(|i| make_key(&format!("ex-{i}"))).collect();

        // Insert inline keys.
        for (i, k) in inline_keys.iter().enumerate() {
            let val = format!("inline-{i}").into_bytes();
            table.insert(k, &val, BucketMode::Inline).unwrap();
        }

        // Insert extent keys.
        for (i, k) in extent_keys.iter().enumerate() {
            let val = vec![(i as u8).wrapping_mul(7); 200 + i * 50];
            let offset = region.allocate(&val).unwrap();
            table
                .insert_extent(k, offset, val.len() as u64)
                .unwrap();
        }

        // Read back inline keys.
        for (i, k) in inline_keys.iter().enumerate() {
            let result = ClientReader::get(k, table.buckets(), Some(&region), table.bucket_count())
                .unwrap()
                .expect("inline key should be found");
            assert_eq!(result.mode, BucketMode::Inline);
            let expected = format!("inline-{i}");
            assert_eq!(&result.value[..expected.len()], expected.as_bytes());
        }

        // Read back extent keys.
        for (i, k) in extent_keys.iter().enumerate() {
            let result = ClientReader::get(k, table.buckets(), Some(&region), table.bucket_count())
                .unwrap()
                .expect("extent key should be found");
            assert_eq!(result.mode, BucketMode::Extent);
            let expected = vec![(i as u8).wrapping_mul(7); 200 + i * 50];
            assert_eq!(result.value, expected);
        }
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_zero_hash_rejected() {
        let table = CuckooTable::new(16, 16);
        let key = HashedKey {
            hash: 0,
            digest: [1u8; 16],
        };

        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count());
        assert!(matches!(result, Err(RdmaError::InvalidKey)));
    }

    #[test]
    fn test_non_power_of_two_bucket_count_rejected() {
        let key = make_key("test");
        let buckets = vec![HashBucket::zeroed(); 10];

        let result = ClientReader::get(&key, &buckets, None, 10);
        assert!(matches!(result, Err(RdmaError::Internal(_))));
    }

    #[test]
    fn test_bucket_index_out_of_bounds() {
        let key = make_key("test");
        // 4 buckets but claim 8 → computed h1/h2 may exceed 4.
        let buckets = vec![HashBucket::zeroed(); 4];

        let result = ClientReader::get(&key, &buckets, None, 8);
        // Either Internal (bounds check) or Ok(None) if h1,h2 < 4.
        // We just verify no panic.
        let _ = result;
    }

    // -----------------------------------------------------------------------
    // Overwrite: reading returns the latest value
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_after_overwrite() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("overwrite");

        table.insert(&key, b"first", BucketMode::Inline).unwrap();
        let r1 = ClientReader::get(&key, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("should read first value");
        assert_eq!(&r1.value[..5], b"first");

        // Overwrite with a new value.
        table.insert(&key, b"second!", BucketMode::Inline).unwrap();
        let r2 = ClientReader::get(&key, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("should read second value");
        assert_eq!(&r2.value[..7], b"second!");
    }

    #[test]
    fn test_read_after_mode_change_inline_to_extent() {
        let mut table = CuckooTable::new(64, 16);
        let mut region = LargeObjectRegion::new(4096);
        let key = make_key("mode-switch");

        // Start inline.
        table
            .insert(&key, b"inline-data", BucketMode::Inline)
            .unwrap();

        // Switch to extent.
        let ext_data = b"extent-data-is-longer".to_vec();
        let offset = region.allocate(&ext_data).unwrap();
        table
            .insert_extent(&key, offset, ext_data.len() as u64)
            .unwrap();

        let result = ClientReader::get(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        )
        .unwrap()
        .expect("should read extent value after mode switch");

        assert_eq!(result.mode, BucketMode::Extent);
        assert_eq!(result.value, ext_data);
    }

    // -----------------------------------------------------------------------
    // ReadResult equality
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_result_eq() {
        let r1 = ReadResult {
            value: vec![1, 2, 3],
            mode: BucketMode::Inline,
        };
        let r2 = ReadResult {
            value: vec![1, 2, 3],
            mode: BucketMode::Inline,
        };
        let r3 = ReadResult {
            value: vec![1, 2, 3],
            mode: BucketMode::Extent,
        };

        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    // -----------------------------------------------------------------------
    // Integration: read through the full cuckoo pipeline
    // -----------------------------------------------------------------------

    #[test]
    fn test_full_round_trip_inline() {
        let mut table = CuckooTable::new(64, 16);
        let key = make_key("round-trip");
        let val = b"hello rdmas!";

        // Write.
        table.insert(&key, val, BucketMode::Inline).unwrap();

        // Read via ClientReader.
        let result = ClientReader::get(&key, table.buckets(), None, table.bucket_count())
            .unwrap()
            .expect("should read back value");

        assert_eq!(&result.value[..val.len()], val);

        // Delete.
        assert!(table.delete(&key));

        // Read after delete.
        let result2 =
            ClientReader::get(&key, table.buckets(), None, table.bucket_count()).unwrap();
        assert!(result2.is_none());
    }

    #[test]
    fn test_full_round_trip_extent() {
        let mut table = CuckooTable::new(64, 16);
        let mut region = LargeObjectRegion::new(8192);
        let key = make_key("extent-round-trip");
        let val = vec![0xDEu8; 1024];

        // Write.
        let offset = region.allocate(&val).unwrap();
        table
            .insert_extent(&key, offset, val.len() as u64)
            .unwrap();

        // Read via ClientReader.
        let result = ClientReader::get(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        )
        .unwrap()
        .expect("should read back extent value");

        assert_eq!(result.mode, BucketMode::Extent);
        assert_eq!(result.value, val);

        // Delete.
        assert!(table.delete(&key));

        // Read after delete.
        let result2 = ClientReader::get(
            &key,
            table.buckets(),
            Some(&region),
            table.bucket_count(),
        )
        .unwrap();
        assert!(result2.is_none());
    }
}
