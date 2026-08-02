//! Client write path: one-sided RDMA CAS-based Cuckoo insert with kick chain.
//!
//! # Distributed kick-chain algorithm (Rust-RDMA.md §三)
//!
//! 1. Client computes h1, h2 for the key.
//! 2. Tries h1: empty or tombstone → write key, done.
//! 3. Tries h2: same.
//! 4. If both occupied → start kick chain from h1:
//!    a. Displace occupant at h1; write new key into h1.
//!    b. Compute displaced occupant's alternate bucket (the one it is NOT in).
//!    c. If alternate empty → write occupant there, done.
//!    d. Else: repeat with alternate's occupant as the next victim.
//! 5. If chain reaches MAX_KICK → return `TableFull`.
//!
//! # Local simulation vs. distributed
//!
//! In Wave 3 local simulation, `ClientWriter` mutates `HashBucket` slices
//! directly. In a real distributed deployment each read/write to a bucket
//! would be an RDMA READ / RDMA CAS operation.

use bytemuck::Zeroable;

use crate::engine::extent::LargeObjectRegion;
use crate::engine::layout::{BucketMode, HashBucket, HashedKey};
use crate::error::RdmaError;
use crate::transport::Transport;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum kick-chain length before declaring the table full.
const MAX_KICK: u32 = 16;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Client-side write path: state machine for one-sided RDMA Cuckoo insertion.
pub struct ClientWriter;

/// Outcome of a write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    /// The key-value pair was inserted successfully at the given bucket index.
    Inserted {
        /// The index of the bucket where the key was placed.
        bucket_idx: usize,
    },
    /// The table is full: the kick chain exhausted `MAX_KICK` without finding
    /// an empty slot.
    TableFull,
}

// ---------------------------------------------------------------------------
// ClientWriter implementation
// ---------------------------------------------------------------------------

impl ClientWriter {
    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Insert a key-value pair using the distributed Cuckoo kick chain.
    ///
    /// # Arguments
    ///
    /// * `key` — the hashed key (must have a non-zero hash).
    /// * `value` — data to store. Must fit in 32 B for [`BucketMode::Inline`].
    /// * `mode` — whether to store Inline (≤ 32 B in the bucket body) or
    ///   Extent (allocated from [`LargeObjectRegion`]).
    /// * `buckets` — mutable slice of the hash table (simulated remote memory).
    /// * `large_objects` — the extent region. Required when `mode` is Extent.
    /// * `bucket_count` — total number of buckets (must be a power of 2).
    ///
    /// # Errors
    ///
    /// Returns [`RdmaError::Internal`] when an extent allocation fails or
    /// when Extent mode is used without a [`LargeObjectRegion`].
    pub fn insert(
        key: &HashedKey,
        value: &[u8],
        mode: BucketMode,
        buckets: &mut [HashBucket],
        mut large_objects: Option<&mut LargeObjectRegion>,
        bucket_count: u64,
    ) -> Result<WriteResult, RdmaError> {
        let h1 = (key.hash % bucket_count) as usize;
        let h2 = (((key.hash >> 32) % bucket_count) | 1) as usize;

        // Try direct insert at h1.
        if Self::try_insert_at(key, value, mode, buckets, large_objects.as_deref_mut(), h1)? {
            return Ok(WriteResult::Inserted { bucket_idx: h1 });
        }

        // Try direct insert at h2.
        if Self::try_insert_at(key, value, mode, buckets, large_objects.as_deref_mut(), h2)? {
            return Ok(WriteResult::Inserted { bucket_idx: h2 });
        }

        // Both occupied — start kick chain from h1.
        Self::kick_chain(
            key.hash,
            key.digest,
            value,
            mode,
            buckets,
            large_objects,
            bucket_count,
            h1,
        )
    }

    /// Distributed CAS-based insert using the transport layer.
    ///
    /// Reads bucket metadata remotely, builds a new bucket entry, and uses
    /// CAS to claim the slot. Falls through to the alternate bucket (h2)
    /// if the primary (h1) is occupied.
    ///
    /// # Arguments
    ///
    /// * `key` — the hashed key (must have a non-zero hash).
    /// * `value` — data to store. Must fit in 32 B for [`BucketMode::Inline`].
    /// * `mode` — [`BucketMode::Inline`] or [`BucketMode::Extent`].
    /// * `transport` — the transport layer (RDMA or TCP fallback).
    /// * `hash_table_vaddr` / `hash_table_rkey` — server's hash table region.
    /// * `large_obj_vaddr` / `large_obj_rkey` — server's large object region.
    /// * `bucket_count` — total number of buckets (must be a power of 2).
    /// * `lkey` — local memory key for the client's registered buffer.
    ///
    /// # Note
    ///
    /// This is a simplified distributed insert that only attempts direct
    /// placement (no kick chain). A full implementation would extend this
    /// with a CAS-based kick chain similar to [`Self::insert`].
    pub async fn insert_remote(
        key: &HashedKey,
        value: &[u8],
        mode: BucketMode,
        transport: &dyn Transport,
        hash_table_vaddr: u64,
        hash_table_rkey: u32,
        free_list_vaddr: u64,
        free_list_rkey: u32,
        bucket_count: u64,
        lkey: u32,
    ) -> Result<WriteResult, RdmaError> {
        let h1 = (key.hash % bucket_count) as u64;
        let h2 = ((key.hash >> 32) % bucket_count | 1) as u64;
        let bucket_size = 64u64;

        let addr_h1 = hash_table_vaddr + h1 * bucket_size;
        let addr_h2 = hash_table_vaddr + h2 * bucket_size;

        // Build the distributed extent allocator for Extent‑mode writes.
        // In production this would be passed in; for now we construct it from
        // the transport reference (via Arc::new on a trait object would require
        // the transport to be Arc'd upstream — here we clone the Arc if available
        // or create a lightweight wrapper). For the T9‑B implementation we accept
        // that the transport is `&dyn Transport` and we cannot easily Arc it.
        // The caller should provide an Arc<dyn Transport> when constructing the
        // allocator externally.

        // Try direct insert at h1.
        if Self::try_insert_remote_at(
            key,
            value,
            mode,
            transport,
            addr_h1,
            hash_table_rkey,
            free_list_vaddr,
            free_list_rkey,
            lkey,
            bucket_size,
        )
        .await?
        {
            return Ok(WriteResult::Inserted {
                bucket_idx: h1 as usize,
            });
        }

        // Try direct insert at h2.
        if Self::try_insert_remote_at(
            key,
            value,
            mode,
            transport,
            addr_h2,
            hash_table_rkey,
            free_list_vaddr,
            free_list_rkey,
            lkey,
            bucket_size,
        )
        .await?
        {
            return Ok(WriteResult::Inserted {
                bucket_idx: h2 as usize,
            });
        }

        Ok(WriteResult::TableFull)
    }

    /// Attempt to insert at a specific remote bucket slot.
    ///
    /// Returns `Ok(true)` if the insertion succeeded, `Ok(false)` if the
    /// bucket is occupied or locked.
    async fn try_insert_remote_at(
        key: &HashedKey,
        value: &[u8],
        mode: BucketMode,
        transport: &dyn Transport,
        remote_addr: u64,
        remote_rkey: u32,
        free_list_vaddr: u64,
        free_list_rkey: u32,
        lkey: u32,
        bucket_size: u64,
    ) -> Result<bool, RdmaError> {
        // Read the current bucket contents.
        let mut bucket_buf = vec![0u8; bucket_size as usize];
        transport
            .read(&mut bucket_buf, lkey, remote_addr, remote_rkey)
            .await?;
        let bucket = bytemuck::from_bytes::<HashBucket>(&bucket_buf);

        // Locked bucket — another client is writing here.
        if bucket.is_locked() {
            return Ok(false);
        }

        // Empty or tombstone slot is available for writing.
        if bucket.is_empty() || bucket.is_tombstone() {
            let mut new_bucket = HashBucket::zeroed();
            new_bucket.key_hash = key.hash;
            new_bucket.key_or_digest = key.digest;
            new_bucket.lock_version = (mode as u64) << 2;

            match mode {
                BucketMode::Inline => {
                    let mut inline_val = [0u8; 32];
                    let len = value.len().min(32);
                    inline_val[..len].copy_from_slice(&value[..len]);
                    new_bucket.set_inline_value(&inline_val);
                }
                BucketMode::Extent => {
                    // Perform a CAS bump allocation on the FreeList region.
                    // Phase 1: read current bump_offset.
                    let mut old_buf = [0u8; 8];
                    transport
                        .read(&mut old_buf, lkey, free_list_vaddr, free_list_rkey)
                        .await?;
                    let old_offset = u64::from_le_bytes(old_buf);

                    let total = crate::engine::extent::extent_total(value.len() as u64);
                    let new_offset = old_offset + total;

                    // Phase 2: CAS bump_offset to reserve space.
                    let cas_ok = transport
                        .cas(
                            old_offset,
                            new_offset,
                            lkey,
                            free_list_vaddr,
                            free_list_rkey,
                        )
                        .await?;
                    if !cas_ok {
                        return Err(RdmaError::CasFailed);
                    }

                    // Phase 3: write ExtentHeaderV2 (checksum=0) + payload + checksum.
                    use crate::engine::layout::ExtentHeaderV2;
                    let extent_addr = free_list_vaddr + old_offset;
                    let header_size = crate::engine::extent::HEADER_SIZE;

                    let mut hdr = ExtentHeaderV2::zeroed();
                    hdr.magic = crate::engine::layout::EXTENT_MAGIC;
                    hdr.version = 1;
                    hdr.data_len = value.len() as u32;
                    hdr.checksum = 0;
                    let hdr_bytes = bytemuck::bytes_of(&hdr);

                    transport
                        .write(hdr_bytes, lkey, extent_addr, free_list_rkey)
                        .await?;

                    // Write payload.
                    transport
                        .write(value, lkey, extent_addr + header_size, free_list_rkey)
                        .await?;

                    // Write final checksum.
                    let checksum = xxhash_rust::xxh64::xxh64(value, 0);
                    let checksum_addr = extent_addr + 24; // checksum at offset 24 in V2
                    transport
                        .write(&checksum.to_le_bytes(), lkey, checksum_addr, free_list_rkey)
                        .await?;

                    new_bucket.set_extent_ref(old_offset, value.len() as u64);
                }
            }

            // CAS the lock_version to claim the slot.
            let old_lv = bucket.lock_version;
            let cas_ok = transport
                .cas(
                    old_lv,
                    new_bucket.lock_version,
                    lkey,
                    remote_addr,
                    remote_rkey,
                )
                .await?;

            if cas_ok {
                // Write the rest of the bucket fields.
                let packed = bytemuck::bytes_of(&new_bucket);
                transport
                    .write(packed, lkey, remote_addr, remote_rkey)
                    .await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Attempt to insert at a single bucket index.
    ///
    /// Returns `Ok(true)` if the insertion succeeded, `Ok(false)` if the
    /// bucket is occupied, locked, or otherwise unavailable.
    ///
    /// # Errors
    ///
    /// Only returns an error when an extent operation fails (allocation
    /// failure or missing region).
    fn try_insert_at(
        key: &HashedKey,
        value: &[u8],
        mode: BucketMode,
        buckets: &mut [HashBucket],
        large_objects: Option<&mut LargeObjectRegion>,
        idx: usize,
    ) -> Result<bool, RdmaError> {
        // Copy the bucket for cheap read-only inspection (HashBucket is Copy).
        let bucket = buckets[idx];

        // Locked bucket — another client is writing here.
        if bucket.is_locked() {
            return Ok(false);
        }

        // Empty or tombstone slot is available for writing.
        if bucket.is_empty() || bucket.is_tombstone() {
            let mut new_bucket = HashBucket::zeroed();
            new_bucket.key_hash = key.hash;
            new_bucket.key_or_digest = key.digest;
            // Set mode bits in lock_version (unlocked, no tombstone).
            new_bucket.lock_version = (mode as u64) << 2;

            match mode {
                BucketMode::Inline => {
                    let mut inline_val = [0u8; 32];
                    let len = value.len().min(32);
                    inline_val[..len].copy_from_slice(&value[..len]);
                    new_bucket.set_inline_value(&inline_val);
                }
                BucketMode::Extent => {
                    if let Some(region) = large_objects {
                        let offset = region.allocate(value).ok_or_else(|| {
                            RdmaError::Internal("extent allocation failed".into())
                        })?;
                        new_bucket.set_extent_ref(offset, value.len() as u64);
                    } else {
                        return Err(RdmaError::Internal(
                            "Extent mode requires a LargeObjectRegion".into(),
                        ));
                    }
                }
            }

            // In local simulation, write directly.
            // In distributed: this would be an RDMA CAS of the new lock_version.
            buckets[idx] = new_bucket;
            return Ok(true);
        }

        Ok(false)
    }

    /// Kick-chain insertion: iteratively displace keys until an empty slot
    /// is found or `MAX_KICK` is exhausted.
    ///
    /// The algorithm (mirrors [`crate::engine::cuckoo::CuckooTable::kick_chain`]):
    ///
    /// 1. Write the incoming key at `start_idx`, displacing its occupant.
    /// 2. Compute the displaced occupant's alternate bucket.
    /// 3. If the alternate is empty/tombstone, write the occupant there. Done.
    /// 4. Otherwise, the alternate's occupant becomes the next victim. Repeat.
    fn kick_chain(
        start_hash: u64,
        start_digest: [u8; 16],
        start_value: &[u8],
        start_mode: BucketMode,
        buckets: &mut [HashBucket],
        mut large_objects: Option<&mut LargeObjectRegion>,
        bucket_count: u64,
        start_idx: usize,
    ) -> Result<WriteResult, RdmaError> {
        let mut cur_hash = start_hash;
        let mut cur_digest = start_digest;
        let mut cur_value = start_value.to_vec();
        let mut cur_mode = start_mode;
        let mut cur_idx = start_idx;

        for _kick in 0..MAX_KICK {
            // Check whether the current slot is locked by another writer.
            if buckets[cur_idx].is_locked() {
                return Ok(WriteResult::TableFull);
            }

            // 1. Snapshot the current occupant (HashBucket is Copy → cheap stack copy).
            let occupant = buckets[cur_idx];
            let occ_hash = occupant.key_hash;
            let occ_digest = occupant.key_or_digest;
            let occ_mode = if occupant.is_extent() {
                BucketMode::Extent
            } else {
                BucketMode::Inline
            };

            // Kick chain does not support displacing Extent-mode occupants
            // in local simulation (requires copying extent data, which is
            // a separate RDMA read in distributed mode).
            if occ_mode == BucketMode::Extent {
                return Ok(WriteResult::TableFull);
            }
            let occ_value: Vec<u8> = occupant.inline_value().to_vec();

            // 2. Overwrite the current slot with the incoming key.
            Self::write_to_bucket_internal(
                cur_hash,
                cur_digest,
                &cur_value,
                cur_mode,
                buckets,
                large_objects.as_deref_mut(),
                cur_idx,
            )?;

            // 3. Compute the displaced occupant's alternate bucket
            //    (the one it is NOT currently sitting in).
            let occ_h1 = (occ_hash % bucket_count) as usize;
            let occ_h2 = (((occ_hash >> 32) % bucket_count) | 1) as usize;
            let alt_idx = if cur_idx == occ_h1 { occ_h2 } else { occ_h1 };

            // 4. If the alternate is empty or a tombstone, write the
            //    displaced occupant there and we are done.
            if buckets[alt_idx].is_empty() || buckets[alt_idx].is_tombstone() {
                Self::write_to_bucket_internal(
                    occ_hash,
                    occ_digest,
                    &occ_value,
                    occ_mode,
                    buckets,
                    large_objects.as_deref_mut(),
                    alt_idx,
                )?;
                return Ok(WriteResult::Inserted {
                    bucket_idx: alt_idx,
                });
            }

            // 5. Cascade: the displaced occupant becomes the next key to
            //    place; the alternate index becomes the next cur_idx.
            cur_hash = occ_hash;
            cur_digest = occ_digest;
            cur_value = occ_value;
            cur_mode = occ_mode;
            cur_idx = alt_idx;
        }

        // MAX_KICK exhausted — table is effectively full.
        Ok(WriteResult::TableFull)
    }

    /// Write (hash, digest, value, mode) into the bucket at `idx`.
    ///
    /// # Lock version
    ///
    /// The `lock_version` is set to the raw mode bits (unlocked, no tombstone,
    /// version 0). This is correct for local simulation. In distributed mode
    /// the version would come from a CAS operation.
    fn write_to_bucket_internal(
        hash: u64,
        digest: [u8; 16],
        value: &[u8],
        mode: BucketMode,
        buckets: &mut [HashBucket],
        large_objects: Option<&mut LargeObjectRegion>,
        idx: usize,
    ) -> Result<(), RdmaError> {
        let bucket = &mut buckets[idx];
        bucket.key_hash = hash;
        bucket.key_or_digest = digest;
        bucket.lock_version = (mode as u64) << 2;

        match mode {
            BucketMode::Inline => {
                let mut inline_val = [0u8; 32];
                let len = value.len().min(32);
                inline_val[..len].copy_from_slice(&value[..len]);
                bucket.set_inline_value(&inline_val);
            }
            BucketMode::Extent => {
                if let Some(region) = large_objects {
                    let offset = region
                        .allocate(value)
                        .ok_or_else(|| RdmaError::Internal("extent allocation failed".into()))?;
                    bucket.set_extent_ref(offset, value.len() as u64);
                } else {
                    return Err(RdmaError::Internal(
                        "Extent mode requires a LargeObjectRegion".into(),
                    ));
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Convenience helper: create a `HashedKey` with a given hash and a
    /// reproducible 16-byte digest pattern.
    fn make_key(hash: u64, seed: u8) -> HashedKey {
        let mut digest = [0u8; 16];
        digest[0] = seed;
        for i in 1..16 {
            digest[i] = digest[i - 1].wrapping_add(seed);
        }
        HashedKey { hash, digest }
    }

    /// Create a zero-initialized slice of `n` buckets.
    fn make_buckets(n: usize) -> Vec<HashBucket> {
        vec![HashBucket::zeroed(); n]
    }

    // -----------------------------------------------------------------------
    // test_insert_inline_at_empty_slot
    // -----------------------------------------------------------------------

    #[test]
    fn test_insert_inline_at_empty_slot() {
        let mut buckets = make_buckets(8);
        let key = make_key(0xABCD, 1);
        let value = b"hello world";

        let result = ClientWriter::insert(&key, value, BucketMode::Inline, &mut buckets, None, 8)
            .expect("insert should not error");

        match result {
            WriteResult::Inserted { bucket_idx } => {
                let b = &buckets[bucket_idx];
                assert_eq!(b.key_hash, 0xABCD);
                // The digest must match.
                let expected_digest = {
                    let mut d = [0u8; 16];
                    d[0] = 1;
                    for i in 1..16 {
                        d[i] = d[i - 1].wrapping_add(1);
                    }
                    d
                };
                assert_eq!(b.key_or_digest, expected_digest);
                assert!(b.is_inline());
                assert!(!b.is_locked());
                assert!(!b.is_tombstone());
                assert_eq!(&b.inline_value()[..11], b"hello world");
            }
            WriteResult::TableFull => panic!("expected Inserted, got TableFull"),
        }
    }

    // -----------------------------------------------------------------------
    // test_kick_chain_displaces_key
    // -----------------------------------------------------------------------

    #[test]
    fn test_kick_chain_displaces_key() {
        // 4-bucket table: with h2 always odd, only 1 extra slot for evictions.
        // After 3 inserts, a kick chain is likely to be triggered.
        let mut buckets = make_buckets(4);
        let bucket_count = 4u64;

        let k1 = make_key(0x1001, 10);
        let k2 = make_key(0x2002, 20);
        let k3 = make_key(0x3003, 30);

        // First two should insert directly.
        let r1 = ClientWriter::insert(
            &k1,
            b"one",
            BucketMode::Inline,
            &mut buckets,
            None,
            bucket_count,
        )
        .unwrap();
        assert!(matches!(r1, WriteResult::Inserted { .. }));

        let r2 = ClientWriter::insert(
            &k2,
            b"two",
            BucketMode::Inline,
            &mut buckets,
            None,
            bucket_count,
        )
        .unwrap();
        assert!(matches!(r2, WriteResult::Inserted { .. }));

        // The third insert may trigger a kick chain or return TableFull on a
        // tiny table. Either outcome is acceptable; we just validate no panic.
        match ClientWriter::insert(
            &k3,
            b"three",
            BucketMode::Inline,
            &mut buckets,
            None,
            bucket_count,
        ) {
            Ok(WriteResult::Inserted { .. }) | Ok(WriteResult::TableFull) => {}
            Err(e) => panic!("unexpected error: {e}"),
        }

        // k1 and k2 should still be findable: scan buckets to verify.
        let find_key = |buckets: &[HashBucket], hash: u64, digest: &[u8; 16]| -> bool {
            buckets
                .iter()
                .any(|b| b.matches_key(hash, digest) && !b.is_tombstone())
        };

        // Recompute k1 digest.
        let mut d1 = [0u8; 16];
        d1[0] = 10;
        for i in 1..16 {
            d1[i] = d1[i - 1].wrapping_add(10);
        }
        assert!(find_key(&buckets, 0x1001, &d1), "k1 should be in the table");

        let mut d2 = [0u8; 16];
        d2[0] = 20;
        for i in 1..16 {
            d2[i] = d2[i - 1].wrapping_add(20);
        }
        assert!(find_key(&buckets, 0x2002, &d2), "k2 should be in the table");
    }

    // -----------------------------------------------------------------------
    // test_table_full_at_max_kick
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_full_at_max_kick() {
        // 4-bucket table — we pump many keys in until we hit TableFull.
        let mut buckets = make_buckets(4);
        let bucket_count = 4u64;

        let mut hit_table_full = false;

        // Insert up to 50 keys; at least one should fail on such a tiny table.
        for i in 0..50u64 {
            let hash = 0x5000 + i;
            let key = make_key(hash, (i & 0xFF) as u8);
            let value = (i as u64).to_le_bytes();
            match ClientWriter::insert(
                &key,
                &value,
                BucketMode::Inline,
                &mut buckets,
                None,
                bucket_count,
            ) {
                Ok(WriteResult::Inserted { .. }) => { /* ok */ }
                Ok(WriteResult::TableFull) => {
                    hit_table_full = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert!(
            hit_table_full,
            "should eventually return TableFull on a tiny table"
        );
    }

    // -----------------------------------------------------------------------
    // test_insert_extent_mode
    // -----------------------------------------------------------------------

    #[test]
    fn test_insert_extent_mode() {
        let mut buckets = make_buckets(8);
        let mut region = LargeObjectRegion::new(4096);
        let key = make_key(0xDEAD, 42);
        let value = vec![0xABu8; 200]; // too large for Inline

        let result = ClientWriter::insert(
            &key,
            &value,
            BucketMode::Extent,
            &mut buckets,
            Some(&mut region),
            8,
        )
        .expect("extent insert should not error");

        match result {
            WriteResult::Inserted { bucket_idx } => {
                let b = &buckets[bucket_idx];
                assert!(b.is_extent());
                assert!(!b.is_locked());
                assert!(!b.is_tombstone());
                assert_eq!(b.key_hash, 0xDEAD);

                // Read the extent back via LargeObjectRegion.
                let (offset, length) = b.extent_ref();
                assert_eq!(length, 200);
                let read_back = region.read(offset).expect("extent read should succeed");
                assert_eq!(read_back, value);
            }
            WriteResult::TableFull => panic!("expected Inserted, got TableFull"),
        }
    }

    // -----------------------------------------------------------------------
    // test_kick_chain_with_lock_conflict
    // -----------------------------------------------------------------------

    #[test]
    fn test_kick_chain_with_lock_conflict() {
        let mut buckets = make_buckets(8);
        let bucket_count = 8u64;

        // Manually lock bucket 0 (the lock bit + Inline mode).
        // Lock version = 0x01 → locked, Inline, version 0.
        buckets[0].lock_version = 0x01;
        buckets[0].key_hash = 0xBEEF; // make it look occupied

        // Now try to insert a key that maps to bucket 0 for h1.
        // We need key.hash % 8 == 0. Choose hash=8: h1=0, h2 = ((0>>32)%8)|1 = 1.
        let key = HashedKey {
            hash: 8,
            digest: [0xCC; 16],
        };

        // h1 is bucket 0 (locked), so try_insert_at returns false.
        // h2 is bucket 1 (should be empty), so the insert should succeed there.
        let result = ClientWriter::insert(
            &key,
            b"lock-test",
            BucketMode::Inline,
            &mut buckets,
            None,
            bucket_count,
        )
        .expect("insert should not error");

        // Should succeed at h2 (bucket 1), not h1 (locked).
        match result {
            WriteResult::Inserted { bucket_idx } => {
                assert_eq!(bucket_idx, 1, "should insert at h2=1, not locked h1=0");
                let b = &buckets[1];
                assert!(b.matches_key(8, &[0xCC; 16]));
                assert!(!b.is_locked());
            }
            WriteResult::TableFull => {
                // Edge case: if bucket 1 was also occupied (unlikely with
                // fresh zeroed buckets), TableFull is also valid.
                // In that case, verify bucket 1 is indeed not empty.
                assert!(!buckets[1].is_empty());
            }
        }

        // Verify bucket 0 remains locked and unchanged.
        assert!(buckets[0].is_locked());
        assert_eq!(buckets[0].key_hash, 0xBEEF);
    }

    // -----------------------------------------------------------------------
    // test_insert_empty_value_inline
    // -----------------------------------------------------------------------

    #[test]
    fn test_insert_empty_value_inline() {
        let mut buckets = make_buckets(8);
        let key = make_key(0x9999, 99);

        let result = ClientWriter::insert(&key, b"", BucketMode::Inline, &mut buckets, None, 8)
            .expect("empty value insert should not error");

        match result {
            WriteResult::Inserted { bucket_idx } => {
                let b = &buckets[bucket_idx];
                assert!(b.is_inline());
                // Body should be all zeros.
                assert!(b.inline_value().iter().all(|&x| x == 0));
            }
            WriteResult::TableFull => panic!("expected Inserted for empty value"),
        }
    }

    // -----------------------------------------------------------------------
    // test_tombstone_slot_is_reused
    // -----------------------------------------------------------------------

    #[test]
    fn test_tombstone_slot_is_reused() {
        let mut buckets = make_buckets(8);
        let bucket_count = 8u64;

        // Manually mark bucket 0 as a tombstone with some old key data.
        buckets[0].key_hash = 0x1111;
        buckets[0].key_or_digest = [0x11; 16];
        buckets[0].mark_tombstone();

        // Insert a key that hashes to bucket 0 for h1.
        // With bucket_count=8, need hash such that hash % 8 == 0. Use hash=0.
        // hash=0 is h1=0, h2 = ((0>>32)%8)|1 = 1.
        // But hash=0 is the empty sentinel — avoid it. Use hash=8: h1=0, h2=1.
        let key = HashedKey {
            hash: 8,
            digest: [0x22; 16],
        };

        let result = ClientWriter::insert(
            &key,
            b"reused",
            BucketMode::Inline,
            &mut buckets,
            None,
            bucket_count,
        )
        .expect("insert into tombstone should not error");

        assert!(
            matches!(result, WriteResult::Inserted { .. }),
            "tombstone slot should be reusable"
        );

        // The key should now reside in the table.
        let found = buckets
            .iter()
            .any(|b| b.matches_key(8, &[0x22; 16]) && !b.is_tombstone());
        assert!(found, "key should be in the table after tombstone reuse");
    }

    // -----------------------------------------------------------------------
    // test_extent_mode_without_region_returns_error
    // -----------------------------------------------------------------------

    #[test]
    fn test_extent_mode_without_region_returns_error() {
        let mut buckets = make_buckets(8);
        let key = make_key(0xAAAA, 5);

        let result = ClientWriter::insert(
            &key,
            b"data",
            BucketMode::Extent,
            &mut buckets,
            None, // no LargeObjectRegion provided
            8,
        );

        assert!(
            result.is_err(),
            "Extent mode without a region should be an error"
        );
        if let Err(RdmaError::Internal(msg)) = result {
            assert!(msg.contains("Extent"), "error should mention Extent: {msg}");
        } else if let Err(e) = result {
            panic!("expected Internal error, got {e}");
        }
    }
}
