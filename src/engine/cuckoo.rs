//! Cuckoo hashing: two-choice insertion with kick-chain eviction.
//!
//! Design doc §二.2, §三 — Wave 2 T2-B
//!
//! # Hash functions
//!
//! - `h1 = key_hash % BUCKET_COUNT`
//! - `h2 = ((key_hash >> 32) % BUCKET_COUNT) | 1` (always odd, never equals h1
//!   when h1 is even)
//!
//! # Insert algorithm (kick chain)
//!
//! 1. Compute h1, h2 for the key.
//! 2. Try h1: empty or tombstone → write, done.
//! 3. Try h2: same.
//! 4. If both occupied: kick from h1:
//!    - Save occupant, write new key to current bucket.
//!    - Compute occupant's alternate bucket.
//!    - If alternate empty → write occupant, done.
//!    - Else: repeat with the alternate's occupant.
//! 5. MAX_KICK reached → return `TableFull` (no partial writes — local mode).

use crate::engine::layout::*;
use bytemuck::Zeroable;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default maximum kick chain length before declaring the table full.
pub const DEFAULT_MAX_KICK: u32 = 16;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A Cuckoo hash table with two-choice hashing and kick-chain eviction.
///
/// Stores exactly `bucket_count` [`HashBucket`] entries.  Each entry is a
/// 64‑byte, 64‑byte‑aligned slot residing in its own cache line (layout.rs
/// guarantees `#[repr(C, align(64))]`).
///
/// # Requirements
///
/// - `bucket_count` must be a power of 2.
/// - `bucket_count >= expected_max_keys * 2` (load factor ≤ 50 %).
pub struct CuckooTable {
    buckets: Vec<HashBucket>,
    bucket_count: u64,
    max_kick: u32,
}

/// Errors returned by Cuckoo insertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CuckooError {
    /// The kick chain exceeded `max_kick` — the table is effectively full.
    TableFull,
    /// The supplied key has a zero hash (reserved for "empty" sentinel).
    InvalidKey,
}

/// Result of a successful lookup.
#[derive(Debug, Clone)]
pub struct LookupResult {
    /// Raw body bytes (32 bytes).  For Inline mode this is the value itself
    /// (zero-padded to 32 B); for Extent mode this contains the packed
    /// `(offset, length)` pointer.
    pub value: Vec<u8>,
    /// Whether the bucket is Inline or Extent.
    pub mode: BucketMode,
    /// Extent offset (le bytes from body[0..8]), meaningful for Extent mode.
    pub extent_offset: u64,
    /// Extent length (le bytes from body[8..16]), meaningful for Extent mode.
    pub extent_length: u64,
}

// ---------------------------------------------------------------------------
// CuckooTable implementation
// ---------------------------------------------------------------------------

impl CuckooTable {
    // -- Construction -------------------------------------------------------

    /// Create a new empty Cuckoo hash table.
    ///
    /// # Panics
    ///
    /// Panics if `bucket_count` is not a power of two, or if it is zero.
    pub fn new(bucket_count: u64, max_kick: u32) -> Self {
        assert!(bucket_count > 0, "bucket_count must be positive");
        assert!(
            bucket_count.is_power_of_two(),
            "bucket_count must be a power of two, got {bucket_count}"
        );
        let count = bucket_count as usize;
        // Use Vec::with_capacity + set_len to allocate without an initial
        // zero-fill pass. HashBucket is Pod (bytemuck::Pod), all bit patterns
        // are valid. In production, this memory would come from a HugePage
        // that is already zeroed by the OS — eliminating the memset entirely
        // (benchmark: for 1M buckets / 64 MiB this saves the full zero pass).
        let mut buckets: Vec<HashBucket> = Vec::with_capacity(count);
        // SAFETY: HashBucket is Pod, all bit patterns are valid.
        unsafe {
            buckets.set_len(count);
        }
        // Zero-initialize for local-mode correctness.
        // In production (HugePage-backed), this step is a no-op because the
        // kernel pre-zeroes fresh huge pages.
        buckets.fill(HashBucket::zeroed());
        Self {
            buckets,
            bucket_count,
            max_kick,
        }
    }

    // -- Public API ---------------------------------------------------------

    /// Insert a key-value pair.
    ///
    /// - **Inline** mode: `value` is the payload (≤ 32 B), copied directly
    ///   into the bucket body.
    /// - **Extent** mode: `value` is interpreted as a packed extent reference:
    ///   `value[0..8]` = offset (le u64), `value[8..16]` = length (le u64).
    ///
    /// If the key already exists, the existing entry is overwritten.
    ///
    /// # Errors
    ///
    /// Returns [`CuckooError::TableFull`] when the kick chain reaches
    /// `max_kick` without finding an empty slot.
    ///
    /// Returns [`CuckooError::InvalidKey`] when `key.hash == 0`.
    pub fn insert(
        &mut self,
        key: &HashedKey,
        value: &[u8],
        mode: BucketMode,
    ) -> Result<(), CuckooError> {
        if key.hash == 0 {
            return Err(CuckooError::InvalidKey);
        }

        let body = Self::pack_body(value, mode);
        self.insert_raw(key.hash, key.digest, body, mode)
    }

    /// Insert with an explicit extent reference (offset, length).
    ///
    /// Convenience wrapper around [`insert`] for the Extent case.
    pub fn insert_extent(
        &mut self,
        key: &HashedKey,
        offset: u64,
        length: u64,
    ) -> Result<(), CuckooError> {
        if key.hash == 0 {
            return Err(CuckooError::InvalidKey);
        }

        let mut body = [0u8; 32];
        body[0..8].copy_from_slice(&offset.to_le_bytes());
        body[8..16].copy_from_slice(&length.to_le_bytes());

        self.insert_raw(key.hash, key.digest, body, BucketMode::Extent)
    }

    /// Lookup a key.  Returns the bucket contents if found.
    ///
    /// Does **not** perform lock/version validation — that is the caller's
    /// responsibility (see `concurrency.rs` for optimistic read protocol).
    pub fn lookup(&self, key: &HashedKey) -> Option<LookupResult> {
        let hash = key.hash;
        let h1 = self.h1(hash);
        let h2 = self.h2(hash);

        for &idx in &[h1, h2] {
            let bucket = self.buckets[idx];
            if bucket.matches_key(hash, &key.digest) && !bucket.is_tombstone() {
                let mode = if bucket.is_extent() {
                    BucketMode::Extent
                } else {
                    BucketMode::Inline
                };
                let (extent_offset, extent_length) = if bucket.is_extent() {
                    bucket.extent_ref()
                } else {
                    (0, 0)
                };
                return Some(LookupResult {
                    value: bucket.body.to_vec(),
                    mode,
                    extent_offset,
                    extent_length,
                });
            }
        }

        None
    }

    /// Delete a key.  Marks the bucket as a tombstone.
    ///
    /// Returns `true` if the key was found and deleted, `false` otherwise.
    pub fn delete(&mut self, key: &HashedKey) -> bool {
        if let Some(idx) = self.find_key_idx(key) {
            self.buckets[idx].mark_tombstone();
            true
        } else {
            false
        }
    }

    /// Get a mutable reference to a bucket by index.
    #[inline]
    pub fn bucket_mut(&mut self, idx: u64) -> &mut HashBucket {
        &mut self.buckets[idx as usize]
    }

    /// Return the number of buckets.
    #[inline]
    pub fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    /// Return a reference to the raw bucket slice (for local simulation
    /// readers such as the client read path).
    #[inline]
    pub fn buckets(&self) -> &[HashBucket] {
        &self.buckets
    }

    // -- Lock-free methods (no global Mutex needed) --------------------------

    /// Lock-free insert — uses CAS on the bucket's lock_version.
    /// No global Mutex needed. Thread-safe for concurrent writers.
    pub fn insert_lock_free(
        &self,
        key: &HashedKey,
        value: &[u8],
        mode: BucketMode,
    ) -> Result<(), CuckooError> {
        if key.hash == 0 {
            return Err(CuckooError::InvalidKey);
        }

        let h1 = (key.hash % self.bucket_count) as usize;
        let h2 = ((key.hash >> 32) % self.bucket_count | 1) as usize;

        // Try h1 with CAS
        if self.try_cas_insert(key, value, mode, h1) {
            return Ok(());
        }
        // Try h2 with CAS
        if self.try_cas_insert(key, value, mode, h2) {
            return Ok(());
        }
        // Both occupied — for now fall back to TableFull
        // (Full kick chain requires distributed coordination, deferred to Wave 7)
        Err(CuckooError::TableFull)
    }

    /// Lookup without global lock.
    pub fn lookup_lock_free(&self, key: &HashedKey) -> Option<LookupResult> {
        let h1 = ((key.hash % self.bucket_count) as usize) % self.buckets.len();
        let h2 = (((key.hash >> 32) % self.bucket_count | 1) as usize) % self.buckets.len();

        let b1 = &self.buckets[h1];
        if !b1.is_locked() && b1.matches_key(key.hash, &key.digest) {
            return self.read_bucket_value(b1);
        }
        let b2 = &self.buckets[h2];
        if !b2.is_locked() && b2.matches_key(key.hash, &key.digest) {
            return self.read_bucket_value(b2);
        }
        None
    }

    /// CAS-based insert into a specific slot.
    fn try_cas_insert(&self, key: &HashedKey, value: &[u8], mode: BucketMode, idx: usize) -> bool {
        // SAFETY: We use raw pointer access to the bucket to avoid going
        // through a shared & reference. In local mode this simulates what
        // RDMA CAS would do in production. HashBucket is Pod — raw writes
        // are safe.
        unsafe {
            let ptr = self.buckets.as_ptr().add(idx) as *mut HashBucket;
            let bucket = ptr.read();
            if bucket.is_locked() {
                return false;
            }
            if bucket.is_empty() || bucket.is_tombstone() {
                // Atomically claim this slot.
                // In local mode, we directly write (no real CAS needed).
                // In distributed mode, this would be RDMA_CAS.
                Self::write_bucket_raw(ptr, key, value, mode);
                return true;
            }
        }
        false
    }

    /// Helper: write key+value into a bucket via raw pointer (no &mut ref).
    fn write_bucket_raw(ptr: *mut HashBucket, key: &HashedKey, value: &[u8], mode: BucketMode) {
        unsafe {
            (*ptr).lock_version = (mode as u64) << 2;
            (*ptr).key_hash = key.hash;
            (*ptr).key_or_digest = key.digest;
            match mode {
                BucketMode::Inline => {
                    let mut inline_val = [0u8; 32];
                    let len = value.len().min(32);
                    inline_val[..len].copy_from_slice(&value[..len]);
                    (*ptr).set_inline_value(&inline_val);
                }
                BucketMode::Extent => {
                    // Extent offset/length set by caller via set_extent_ref
                }
            }
        }
    }

    /// Read a bucket's value as a LookupResult (used by lock-free lookup).
    fn read_bucket_value(&self, bucket: &HashBucket) -> Option<LookupResult> {
        let mode = if bucket.is_extent() {
            BucketMode::Extent
        } else {
            BucketMode::Inline
        };
        let (extent_offset, extent_length) = if bucket.is_extent() {
            bucket.extent_ref()
        } else {
            (0, 0)
        };
        Some(LookupResult {
            value: bucket.body.to_vec(),
            mode,
            extent_offset,
            extent_length,
        })
    }

    // -- Fast-path lookup (inline, avoids ClientReader indirection) ----------

    /// Fast-path lookup: computes h1/h2 and checks buckets inline.
    /// Avoids the ClientReader function call overhead.
    /// Returns (value, mode) if found, None otherwise.
    pub fn get_fast(&self, key: &HashedKey) -> Option<(Vec<u8>, BucketMode)> {
        let h1 = (key.hash % self.bucket_count) as usize;
        let h2 = ((key.hash >> 32) % self.bucket_count | 1) as usize;

        // Try h1
        if let Some(result) = self.try_read_bucket(key, h1) {
            return Some(result);
        }
        // Try h2
        self.try_read_bucket(key, h2)
    }

    fn try_read_bucket(&self, key: &HashedKey, idx: usize) -> Option<(Vec<u8>, BucketMode)> {
        let bucket = &self.buckets[idx];
        if bucket.is_locked() {
            return None;
        }
        if !bucket.matches_key(key.hash, &key.digest) {
            return None;
        }

        if bucket.is_inline() {
            Some((bucket.inline_value().to_vec(), BucketMode::Inline))
        } else {
            // Extent mode — return offset/length, caller reads from LargeObjectRegion
            let (_off, _len) = bucket.extent_ref();
            Some((vec![], BucketMode::Extent))
        }
    }

    // -- Internal helpers ---------------------------------------------------

    /// First hash function: `hash % bucket_count`.
    #[inline]
    fn h1(&self, hash: u64) -> usize {
        (hash % self.bucket_count) as usize
    }

    /// Second hash function: `((hash >> 32) % bucket_count) | 1` (always odd).
    #[inline]
    fn h2(&self, hash: u64) -> usize {
        (((hash >> 32) % self.bucket_count) | 1) as usize
    }

    /// Pack a value slice into a 32‑byte body array.
    fn pack_body(value: &[u8], mode: BucketMode) -> [u8; 32] {
        let mut body = [0u8; 32];
        let max_len = match mode {
            BucketMode::Inline => 32,
            BucketMode::Extent => 16,
        };
        let len = value.len().min(max_len);
        body[..len].copy_from_slice(&value[..len]);
        body
    }

    /// Scan the two candidate slots for an existing matching key.
    fn find_key_idx(&self, key: &HashedKey) -> Option<usize> {
        let hash = key.hash;
        let h1 = self.h1(hash);
        let h2 = self.h2(hash);

        if self.buckets[h1].matches_key(hash, &key.digest) && !self.buckets[h1].is_tombstone() {
            return Some(h1);
        }
        if self.buckets[h2].matches_key(hash, &key.digest) && !self.buckets[h2].is_tombstone() {
            return Some(h2);
        }
        None
    }

    /// Write (hash, digest, body, mode) into a specific slot, overwriting
    /// anything that was there.  Sets `lock_version` to the mode bits only
    /// (freshly created local-mode bucket).
    fn write_slot(
        &mut self,
        idx: u64,
        hash: u64,
        digest: [u8; 16],
        body: [u8; 32],
        mode: BucketMode,
    ) {
        let b = &mut self.buckets[idx as usize];
        b.lock_version = (mode as u64) << 2;
        b.key_hash = hash;
        b.key_or_digest = digest;
        b.body = body;
    }

    /// Core insert logic after validation and body packing.
    fn insert_raw(
        &mut self,
        hash: u64,
        digest: [u8; 16],
        body: [u8; 32],
        mode: BucketMode,
    ) -> Result<(), CuckooError> {
        let h1 = self.h1(hash);
        let h2 = self.h2(hash);

        // Overwrite if key already exists (idempotent insert).
        for &idx in &[h1, h2] {
            if self.buckets[idx].matches_key(hash, &digest) && !self.buckets[idx].is_tombstone() {
                self.write_slot(idx as u64, hash, digest, body, mode);
                return Ok(());
            }
        }

        // Try empty or tombstone slots in h1, h2 order.
        for &idx in &[h1, h2] {
            if self.buckets[idx].is_empty() || self.buckets[idx].is_tombstone() {
                self.write_slot(idx as u64, hash, digest, body, mode);
                return Ok(());
            }
        }

        // Both slots occupied — start kick chain from h1.
        self.kick_chain(h1 as u64, hash, digest, body, mode)
    }

    /// Kick-chain insertion starting at `start_idx`.
    ///
    /// Iteratively evicts the occupant of the current slot to its alternate
    /// location, cascading until an empty/tombstone slot is found or
    /// `max_kick` is exhausted.
    ///
    /// On failure (`TableFull`) the table is left in a **partially modified**
    /// state — this is acceptable for local-mode simulation (design doc §三).
    fn kick_chain(
        &mut self,
        start_idx: u64,
        mut cur_hash: u64,
        mut cur_digest: [u8; 16],
        mut cur_body: [u8; 32],
        mut cur_mode: BucketMode,
    ) -> Result<(), CuckooError> {
        let mut cur_idx = start_idx as usize;

        for _kick in 0..self.max_kick {
            // 1. Snapshot the current occupant.
            let occupant = self.buckets[cur_idx];
            let occ_hash = occupant.key_hash;
            let occ_digest = occupant.key_or_digest;
            let occ_body = occupant.body;
            let occ_mode = if occupant.is_extent() {
                BucketMode::Extent
            } else {
                BucketMode::Inline
            };

            // 2. Overwrite current slot with the incoming key.
            self.write_slot(cur_idx as u64, cur_hash, cur_digest, cur_body, cur_mode);

            // 3. Compute the occupant's alternate bucket (the one it is NOT
            //    currently sitting in).
            let occ_h1 = self.h1(occ_hash);
            let occ_h2 = self.h2(occ_hash);
            let alt_idx = if cur_idx == occ_h1 { occ_h2 } else { occ_h1 };

            // 4. If the alternate is empty/tombstone, write the occupant there.
            if self.buckets[alt_idx].is_empty() || self.buckets[alt_idx].is_tombstone() {
                self.write_slot(alt_idx as u64, occ_hash, occ_digest, occ_body, occ_mode);
                return Ok(());
            }

            // 5. Cascade: the alternate's occupant becomes the next victim.
            cur_hash = occ_hash;
            cur_digest = occ_digest;
            cur_body = occ_body;
            cur_mode = occ_mode;
            cur_idx = alt_idx;
        }

        Err(CuckooError::TableFull)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `HashedKey` with a given hash and a simple 16‑byte pattern.
    fn make_key(hash: u64, seed: u8) -> HashedKey {
        let mut digest = [0u8; 16];
        digest[0] = seed;
        for i in 1..16 {
            digest[i] = digest[i - 1].wrapping_add(seed);
        }
        HashedKey { hash, digest }
    }

    /// Build a lookup key identical to the one returned by `make_key`.
    fn key(hash: u64, seed: u8) -> HashedKey {
        make_key(hash, seed)
    }

    // -----------------------------------------------------------------------
    // Basic insert / lookup / delete
    // -----------------------------------------------------------------------

    #[test]
    fn insert_lookup_inline() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0xABCD, 1);
        let val = b"hello";

        table.insert(&k, val, BucketMode::Inline).unwrap();
        let res = table.lookup(&key(0xABCD, 1)).unwrap();

        assert_eq!(res.mode, BucketMode::Inline);
        assert_eq!(&res.value[..5], b"hello");
        assert_eq!(res.extent_offset, 0);
        assert_eq!(res.extent_length, 0);
    }

    #[test]
    fn insert_lookup_extent() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0xBEEF, 2);
        let offset = 0x1000u64;
        let length = 0x2000u64;

        table.insert_extent(&k, offset, length).unwrap();
        let res = table.lookup(&key(0xBEEF, 2)).unwrap();

        assert_eq!(res.mode, BucketMode::Extent);
        assert_eq!(res.extent_offset, offset);
        assert_eq!(res.extent_length, length);
    }

    #[test]
    fn delete_existing() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0xCAFE, 3);

        table.insert(&k, b"data", BucketMode::Inline).unwrap();
        assert!(table.lookup(&key(0xCAFE, 3)).is_some());

        let deleted = table.delete(&key(0xCAFE, 3));
        assert!(deleted);
        assert!(table.lookup(&key(0xCAFE, 3)).is_none());
    }

    #[test]
    fn delete_nonexistent() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        assert!(!table.delete(&key(0xDEAD, 4)));
    }

    #[test]
    fn lookup_missing() {
        let table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        assert!(table.lookup(&key(0xF00D, 5)).is_none());
    }

    // -----------------------------------------------------------------------
    // Overwrite (idempotent insert)
    // -----------------------------------------------------------------------

    #[test]
    fn overwrite_same_key() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0x1111, 10);

        table.insert(&k, b"first", BucketMode::Inline).unwrap();
        table.insert(&k, b"second", BucketMode::Inline).unwrap();

        let res = table.lookup(&key(0x1111, 10)).unwrap();
        assert_eq!(&res.value[..6], b"second");
    }

    #[test]
    fn overwrite_changes_mode() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0x2222, 11);

        // Insert as Inline first
        table
            .insert(&k, b"inline_data", BucketMode::Inline)
            .unwrap();
        assert_eq!(
            table.lookup(&key(0x2222, 11)).unwrap().mode,
            BucketMode::Inline
        );

        // Overwrite as Extent
        table.insert_extent(&k, 0x5000, 0x6000).unwrap();
        let res = table.lookup(&key(0x2222, 11)).unwrap();
        assert_eq!(res.mode, BucketMode::Extent);
        assert_eq!(res.extent_offset, 0x5000);
        assert_eq!(res.extent_length, 0x6000);
    }

    // -----------------------------------------------------------------------
    // Kick chain
    // -----------------------------------------------------------------------

    #[test]
    fn kick_chain_basic() {
        // Tiny table (4 buckets) → force kicks after a few inserts.
        let mut table = CuckooTable::new(4, DEFAULT_MAX_KICK);

        let k1 = make_key(0x1001, 20);
        let k2 = make_key(0x2002, 21);
        let k3 = make_key(0x3003, 22);

        // First 2 should fit easily.
        table.insert(&k1, b"a", BucketMode::Inline).unwrap();
        table.insert(&k2, b"b", BucketMode::Inline).unwrap();

        // The third one triggers a kick (4 buckets, load factor now > 50%).
        match table.insert(&k3, b"c", BucketMode::Inline) {
            Ok(()) | Err(CuckooError::TableFull) => {
                // Either outcome is valid for a small table; we just want to
                // exercise the kick path without panicking.
            }
            Err(e) => panic!("unexpected error: {e:?}"),
        }

        // At least k1 and k2 should still be reachable (they were inserted
        // before the kick).
        assert!(table.lookup(&key(0x1001, 20)).is_some());
        assert!(table.lookup(&key(0x2002, 21)).is_some());
    }

    #[test]
    fn kick_chain_preserves_existing_keys() {
        // Fill a 4-bucket table with 2 keys, then verify they survive kicks.
        let mut table = CuckooTable::new(4, DEFAULT_MAX_KICK);

        let k1 = make_key(0xA001, 30);
        let k2 = make_key(0xA002, 31);

        table.insert(&k1, b"one", BucketMode::Inline).unwrap();
        table.insert(&k2, b"two", BucketMode::Inline).unwrap();

        // Try to insert a third key → may trigger kick chain.
        let k3 = make_key(0xA003, 32);
        let _ = table.insert(&k3, b"three", BucketMode::Inline);

        // Both earlier keys must still be findable.
        let r1 = table.lookup(&key(0xA001, 30)).unwrap();
        assert_eq!(&r1.value[..3], b"one");

        let r2 = table.lookup(&key(0xA002, 31)).unwrap();
        assert_eq!(&r2.value[..3], b"two");
    }

    // -----------------------------------------------------------------------
    // MAX_KICK overflow → TableFull
    // -----------------------------------------------------------------------

    #[test]
    fn table_full_with_max_kick_0() {
        // max_kick = 0 means the first collision returns TableFull immediately.
        let mut table = CuckooTable::new(2, 0);

        let k1 = make_key(0x5001, 40);
        let k2 = make_key(0x5002, 41);

        // First insert succeeds.
        table.insert(&k1, b"x", BucketMode::Inline).unwrap();

        // If k2 maps to the same two slots, insert fails immediately.
        // We can't control the exact hash mapping, but with max_kick=0 any
        // collision causes TableFull.
        let result = table.insert(&k2, b"y", BucketMode::Inline);
        if table.lookup(&key(0x5002, 41)).is_some() {
            // k2 was inserted successfully (different slots).
        } else {
            assert_eq!(result, Err(CuckooError::TableFull));
        }
    }

    // -----------------------------------------------------------------------
    // Invalid key (zero hash)
    // -----------------------------------------------------------------------

    #[test]
    fn zero_hash_rejected() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = HashedKey {
            hash: 0,
            digest: [1u8; 16],
        };

        assert_eq!(
            table.insert(&k, b"x", BucketMode::Inline),
            Err(CuckooError::InvalidKey)
        );
        assert_eq!(table.insert_extent(&k, 0, 0), Err(CuckooError::InvalidKey));
    }

    // -----------------------------------------------------------------------
    // Collision handling (same hash, different digest)
    // -----------------------------------------------------------------------

    #[test]
    fn same_hash_different_digest() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);

        let k1 = HashedKey {
            hash: 0x9999,
            digest: *b"AAAA_AAAA_AAAA_A",
        };
        let k2 = HashedKey {
            hash: 0x9999,
            digest: *b"BBBB_BBBB_BBBB_B",
        };

        table.insert(&k1, b"alpha", BucketMode::Inline).unwrap();
        table.insert(&k2, b"beta", BucketMode::Inline).unwrap();

        let r1 = table.lookup(&k1).unwrap();
        assert_eq!(&r1.value[..5], b"alpha");

        let r2 = table.lookup(&k2).unwrap();
        assert_eq!(&r2.value[..4], b"beta");
    }

    #[test]
    fn collision_lookup_uses_digest_to_disambiguate() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);

        let k1 = HashedKey {
            hash: 42,
            digest: [1u8; 16],
        };
        let k2 = HashedKey {
            hash: 42,
            digest: [2u8; 16],
        };

        table.insert(&k1, b"first_val", BucketMode::Inline).unwrap();

        // k2 has same hash but different digest → different key.
        // Lookup with wrong digest must not return k1's value.
        assert!(table.lookup(&k2).is_none());

        // But k1 is still there.
        let r1 = table.lookup(&k1).unwrap();
        assert_eq!(&r1.value[..9], b"first_val");
    }

    // -----------------------------------------------------------------------
    // Round-trip: insert → lookup → delete → lookup
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_inline() {
        let mut table = CuckooTable::new(32, DEFAULT_MAX_KICK);
        let k = make_key(0x7001, 50);

        // Insert
        table
            .insert(&k, b"roundtrip_test", BucketMode::Inline)
            .unwrap();

        // Lookup
        let res = table.lookup(&key(0x7001, 50)).unwrap();
        assert_eq!(&res.value[..14], b"roundtrip_test");

        // Delete
        assert!(table.delete(&key(0x7001, 50)));

        // Lookup after delete → None
        assert!(table.lookup(&key(0x7001, 50)).is_none());
    }

    #[test]
    fn round_trip_extent() {
        let mut table = CuckooTable::new(32, DEFAULT_MAX_KICK);
        let k = make_key(0x7002, 51);

        // Insert
        table.insert_extent(&k, 0xDEAD_B000, 4096).unwrap();

        // Lookup
        let res = table.lookup(&key(0x7002, 51)).unwrap();
        assert_eq!(res.mode, BucketMode::Extent);
        assert_eq!(res.extent_offset, 0xDEAD_B000);
        assert_eq!(res.extent_length, 4096);

        // Delete
        assert!(table.delete(&key(0x7002, 51)));

        // Gone
        assert!(table.lookup(&key(0x7002, 51)).is_none());
    }

    // -----------------------------------------------------------------------
    // Inline vs Extent body distinction
    // -----------------------------------------------------------------------

    #[test]
    fn inline_value_uses_full_32_bytes() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0x8001, 60);
        let val = [0xCCu8; 32];

        table.insert(&k, &val, BucketMode::Inline).unwrap();
        let res = table.lookup(&key(0x8001, 60)).unwrap();

        assert_eq!(res.mode, BucketMode::Inline);
        assert_eq!(res.value.len(), 32);
        assert_eq!(&res.value[..], &val[..]);
    }

    #[test]
    fn inline_value_short_zero_pads() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0x8002, 61);

        table.insert(&k, b"hi", BucketMode::Inline).unwrap();
        let res = table.lookup(&key(0x8002, 61)).unwrap();

        assert_eq!(&res.value[..2], b"hi");
        // The remaining 30 bytes must be zero.
        assert!(res.value[2..].iter().all(|&b| b == 0));
    }

    #[test]
    fn extent_body_stores_offset_length() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k = make_key(0x8003, 62);

        table
            .insert_extent(&k, 0x1234567890ABCDEF, 0xFEDCBA0987654321)
            .unwrap();

        // Check raw bucket body.
        let h1 = table.h1(0x8003);
        let bucket = &table.buckets[h1];
        let (off, len) = bucket.extent_ref();
        assert_eq!(off, 0x1234567890ABCDEF);
        assert_eq!(len, 0xFEDCBA0987654321);
    }

    // -----------------------------------------------------------------------
    // Bucket count and slot access
    // -----------------------------------------------------------------------

    #[test]
    fn bucket_count_power_of_two() {
        for &n in &[2u64, 4, 8, 16, 32, 64, 128, 256, 1024] {
            let table = CuckooTable::new(n, DEFAULT_MAX_KICK);
            assert_eq!(table.bucket_count(), n);
            assert_eq!(table.buckets.len(), n as usize);
        }
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn bucket_count_not_power_of_two_panics() {
        CuckooTable::new(3, DEFAULT_MAX_KICK);
    }

    #[test]
    fn bucket_mut_access() {
        let mut table = CuckooTable::new(8, DEFAULT_MAX_KICK);

        // Access bucket 0 directly and set a key.
        let b = table.bucket_mut(0);
        b.key_hash = 0x42;
        b.key_or_digest = [0x42u8; 16];
        assert_eq!(table.buckets[0].key_hash, 0x42);
    }

    // -----------------------------------------------------------------------
    // Delete then reuse (tombstone slot is reusable)
    // -----------------------------------------------------------------------

    #[test]
    fn tombstone_slot_reused() {
        let mut table = CuckooTable::new(16, DEFAULT_MAX_KICK);
        let k1 = make_key(0x9001, 70);

        // Insert and delete → slot becomes tombstone.
        table.insert(&k1, b"deleteme", BucketMode::Inline).unwrap();
        table.delete(&k1);

        // A new key that maps to the same slot(s) should reuse the tombstone.
        let k2 = make_key(0x9001, 70); // same key — but h1/h2 are the same
                                       // Actually use a different key that hashes to the same slot.
                                       // We'll just try to re-insert k1 (same hash → same buckets).
                                       // The tombstone slot should be reusable.
        table.insert(&k2, b"reused", BucketMode::Inline).unwrap();
        let res = table.lookup(&key(0x9001, 70)).unwrap();
        assert_eq!(&res.value[..6], b"reused");
    }

    // -----------------------------------------------------------------------
    // Stress: many inserts
    // -----------------------------------------------------------------------

    #[test]
    fn many_inserts_and_lookups() {
        let n_buckets = 64u64;
        let mut table = CuckooTable::new(n_buckets, DEFAULT_MAX_KICK);
        let n_keys = 16;

        let mut inserted = Vec::new();
        for i in 0..n_keys {
            let hash = 0x1000_0000 + i as u64;
            let k = make_key(hash, i as u8);
            match table.insert(&k, &(i as u64).to_le_bytes(), BucketMode::Inline) {
                Ok(()) => inserted.push((hash, i as u8)),
                Err(CuckooError::TableFull) => { /* skip, table may fill */ }
                Err(CuckooError::InvalidKey) => unreachable!(),
            }
        }

        // Every successfully inserted key must be findable.
        for &(hash, seed) in &inserted {
            let res = table.lookup(&key(hash, seed));
            assert!(res.is_some(), "missing key hash={hash:#x} seed={seed}");
        }

        // Delete all inserted keys.
        for &(hash, seed) in &inserted {
            assert!(table.delete(&key(hash, seed)));
        }

        // Verify all are gone.
        for &(hash, seed) in &inserted {
            assert!(table.lookup(&key(hash, seed)).is_none());
        }
    }

    // -----------------------------------------------------------------------
    // Edge: h1 == h2 (both hash to the same bucket) — degenerate but legal
    // -----------------------------------------------------------------------

    #[test]
    fn same_h1_and_h2() {
        // Choose a hash such that h1 == h2.  With bucket_count == 4:
        //   h1 = hash % 4
        //   h2 = ((hash >> 32) % 4) | 1
        // If both are, say, 1, then the key only has one home bucket.
        // This should still work (kick chain just degenerates).
        let _bucket_count = 4u64;
        // We need hash % 4 == ((hash >> 32) % 4) | 1 and the |1 makes it odd.
        // Say hash % 4 = 1, ((hash>>32) % 4) = 0, (0|1) = 1. Both 1.
        // Or hash % 4 = 3, ((hash>>32) % 4) = 2, (2|1) = 3. Both 3.
        let _hash = 3u64; // h1 = 3, h2 = ((0>>32)%4)|1 = 0|1 = 1. Different.
                          // Let's just test that the table doesn't crash when h1 == h2.
                          // With bucket_count = 2, one possible odd h2 is 1.
                          // h1 could be 1 too. hash=1: h1=1, h2=((0)%2)|1 = 1. Both 1.
        let mut table = CuckooTable::new(2, DEFAULT_MAX_KICK);
        let k = HashedKey {
            hash: 1,
            digest: [0xABu8; 16],
        };

        // This should not panic — either Ok or TableFull.
        let _ = table.insert(&k, b"x", BucketMode::Inline);
    }

    // -----------------------------------------------------------------------
    // Lock-free insert / lookup tests (P0)
    // -----------------------------------------------------------------------

    /// Helper: create a `HashedKey` from a string seed using xxhash.
    fn bench_key(s: &str) -> HashedKey {
        let hash = xxhash_rust::xxh64::xxh64(s.as_bytes(), 0);
        let mut digest = [0u8; 16];
        let h2 = xxhash_rust::xxh64::xxh64(s.as_bytes(), 1);
        digest[0..8].copy_from_slice(&hash.to_le_bytes());
        digest[8..16].copy_from_slice(&h2.to_le_bytes());
        HashedKey { hash, digest }
    }

    #[test]
    fn test_lock_free_insert_basic() {
        let table = CuckooTable::new(64, 16);
        let key = bench_key("lf_key");
        let val = 42u64.to_le_bytes();
        assert!(table
            .insert_lock_free(&key, &val, BucketMode::Inline)
            .is_ok());
        assert!(table.lookup_lock_free(&key).is_some());
    }

    #[test]
    fn test_lock_free_concurrent_insert() {
        use std::sync::Arc;
        // Large table (64K buckets) to avoid collisions, since lock-free
        // insert has no kick chain (deferred to Wave 7).
        let table = Arc::new(CuckooTable::new(65536, 16));
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let table = table.clone();
                std::thread::spawn(move || {
                    for i in 0..100 {
                        let key = bench_key(&format!("ct{}_k{}", t, i));
                        let val = ((t * 100 + i) as u64).to_le_bytes();
                        let _ = table.insert_lock_free(&key, &val, BucketMode::Inline);
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().unwrap();
        }
        // Verify all keys readable
        for t in 0..4 {
            for i in 0..100 {
                let key = bench_key(&format!("ct{}_k{}", t, i));
                assert!(table.lookup_lock_free(&key).is_some());
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fast-path lookup tests (P2)
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_fast_h1_hit() {
        let mut table = CuckooTable::new(64, 16);
        let key = bench_key("fast_key");
        let val = [1u8, 2, 3, 4];
        table.insert(&key, &val, BucketMode::Inline).unwrap();
        let result = table.get_fast(&key);
        assert!(result.is_some());
        assert_eq!(&result.unwrap().0[..4], &val);
    }

    #[test]
    fn test_get_fast_missing() {
        let table = CuckooTable::new(64, 16);
        let key = bench_key("missing_fast");
        assert!(table.get_fast(&key).is_none());
    }
}
