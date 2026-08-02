//! Data layout: dual-mode `HashBucket` (Inline/Extent), `ExtentHeader`, and key types.
//!
//! Design spec: Rust-RDMA.md §二.1 — Wave 2 T2-A
//!
//! # Bit Layout of `HashBucket::lock_version` (u64, little‑endian)
//!
//! ```text
//! [63..32] version (32b)
//! [31.. 8] lease_ts (24b)
//! [ 7.. 3] reserved
//! [ 2]     mode  (0=Inline, 1=Extent)
//! [ 1]     tombstone
//! [ 0]     locked
//! ```
//!
//! Legal states:
//!   `0b000` — idle, alive, Inline
//!   `0b001` — locked + Inline
//!   `0b010` — tombstone
//!   `0b100` — idle, alive, Extent
//!   `0b101` — locked + Extent

use bytemuck::{Pod, Zeroable};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default lease timeout in milliseconds.
pub const LEASE_TIMEOUT_MS: u32 = 100;

/// Extent-header magic value: ASCII "RDMA" = `0x52444D41`.
pub const EXTENT_MAGIC: u32 = 0x5244_4D41;

// ---------------------------------------------------------------------------
// BucketMode
// ---------------------------------------------------------------------------

/// Dual‑mode storage selector: Inline stores the value inside the bucket;
/// Extent stores an (offset, length) pointer into the extent region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketMode {
    Inline = 0,
    Extent = 1,
}

// ---------------------------------------------------------------------------
// HashedKey
// ---------------------------------------------------------------------------

/// A 64‑bit XXH64 hash together with a 16‑byte digest for collision verification.
///
/// In **Inline** mode the digest field also serves as the raw key (padded with
/// zeros when shorter than 16 bytes).  In **Extent** mode it is a
/// cryptographic/XXH128 digest for exact-match verification.
#[derive(Debug, Clone)]
pub struct HashedKey {
    pub hash: u64,
    pub digest: [u8; 16],
}

// ---------------------------------------------------------------------------
// HashBucket — exactly 64 bytes, dual‑mode (Inline / Extent)
// ---------------------------------------------------------------------------

/// The fundamental record of the Cuckoo hash table.
///
/// # Layout (64 bytes, align‑64)
///
/// | Offset | Field           | Size |
/// |--------|-----------------|------|
/// | 0      | `lock_version`  | 8    |
/// | 8      | `key_hash`      | 8    |
/// | 16     | `key_or_digest` | 16   |
/// | 32     | `body`          | 32   |
///
/// # Dual‑mode body
///
/// - **Inline**: `body[0..32]` = raw value (max 32 bytes).
/// - **Extent**: `body[0.. 8]` = offset (u64 le), `body[8..16]` = length (u64 le),
///   `body[16..32]` = reserved.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C, align(64))]
pub struct HashBucket {
    /// Combined lock‑word: version, lease timestamp, mode, tombstone, locked.
    ///
    /// See module‑level documentation for the bit layout.
    pub lock_version: u64,

    /// 64‑bit XXH64 hash of the key.  `0` means "empty" when the tombstone
    /// bit is not set.
    pub key_hash: u64,

    /// Inline: raw key (≤ 16 bytes, zero‑padded).  Extent: 16‑byte digest.
    pub key_or_digest: [u8; 16],

    /// Value storage.  Interpretation depends on `mode` bit.
    pub body: [u8; 32],
}

// --- Compile‑time assertions ---

const _: () = assert!(core::mem::size_of::<HashBucket>() == 64);
const _: () = assert!(core::mem::align_of::<HashBucket>() == 64);

// --- Bitfield helpers ---

impl HashBucket {
    // -- Read accessors --

    /// Returns `true` when bit‑0 (locked) is set.
    #[inline]
    pub fn is_locked(&self) -> bool {
        (self.lock_version & 0x01) != 0
    }

    /// Returns `true` when bit‑1 (tombstone) is set.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        (self.lock_version & 0x02) != 0
    }

    /// Returns `true` when bit‑2 (mode) is **0** → Inline storage.
    #[inline]
    pub fn is_inline(&self) -> bool {
        (self.lock_version & 0x04) == 0
    }

    /// Returns `true` when bit‑2 (mode) is **1** → Extent storage.
    #[inline]
    pub fn is_extent(&self) -> bool {
        (self.lock_version & 0x04) != 0
    }

    /// Reads the 32‑bit version counter from bits `[63..32]`.
    #[inline]
    pub fn version(&self) -> u32 {
        (self.lock_version >> 32) as u32
    }

    /// Reads the 24‑bit lease timestamp from bits `[31..8]`.
    #[inline]
    pub fn lease_ts(&self) -> u32 {
        ((self.lock_version >> 8) & 0xFF_FFFF) as u32
    }

    // -- Write helpers (local; CAS is handled by the concurrency layer) --

    /// Construct the `lock_version` word for a newly‑locked bucket.
    ///
    /// Preserves the current `version`, sets the lease timestamp, mode bit,
    /// and the locked flag.  Tombstone is cleared.
    #[inline]
    pub fn set_locked(&mut self, lease_ts_ms: u32, mode: BucketMode) {
        let version = (self.lock_version >> 32) as u64;
        let lease = ((lease_ts_ms as u64) & 0xFF_FFFF) << 8;
        let mode_bit = (mode as u64) << 2;
        self.lock_version = (version << 32) | lease | mode_bit | 0x01;
    }

    /// Unlock the bucket: write a new version counter, preserve the mode
    /// bit, and clear locked / tombstone / lease timestamp.
    #[inline]
    pub fn unlock_bucket(&mut self, new_version: u32) {
        let mode = self.lock_version & 0x04;
        self.lock_version = ((new_version as u64) << 32) | mode;
    }

    /// Mark the bucket as a tombstone: set bit‑1, clear locked (bit‑0) and
    /// the lease timestamp.  Version and mode are preserved.
    #[inline]
    pub fn mark_tombstone(&mut self) {
        let mode = self.lock_version & 0x04;
        let version = self.lock_version & 0xFFFF_FFFF_0000_0000;
        self.lock_version = version | mode | 0x02;
    }

    /// Returns `true` when the bucket is locked **and** the lease has expired.
    ///
    /// `now_ms` and `timeout_ms` are both in the same unit (typically
    /// milliseconds).  Uses wrapping subtraction for overflow safety.
    #[inline]
    pub fn is_expired(&self, now_ms: u32, timeout_ms: u32) -> bool {
        self.is_locked() && now_ms.wrapping_sub(self.lease_ts()) >= timeout_ms
    }

    // -- Inline value access --

    /// Return a reference to the 32‑byte inline value.
    #[inline]
    pub fn inline_value(&self) -> &[u8; 32] {
        &self.body
    }

    /// Copy a 32‑byte value into the inline storage.
    #[inline]
    pub fn set_inline_value(&mut self, value: &[u8; 32]) {
        self.body = *value;
    }

    // -- Extent reference (offset + length) --

    /// Write an extent pointer into `body[0..16]`.
    ///
    /// `body[0..8]` = `offset` (little‑endian u64)  
    /// `body[8..16]` = `length` (little‑endian u64)
    #[inline]
    pub fn set_extent_ref(&mut self, offset: u64, length: u64) {
        self.body[0..8].copy_from_slice(&offset.to_le_bytes());
        self.body[8..16].copy_from_slice(&length.to_le_bytes());
    }

    /// Read the extent pointer from `body[0..16]`.
    ///
    /// Returns `(offset, length)`.
    #[inline]
    pub fn extent_ref(&self) -> (u64, u64) {
        let offset = u64::from_le_bytes(self.body[0..8].try_into().unwrap());
        let length = u64::from_le_bytes(self.body[8..16].try_into().unwrap());
        (offset, length)
    }

    // -- Logical state queries --

    /// A bucket is considered "empty" when its `key_hash` is zero **and** it
    /// is not a tombstone.  (A locked bucket with `key_hash == 0` is still
    /// empty — insertion is in‑flight.)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.key_hash == 0 && !self.is_tombstone()
    }

    /// Exact‑match check: the stored hash **and** digest must both match.
    #[inline]
    pub fn matches_key(&self, hash: u64, digest: &[u8; 16]) -> bool {
        self.key_hash == hash && self.key_or_digest == *digest
    }
}

// ---------------------------------------------------------------------------
// ExtentHeader — large‑object metadata (24 bytes)
// ---------------------------------------------------------------------------

/// Header placed at the start of every allocated extent in the large‑object
/// region.
///
/// # Layout (24 bytes)
///
/// | Offset | Field        | Size |
/// |--------|--------------|------|
/// | 0      | `length`     | 8    |
/// | 8      | `epoch_mark` | 8    |
/// | 16     | `magic`      | 4    |
/// | 20     | `_pad`       | 4    |
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct ExtentHeader {
    /// Payload length in bytes (not including this header).
    pub length: u64,

    /// Global‑GC death timestamp (epoch).  Stale extents whose epoch has
    /// passed are eligible for collection.
    pub epoch_mark: u64,

    /// Magic constant: `EXTENT_MAGIC = 0x52444D41` ("RDMA").
    pub magic: u32,

    /// Explicit padding to reach 24 bytes.
    pub _pad: u32,
}

// --- Compile‑time assertions ---

const _: () = assert!(core::mem::size_of::<ExtentHeader>() == 24);

// ---------------------------------------------------------------------------
// ExtentHeaderV2 — large‑object metadata (32 bytes, Wave 9 T9-D)
// ---------------------------------------------------------------------------

/// Header placed at the start of every allocated extent in the large‑object
/// region (V2 format, 32 bytes).
///
/// # Layout (32 bytes)
///
/// | Offset | Field        | Size |
/// |--------|--------------|------|
/// | 0      | `magic`      | 4    |
/// | 4      | `version`    | 1    |
/// | 5      | `_pad1`      | 3    |
/// | 8      | `data_len`   | 4    |
/// | 12     | `_pad2`      | 4    |
/// | 16     | `epoch_mark` | 8    |
/// | 24     | `checksum`   | 8    |
///
/// `checksum` is an XXH64 of the payload. A value of 0 means the write is
/// still in progress. Readers must verify checksum before trusting the data.
#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct ExtentHeaderV2 {
    /// Magic constant: `EXTENT_MAGIC = 0x52444D41` ("RDMA").
    pub magic: u32,

    /// Header format version: 1 = ExtentHeaderV2.
    /// 0 is reserved for V1 (24‑byte ExtentHeader).
    pub version: u8,

    /// Padding: align `data_len` to a 4‑byte boundary.
    pub _pad1: [u8; 3],

    /// Payload length in bytes (not including this header). u32 supports up to
    /// 4 GiB per extent.
    pub data_len: u32,

    /// Padding: align `epoch_mark` to an 8‑byte boundary.
    pub _pad2: [u8; 4],

    /// Global‑GC death timestamp (epoch). Stale extents whose epoch has
    /// passed are eligible for collection.
    pub epoch_mark: u64,

    /// XXH64 checksum of the payload data. 0 = write‑in‑progress.
    pub checksum: u64,
}

// --- Compile‑time assertions for V2 ---

const _: () = assert!(core::mem::size_of::<ExtentHeaderV2>() == 32);
const _: () = assert!(core::mem::align_of::<ExtentHeaderV2>() >= 8);

/// Size of an [`ExtentHeaderV2`] in bytes.
pub const HEADER_SIZE_V2: u64 = 32;

impl ExtentHeaderV2 {
    /// Compute the total extent footprint (header + data, 8‑byte aligned).
    #[inline]
    pub fn header_total(data_len: u64) -> u64 {
        crate::engine::extent::align_up(HEADER_SIZE_V2 + data_len, 8)
    }
}

/// Check whether a byte slice starts with a valid V2 header (magic + version == 1).
#[inline]
pub fn is_v2(header_bytes: &[u8]) -> bool {
    if header_bytes.len() < 5 {
        return false;
    }
    let magic = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
    magic == EXTENT_MAGIC && header_bytes[4] == 1
}

/// Return the header size in bytes for a given extent header version.
///
/// - Version 0 (V1 / original [`ExtentHeader`]): 24 bytes
/// - Version 1 (V2 / [`ExtentHeaderV2`]): 32 bytes
/// - Unknown version: defaults to 32 bytes (conservative V2 fallback)
#[inline]
pub const fn header_size_for_version(version: u8) -> u64 {
    match version {
        0 => 24,
        _ => 32,
    }
}

// ---------------------------------------------------------------------------
// FreeListHeader — bump-allocator metadata for the Free List region (64 bytes)
// ---------------------------------------------------------------------------

/// Header at the start of the Free List region.
/// Clients CAS on `bump_offset` to atomically reserve extent space.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct FreeListHeader {
    /// Monotonic bump allocator offset.
    /// Initialized to 0; advanced by extent_total(data_len) on each allocation.
    pub bump_offset: u64,
    /// Padding to fill a full cache line (64 bytes total).
    pub _pad: [u8; 56],
}

// Safety: FreeListHeader is composed entirely of Pod/Zeroable primitives
// (u64 and [u8; 56]), has no padding bytes, and all bit patterns are valid.
unsafe impl Pod for FreeListHeader {}
unsafe impl Zeroable for FreeListHeader {}

// Compile-time size check
const _: () = assert!(std::mem::size_of::<FreeListHeader>() == 64);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a zero‑initialized (idle, alive, Inline) bucket.
    fn zero_bucket() -> HashBucket {
        HashBucket {
            lock_version: 0,
            key_hash: 0,
            key_or_digest: [0u8; 16],
            body: [0u8; 32],
        }
    }

    // -----------------------------------------------------------------------
    // Bitfield read tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bitfield_idle_inline() {
        let b = zero_bucket();
        assert!(!b.is_locked());
        assert!(!b.is_tombstone());
        assert!(b.is_inline());
        assert!(!b.is_extent());
        assert_eq!(b.version(), 0);
        assert_eq!(b.lease_ts(), 0);
    }

    #[test]
    fn test_bitfield_locked_inline() {
        let mut b = zero_bucket();
        b.lock_version = 0x01; // locked, Inline
        assert!(b.is_locked());
        assert!(!b.is_tombstone());
        assert!(b.is_inline());
        assert!(!b.is_extent());
    }

    #[test]
    fn test_bitfield_tombstone() {
        let mut b = zero_bucket();
        b.lock_version = 0x02; // tombstone
        assert!(!b.is_locked());
        assert!(b.is_tombstone());
        // Mode is still Inline (bit2 == 0)
        assert!(b.is_inline());
        assert!(!b.is_extent());
    }

    #[test]
    fn test_bitfield_extent_idle() {
        let mut b = zero_bucket();
        b.lock_version = 0x04; // mode=Extent, not locked, not tombstone
        assert!(!b.is_locked());
        assert!(!b.is_tombstone());
        assert!(!b.is_inline());
        assert!(b.is_extent());
    }

    #[test]
    fn test_bitfield_locked_extent() {
        let mut b = zero_bucket();
        b.lock_version = 0x05; // locked + Extent
        assert!(b.is_locked());
        assert!(!b.is_tombstone());
        assert!(!b.is_inline());
        assert!(b.is_extent());
    }

    #[test]
    fn test_version_and_lease() {
        let mut b = zero_bucket();
        b.lock_version = (42u64 << 32) | (1234u64 << 8) | 0x01;
        assert_eq!(b.version(), 42);
        assert_eq!(b.lease_ts(), 1234);
        assert!(b.is_locked());
    }

    // -----------------------------------------------------------------------
    // set_locked / unlock_bucket / mark_tombstone
    // -----------------------------------------------------------------------

    #[test]
    fn test_set_locked_inline() {
        let mut b = zero_bucket();
        b.lock_version = 5u64 << 32; // version 5, unlocked, idle
        b.set_locked(999, BucketMode::Inline);
        assert!(b.is_locked());
        assert!(b.is_inline());
        assert_eq!(b.version(), 5);
        assert_eq!(b.lease_ts(), 999);
    }

    #[test]
    fn test_set_locked_extent() {
        let mut b = zero_bucket();
        b.lock_version = 3u64 << 32;
        b.set_locked(500, BucketMode::Extent);
        assert!(b.is_locked());
        assert!(b.is_extent());
        assert_eq!(b.version(), 3);
        assert_eq!(b.lease_ts(), 500);
    }

    #[test]
    fn test_set_locked_clamps_lease_24bit() {
        let mut b = zero_bucket();
        b.set_locked(0xFF_FFFF + 10, BucketMode::Inline); // exceeds 24 bits
                                                          // Only lower 24 bits should be stored
        assert_eq!(b.lease_ts(), (0xFF_FFFF + 10) & 0xFF_FFFF);
    }

    #[test]
    fn test_unlock_inline() {
        let mut b = zero_bucket();
        b.lock_version = (7u64 << 32) | (100u64 << 8) | 0x01; // locked inline
        b.unlock_bucket(8);
        assert!(!b.is_locked());
        assert!(b.is_inline());
        assert_eq!(b.version(), 8);
        assert_eq!(b.lease_ts(), 0);
    }

    #[test]
    fn test_unlock_extent_preserves_mode() {
        let mut b = zero_bucket();
        b.lock_version = (2u64 << 32) | 0x05; // locked extent
        b.unlock_bucket(3);
        assert!(!b.is_locked());
        assert!(b.is_extent());
        assert_eq!(b.version(), 3);
    }

    #[test]
    fn test_mark_tombstone_from_locked() {
        let mut b = zero_bucket();
        b.lock_version = (10u64 << 32) | (200u64 << 8) | 0x01; // locked inline
        b.mark_tombstone();
        assert!(!b.is_locked());
        assert!(b.is_tombstone());
        // version preserved
        assert_eq!(b.version(), 10);
        // lease cleared
        assert_eq!(b.lease_ts(), 0);
    }

    #[test]
    fn test_mark_tombstone_from_idle() {
        let mut b = zero_bucket();
        b.lock_version = 1u64 << 32; // unlocked, idle, Inline
        b.mark_tombstone();
        assert!(!b.is_locked());
        assert!(b.is_tombstone());
        assert_eq!(b.version(), 1);
    }

    // -----------------------------------------------------------------------
    // is_expired
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_expired_true() {
        let mut b = zero_bucket();
        b.lock_version = (0u64 << 32) | (0u64 << 8) | 0x01; // locked, lease_ts=0
                                                            // now=200, timeout=100 → 200-0 >= 100
        assert!(b.is_expired(200, 100));
    }

    #[test]
    fn test_is_expired_false_still_valid() {
        let mut b = zero_bucket();
        b.lock_version = (0u64 << 32) | (100u64 << 8) | 0x01; // locked, lease_ts=100
                                                              // now=150, timeout=100 → 150-100=50 < 100
        assert!(!b.is_expired(150, 100));
    }

    #[test]
    fn test_is_expired_false_unlocked() {
        let b = zero_bucket();
        // Not locked, so never expired
        assert!(!b.is_expired(999, 1));
    }

    #[test]
    fn test_is_expired_exact_boundary() {
        let mut b = zero_bucket();
        b.lock_version = (0u64 << 32) | (50u64 << 8) | 0x01; // locked, lease_ts=50
                                                             // now=150, timeout=100 → 150-50=100 == 100 → expired
        assert!(b.is_expired(150, 100));
    }

    #[test]
    fn test_is_expired_wrapping() {
        // lease_ts is only 24 bits, but `now` is a full u32.  When `now`
        // wraps around (e.g. it is smaller than the lease timestamp),
        // `wrapping_sub` correctly handles the overflow.
        let mut b = zero_bucket();
        // lease_ts at the max 24‑bit value (16_777_215)
        b.lock_version = (0u64 << 32) | ((0xFF_FFFFu64) << 8) | 0x01;

        // now = 10, timeout = 20
        // 10.wrapping_sub(16_777_215) wraps to ~4_294_950_091, which >= 20
        assert!(b.is_expired(10, 20));
    }

    // -----------------------------------------------------------------------
    // ExtentRef round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_extent_ref_round_trip() {
        let mut b = zero_bucket();
        b.set_extent_ref(0xDEAD_BEEF_CAFE, 65536);
        let (off, len) = b.extent_ref();
        assert_eq!(off, 0xDEAD_BEEF_CAFE);
        assert_eq!(len, 65536);
    }

    #[test]
    fn test_extent_ref_zero() {
        let mut b = zero_bucket();
        b.set_extent_ref(0, 0);
        let (off, len) = b.extent_ref();
        assert_eq!(off, 0);
        assert_eq!(len, 0);
    }

    #[test]
    fn test_extent_ref_max() {
        let mut b = zero_bucket();
        b.set_extent_ref(u64::MAX, u64::MAX);
        let (off, len) = b.extent_ref();
        assert_eq!(off, u64::MAX);
        assert_eq!(len, u64::MAX);
    }

    // -----------------------------------------------------------------------
    // Inline value round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_inline_value_round_trip() {
        let mut b = zero_bucket();
        let val: [u8; 32] = [0xAA; 32];
        b.set_inline_value(&val);
        assert_eq!(b.inline_value(), &val);
    }

    #[test]
    fn test_inline_value_partial() {
        let mut b = zero_bucket();
        let mut val = [0u8; 32];
        val[0] = b'H';
        val[1] = b'i';
        b.set_inline_value(&val);
        assert_eq!(b.inline_value()[0], b'H');
        assert_eq!(b.inline_value()[1], b'i');
        // The rest should be zero
        assert!(b.inline_value()[2..].iter().all(|&b| b == 0));
    }

    // -----------------------------------------------------------------------
    // is_empty
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_empty_fresh() {
        let b = zero_bucket();
        assert!(b.is_empty());
    }

    #[test]
    fn test_is_empty_with_hash() {
        let mut b = zero_bucket();
        b.key_hash = 12345;
        assert!(!b.is_empty());
    }

    #[test]
    fn test_is_empty_tombstone_not_empty() {
        let mut b = zero_bucket();
        b.lock_version = 0x02; // tombstone, key_hash==0
        assert!(!b.is_empty()); // tombstone is not empty
    }

    #[test]
    fn test_is_empty_locked_but_zero_hash() {
        // A locked bucket with key_hash==0 is still "empty" (insertion
        // in‑flight).
        let mut b = zero_bucket();
        b.lock_version = 0x01; // locked, Inline
        assert!(b.is_empty());
    }

    // -----------------------------------------------------------------------
    // matches_key
    // -----------------------------------------------------------------------

    #[test]
    fn test_matches_key_exact() {
        let mut b = zero_bucket();
        b.key_hash = 0xABCD;
        b.key_or_digest = *b"hello world 1234"; // 16 bytes exactly
        let digest = *b"hello world 1234";
        assert!(b.matches_key(0xABCD, &digest));
    }

    #[test]
    fn test_matches_key_wrong_hash() {
        let mut b = zero_bucket();
        b.key_hash = 0xABCD;
        let digest = *b"hello world 1234";
        b.key_or_digest = digest;
        assert!(!b.matches_key(0x9999, &digest));
    }

    #[test]
    fn test_matches_key_wrong_digest() {
        let mut b = zero_bucket();
        b.key_hash = 0xABCD;
        b.key_or_digest = *b"hello world 1234";
        assert!(!b.matches_key(0xABCD, b"wrong digest!!  "));
    }

    // -----------------------------------------------------------------------
    // Size / alignment assertions (re‑checked at runtime for good measure)
    // -----------------------------------------------------------------------

    #[test]
    fn test_hash_bucket_size() {
        assert_eq!(core::mem::size_of::<HashBucket>(), 64);
    }

    #[test]
    fn test_hash_bucket_align() {
        assert_eq!(core::mem::align_of::<HashBucket>(), 64);
    }

    #[test]
    fn test_extent_header_size() {
        assert_eq!(core::mem::size_of::<ExtentHeader>(), 24);
    }

    #[test]
    fn test_extent_header_v2_size() {
        assert_eq!(core::mem::size_of::<ExtentHeaderV2>(), 32);
    }

    #[test]
    fn test_header_size_for_version() {
        assert_eq!(header_size_for_version(0), 24); // V1
        assert_eq!(header_size_for_version(1), 32); // V2
        assert_eq!(header_size_for_version(2), 32); // Unknown → V2 fallback
        assert_eq!(header_size_for_version(255), 32); // Unknown max
    }

    // -----------------------------------------------------------------------
    // FreeListHeader tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_free_list_header_default_is_zero() {
        let h = FreeListHeader {
            bump_offset: 0,
            _pad: [0u8; 56],
        };
        assert_eq!(h.bump_offset, 0);
        // Verify zeroed padding bytes
        assert!(h._pad.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_free_list_header_size() {
        assert_eq!(core::mem::size_of::<FreeListHeader>(), 64);
    }

    #[test]
    fn test_free_list_header_is_pod() {
        // Must compile: Pod types round-trip through bytemuck
        let h = FreeListHeader {
            bump_offset: 42,
            _pad: [0u8; 56],
        };
        let bytes = bytemuck::bytes_of(&h);
        let h2: &FreeListHeader = bytemuck::from_bytes(bytes);
        assert_eq!(h2.bump_offset, 42);
    }
}
