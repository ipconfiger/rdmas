//! Lock-free concurrency: CAS lock + lease + version-based optimistic reads.
//!
//! T2-C: Two-phase CAS lock with lease-based deadlock prevention.
//!
//! # Two-phase lock protocol (design doc §二.3)
//!
//! **Phase 1 (acquire):**
//! 1. Client reads current `lock_version` from bucket
//! 2. If locked AND not expired → retry
//! 3. Construct `new_lock_version`: version | (now_ms << 8) | mode_bit | LOCKED_BIT
//! 4. CAS(addr, old_lock_version, new_lock_version)
//! 5. If CAS succeeds → hold lock; else → retry
//!
//! **Phase 2 (release):**
//! 1. RDMA_WRITE back: (version+1) | cleared_lease | UNLOCKED | mode_bit
//! 2. Version monotonically increments
//!
//! # Local simulation
//!
//! Since Wave 2 simulates with local memory (`Vec<HashBucket>`), we use
//! `std::sync::atomic::AtomicU64` for CAS on `lock_version`. `HashBucket`
//! is kept as a plain `Pod` type; atomic operations are performed at the
//! call site via the `AtomicBucket` wrapper.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::layout::*;

// ---------------------------------------------------------------------------
// Time utilities
// ---------------------------------------------------------------------------

/// Current time in milliseconds as a wrapping `u32` (for lease timestamps).
///
/// The 24-bit lease timestamp naturally wraps every ~194 days.  All lease
/// comparisons use `wrapping_sub` so this is handled correctly.
#[inline]
pub fn now_ms() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32
}

/// Returns `true` when `old_ts` has expired relative to `now`.
///
/// Lease timestamps are 24-bit values stored in bits [31:8] of the lock
/// word.  Both arguments are masked to 24 bits before comparison so that
/// a full 32-bit `now_ms()` value is compared correctly against a truncated
/// 24-bit lease timestamp.  Wrapping subtraction handles the 24-bit era
/// rollover (every ~194 days).
#[inline]
fn is_lease_expired(old_ts: u32, now: u32, timeout_ms: u32) -> bool {
    let old_24 = old_ts & 0xFF_FFFF;
    let now_24 = now & 0xFF_FFFF;
    now_24.wrapping_sub(old_24) > timeout_ms
}

// ---------------------------------------------------------------------------
// AtomicBucket — atomic access to a HashBucket's lock_version
// ---------------------------------------------------------------------------

/// Convenience wrapper that provides atomic CAS access to a `HashBucket`'s
/// `lock_version` field.
///
/// In local simulation this wraps a `&HashBucket`.  In a real distributed
/// deployment the same operations would target remote RDMA memory.
///
/// # Safety of the pointer cast
///
/// `HashBucket` is `Pod`, `#[repr(C, align(64))]`, with `lock_version` at
/// offset 0.  On x86_64 and aarch64, `AtomicU64` has the same size and
/// alignment as `u64`, so the transmute is sound.  The 64-byte alignment of
/// the bucket guarantees proper alignment for the atomic.
pub struct AtomicBucket<'a> {
    bucket: &'a HashBucket,
}

impl<'a> AtomicBucket<'a> {
    /// Wrap a `HashBucket` reference for atomic access.
    #[inline]
    pub fn new(bucket: &'a HashBucket) -> Self {
        Self { bucket }
    }

    /// Obtain an `&AtomicU64` view of the `lock_version` field.
    #[inline]
    fn lock_atomic(&self) -> &AtomicU64 {
        // SAFETY: HashBucket is Pod + repr(C) with lock_version at offset 0.
        // AtomicU64 layout matches u64 on all Tier-1 platforms (x86_64, aarch64).
        unsafe { &*(&self.bucket.lock_version as *const u64 as *const AtomicU64) }
    }

    /// Atomically load `lock_version` with `Acquire` ordering.
    #[inline]
    pub fn load_lock(&self) -> u64 {
        self.lock_atomic().load(Ordering::Acquire)
    }

    /// Atomically store `lock_version` with `Release` ordering.
    #[inline]
    pub fn store_lock(&self, val: u64) {
        self.lock_atomic().store(val, Ordering::Release);
    }

    /// CAS the `lock_version` field: if the current value equals `old`,
    /// atomically replace it with `new` using `AcqRel` ordering.
    ///
    /// Returns `Ok(previous_value)` on success, `Err(actual_value)` on failure.
    #[inline]
    pub fn cas_lock(&self, old: u64, new: u64) -> Result<u64, u64> {
        self.lock_atomic()
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Lock acquire / release (two-phase protocol)
// ---------------------------------------------------------------------------

/// Attempt to acquire the CAS lock on a bucket (Phase 1).
///
/// # Returns
///
/// - `Ok(old_lock_version)` — lock acquired; the old value is returned for
///   callers that need the previous version number.
/// - `Err(LockError::Locked)` — another client holds the (unexpired) lock.
///
/// # Force takeover
///
/// If the bucket is locked but the lease has expired, this function treats
/// the bucket as unlocked and proceeds with CAS.  The stale holder's lease
/// is overwritten.
pub fn try_acquire_lock(
    bucket: &AtomicBucket,
    mode: BucketMode,
) -> Result<u64, LockError> {
    loop {
        let old_lv = bucket.load_lock();

        // Check if someone else holds the lock.
        if (old_lv & 0x01) != 0 {
            let lease = ((old_lv >> 8) & 0xFF_FFFF) as u32;
            let now = now_ms();
            if !is_lease_expired(lease, now, LEASE_TIMEOUT_MS) {
                return Err(LockError::Locked);
            }
            // Lease expired → force takeover (fall through to CAS).
        }

        // Build the new lock word: preserve version, stamp the lease, set
        // mode and locked bit, clear tombstone.
        let version = (old_lv >> 32) as u64;
        let now = now_ms();
        let lease_bits = ((now as u64) & 0xFF_FFFF) << 8;
        let mode_bit = (mode as u64) << 2;
        let new_lv = (version << 32) | lease_bits | mode_bit | 0x01;

        match bucket.cas_lock(old_lv, new_lv) {
            Ok(prev) => return Ok(prev),
            Err(_) => continue, // CAS failed — another thread raced; retry
        }
    }
}

/// Release the lock on a bucket (Phase 2).
///
/// Bumps `version` by 1 (wrapping), clears the lease timestamp and lock
/// bit, and preserves the mode bit.  Written with `Release` ordering.
pub fn release_lock(bucket: &AtomicBucket, mode: BucketMode, version: u32) {
    let new_version = version.wrapping_add(1);
    let mode_bit = (mode as u64) << 2;
    let new_lv = ((new_version as u64) << 32) | mode_bit;
    bucket.store_lock(new_lv);
}

// ---------------------------------------------------------------------------
// Optimistic read (version-based OCC)
// ---------------------------------------------------------------------------

/// Snapshot of a bucket read without holding a lock.
///
/// After reading data, the caller must call [`verify_read`] to check that
/// no concurrent writer modified the bucket during the read.
#[derive(Debug, Clone)]
pub struct ReadGuard {
    pub version: u32,
    pub key_hash: u64,
    pub key_or_digest: [u8; 16],
    pub body: [u8; 32],
    pub lock_version: u64,
}

/// Attempt an optimistic read of a bucket.
///
/// Returns `Ok(ReadGuard)` with the bucket's contents and version if the
/// bucket is not currently locked.  Returns `Err(LockError::Locked)` if
/// a writer holds the lock.
///
/// The caller must subsequently call [`verify_read`] to confirm that the
/// data was not modified concurrently.
pub fn optimistic_read(bucket: &HashBucket) -> Result<ReadGuard, LockError> {
    if bucket.is_locked() {
        return Err(LockError::Locked);
    }
    Ok(ReadGuard {
        version: bucket.version(),
        key_hash: bucket.key_hash,
        key_or_digest: bucket.key_or_digest,
        body: bucket.body,
        lock_version: bucket.lock_version,
    })
}

/// Verify that the bucket's contents haven't changed since the optimistic read.
///
/// Returns `true` if the bucket is still unlocked and the version matches the
/// guard's recorded version.  Uses an atomic load to avoid torn reads across
/// the lock word.
pub fn verify_read(guard: &ReadGuard, bucket: &AtomicBucket) -> bool {
    let current_lv = bucket.load_lock();
    let locked = (current_lv & 0x01) != 0;
    let version = (current_lv >> 32) as u32;
    !locked && version == guard.version
}

// ---------------------------------------------------------------------------
// Extent checksum verification (T9-D)
// ---------------------------------------------------------------------------

/// Verify payload integrity using [`ExtentHeaderV2`] checksum.
///
/// Returns `true` if the checksum matches (and is non‑zero, meaning the write
/// is complete). A zero checksum indicates a write‑in‑progress that should not
/// be consumed.
pub fn verify_extent_checksum(header: &ExtentHeaderV2, payload: &[u8]) -> bool {
    if header.checksum == 0 {
        return false; // write in progress
    }
    let computed = xxhash_rust::xxh64::xxh64(payload, 0);
    computed == header.checksum
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by lock acquisition and optimistic reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockError {
    /// Another client holds an unexpired lock on the bucket.
    Locked,
    /// The lease has expired (returned by higher-level callers; force
    /// takeover is handled internally by `try_acquire_lock`).
    Expired,
}

// ---------------------------------------------------------------------------
// Lease expiration & force takeover (T4-A)
// ---------------------------------------------------------------------------

/// Result of a force takeover attempt on an expired lock.
#[derive(Debug, PartialEq, Eq)]
pub enum TakeoverResult {
    /// Successfully took over the lock. Contains the new `lock_version`.
    Success { new_version: u64 },
    /// The lock is not expired — another client still holds it.
    NotExpired,
    /// The bucket was not locked at all.
    NotLocked,
}

/// Force-take over a lock that has exceeded its lease timeout.
///
/// # Safety
/// This should only be called after confirming lease expiry.
/// The caller must verify bucket state after takeover:
/// - If the previous holder was mid-operation, the bucket may be in
///   an inconsistent state (e.g., during a kick chain).
/// - Caller should re-read the bucket and validate key_hash/digest.
///
/// # Returns
/// - `TakeoverResult::Success { new_version }` if takeover succeeded.
///   Caller should then repair the bucket (mark as tombstone if the
///   previous operation was a delete, or leave as-is for re-insert).
/// - `TakeoverResult::NotExpired` if the lock is still valid.
/// - `TakeoverResult::NotLocked` if the bucket was not locked.
pub fn force_takeover(
    bucket: &mut HashBucket,
    timeout_ms: u32,
) -> TakeoverResult {
    if !bucket.is_locked() {
        return TakeoverResult::NotLocked;
    }

    let lease = bucket.lease_ts();
    let now = now_ms();

    if !is_lease_expired(lease, now, timeout_ms) {
        return TakeoverResult::NotExpired;
    }

    // Force takeover: unlock the bucket, keeping mode and bumping version
    let version = bucket.version();
    let mode_bit = bucket.lock_version & 0x04; // Preserve mode
    let new_version = (version.wrapping_add(1) as u64) << 32;

    // New lock_version: version+1 | cleared lease | UNLOCKED | mode
    bucket.lock_version = new_version | mode_bit;

    TakeoverResult::Success {
        new_version: bucket.lock_version,
    }
}

/// Repair a bucket after force takeover.
///
/// After a crash, the bucket may be in an inconsistent state:
/// - Client was mid-write → bucket has partial data. Repair by clearing.
/// - Client was mid-delete → bucket should be tombstone. Set tombstone.
/// - Client was mid-kick → bucket displaced; re-insert may be needed.
///
/// This function conservatively clears the bucket so it can be reused.
pub fn repair_after_crash(bucket: &mut HashBucket) {
    // Clear the bucket entirely — the data may be corrupted.
    // The key is lost (must be re-inserted by the application).
    bucket.lock_version = 0;
    bucket.key_hash = 0;
    bucket.key_or_digest = [0u8; 16];
    bucket.body = [0u8; 32];
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a zero-initialized bucket for testing.
    fn zero_bucket() -> HashBucket {
        HashBucket {
            lock_version: 0,
            key_hash: 0,
            key_or_digest: [0u8; 16],
            body: [0u8; 32],
        }
    }

    // -----------------------------------------------------------------------
    // Basic acquire → release → version increment
    // -----------------------------------------------------------------------

    #[test]
    fn test_acquire_release_basic() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        // Acquire
        let old = try_acquire_lock(&ab, BucketMode::Inline).expect("acquire should succeed");
        assert_eq!(old, 0);
        assert!(b.is_locked());
        assert!(b.is_inline());

        // Release with version bump
        let version = (old >> 32) as u32;
        release_lock(&ab, BucketMode::Inline, version);

        assert!(!b.is_locked());
        assert_eq!(b.version(), 1);
        assert!(b.is_inline());
    }

    #[test]
    fn test_version_monotonic() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        for i in 0..5 {
            let old = try_acquire_lock(&ab, BucketMode::Inline).unwrap();
            let version = (old >> 32) as u32;
            assert_eq!(version, i, "version should be {i} on iteration {i}");
            release_lock(&ab, BucketMode::Inline, version);
            assert_eq!(b.version(), i + 1);
        }
    }

    #[test]
    fn test_acquire_release_extent() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        try_acquire_lock(&ab, BucketMode::Extent).unwrap();
        assert!(b.is_locked());
        assert!(b.is_extent());

        release_lock(&ab, BucketMode::Extent, 0);
        assert!(!b.is_locked());
        assert!(b.is_extent());
        assert_eq!(b.version(), 1);
    }

    // -----------------------------------------------------------------------
    // Lock conflict detection
    // -----------------------------------------------------------------------

    #[test]
    fn test_lock_conflict_while_held() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        // First acquire succeeds.
        let old = try_acquire_lock(&ab, BucketMode::Inline).unwrap();
        assert!(b.is_locked());

        // Second acquire on the same (still-locked) bucket must fail.
        let result = try_acquire_lock(&ab, BucketMode::Inline);
        assert!(matches!(result, Err(LockError::Locked)));

        // Release; second acquire should now succeed.
        let version = (old >> 32) as u32;
        release_lock(&ab, BucketMode::Inline, version);

        let result2 = try_acquire_lock(&ab, BucketMode::Inline);
        assert!(result2.is_ok());
    }

    // -----------------------------------------------------------------------
    // Optimistic read → verify pattern
    // -----------------------------------------------------------------------

    #[test]
    fn test_optimistic_read_unlocked() {
        let mut b = zero_bucket();
        b.body[0..5].copy_from_slice(b"hello");

        let guard = optimistic_read(&b).expect("read should succeed on unlocked bucket");
        assert_eq!(guard.version, 0);
        assert_eq!(&guard.body[0..5], b"hello");

        let ab = AtomicBucket::new(&b);
        assert!(verify_read(&guard, &ab));
    }

    #[test]
    fn test_optimistic_read_locked_fails() {
        let mut b = zero_bucket();
        b.lock_version = 0x01; // locked, Inline

        let result = optimistic_read(&b);
        assert!(matches!(result, Err(LockError::Locked)));
    }

    #[test]
    fn test_optimistic_read_detects_concurrent_write() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        let guard = optimistic_read(&b).expect("read should succeed");

        // Simulate a concurrent write: acquire, modify, release.
        let old = try_acquire_lock(&ab, BucketMode::Inline).unwrap();
        let version = (old >> 32) as u32;
        release_lock(&ab, BucketMode::Inline, version);

        // verify_read must detect the version change.
        assert!(!verify_read(&guard, &ab));
    }

    // -----------------------------------------------------------------------
    // Lease expiration & force takeover
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_lease_expired_basic() {
        // now=200, old_ts=0, timeout=100 → 200-0=200 > 100 → expired
        assert!(is_lease_expired(0, 200, 100));
        // now=150, old_ts=100, timeout=100 → 150-100=50 > 100 → not expired
        assert!(!is_lease_expired(100, 150, 100));
        // Boundary: now=150, old_ts=50, timeout=100 → 150-50=100 > 100 → false (not expired)
        assert!(!is_lease_expired(50, 150, 100));
        // Just past boundary: now=151, old_ts=50, timeout=100 → 151-50=101 > 100 → expired
        assert!(is_lease_expired(50, 151, 100));
    }

    #[test]
    fn test_is_lease_expired_wrapping() {
        // Lease timestamps are 24-bit (0 .. 0xFF_FFFF = 16_777_215).
        // When now wraps past the 24-bit boundary back to a small value
        // while old_ts is near the top of the range, wrapping_sub produces
        // a large positive value, correctly signalling expiry.
        // old_ts = 0xFF_FFFF (near max 24-bit), now = 10, timeout = 50
        // 10.wrapping_sub(0xFF_FFFF) >> 50 → expired
        assert!(is_lease_expired(0xFF_FFFF, 10, 50));
    }

    #[test]
    fn test_force_takeover_after_expiry() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        // Manually set a locked state with a very old lease (timestamp ~0).
        // Lease at timestamp 0, now is > 100ms → expired.
        let old_lv = (0u64 << 32) | (0u64 << 8) | 0x01;
        ab.store_lock(old_lv);

        // try_acquire_lock should detect expiry and succeed (force takeover).
        let result = try_acquire_lock(&ab, BucketMode::Inline);
        assert!(result.is_ok(), "force takeover should succeed on expired lease");

        assert!(b.is_locked());
    }

    #[test]
    fn test_no_takeover_when_lease_valid() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        // Recent lease: lease_ts ~ now_ms() (so not expired).
        let now = now_ms();
        let old_lv = (0u64 << 32) | (((now as u64) & 0xFF_FFFF) << 8) | 0x01;
        ab.store_lock(old_lv);

        let result = try_acquire_lock(&ab, BucketMode::Inline);
        assert!(matches!(result, Err(LockError::Locked)));
    }

    // -----------------------------------------------------------------------
    // AtomicBucket CAS correctness
    // -----------------------------------------------------------------------

    #[test]
    fn test_atomic_bucket_load_store() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);
        assert_eq!(ab.load_lock(), 0);

        ab.store_lock(0xABCD);
        assert_eq!(ab.load_lock(), 0xABCD);
    }

    #[test]
    fn test_atomic_bucket_cas_success() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        let result = ab.cas_lock(0, 42);
        assert_eq!(result, Ok(0));
        assert_eq!(ab.load_lock(), 42);
    }

    #[test]
    fn test_atomic_bucket_cas_failure() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        // Pre-set to something so 0 → 99 fails.
        ab.store_lock(7);
        let result = ab.cas_lock(0, 99);
        assert_eq!(result, Err(7));
        assert_eq!(ab.load_lock(), 7);
    }

    // -----------------------------------------------------------------------
    // Version wrapping
    // -----------------------------------------------------------------------

    #[test]
    fn test_version_wrapping() {
        let b = zero_bucket();
        let ab = AtomicBucket::new(&b);

        // Set version to u32::MAX - 1
        let init_lv = ((u32::MAX as u64) - 1) << 32;
        ab.store_lock(init_lv);

        // Acquire: preserves version (u32::MAX - 1)
        let old = try_acquire_lock(&ab, BucketMode::Inline).unwrap();
        assert_eq!((old >> 32) as u32, u32::MAX - 1);

        // Release: version becomes u32::MAX (MAX - 1 + 1)
        release_lock(&ab, BucketMode::Inline, u32::MAX - 1);
        assert_eq!(b.version(), u32::MAX);

        // Acquire again: version u32::MAX
        let old2 = try_acquire_lock(&ab, BucketMode::Inline).unwrap();
        assert_eq!((old2 >> 32) as u32, u32::MAX);

        // Release: wraps to 0
        release_lock(&ab, BucketMode::Inline, u32::MAX);
        assert_eq!(b.version(), 0);
    }

    // -----------------------------------------------------------------------
    // T4-A: Lease expiration force takeover
    // -----------------------------------------------------------------------

    #[test]
    fn test_force_takeover_expired_lock() {
        let mut bucket = zero_bucket();
        let now = now_ms();

        // Set an expired lock: lock set, lease in the past
        let old_lease = now.wrapping_sub(LEASE_TIMEOUT_MS + 1);
        bucket.lock_version =
            ((1u64) << 32) | ((old_lease as u64) << 8) | 0x01; // version=1, locked
        // mode = 0 (Inline)

        let result = force_takeover(&mut bucket, LEASE_TIMEOUT_MS);
        match result {
            TakeoverResult::Success { .. } => {
                assert!(!bucket.is_locked(), "Bucket should be unlocked after takeover");
                assert!(bucket.version() > 1, "Version should have incremented");
            }
            _ => panic!("Expected Success, got {:?}", result),
        }
    }

    #[test]
    fn test_force_takeover_not_expired() {
        let mut bucket = zero_bucket();
        let now = now_ms();
        bucket.lock_version =
            ((1u64) << 32) | ((now as u64) << 8) | 0x01; // freshly locked

        let result = force_takeover(&mut bucket, LEASE_TIMEOUT_MS);
        assert_eq!(result, TakeoverResult::NotExpired);
        assert!(bucket.is_locked()); // Still locked
    }

    #[test]
    fn test_force_takeover_not_locked() {
        let mut bucket = zero_bucket();
        let result = force_takeover(&mut bucket, LEASE_TIMEOUT_MS);
        assert_eq!(result, TakeoverResult::NotLocked);
    }

    #[test]
    fn test_takeover_preserves_mode() {
        let mut bucket = zero_bucket();
        let now = now_ms();
        // Extent mode (bit2=1), locked (bit0=1) = 0x05
        let old_lease = now.wrapping_sub(LEASE_TIMEOUT_MS + 1);
        bucket.lock_version =
            ((1u64) << 32) | ((old_lease as u64) << 8) | 0x05;

        let result = force_takeover(&mut bucket, LEASE_TIMEOUT_MS);
        assert!(matches!(result, TakeoverResult::Success { .. }));
        assert!(!bucket.is_locked());
        assert!(bucket.is_extent(), "Extent mode should be preserved");
    }

    #[test]
    fn test_repair_after_crash_clears_bucket() {
        let mut bucket = zero_bucket();
        bucket.key_hash = 0xDEADBEEF;
        bucket.set_inline_value(&[42u8; 32]);

        repair_after_crash(&mut bucket);

        assert_eq!(bucket.key_hash, 0);
        assert!(!bucket.is_locked());
        assert!(!bucket.is_tombstone());
    }
}
