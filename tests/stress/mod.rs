//! Stress test suite for the RDMAS RDMA KV store.
//!
//! All tests run in local simulation mode (no RDMA hardware required).
//! Default durations are CI-friendly (seconds, not hours). Set the
//! `STRESS_DURATION_SECS` environment variable to override.

pub mod stability;
pub mod concurrency;
pub mod fault_injection;
pub mod throughput;

/// Shared helper: hash a string key into a [`HashedKey`].
///
/// Uses xxh64 with two seeds to produce a 64-bit hash and a 16-byte digest,
/// matching the convention in `tests/engine/integration.rs`.
pub(crate) fn hash_key(key: &str) -> rdmas::engine::layout::HashedKey {
    use rdmas::engine::layout::HashedKey;
    use xxhash_rust::xxh64::xxh64;

    let hash = xxh64(key.as_bytes(), 0);
    let mut digest = [0u8; 16];
    let h2 = xxh64(key.as_bytes(), 1);
    digest[0..8].copy_from_slice(&hash.to_le_bytes());
    digest[8..16].copy_from_slice(&h2.to_le_bytes());
    HashedKey { hash, digest }
}
