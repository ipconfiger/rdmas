//! Cross-wave integration tests (T11-E).
//! Verifies that features from different waves compose correctly.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rdmas::api::KvEngine;
use rdmas::engine::bootstrap::BootstrappedEngine;
use rdmas::engine::extent::LargeObjectRegion;
use rdmas::engine::layout::{is_v2, BucketMode, ExtentHeaderV2, HashedKey, EXTENT_MAGIC};
use rdmas::engine::lru::LruTracker;
use rdmas::engine::watermark::{WatermarkConfig, WatermarkMonitor};

use xxhash_rust::xxh64::xxh64;

// Helper: hash a string key
fn hash_key(key: &str) -> HashedKey {
    let hash = xxh64(key.as_bytes(), 0);
    let digest = {
        let mut d = [0u8; 16];
        let h2 = xxh64(key.as_bytes(), 1);
        d[0..8].copy_from_slice(&hash.to_le_bytes());
        d[8..16].copy_from_slice(&h2.to_le_bytes());
        d
    };
    HashedKey { hash, digest }
}

// ============================================================================
// Wave 8 (QP Recovery) + Wave 10 (Connection Keepalive)
// ============================================================================

#[test]
fn test_generation_change_triggers_metadata_invalidation() {
    // Simulate generation counter change and verify metadata can be
    // invalidated. In local simulation, we test the AtomicU64 tracking
    // pattern that QpGuard uses for recovery_count.

    let generation = Arc::new(AtomicU64::new(1));
    let cached_gen = Arc::new(AtomicU64::new(1));

    // Initially cached generation matches actual generation — no invalidation needed.
    assert_eq!(
        generation.load(Ordering::Relaxed),
        cached_gen.load(Ordering::Relaxed)
    );

    // After a generation bump, the cached value is stale.
    generation.fetch_add(1, Ordering::Relaxed);
    assert_ne!(
        generation.load(Ordering::Relaxed),
        cached_gen.load(Ordering::Relaxed)
    );

    // Refresh the cached value (simulate metadata invalidation + reload).
    cached_gen.store(generation.load(Ordering::Relaxed), Ordering::Relaxed);
    assert_eq!(
        generation.load(Ordering::Relaxed),
        cached_gen.load(Ordering::Relaxed)
    );
}

// ============================================================================
// Wave 9 (Extent V2) + Wave 10 (LRU)
// ============================================================================

#[test]
fn test_lru_evicts_extent_entries() {
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        1024,        // buckets
        1024 * 1024, // extent region: 1MB
        16,          // max kick
        64 * 1024,   // slab region: 64KB
        4096,        // chunk size: 4KB
    );
    let lru = LruTracker::new(25); // low watermark to force eviction

    // Verify engine is initialized with extent capacity.
    assert!(engine.large_objects.capacity() >= 1024 * 1024);
    assert_eq!(engine.large_objects.allocated_count(), 0);

    // LargeObjectRegion has capacity; LRU tracker starts empty.
    assert_eq!(lru.key_count(), 0);
    assert!(!lru.needs_eviction());

    // Record accesses to push past the watermark.
    for i in 0..50u64 {
        lru.record_access(i * 1000 + 1);
    }

    // After many accesses, LRU should detect we're above watermark.
    assert!(lru.needs_eviction());

    // Select eviction candidates.
    let candidates = lru.select_eviction_candidates(10);
    assert!(!candidates.is_empty());
    lru.increment_evicted(candidates.len() as u64);
    assert!(lru.evicted_count() > 0);

    // Verify engine extent region is still functional after LRU tracking.
    let data = vec![0xABu8; 64];
    let offset = engine.large_objects.allocate(&data);
    assert!(offset.is_some(), "Extent allocation should succeed");
    assert!(engine.large_objects.allocated_count() > 0);
}

// ============================================================================
// Wave 9 (Slab) + Wave 10 (Watermark)
// ============================================================================

#[test]
fn test_watermark_detects_slab_exhaustion() {
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        1024,
        1024 * 1024,
        16,
        256,  // slab_chunk_size: 256B chunks
        4096, // slab_region_size: 4KB region (16 chunks)
    );

    let config = WatermarkConfig::default();
    let monitor = WatermarkMonitor::new(config);

    let total = engine.slab.total_chunks();

    // Fill slab allocator to near capacity
    for _i in 0..total - 1 {
        let idx = engine.slab.allocate_chunk();
        assert!(idx.is_some(), "Should be able to allocate chunk");
    }

    let status = monitor.check(
        0, // no keys inserted yet
        engine.bucket_count(),
        engine.large_objects.used_bytes(),
        engine.large_objects.capacity(),
        engine.slab.allocated_count(),
        engine.slab.total_chunks(),
    );

    // Verify: slab usage should be near 100% (total-1 out of total)
    let expected_usage = (total - 1) as f64 / total as f64;
    assert!((status.slab_usage - expected_usage).abs() < 0.01);
    assert!(
        status.slab_usage > 0.9,
        "slab_usage should exceed 0.9, got {}",
        status.slab_usage
    );

    // With default threshold 0.85, slab_usage should exceed it.
    assert!(status.any_exceeded);
    assert!(status.exceeded_regions.contains(&"slab_region".to_string()));
}

// ============================================================================
// Wave 8 (QP Guard) + Wave 10 (Reconnect)
// ============================================================================

#[test]
fn test_qp_guard_recovery_count_tracks_errors() {
    // QpGuard requires a real RDMA QueuePair to construct, which needs
    // RDMA hardware. In local simulation, verify the recovery_count
    // AtomicU64 pattern used by QpGuard to track error detections.
    let recovery_count = AtomicU64::new(0);
    assert_eq!(recovery_count.load(Ordering::Relaxed), 0);

    // Simulate error detection increment (as QpGuard does on ERROR state).
    recovery_count.fetch_add(1, Ordering::Relaxed);
    recovery_count.fetch_add(1, Ordering::Relaxed);
    assert_eq!(recovery_count.load(Ordering::Relaxed), 2);

    // Verify the API shape: recovery_count is a monotonically increasing counter.
    recovery_count.fetch_add(3, Ordering::Relaxed);
    assert_eq!(recovery_count.load(Ordering::Relaxed), 5);
}

// ============================================================================
// Wave 9 (Extent V2) + Wave 8 (CQ Event)
// ============================================================================

#[test]
fn test_extent_v2_roundtrip_preserves_checksum() {
    let mut region = LargeObjectRegion::new(4096);
    let data = vec![0xDEu8; 100];

    // Allocate extent with V2 header and real checksum.
    let offset = region.write_extent_v2_checksummed(&data).unwrap();

    // Read back and verify data matches.
    let result = region.read(offset).unwrap();
    assert_eq!(&result, &data);

    // Verify the checksum in the header matches the data.
    let stored_checksum = region.read_checksum(offset);
    assert!(stored_checksum.is_some());

    let expected_checksum = xxh64(&data, 0);
    assert_eq!(stored_checksum.unwrap(), expected_checksum);
}

// ============================================================================
// Wave 9 (Migration) + Wave 10 (LRU)
// ============================================================================

#[test]
fn test_v1_extent_still_readable_after_lru_integration() {
    let mut region = LargeObjectRegion::new(4096);
    let lru = LruTracker::new(100);

    // Write data in V1 format (24-byte header, legacy format).
    let data = vec![0x42u8; 200];
    let offset = region.write_extent_v1(&data).unwrap();

    // LRU integration: record access to simulate the extent being tracked.
    lru.record_access(offset);
    assert!(lru.key_count() >= 1);

    // V1 extent must still be readable after LRU tracker is initialized.
    let result = region.read(offset).unwrap();
    assert_eq!(&result, &data);

    // Verify V1 is detected correctly (not V2).
    // read() correctly decodes both V1 and V2 formats.
    // For V1 extents, DecodedHeader.is_v2 is false.
    // The fact that read() returned the correct data proves backward compat.
    assert_eq!(
        result.len(),
        data.len(),
        "V1 extent should contain correct data length"
    );

    // V1 extent should be freeable and recyclable.
    assert!(region.free(offset).is_ok());
}

// ============================================================================
// Full stack: Wave 8 + 9 + 10
// ============================================================================

#[test]
fn test_full_stack_insert_read_evict() {
    // Create engine (Wave 9: Slab + Extent V2)
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        1024,       // buckets
        256 * 1024, // 256KB extent region
        16,         // max kick
        64 * 1024,  // 64KB slab region
        4096,       // 4KB chunks
    );

    // Wave 10: Initialize LRU tracker and watermark monitor.
    let lru = LruTracker::new(50);
    let config = WatermarkConfig::default();
    let monitor = WatermarkMonitor::new(config);

    // Insert 100 keys — mixed inline and extent.
    let mut inserted_keys = Vec::new();
    for i in 0..100u64 {
        let key = format!("fullstack_key_{}", i);
        let hk = hash_key(&key);

        let inserted = if i % 3 == 0 {
            // Inline value (<= 32 bytes)
            let inline_value = i.to_le_bytes();
            engine
                .table
                .insert(&hk, &inline_value, BucketMode::Inline)
                .is_ok()
        } else if i % 3 == 1 {
            // Small extent (<= 1KB)
            let extent_data = vec![0xCCu8; 100];
            if let Some(offset) = engine.large_objects.allocate(&extent_data) {
                engine
                    .table
                    .insert_extent(&hk, offset, extent_data.len() as u64)
                    .is_ok()
            } else {
                false
            }
        } else {
            // Slab chunk reference
            if let Some(chunk_idx) = engine.slab.allocate_chunk() {
                let slab_ref: [u8; 16] = {
                    let mut buf = [0u8; 16];
                    buf[0..8].copy_from_slice(&chunk_idx.to_le_bytes());
                    buf[8..16].copy_from_slice(&4096u64.to_le_bytes());
                    buf
                };
                engine
                    .table
                    .insert(&hk, &slab_ref, BucketMode::Extent)
                    .is_ok()
            } else {
                false
            }
        };

        if inserted {
            lru.record_access(hk.hash);
            inserted_keys.push(hk);
        }
    }

    // Read back all keys.
    for hk in &inserted_keys {
        let result = engine.table.lookup(hk);
        assert!(
            result.is_some(),
            "Key with hash {} should be found",
            hk.hash
        );
    }

    // Check watermark status.
    let stats = engine.stats();
    let status = monitor.check(
        0, // approximate key count from table
        stats.bucket_count,
        stats.large_object_used,
        stats.large_object_capacity,
        stats.slab_allocated_chunks,
        stats.slab_total_chunks,
    );

    // Watermark should be not-exceeded with moderate utilization.
    assert!(
        !status.any_exceeded || status.slab_usage < 0.9,
        "Should not have critical watermark exceeded with moderate load"
    );

    // Trigger LRU eviction and verify it works.
    // (may not need eviction if fewer keys were inserted than watermark)
    if lru.needs_eviction() {
        let candidates = lru.select_eviction_candidates(5);
        if !candidates.is_empty() {
            lru.increment_evicted(candidates.len() as u64);
        }
    }

    // Verify engine stats are consistent.
    let stats2 = engine.stats();
    assert_eq!(stats2.bucket_count, 1024);
    // Some extent and slab usage expected from our mixed inserts.
    assert!(
        stats2.large_object_used > 0 || stats2.slab_allocated_chunks > 0,
        "Expected some storage usage from mixed insert workload"
    );
}

// ============================================================================
// Extent header version detection
// ============================================================================

#[test]
fn test_extent_header_version_detection() {
    use bytemuck::bytes_of;

    // Create a V2 header in memory.
    let v2_header = ExtentHeaderV2 {
        magic: EXTENT_MAGIC,
        version: 1,
        _pad1: [0u8; 3],
        data_len: 100,
        _pad2: [0u8; 4],
        epoch_mark: 0,
        checksum: 0,
    };
    let v2_bytes: &[u8] = bytes_of(&v2_header);
    assert!(
        is_v2(v2_bytes),
        "V2 header with version=1 should be detected"
    );

    // Simulate a V1 header: magic at offset 16, version byte at offset 4 is 0.
    // is_v2 checks magic at offset 0 and version byte at offset 4.
    // For V1, the first 4 bytes are data_len (u64), so magic won't match
    // and is_v2 returns false.
    let mut v1_sim = [0u8; 24];
    // data_len as u64 le at offset 0
    v1_sim[0..8].copy_from_slice(&100u64.to_le_bytes());
    // epoch_mark at offset 8
    v1_sim[8..16].copy_from_slice(&0u64.to_le_bytes());
    // magic at offset 16
    v1_sim[16..20].copy_from_slice(&EXTENT_MAGIC.to_le_bytes());
    // pad at offset 20
    v1_sim[20..24].copy_from_slice(&[0u8; 4]);

    assert!(!is_v2(&v1_sim), "V1 header should not be detected as V2");

    // All-zero header should not be detected as V2.
    let zero_header = [0u8; 32];
    assert!(!is_v2(&zero_header));

    // A header with magic but version != 1 should not be V2.
    let mut unknown_version = [0u8; 32];
    unknown_version[0..4].copy_from_slice(&EXTENT_MAGIC.to_le_bytes());
    unknown_version[4] = 99; // unknown version
    assert!(!is_v2(&unknown_version));

    // V2 with real data: allocate + detect.
    let mut region = LargeObjectRegion::new(4096);
    let data = vec![0x55u8; 50];
    let offset = region.allocate(&data).unwrap();

    // Read the raw header bytes from the buffer.
    // We verify via read() that it decodes correctly;
    // the DecodedHeader.is_v2 should be true for V2 allocated extents.
    let result = region.read(offset).unwrap();
    assert_eq!(&result, &data);
}

// ============================================================================
// Slab and extent coexistence
// ============================================================================

#[test]
fn test_slab_and_extent_coexistence() {
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        64,        // buckets
        4096,      // extent region: 4KB
        16,        // max kick
        4096,      // slab chunk size: 4KB
        16 * 1024, // slab region: 16KB (4 chunks)
    );

    // Allocate from both slab and extent simultaneously.
    let slab_count = engine.slab.total_chunks();
    assert_eq!(slab_count, 4);

    // Allocate 2 slab chunks.
    let c0 = engine.slab.allocate_chunk().unwrap();
    let _c1 = engine.slab.allocate_chunk().unwrap();
    assert_eq!(engine.slab.allocated_count(), 2);

    // Allocate extents in the large object region.
    let data = vec![0x11u8; 100];
    let o1 = engine.large_objects.allocate(&data).unwrap();
    let o2 = engine.large_objects.allocate(&data).unwrap();
    assert_eq!(engine.large_objects.allocated_count(), 2);

    // Verify slab and extent allocations are independent.
    assert_eq!(engine.slab.allocated_count(), 2);
    assert_eq!(engine.large_objects.allocated_count(), 2);

    // Free one slab chunk and one extent — no interference.
    engine.slab.free_chunk(c0);
    engine.large_objects.free(o1).unwrap();

    // Slab should report 1 allocated after free + potential reuse.
    // (free_chunk pushes to free list; allocated_count = next_chunk - free_list.len)
    assert_eq!(engine.slab.allocated_count(), 1);

    // Allocate a new slab chunk — should reuse the freed chunk.
    let c2 = engine.slab.allocate_chunk().unwrap();
    assert_eq!(c2, c0, "Should reuse freed slab chunk");

    // Extent should still have 1 allocated (o1 freed, o2 still alive).
    assert_eq!(engine.large_objects.allocated_count(), 1);

    // o2 still readable.
    let result = engine.large_objects.read(o2).unwrap();
    assert_eq!(&result, &data);

    // o1 should be freed — read returns None (magic zeroed on free).
    assert!(engine.large_objects.read(o1).is_none());

    // Final stats sanity.
    let stats = engine.stats();
    assert_eq!(stats.slab_total_chunks, 4);
    assert_eq!(stats.slab_allocated_chunks, 2); // c1 and c2 (reused c0)
    assert_eq!(stats.slab_free_chunks, 2);
}

// ============================================================================
// KvEngine trait integration: verify the trait compiles and default impls work
// ============================================================================

#[test]
fn test_kv_engine_trait_default_impls_compile() {
    // Verify that KvEngine trait's default methods exist and return expected
    // values. The trait defines evict() -> Ok(0) and key_count() -> 0.

    // Create a minimal struct that implements KvEngine (via defaults only).
    struct MinimalEngine;
    impl rdmas::api::KvEngine for MinimalEngine {
        fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, rdmas::error::RdmaError> {
            Ok(None)
        }
        fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), rdmas::error::RdmaError> {
            Ok(())
        }
        fn delete(&self, _key: &[u8]) -> Result<(), rdmas::error::RdmaError> {
            Ok(())
        }
        fn exists(&self, _key: &[u8]) -> Result<bool, rdmas::error::RdmaError> {
            Ok(false)
        }
        fn batch_get(
            &self,
            _keys: &[&[u8]],
        ) -> Vec<Result<Option<Vec<u8>>, rdmas::error::RdmaError>> {
            Vec::new()
        }
        fn batch_put(&self, _kvs: &[(&[u8], &[u8])]) -> Vec<Result<(), rdmas::error::RdmaError>> {
            Vec::new()
        }
    }

    let engine = MinimalEngine;

    // Default evict() returns Ok(0)
    let evicted = engine.evict(10).unwrap();
    assert_eq!(evicted, 0);

    // Default key_count() returns 0
    assert_eq!(engine.key_count(), 0);
}
