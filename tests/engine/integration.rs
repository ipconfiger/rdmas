use std::sync::Arc;
use std::thread;

use rdmas::engine::bootstrap::BootstrappedEngine;
use rdmas::engine::cuckoo::CuckooTable;
use rdmas::engine::extent::LargeObjectRegion;
use rdmas::engine::layout::{BucketMode, HashedKey};

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
// Test 1: Single-threaded Inline KV operations
// ============================================================================

#[test]
fn test_inline_insert_lookup_delete() {
    let mut table = CuckooTable::new(1024, 16);

    // Insert 100 Inline keys
    for i in 0..100u64 {
        let key = format!("key_{}", i);
        let hk = hash_key(&key);
        let value = i.to_le_bytes(); // 8 bytes, fits Inline
        table.insert(&hk, &value, BucketMode::Inline).unwrap();
    }

    // Lookup all 100 keys
    for i in 0..100u64 {
        let key = format!("key_{}", i);
        let hk = hash_key(&key);
        let result = table.lookup(&hk).unwrap();
        let stored_val = u64::from_le_bytes(result.value[0..8].try_into().unwrap());
        assert_eq!(stored_val, i, "Key {} lookup mismatch", i);
    }

    // Delete half
    for i in 0..50u64 {
        let key = format!("key_{}", i);
        let hk = hash_key(&key);
        assert!(table.delete(&hk));
    }

    // Deleted keys not found
    for i in 0..50u64 {
        let key = format!("key_{}", i);
        let hk = hash_key(&key);
        assert!(table.lookup(&hk).is_none());
    }

    // Remaining keys still found
    for i in 50..100u64 {
        let key = format!("key_{}", i);
        let hk = hash_key(&key);
        assert!(table.lookup(&hk).is_some());
    }
}

// ============================================================================
// Test 2: Extent operations (large values)
// ============================================================================

#[test]
fn test_extent_insert_read() {
    let mut table = CuckooTable::new(256, 16);
    let mut region = LargeObjectRegion::new(1024 * 1024);

    let data = vec![0xAAu8; 1024]; // 1KB data, too large for Inline
    let hk = hash_key("large_key");
    let offset = region.allocate(&data).unwrap();
    table.insert_extent(&hk, offset, data.len() as u64).unwrap();

    let result = table.lookup(&hk).unwrap();
    let stored = region.read(result.extent_offset).unwrap();
    assert_eq!(stored, data);
}

// ============================================================================
// Test 3: Concurrency — multi-threaded insert/lookup (8 threads × 1000 keys)
// ============================================================================

#[test]
fn test_concurrent_insert_lookup() {
    use std::sync::Mutex;

    let table = Arc::new(Mutex::new(CuckooTable::new(65536, 16)));
    let num_threads = 8;
    let keys_per_thread = 1000u64;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let table = table.clone();
            thread::spawn(move || {
                for i in 0..keys_per_thread {
                    let key = format!("t{}_k{}", t, i);
                    let hk = hash_key(&key);
                    let value = ((t as u64) << 32 | i).to_le_bytes();
                    let mut tbl = table.lock().unwrap();
                    tbl.insert(&hk, &value, BucketMode::Inline).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all keys
    let tbl = table.lock().unwrap();
    for t in 0..num_threads {
        for i in 0..keys_per_thread {
            let key = format!("t{}_k{}", t, i);
            let hk = hash_key(&key);
            assert!(tbl.lookup(&hk).is_some(), "Missing key: {}", key);
        }
    }
}

// ============================================================================
// Test 4: Stress — 100K KV insert/lookup
// ============================================================================

#[test]
fn test_stress_100k() {
    let mut table = CuckooTable::new(1 << 20, 16); // 1048576 buckets, >10x headroom
    let count = 100_000u64;

    // Insert
    for i in 0..count {
        let key = format!("s_{}", i);
        let hk = hash_key(&key);
        let value = i.to_le_bytes();
        table
            .insert(&hk, &value, BucketMode::Inline)
            .expect(&format!("Insert failed at {}", i));
    }

    // Verify
    for i in 0..count {
        let key = format!("s_{}", i);
        let hk = hash_key(&key);
        let result = table.lookup(&hk).expect(&format!("Missing key {}", i));
        let v = u64::from_le_bytes(result.value[0..8].try_into().unwrap());
        assert_eq!(v, i);
    }
}

// ============================================================================
// Test 5: TableFull at MAX_KICK
// ============================================================================

#[test]
fn test_table_full_triggers_when_buckets_saturated() {
    // Use a very small table — 8 buckets
    let mut table = CuckooTable::new(8, 4); // Small MAX_KICK to trigger early
    let mut inserted = 0u64;

    for i in 0..1000u64 {
        let key = format!("tf_{}", i);
        let hk = hash_key(&key);
        let value = i.to_le_bytes();
        match table.insert(&hk, &value, BucketMode::Inline) {
            Ok(()) => inserted += 1,
            Err(_) => break, // TableFull
        }
    }

    // Should not be able to insert more than ~bucket_count keys (8 buckets = 8 keys max)
    assert!(
        inserted <= 16,
        "Inserted {} keys into 8-bucket table",
        inserted
    );
}

// ============================================================================
// Test 6: Extent allocation + GC sweep
// ============================================================================

#[test]
fn test_extent_gc_sweep() {
    let mut region = LargeObjectRegion::new(64 * 1024);

    // Allocate 4 extents
    let data1k = vec![1u8; 1024];
    let o1 = region.allocate(&data1k).unwrap();
    let o2 = region.allocate(&data1k).unwrap();
    let o3 = region.allocate(&data1k).unwrap();

    // Mark o1 and o3 for GC
    region.mark_for_gc(o1, 10).unwrap();
    region.mark_for_gc(o3, 10).unwrap();

    // Sweep with min_active_epoch = 20 (all marked extents should be freed)
    let freed = region.sweep(20);
    assert_eq!(freed, 2);

    // o1 and o3 should be freed (magic zeroed)
    assert!(region.read(o1).is_none());
    assert!(region.read(o3).is_none());

    // o2 should still be readable
    assert!(region.read(o2).is_some());
}

// ============================================================================
// Test 7: Slab allocator integration with engine (T9-C)
// ============================================================================

#[test]
fn test_slab_with_engine() {
    // Create engine with slab support: 64-byte chunks, 10 total chunks.
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        64,   // buckets
        1024, // large object region
        16,   // max_kick
        64,   // slab chunk size
        640,  // slab region (10 chunks × 64 bytes)
    );

    assert_eq!(engine.slab_chunk_count(), 10);

    // Allocate chunks and verify indices are sequential
    let c0 = engine.slab.allocate_chunk().unwrap();
    let c1 = engine.slab.allocate_chunk().unwrap();
    let c2 = engine.slab.allocate_chunk().unwrap();

    assert_eq!(c0, 0);
    assert_eq!(c1, 1);
    assert_eq!(c2, 2);

    // Verify offsets
    assert_eq!(engine.slab.chunk_offset(c0), 0);
    assert_eq!(engine.slab.chunk_offset(c1), 64);
    assert_eq!(engine.slab.chunk_offset(c2), 128);

    // Free a chunk and re-allocate
    engine.slab.free_chunk(c1);
    let c3 = engine.slab.allocate_chunk().unwrap();
    assert_eq!(c3, 1, "should reuse freed chunk index 1");

    // Verify stats
    let stats = engine.stats();
    assert_eq!(stats.slab_total_chunks, 10);
    assert_eq!(stats.slab_allocated_chunks, 3);
    assert_eq!(stats.slab_free_chunks, 7);
}

#[test]
fn test_slab_full_engine_capacity() {
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        64, 1024, 16, 128,  // chunk size
        1280, // 10 chunks
    );

    assert_eq!(engine.slab_chunk_count(), 10);
    assert_eq!(engine.slab.capacity(), 1280);

    // Allocate all 10 chunks
    for i in 0..10 {
        let idx = engine.slab.allocate_chunk().unwrap();
        assert_eq!(idx, i);
    }

    // Should be full
    assert!(engine.slab.allocate_chunk().is_none());

    let stats = engine.stats();
    assert_eq!(stats.slab_allocated_chunks, 10);
    assert_eq!(stats.slab_free_chunks, 0);
}

#[test]
fn test_slab_write_and_read_within_chunks() {
    let mut engine = BootstrappedEngine::bootstrap_with_slab(
        64,
        1024,
        16,
        256,     // 256-byte chunks
        256 * 4, // 4 chunks
    );

    // Allocate all 4 chunks
    let chunks: Vec<u64> = (0..4)
        .map(|_| engine.slab.allocate_chunk().unwrap())
        .collect();

    // Each chunk offset should be chunk_index * 256
    for (i, &chunk_idx) in chunks.iter().enumerate() {
        let expected_offset = (i as u64) * 256;
        assert_eq!(engine.slab.chunk_offset(chunk_idx), expected_offset);
    }
}
