//! Fault injection: simulating resource exhaustion and recovery scenarios.

use std::sync::{Arc, Mutex};
use std::thread;

use rdmas::engine::cuckoo::{CuckooError, CuckooTable};
use rdmas::engine::extent::LargeObjectRegion;
use rdmas::engine::layout::BucketMode;

use super::hash_key;

// --- test_table_full_recovery --------------------------------------------

#[test]
fn test_table_full_recovery() {
    // Use a tiny table (8 buckets) so it fills quickly.
    let mut table = CuckooTable::new(8, 4);

    // Fill the table to capacity.
    let mut inserted = 0u64;
    let mut last_failure = false;

    for i in 0..1000u64 {
        let key = format!("fill_{}", i);
        let hk = hash_key(&key);
        let val = i.to_le_bytes();
        match table.insert(&hk, &val, BucketMode::Inline) {
            Ok(()) => inserted += 1,
            Err(CuckooError::TableFull) => {
                last_failure = true;
                break;
            }
            Err(_) => unreachable!(),
        }
    }

    assert!(last_failure, "Table should have filled up");
    assert!(inserted > 0, "Should have inserted some keys before filling");

    // Free space by deleting a few entries.
    // We know at least the first key was inserted, so delete it.
    for i in 0..inserted {
        if i >= inserted {
            break;
        }
        let key = format!("fill_{}", i);
        let hk = hash_key(&key);
        if table.delete(&hk) {
            // After deletion, a new write should succeed.
            let new_key = format!("recovery_{}", i);
            let new_hk = hash_key(&new_key);
            let val = (1000 + i).to_le_bytes();
            assert!(
                table.insert(&new_hk, &val, BucketMode::Inline).is_ok(),
                "Should be able to write after freeing space (deleted fill_{})",
                i,
            );
            break;
        }
    }
}

// --- test_write_after_read_consistency -----------------------------------

#[test]
fn test_write_after_read_consistency() {
    let mut table = CuckooTable::new(256, 16);
    let key = "consistency_key";

    // Write → read → verify
    let hk = hash_key(key);
    let v1 = [0xAAu8; 8];
    table.insert(&hk, &v1, BucketMode::Inline).unwrap();
    let res = table.lookup(&hk).unwrap();
    assert_eq!(&res.value[..8], &v1[..]);

    // Overwrite → read → verify new value
    let v2 = [0xBBu8; 8];
    table.insert(&hk, &v2, BucketMode::Inline).unwrap();
    let res = table.lookup(&hk).unwrap();
    assert_eq!(&res.value[..8], &v2[..]);

    // Delete → read → verify None
    assert!(table.delete(&hk));
    assert!(table.lookup(&hk).is_none());

    // Concurrent writer + reader on same key: reader sees either old or new,
    // never corrupt.
    let table = Arc::new(Mutex::new(CuckooTable::new(64, 16)));
    let shared_key = "concurrent_consistency";
    let known_patterns: Arc<Mutex<Vec<[u8; 8]>>> = Arc::new(Mutex::new(Vec::new()));

    // Writer: repeatedly overwrite the key with incrementing values.
    // IMPORTANT: push to known_patterns *before* inserting so the reader
    // never sees a value that isn't yet in the known set.
    let wt = {
        let table = table.clone();
        let known = known_patterns.clone();
        thread::spawn(move || {
            for i in 0u64..5000 {
                let val = i.to_le_bytes();
                known.lock().unwrap().push(val);
                {
                    let mut tbl = table.lock().unwrap();
                    let hk = hash_key(shared_key);
                    tbl.insert(&hk, &val, BucketMode::Inline).ok();
                }
            }
        })
    };

    // Reader: repeatedly read, verify value matches one of the known patterns.
    let rt = {
        let table = table.clone();
        let known = known_patterns.clone();
        thread::spawn(move || {
            for _ in 0..10_000 {
                let tbl = table.lock().unwrap();
                let hk = hash_key(shared_key);
                if let Some(res) = tbl.lookup(&hk) {
                    let seen: [u8; 8] = res.value[..8].try_into().unwrap();
                    let known_vals = known.lock().unwrap();
                    // The value must match a pattern that was written.
                    assert!(
                        known_vals.contains(&seen),
                        "Reader saw corrupt value: {:?}, known patterns: {:?}",
                        seen,
                        known_vals.len(),
                    );
                }
                // Ok if key doesn't exist yet (writer hasn't started or
                // it was deleted in a prior operation).
            }
        })
    };

    wt.join().unwrap();
    rt.join().unwrap();
}

// --- test_extent_allocation_exhaustion -----------------------------------

#[test]
fn test_extent_allocation_exhaustion() {
    // Small region: enough for ~4 extents of 512 bytes each.
    // extent_total(512) = align_up(32 + 512, 8) = align_up(544, 8) = 544.
    // 4 * 544 = 2176 → region size = 2200.
    let mut region = LargeObjectRegion::new(2200);

    let data = vec![0xDEu8; 512];

    // Fill the region.
    let mut offsets = Vec::new();
    loop {
        match region.allocate(&data) {
            Some(offset) => offsets.push(offset),
            None => break, // OutOfSpace
        }
    }

    assert!(!offsets.is_empty(), "Should have allocated at least one extent");
    assert!(offsets.len() >= 3, "Should fit several extents before exhaustion");

    // Verify OutOfSpace: next allocation returns None.
    assert!(region.allocate(&data).is_none(), "Region should be full");

    // Free extents via GC: mark first half for collection.
    let mid = offsets.len() / 2;
    for &off in &offsets[..mid] {
        region.mark_for_gc(off, 10).expect("mark_for_gc should succeed");
    }

    // Sweep with min_active_epoch > 10.
    let freed = region.sweep(20);
    assert!(freed > 0, "Should have freed some extents via GC");

    // Now new allocations should succeed.
    let new_off = region.allocate(&data);
    assert!(new_off.is_some(), "Should be able to allocate after GC sweep");
}
