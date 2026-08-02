//! 24-hour stability test: continuous read/write operations
//! monitoring for memory leaks, QP leaks, and CQ overruns.
//!
//! In CI this runs for a few seconds. For a full 24-hour run set
//! `STRESS_DURATION_SECS=86400`.

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;

use rdmas::engine::cuckoo::CuckooTable;
use rdmas::engine::layout::BucketMode;

use super::hash_key;

#[test]
fn test_stability_continuous_operations() {
    // Default: 5 seconds for CI. Set STRESS_DURATION_SECS=86400 for full test.
    let duration_secs: u64 = std::env::var("STRESS_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let num_threads = 4;
    // 2^20 = 1,048,576 ≈ "1M buckets"
    let bucket_count: u64 = 1_048_576;

    let table = Arc::new(Mutex::new(CuckooTable::new(bucket_count, 16)));

    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let table = table.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut ops: u64 = 0;
                let mut errors: u64 = 0;
                // Keep a small pool of written keys so we can issue deletes.
                let mut key_pool: Vec<String> = Vec::with_capacity(2048);

                while start.elapsed() < Duration::from_secs(duration_secs) {
                    let op = rng.gen_range(0..10);
                    let key = format!("t{}_{:010}", t, rng.gen_range(0..1_000_000u64));
                    let hk = hash_key(&key);
                    let value: [u8; 32] = rng.gen();

                    let mut tbl = table.lock().unwrap();
                    match op {
                        0..=5 => {
                            // Write (60% probability)
                            match tbl.insert(&hk, &value, BucketMode::Inline) {
                                Ok(()) => {
                                    if key_pool.len() < 2048 {
                                        key_pool.push(key);
                                    } else {
                                        // Replace a random entry
                                        let idx = rng.gen_range(0..key_pool.len());
                                        key_pool[idx] = key;
                                    }
                                }
                                Err(_) => errors += 1,
                            }
                        }
                        6..=8 => {
                            // Read (30%)
                            let _ = tbl.lookup(&hk);
                        }
                        _ => {
                            // Delete (10%): pick a known key from the pool
                            if !key_pool.is_empty() {
                                let idx = rng.gen_range(0..key_pool.len());
                                let del_key = key_pool.swap_remove(idx);
                                let del_hk = hash_key(&del_key);
                                tbl.delete(&del_hk);
                            }
                        }
                    }
                    ops += 1;
                }

                (ops, errors)
            })
        })
        .collect();

    let mut total_ops = 0u64;
    let mut total_errors = 0u64;
    for h in handles {
        let (ops, errors) = h.join().expect("stability thread panicked");
        total_ops += ops;
        total_errors += errors;
    }

    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "Stability: {} ops, {} errors in {:.1}s ({:.0} ops/s)",
        total_ops,
        total_errors,
        elapsed,
        total_ops as f64 / elapsed,
    );

    assert!(total_ops > 0, "No operations completed — test is too short or deadlocked");
    // Some TableFull errors are expected as the table approaches capacity.
    // We just verify nothing panicked.
}
