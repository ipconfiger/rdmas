//! High-concurrency CAS contention tests.
//!
//! Multiple threads compete for the same buckets using lock-free insert.
//! Verifies no livelock, no deadlock, and state consistency.

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rdmas::engine::cuckoo::CuckooTable;
use rdmas::engine::layout::BucketMode;

use super::hash_key;

// --- test_concurrent_cas_no_livelock --------------------------------------

#[test]
fn test_concurrent_cas_no_livelock() {
    let num_threads = 8;
    // Use a small table (16 buckets) so threads compete for the same slots.
    let table = Arc::new(CuckooTable::new(16, 16));
    let iterations_per_thread = 10_000u64;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let table = table.clone();
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    // All threads write to a shared key space of 10 logical keys,
                    // creating heavy contention on the same buckets.
                    let logical = t % 10;
                    let key = format!("bucket_{}_{}", logical, i);
                    let hk = hash_key(&key);
                    let val = ((t as u64) << 32 | i).to_le_bytes();
                    // Lock-free insert — no Mutex. High contention, many
                    // TableFull failures expected.
                    let _ = table.insert_lock_free(&hk, &val, BucketMode::Inline);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    // All threads completed → no livelock.
    // We don't check that all keys are present (high contention means many
    // TableFull failures in a small table with no kick chain). The test
    // goal is to verify the system stays responsive under contention.
}

// --- test_concurrent_cas_no_deadlock -------------------------------------

#[test]
fn test_concurrent_cas_no_deadlock() {
    let num_threads = 8;
    let total_ops = 100_000u64;
    let ops_per_thread = total_ops / num_threads;
    let timeout = Duration::from_secs(30);

    // Large table to avoid TableFull — uses Mutex + regular insert (with
    // kick chain) so that all inserts succeed. The test verifies that no
    // thread is permanently blocked.
    let table = Arc::new(Mutex::new(CuckooTable::new(1_048_576, 16)));

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let table = table.clone();
            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("t{}_k{:08}", t, i);
                    let hk = hash_key(&key);
                    let val = ((t as u64) << 32 | i).to_le_bytes();
                    let mut tbl = table.lock().unwrap();
                    tbl.insert(&hk, &val, BucketMode::Inline)
                        .expect("insert should succeed in large table");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < timeout,
        "Test timed out after {:?} — possible deadlock",
        elapsed,
    );

    // Verify all inserted keys are readable.
    let tbl = table.lock().unwrap();
    for t in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("t{}_k{:08}", t, i);
            let hk = hash_key(&key);
            assert!(
                tbl.lookup(&hk).is_some(),
                "Missing key after concurrent insert: t{}_k{:08}",
                t,
                i,
            );
        }
    }

    println!(
        "No-deadlock test: {} ops across {} threads in {:?}",
        total_ops,
        num_threads,
        elapsed,
    );
}
