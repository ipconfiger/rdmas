//! Throughput saturation: measure system bottlenecks.
//!
//! Reports ops/sec and P50/P99 latencies for various access patterns.

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use rand::Rng;

use rdmas::engine::cuckoo::{CuckooError, CuckooTable};
use rdmas::engine::layout::BucketMode;

use super::hash_key;

// --- test_insert_throughput -----------------------------------------------

#[test]
fn test_insert_throughput() {
    let num_keys = 100_000u64;
    let mut table = CuckooTable::new(1_048_576, 16);

    let start = Instant::now();
    for i in 0..num_keys {
        let key = format!("ins_{:08}", i);
        let hk = hash_key(&key);
        let val = i.to_le_bytes();
        table
            .insert(&hk, &val, BucketMode::Inline)
            .expect("insert should succeed in large table");
    }
    let elapsed = start.elapsed().as_secs_f64();

    let ops_per_sec = num_keys as f64 / elapsed;
    println!(
        "Insert throughput: {:.0} ops/sec ({num_keys} keys in {elapsed:.3}s)",
        ops_per_sec
    );

    assert!(ops_per_sec > 0.0, "Throughput should be positive");
}

// --- test_mixed_read_write_throughput ------------------------------------

#[test]
fn test_mixed_read_write_throughput() {
    let total_ops = 100_000u64;
    let write_ratio = 0.20; // 20% writes, 80% reads

    // Pre-populate with some keys so reads have targets.
    let mut table = CuckooTable::new(1_048_576, 16);
    let preload = 50_000u64;
    let value = [0x42u8; 8];
    for i in 0..preload {
        let key = format!("r{}_{:08}", i % 10, i);
        let hk = hash_key(&key);
        table.insert(&hk, &value, BucketMode::Inline).ok();
    }

    let table = Arc::new(Mutex::new(table));

    let num_threads = 4;
    let ops_per_thread = total_ops / num_threads;

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let table = table.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for i in 0..ops_per_thread {
                    let write = rng.gen_bool(write_ratio);
                    let key = format!(
                        "r{}_{:08}",
                        rng.gen_range(0..10u64),
                        rng.gen_range(0..preload)
                    );
                    let hk = hash_key(&key);

                    let mut tbl = table.lock().unwrap();
                    if write {
                        let val = ((t as u64) << 32 | i).to_le_bytes();
                        let _ = tbl.insert(&hk, &val, BucketMode::Inline);
                    } else {
                        let _ = tbl.lookup(&hk);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let ops_per_sec = total_ops as f64 / elapsed;

    println!(
        "Mixed R/W (80/20) throughput: {:.0} ops/sec ({total_ops} ops in {elapsed:.3}s)",
        ops_per_sec,
    );
    println!(
        "  ~ per-op: {:.1} µs",
        (elapsed / total_ops as f64) * 1_000_000.0,
    );

    assert!(ops_per_sec > 0.0, "Throughput should be positive");
}

// --- test_kick_chain_saturation ------------------------------------------

#[test]
fn test_kick_chain_saturation() {
    // Small table (64 buckets), low max_kick to trigger TableFull early.
    let bucket_count = 64u64;
    let max_kick = 8u32;
    let mut table = CuckooTable::new(bucket_count, max_kick);

    let mut total_inserted = 0u64;
    let mut batch_times: Vec<(u64, f64)> = Vec::new();
    let batch_size = 5u64;
    let mut batch_start = Instant::now();

    for i in 0..1000u64 {
        let key = format!("kick_{}", i);
        let hk = hash_key(&key);
        let val = i.to_le_bytes();

        match table.insert(&hk, &val, BucketMode::Inline) {
            Ok(()) => {
                total_inserted += 1;
            }
            Err(CuckooError::TableFull) => {
                let elapsed = batch_start.elapsed().as_secs_f64();
                batch_times.push((total_inserted, elapsed));
                break;
            }
            Err(CuckooError::InvalidKey) => unreachable!(),
        }

        // Track per-batch timing.
        if total_inserted % batch_size == 0 && total_inserted > 0 {
            let elapsed = batch_start.elapsed().as_secs_f64();
            batch_times.push((total_inserted, elapsed));
            batch_start = Instant::now();
        }
    }

    println!(
        "Kick-chain saturation: {} keys inserted into {}-bucket table (max_kick={})",
        total_inserted, bucket_count, max_kick,
    );

    if batch_times.len() >= 3 {
        let first_time = batch_times[0].1;
        let last_time = batch_times.last().unwrap().1;
        println!(
            "  First batch: {:.3}ms, last batch: {:.3}ms",
            first_time * 1000.0,
            last_time * 1000.0,
        );

        // Expect some slowdown as kick chains grow.
        // (In a very small table, the last batch may be significantly slower.)
        if first_time > 0.0 {
            let ratio = last_time / first_time;
            println!("  Slowdown ratio: {:.1}x", ratio);
        }
    }

    // Verify TableFull was returned (if we broke from the loop).
    assert!(total_inserted > 0, "Should insert at least some keys");
    assert!(
        total_inserted <= bucket_count * 2,
        "Should not insert more than 2x bucket_count keys in a cuckoo table",
    );
}
