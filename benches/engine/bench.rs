//! Wave 6: Engine performance benchmarks.
//!
//! Measures the Cuckoo hashing engine's latency and throughput
//! across insert, lookup, delete, and concurrent operations.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::time::Duration;
use xxhash_rust::xxh64::xxh64;

use rdmas::engine::cuckoo::CuckooTable;
use rdmas::engine::extent::LargeObjectRegion;
use rdmas::engine::layout::{HashedKey, BucketMode};
use rdmas::engine::concurrency;
use rdmas::client::read::ClientReader;
use rdmas::client::write::{ClientWriter, WriteResult};

/// Hash a string key for benchmarking.
fn bench_key(s: &str) -> HashedKey {
    let hash = xxh64(s.as_bytes(), 0);
    let mut digest = [0u8; 16];
    let h2 = xxh64(s.as_bytes(), 1);
    digest[0..8].copy_from_slice(&hash.to_le_bytes());
    digest[8..16].copy_from_slice(&h2.to_le_bytes());
    HashedKey { hash, digest }
}

fn bench_insert_inline(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_insert");
    group.measurement_time(Duration::from_secs(3));

    let sizes = [1024u64, 65536, 1 << 20];
    for &buckets in &sizes {
        group.bench_with_input(
            BenchmarkId::new("inline_buckets", buckets),
            &buckets,
            |b, &buckets| {
                b.iter_batched(
                    || (CuckooTable::new(buckets, 16)),
                    |mut table| {
                        let key = bench_key(&format!("k_{}", rand::random::<u64>()));
                        let val = 42u64.to_le_bytes();
                        black_box(table.insert(&key, &val, BucketMode::Inline).unwrap());
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_lookup_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_lookup");
    group.measurement_time(Duration::from_secs(3));

    let count = 100_000u64;
    let buckets = 1u64 << 20;

    // Pre-populate table
    let mut table = CuckooTable::new(buckets, 16);
    let mut keys: Vec<(HashedKey, Vec<u8>)> = Vec::new();
    for i in 0..count {
        let key_str = format!("lookup_{}", i);
        let hk = bench_key(&key_str);
        let val = i.to_le_bytes().to_vec();
        table.insert(&hk, &val, BucketMode::Inline).unwrap();
        keys.push((hk, val));
    }

    group.bench_function("h1_hit", |b| {
        b.iter_batched(
            || &keys,
            |keys| {
                let idx = rand::random::<usize>() % keys.len();
                let (hk, _) = &keys[idx];
                let result = ClientReader::get(hk, table.buckets(), None, buckets);
                black_box(result);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("missing", |b| {
        b.iter_batched(
            || &keys,
            |keys| {
                let hk = bench_key(&format!("missing_{}", rand::random::<u64>()));
                let result = ClientReader::get(&hk, table.buckets(), None, buckets);
                black_box(result);
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_extent_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_extent");
    group.measurement_time(Duration::from_secs(3));

    let sizes = [64usize, 1024, 65536];

    for &size in &sizes {
        group.bench_with_input(
            BenchmarkId::new("allocate", size),
            &size,
            |b, &size| {
                let data = vec![0xAAu8; size];
                let mut region = LargeObjectRegion::new(10 * 1024 * 1024);
                b.iter(|| {
                    black_box(region.allocate(&data));
                });
            },
        );
    }

    group.finish();
}

fn bench_concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_concurrent");
    group.measurement_time(Duration::from_secs(5));

    for &threads in &[1u64, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("insert_threads", threads),
            &threads,
            |b, &threads| {
                let ops_per_thread = 1000u64;
                b.iter_batched(
                    || {
                        std::sync::Arc::new(std::sync::Mutex::new(
                            CuckooTable::new(1 << 18, 16),
                        ))
                    },
                    |table| {
                        let handles: Vec<_> = (0..threads).map(|t| {
                            let table = table.clone();
                            std::thread::spawn(move || {
                                for i in 0..ops_per_thread {
                                    let key_str = format!("t{}_k{}", t, i);
                                    let hk = bench_key(&key_str);
                                    let val = (t * ops_per_thread + i).to_le_bytes();
                                    let mut tbl = table.lock().unwrap();
                                    let _ = tbl.insert(&hk, &val, BucketMode::Inline);
                                }
                            })
                        }).collect();
                        for h in handles { h.join().unwrap(); }
                        black_box(table.lock().unwrap().buckets().len());
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_write_kick_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_kick_chain");
    group.measurement_time(Duration::from_secs(5));

    // Small table to force kicks
    for &buckets in &[8u64, 16, 32] {
        group.bench_with_input(
            BenchmarkId::new("write_with_kicks", buckets),
            &buckets,
            |b, &buckets| {
                b.iter_batched(
                    || {
                        let table = CuckooTable::new(buckets, 16);
                        let buckets_mut: Vec<_> = table.buckets().to_vec();
                        (table, buckets_mut)
                    },
                    |(table, mut buckets_mut)| {
                        let key = bench_key(&format!("kick_{}", rand::random::<u64>()));
                        let val = rand::random::<u64>().to_le_bytes();
                        let result = ClientWriter::insert(
                            &key, &val, BucketMode::Inline,
                            &mut buckets_mut, None, buckets,
                        );
                        black_box(result);
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(insert, bench_insert_inline);
criterion_group!(lookup, bench_lookup_hit);
criterion_group!(extent, bench_extent_alloc);
criterion_group!(concurrent, bench_concurrent_insert);
criterion_group!(kick, bench_write_kick_chain);
criterion_main!(insert, lookup, extent, concurrent, kick);
