//! T1-D: RDMA CAS (Compare-And-Swap) Hardware Verification Benchmark.
//!
//! This is the **highest-priority risk card** from Wave 1.
//! It measures RDMA CAS latency and IOPS on the target hardware
//! to determine whether the One-Sided architecture is viable.
//!
//! ## Pass/Fail Criteria:
//! - CAS latency ≤ 2× RDMA READ latency
//! - CAS IOPS ≥ 50% of RDMA WRITE IOPS
//!
//! ## Usage:
//! ```bash
//! cargo bench --bench cas
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;

mod harness;

fn bench_cas(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdma_ops");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(1000);

    // Setup once per benchmark
    let ctx = harness::setup_rdma().expect("RDMA setup failed");

    group.bench_function("cas_latency", |b| {
        b.iter(|| harness::bench_cas_single(black_box(&ctx)))
    });

    group.bench_function("read_latency", |b| {
        b.iter(|| harness::bench_read_single(black_box(&ctx)))
    });

    group.bench_function("write_latency", |b| {
        b.iter(|| harness::bench_write_single(black_box(&ctx)))
    });

    for batch_size in [4u32, 16, 64] {
        group.bench_with_input(
            BenchmarkId::new("cas_throughput", batch_size),
            &batch_size,
            |b, &n| b.iter(|| harness::bench_cas_batch(black_box(&ctx), black_box(n))),
        );
    }

    group.finish();
}

criterion_group!(benches, bench_cas);
criterion_main!(benches);
