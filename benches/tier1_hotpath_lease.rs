use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::routing::{Route, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 1: HOT PATH MICROBENCHMARKS
//
// Target: Measure PURE actor operations WITHOUT scheduler overhead
// Goal: <5µs p50 for acquire, <5µs p50 for renew, <1µs p50 for release
// Throughput: 200k+ ops/sec for acquire/renew
//
// These benchmarks call actor methods directly to measure the hot path.
// ============================================================================

fn bench_acquire_hotpath(c: &mut Criterion) {
    // Arrange: Setup actor
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    // Pre-generate lease keys outside loop
    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("lease_key_{:03}", i))
        .collect();

    let mut key_idx = 0;

    let mut group = c.benchmark_group("lease_hotpath_acquire");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("acquire_new_lease", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            actor.handle(
                LeaseMessage::Acquire {
                    resource: key,
                    ttl_secs: 30,
                },
            )
        })
    });

    group.finish();
}

fn bench_renew_hotpath(c: &mut Criterion) {
    // Arrange: Setup actor with existing leases
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    // Pre-populate with leases
    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("lease_key_{:03}", i))
        .collect();

    for key in &lease_keys {
        actor.handle(LeaseMessage::Acquire {
            resource: key.clone(),
            ttl_secs: 30,
        });
    }

    let mut key_idx = 0;

    let mut group = c.benchmark_group("lease_hotpath_renew");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("renew_existing_lease", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            actor.handle(LeaseMessage::Renew {
                resource: key,
                ttl_secs: 30,
            })
        })
    });

    group.finish();
}

fn bench_release_hotpath(c: &mut Criterion) {
    // Arrange: Setup actor with existing leases
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    // Pre-populate with many leases to release
    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("lease_key_{:03}", i))
        .collect();

    for key in &lease_keys {
        actor.handle(LeaseMessage::Acquire {
            resource: key.clone(),
            ttl_secs: 30,
        });
    }

    let mut key_idx = 0;

    let mut group = c.benchmark_group("lease_hotpath_release");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("release_held_lease", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            actor.handle(LeaseMessage::Release {
                resource: key,
            })
        })
    });

    group.finish();
}

fn bench_check_hotpath(c: &mut Criterion) {
    // Arrange: Setup actor with existing leases
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    // Pre-populate with leases
    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("lease_key_{:03}", i))
        .collect();

    for key in &lease_keys {
        actor.handle(LeaseMessage::Acquire {
            resource: key.clone(),
            ttl_secs: 30,
        });
    }

    let mut key_idx = 0;

    let mut group = c.benchmark_group("lease_hotpath_check");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("check_lease_status", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            actor.handle(LeaseMessage::Check {
                resource: key,
            })
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_acquire_hotpath, bench_renew_hotpath, bench_release_hotpath, bench_check_hotpath
}
criterion_main!(benches);
