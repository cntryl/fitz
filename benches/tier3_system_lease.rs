use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::routing::RouteFamily;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 3: SYSTEM BENCHMARKS
//
// Target: Measure FULL ENGINE PIPELINE including routing, authorization, TLV
// Goal: <50µs p50 for lease operations through full system
// Throughput: 20k+ leases/sec through full pipeline
//
// These benchmarks measure realistic lease management with all layers.
// ============================================================================

fn bench_concurrent_families_isolation(c: &mut Criterion) {
    // Arrange: Multiple families with leases
    let families: Vec<_> = (0..10).map(RouteFamily::new).collect();
    let mut actors: Vec<_> = families.iter().map(|f| LeaseActor::new(*f)).collect();

    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("lease_{:03}", i))
        .collect();

    // Pre-populate all families
    for actor in &mut actors {
        for key in &lease_keys {
            actor.handle(LeaseMessage::Acquire {
                resource: key.clone(),
                ttl_secs: 30,
            });
        }
    }

    let mut group = c.benchmark_group("lease_system_isolation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut family_idx = 0;
    let mut key_idx = 0;

    group.bench_function("renew_across_isolated_families", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            let actor_idx = family_idx % actors.len();
            family_idx = (family_idx + 1) % actors.len();

            actors[actor_idx].handle(LeaseMessage::Renew {
                resource: key,
                ttl_secs: 30,
            })
        })
    });

    group.finish();
}

fn bench_high_contention_single_resource(c: &mut Criterion) {
    // Arrange: Single family, contention on one resource
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    let contended_key = "hotspot_resource".to_string();

    // Acquire the resource
    actor.handle(LeaseMessage::Acquire {
        resource: contended_key.clone(),
        ttl_secs: 30,
    });

    let mut group = c.benchmark_group("lease_system_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("rapid_renew_contended_resource", |b| {
        b.iter(|| {
            actor.handle(LeaseMessage::Renew {
                resource: black_box(contended_key.clone()),
                ttl_secs: 30,
            })
        })
    });

    group.finish();
}

fn bench_mixed_operation_sequence(c: &mut Criterion) {
    // Arrange: Setup actor
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    let lease_keys: Vec<String> = (0..50)
        .map(|i| format!("lease_{:02}", i))
        .collect();

    // Pre-populate half the leases
    for i in 0..25 {
        actor.handle(LeaseMessage::Acquire {
            resource: lease_keys[i].clone(),
            ttl_secs: 30,
        });
    }

    let mut group = c.benchmark_group("lease_system_mixed");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut operation_idx = 0;

    group.bench_function("mixed_acquire_renew_release_check", |b| {
        b.iter(|| {
            let key_idx = operation_idx % lease_keys.len();
            operation_idx = (operation_idx + 1) % lease_keys.len();
            let key = black_box(lease_keys[key_idx].clone());

            // Cycle through operations
            match operation_idx % 4 {
                0 => actor.handle(LeaseMessage::Acquire {
                    resource: key,
                    ttl_secs: 30,
                }),
                1 => actor.handle(LeaseMessage::Renew {
                    resource: key,
                    ttl_secs: 30,
                }),
                2 => actor.handle(LeaseMessage::Check {
                    resource: key,
                }),
                3 => actor.handle(LeaseMessage::Release {
                    resource: key,
                }),
                _ => unreachable!(),
            }
        })
    });

    group.finish();
}

fn bench_acquire_release_cycle(c: &mut Criterion) {
    // Arrange: Setup actor with cycle keys
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("cycle_lease_{:03}", i))
        .collect();

    let mut group = c.benchmark_group("lease_system_lifecycle");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut key_idx = 0;

    group.bench_function("acquire_then_release_cycle", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            // Acquire
            actor.handle(LeaseMessage::Acquire {
                resource: key.clone(),
                ttl_secs: 30,
            });

            // Release
            actor.handle(LeaseMessage::Release {
                resource: key,
            })
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_concurrent_families_isolation, bench_high_contention_single_resource, bench_mixed_operation_sequence, bench_acquire_release_cycle
}
criterion_main!(benches);
