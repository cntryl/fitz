use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::routing::RouteFamily;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL LIFECYCLE with expiration, GC, and multi-family stress
// Goal: <100µs p50 for integrated workloads
// Throughput: 10k+ leases/sec under realistic load
//
// These benchmarks measure realistic lease management scenarios.
// ============================================================================

fn bench_lease_expiration_check(c: &mut Criterion) {
    // Arrange: Setup actor with expired leases
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    let lease_keys: Vec<String> = (0..1000)
        .map(|i| format!("lease_{:04}", i))
        .collect();

    // Create many leases (normally would expire)
    for key in &lease_keys {
        actor.handle(LeaseMessage::Acquire {
            resource: key.clone(),
            ttl_secs: 1, // Short TTL
        });
    }

    let mut group = c.benchmark_group("lease_integration_expiration");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut key_idx = 0;

    group.bench_function("check_lease_validity_large_set", |b| {
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

fn bench_sustained_high_throughput(c: &mut Criterion) {
    // Arrange: Multiple actors for different families
    let families: Vec<_> = (0..20).map(RouteFamily::new).collect();
    let mut actors: Vec<_> = families.iter().map(|f| LeaseActor::new(*f)).collect();

    let lease_keys: Vec<String> = (0..200)
        .map(|i| format!("lease_{:04}", i))
        .collect();

    let mut group = c.benchmark_group("lease_integration_throughput");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut family_idx = 0;
    let mut key_idx = 0;

    group.bench_function("sustained_high_family_throughput", |b| {
        b.iter(|| {
            let actor_idx = family_idx % actors.len();
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());

            family_idx = (family_idx + 1) % actors.len();
            key_idx = (key_idx + 1) % lease_keys.len();

            // Cycle through operations
            match (family_idx + key_idx) % 3 {
                0 => actors[actor_idx].handle(LeaseMessage::Acquire {
                    resource: key,
                    ttl_secs: 60,
                }),
                1 => actors[actor_idx].handle(LeaseMessage::Renew {
                    resource: key,
                    ttl_secs: 60,
                }),
                _ => actors[actor_idx].handle(LeaseMessage::Check {
                    resource: key,
                }),
            }
        })
    });

    group.finish();
}

fn bench_lease_recovery_scenario(c: &mut Criterion) {
    // Arrange: Simulate recovery with many existing leases
    let family = RouteFamily::new(0);
    let mut actor = LeaseActor::new(family);

    let lease_keys: Vec<String> = (0..500)
        .map(|i| format!("persistent_lease_{:04}", i))
        .collect();

    // Pre-populate as if recovered from persistent storage
    for key in &lease_keys {
        actor.handle(LeaseMessage::Acquire {
            resource: key.clone(),
            ttl_secs: 3600, // 1 hour
        });
    }

    let mut group = c.benchmark_group("lease_integration_recovery");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut key_idx = 0;

    group.bench_function("renew_after_recovery_large_state", |b| {
        b.iter(|| {
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());
            key_idx = (key_idx + 1) % lease_keys.len();

            actor.handle(LeaseMessage::Renew {
                resource: key,
                ttl_secs: 3600,
            })
        })
    });

    group.finish();
}

fn bench_mixed_realistic_workload(c: &mut Criterion) {
    // Arrange: Realistic scenario with multiple families and operation mix
    let families: Vec<_> = (0..10).map(RouteFamily::new).collect();
    let mut actors: Vec<_> = families.iter().map(|f| LeaseActor::new(*f)).collect();

    let lease_keys: Vec<String> = (0..100)
        .map(|i| format!("resource_{:03}", i))
        .collect();

    // Pre-populate with leases
    for actor in &mut actors {
        for (i, key) in lease_keys.iter().enumerate() {
            if i % 3 == 0 {
                // Only populate some resources
                actor.handle(LeaseMessage::Acquire {
                    resource: key.clone(),
                    ttl_secs: 300,
                });
            }
        }
    }

    let mut group = c.benchmark_group("lease_integration_realistic");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut family_idx = 0;
    let mut key_idx = 0;
    let mut op_idx = 0;

    group.bench_function("realistic_mixed_workload", |b| {
        b.iter(|| {
            let actor_idx = family_idx % actors.len();
            let key = black_box(lease_keys[key_idx % lease_keys.len()].clone());

            family_idx = (family_idx + 1) % actors.len();
            key_idx = (key_idx + 1) % lease_keys.len();
            op_idx = (op_idx + 1) % 100;

            // 50% renew, 30% check, 20% acquire
            if op_idx < 50 {
                actors[actor_idx].handle(LeaseMessage::Renew {
                    resource: key,
                    ttl_secs: 300,
                })
            } else if op_idx < 80 {
                actors[actor_idx].handle(LeaseMessage::Check {
                    resource: key,
                })
            } else {
                actors[actor_idx].handle(LeaseMessage::Acquire {
                    resource: key,
                    ttl_secs: 300,
                })
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_lease_expiration_check, bench_sustained_high_throughput, bench_lease_recovery_scenario, bench_mixed_realistic_workload
}
criterion_main!(benches);
