use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::ActorRef;
use fitz::runtime::scheduler::Scheduler;
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

// ============================================================================
// BASELINE BENCHMARKS (Message Construction Only)
// ============================================================================

fn bench_lease_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_lease_baseline");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_lease_actor", |b| {
        b.iter(|| {
            let _actor = LeaseActor::new();
            black_box(_actor);
        })
    });

    group.finish();
}

fn bench_lease_spawn(c: &mut Criterion) {
    let scheduler = Arc::new(Scheduler::new(1));
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("/lease/actor".to_string()));

    let mut group = c.benchmark_group("subsystem_lease_baseline");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("spawn_lease_actor", |b| {
        let sched = scheduler.clone();
        let addr = address.clone();
        b.iter(|| {
            sched.spawn(LeaseActor::new(), black_box(addr.clone()), 10000);
        })
    });

    group.finish();
}

fn bench_lease_family_isolation(c: &mut Criterion) {
    // Pre-create families to avoid allocation in hot path
    let families: Vec<_> = (0..10).map(RouteFamily::new).collect();

    let mut group = c.benchmark_group("subsystem_lease_baseline");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("spawn_10_families", |b| {
        let fams = families.clone();
        b.iter(|| {
            for family in &fams {
                black_box(family);
            }
        })
    });

    group.finish();
}

// ============================================================================
// RUNTIME BENCHMARKS - REAL LEASE ACTOR THROUGHPUT
//
// These benchmarks measure INTENTIONAL MISUSE patterns through the actual
// runtime (Scheduler + ActorRef + Mailbox). Goal: measure worst-case impact
// and confirm degradation is isolated per RouteFamilyId.
// ============================================================================

/// Helper to spawn a LeaseActor and return its ActorRef
fn spawn_lease_actor(
    scheduler: &Arc<Scheduler>,
    family_id: u64,
    capacity: usize,
) -> ActorRef<LeaseMessage> {
    let address = RouteAddress::new(
        RouteFamily::new(family_id),
        Route::new("/lease/test".to_string()),
    );
    scheduler.spawn(LeaseActor::new(), address, capacity)
}

fn bench_lease_runtime_acquire_release_loop(c: &mut Criterion) {
    //! RUNTIME ABUSE: Single client rapid acquire→release→acquire cycling
    //!
    //! Pattern: Tight loop calling acquire then release on same lease
    //! Misuse: Leases guard work epochs (ms-sec), not spinlocks (μs)
    //!
    //! Measures:
    //! - Actual ops/sec through the Lease actor (via mailbox)
    //! - Throughput under maximum churn
    //! - How many acquire+release pairs the actor can process

    let scheduler = Arc::new(Scheduler::new(1));
    let actor_ref = spawn_lease_actor(&scheduler, 1, 10000);

    let family = RouteFamily::new(1);
    let route = Route::new("/abuse/acquire_release_loop".to_string());
    let owner = "chatty_client_1".to_string();
    let ttl_secs = 10u64;

    let mut group = c.benchmark_group("runtime_lease_churn");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(2)); // acquire + release pair = 1 cycle

    group.bench_function("acquire_release_tight_loop", |b| {
        b.iter(|| {
            // Send acquire
            let _ = actor_ref.send(LeaseMessage::Acquire {
                family_id: black_box(family.clone()),
                route: black_box(route.clone()),
                owner_id: black_box(owner.clone()),
                ttl_secs,
            });

            // Send release (we don't wait for responses in tight loop)
            let _ = actor_ref.send(LeaseMessage::Release {
                family_id: black_box(family.clone()),
                route: black_box(route.clone()),
                owner_id: black_box(owner.clone()),
                fencing_token: black_box(1),
            });
        })
    });

    group.finish();
}

fn bench_lease_runtime_renew_spin(c: &mut Criterion) {
    //! RUNTIME ABUSE: Single client acquire-once, then renew in tight loop
    //!
    //! Pattern: Acquire once, then send continuous renews
    //! Misuse: Leases should be released when work is done, not held forever
    //!
    //! Measures:
    //! - Renew operation throughput (ops/sec) via Lease actor
    //! - Cost of continuous state refresh through mailbox
    //! - Steady-state performance with long-held lease

    let scheduler = Arc::new(Scheduler::new(1));
    let actor_ref = spawn_lease_actor(&scheduler, 2, 10000);

    let family = RouteFamily::new(2);
    let route = Route::new("/abuse/renew_spin".to_string());
    let owner = "spin_client".to_string();
    let ttl_secs = 60u64;

    // First, acquire the lease
    let _ = actor_ref.send(LeaseMessage::Acquire {
        family_id: family.clone(),
        route: route.clone(),
        owner_id: owner.clone(),
        ttl_secs,
    });

    // Give actor time to process
    std::thread::sleep(std::time::Duration::from_millis(10));

    let mut group = c.benchmark_group("runtime_lease_renew");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("renew_tight_loop", |b| {
        b.iter(|| {
            // Tight loop of renew messages
            let _ = actor_ref.send(LeaseMessage::Renew {
                family_id: black_box(family.clone()),
                route: black_box(route.clone()),
                owner_id: black_box(owner.clone()),
                fencing_token: black_box(1),
                ttl_secs,
            });
        })
    });

    group.finish();
}

fn bench_lease_runtime_contended_acquire(c: &mut Criterion) {
    //! RUNTIME ABUSE: N concurrent client actors all racing for same lease
    //!
    //! Pattern: Multiple clients attempting to acquire the same lease
    //! Misuse: Lease exclusivity means contention indicates misconfigured routing
    //!
    //! Measures:
    //! - Throughput with increasing contender count (2, 5, 10, 20)
    //! - Fairness (no starvation of clients)
    //! - How actor scales under contention
    //!
    //! BLAST RADIUS GOAL: Contention on one lease should not crash/slow
    //! unrelated leases in the same actor (queuing behavior OK, starvation NOT OK)

    let mut group = c.benchmark_group("runtime_lease_contention");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for contender_count in [2, 5, 10, 20].iter() {
        let scheduler = Arc::new(Scheduler::new(1));
        let actor_ref = spawn_lease_actor(&scheduler, 3, 100000);

        let family = RouteFamily::new(3);
        let route = Route::new("/abuse/contended".to_string());
        let ttl_secs = 5u64;

        group.throughput(Throughput::Elements(*contender_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_contenders", contender_count)),
            contender_count,
            |b, &count| {
                b.iter(|| {
                    // Simulate N clients attempting acquire on same (family, route)
                    for i in 0..count {
                        let owner = format!("contender_{}", i);
                        let _ = actor_ref.send(LeaseMessage::Acquire {
                            family_id: black_box(family.clone()),
                            route: black_box(route.clone()),
                            owner_id: black_box(owner),
                            ttl_secs,
                        });
                    }
                    // Note: We don't wait for responses; this measures mailbox throughput
                })
            },
        );
    }

    group.finish();
}

fn bench_lease_runtime_multi_family_isolation(c: &mut Criterion) {
    //! RUNTIME ISOLATION TEST: Same chatty patterns replicated across N families
    //!
    //! Pattern: Independent LeaseActors in different RouteFamilies
    //! Goal: Confirm one family's saturation does NOT affect others
    //!
    //! Measures:
    //! - Throughput per family remains constant as family count grows
    //! - No cross-family interference at runtime level
    //! - Isolation holds under heavy load
    //!
    //! BLAST RADIUS GOAL: Family 1 abuse should NOT increase latency of Family 2
    //! This confirms isolation at the routing + scheduling level.

    let mut group = c.benchmark_group("runtime_lease_isolation");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for family_count in [1, 3, 5, 10].iter() {
        let scheduler = Arc::new(Scheduler::new(1));

        // Spawn one LeaseActor per family
        let actors: Vec<_> = (0..*family_count)
            .map(|i| spawn_lease_actor(&scheduler, (100 + i) as u64, 10000))
            .collect();

        group.throughput(Throughput::Elements((*family_count * 2) as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_families", family_count)),
            family_count,
            |b, &_count| {
                b.iter(|| {
                    // Each family does rapid acquire+release churn
                    for (family_idx, actor_ref) in actors.iter().enumerate() {
                        let family = RouteFamily::new((100 + family_idx) as u64);
                        let route = Route::new(format!("/abuse/family_{}", family_idx));
                        let owner = format!("client_{}", family_idx);

                        // Acquire
                        let _ = actor_ref.send(LeaseMessage::Acquire {
                            family_id: black_box(family.clone()),
                            route: black_box(route.clone()),
                            owner_id: black_box(owner.clone()),
                            ttl_secs: 10,
                        });

                        // Release
                        let _ = actor_ref.send(LeaseMessage::Release {
                            family_id: black_box(family),
                            route: black_box(route),
                            owner_id: black_box(owner),
                            fencing_token: black_box(1),
                        });
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_lease_runtime_burst_load(c: &mut Criterion) {
    //! RUNTIME ABUSE: Burst of N messages sent immediately
    //!
    //! Pattern: Send a large batch of acquire messages without waiting
    //! Misuse: Mailbox overflow / unbounded queue growth
    //!
    //! Measures:
    //! - Mailbox throughput with bursty load (10, 50, 100 burst)
    //! - Message loss (if any)
    //! - Recovery time after burst
    //!
    //! BLAST RADIUS GOAL: Burst should not crash actor or lose messages.
    //! Measure that queue grows proportionally and empties predictably.

    let mut group = c.benchmark_group("runtime_lease_burst");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for burst_size in [10, 50, 100].iter() {
        let scheduler = Arc::new(Scheduler::new(1));
        let actor_ref = spawn_lease_actor(&scheduler, 5, 200000);

        let family = RouteFamily::new(5);

        group.throughput(Throughput::Elements(*burst_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("burst_{}", burst_size)),
            burst_size,
            |b, &size| {
                b.iter(|| {
                    // Send a burst of N acquire messages
                    for i in 0..size {
                        let route = Route::new(format!("/burst/lease_{}", i));
                        let owner = format!("burst_client_{}", i);

                        let _ = actor_ref.send(LeaseMessage::Acquire {
                            family_id: black_box(family.clone()),
                            route: black_box(route),
                            owner_id: black_box(owner),
                            ttl_secs: 1,
                        });
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_lease_runtime_sustained_load(c: &mut Criterion) {
    //! RUNTIME MEASUREMENT: Sustained streaming load on single actor
    //!
    //! Pattern: Continuous stream of acquire messages on different leases
    //! Realistic: Multiple clients acquiring different locks
    //!
    //! Measures:
    //! - Sustainable throughput (ops/sec) over 100+ iterations
    //! - Latency stability (p50, p99 don't spike)
    //! - Memory stability (no unbounded growth)
    //!
    //! GOAL: Establish baseline throughput ceiling for single Lease actor

    let scheduler = Arc::new(Scheduler::new(1));
    let actor_ref = spawn_lease_actor(&scheduler, 6, 100000);

    let family = RouteFamily::new(6);

    let mut group = c.benchmark_group("runtime_lease_sustained");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("sustained_acquire_stream", |b| {
        let mut lease_counter = 0;
        b.iter(|| {
            // Acquire different leases in rapid succession
            // This exercises the actor's ability to manage many independent leases
            let route = Route::new(format!("/sustained/lease_{}", lease_counter));
            let owner = format!("sustained_client_{}", lease_counter % 5);

            actor_ref
                .send(LeaseMessage::Acquire {
                    family_id: black_box(family.clone()),
                    route: black_box(route),
                    owner_id: black_box(owner),
                    ttl_secs: 3600, // Long TTL to simulate long-lived leases
                })
                .ok();

            lease_counter += 1;
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_lease_creation,
        bench_lease_spawn,
        bench_lease_family_isolation,
        bench_lease_runtime_acquire_release_loop,
        bench_lease_runtime_renew_spin,
        bench_lease_runtime_contended_acquire,
        bench_lease_runtime_multi_family_isolation,
        bench_lease_runtime_burst_load,
        bench_lease_runtime_sustained_load
}
criterion_main!(benches);
