use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::LeaseActor;
use fitz::runtime::scheduler::Scheduler;
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "./config.rs"]
mod config;

fn bench_lease_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_lease");
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

    let mut group = c.benchmark_group("subsystem_lease");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("spawn_lease_actor", |b| {
        let sched = scheduler.clone();
        let addr = address.clone();
        b.iter(|| {
            sched.spawn(LeaseActor::new(), black_box(addr.clone()), 1000);
        })
    });

    group.finish();
}

fn bench_lease_family_isolation(c: &mut Criterion) {
    // Pre-create families to avoid allocation in hot path
    let families: Vec<_> = (0..10).map(RouteFamily::new).collect();

    let mut group = c.benchmark_group("subsystem_lease_isolation");
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

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_lease_creation, bench_lease_spawn, bench_lease_family_isolation
}
criterion_main!(benches);
