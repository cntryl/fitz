use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::LeaseActor;
use fitz::runtime::scheduler::Scheduler;
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};

#[path = "../config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

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
    let scheduler = Scheduler::new(1);
    
    let mut group = c.benchmark_group("subsystem_lease");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("spawn_lease_actor", |b| {
        b.iter(|| {
            scheduler.spawn(
                LeaseActor::new(),
                black_box(test_address(1, "/lease/actor")),
                1000,
            );
        })
    });

    group.finish();
}

fn bench_lease_family_isolation(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_lease_isolation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("spawn_10_families", |b| {
        b.iter(|| {
            for family in 0..10 {
                RouteFamily::new(black_box(family));
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
