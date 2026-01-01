use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

struct SpawnActor;

impl Actor for SpawnActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn bench_scheduler_spawn(c: &mut Criterion) {
    // Create scheduler once, outside benchmark
    let scheduler = Arc::new(Scheduler::new(1));
    let address = test_address(1, "/bench/spawn");

    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("spawn_single_actor", |b| {
        let sched = scheduler.clone();
        let addr = address.clone();
        b.iter(|| {
            sched.spawn(
                SpawnActor,
                black_box(addr.clone()),
                100,
            );
        })
    });

    group.finish();
}

fn bench_scheduler_spawn_cross_family(c: &mut Criterion) {
    let scheduler = Arc::new(Scheduler::new(1));

    // Pre-compute addresses to avoid string allocations in hot path
    let addresses: Vec<_> = (0..5)
        .map(|i| test_address(i, &format!("/bench/family{}/actor", i)))
        .collect();

    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("spawn_different_family", |b| {
        let sched = scheduler.clone();
        let addrs = addresses.clone();
        let mut idx = 0usize;
        b.iter(|| {
            sched.spawn(SpawnActor, black_box(addrs[idx % addrs.len()].clone()), 100);
            idx = idx.wrapping_add(1);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_scheduler_spawn, bench_scheduler_spawn_cross_family
}
criterion_main!(benches);
