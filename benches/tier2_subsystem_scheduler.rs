use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
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

    fn started(&mut self, ctx: &mut Context<Self>) {
        ctx.stop();
    }
}

fn bench_scheduler_spawn(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.throughput(Throughput::Elements(1));

    // Setup: Create scheduler ONCE, outside benchmark loop
    let scheduler = Arc::new(Scheduler::new(1));
    let address = test_address(1, "/bench/spawn");

    // Warmup: ensure any lazy init happens before measurement
    scheduler.spawn(SpawnActor, address.clone(), 100);

    group.bench_function("spawn_single_actor", |b| {
        b.iter(|| {
            // Measure: Spawn actor only (scheduler is persistent)
            scheduler.spawn(SpawnActor, black_box(address.clone()), 100);
        })
    });

    group.finish();
}

fn bench_scheduler_spawn_cross_family(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.throughput(Throughput::Elements(1));

    // Setup: Create scheduler ONCE, precompute addresses
    let scheduler = Arc::new(Scheduler::new(1));
    let addresses: Vec<_> = (0..5)
        .map(|i| test_address(i, &format!("/bench/family{}/actor", i)))
        .collect();

    // Warmup: ensure any lazy init happens before measurement
    scheduler.spawn(SpawnActor, addresses[0].clone(), 100);

    group.bench_function("spawn_different_family", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            // Measure: Spawn actor from rotating address pool
            scheduler.spawn(
                SpawnActor,
                black_box(addresses[idx % addresses.len()].clone()),
                100,
            );
            idx = (idx + 1) % addresses.len();
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
