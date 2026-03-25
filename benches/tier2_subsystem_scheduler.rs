use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

struct SpawnActor;

impl Actor for SpawnActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}

    fn started(&mut self, ctx: &mut Context<Self>) {
        ctx.stop();
    }
}

/// Full spawn cost (mailbox + router register + thread::spawn). High variance is expected from OS scheduling.
fn bench_scheduler_spawn(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let scheduler = Arc::new(Scheduler::new(1));
    let addresses: Vec<_> = (0..16)
        .map(|i| test_address(1, &format!("/bench/spawn/{}", i)))
        .collect();

    scheduler.spawn(SpawnActor, addresses[0].clone(), 100);

    group.bench_function("spawn_single_actor", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let address = addresses[idx % addresses.len()].clone();
            idx = (idx + 1) % addresses.len();
            scheduler.spawn(SpawnActor, black_box(address), 100);
        })
    });

    group.finish();
}

/// Registration only (no thread). Isolates router + mailbox cost from thread creation.
fn bench_scheduler_register_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let scheduler = Arc::new(Scheduler::new(1));
    let router = scheduler.router();
    let addresses: Vec<_> = (0..64)
        .map(|i| test_address(1, &format!("/bench/reg/{}", i)))
        .collect();

    group.bench_function("register_only", |b| {
        let mut idx = 0u64;
        b.iter(|| {
            let address = addresses[(idx as usize) % addresses.len()].clone();
            idx = idx.wrapping_add(1);
            let mailbox = Mailbox::new(100);
            router.register(black_box(address), Arc::new(mailbox));
        })
    });

    group.finish();
}

fn bench_scheduler_spawn_cross_family(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);
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
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_scheduler_spawn, bench_scheduler_register_only, bench_scheduler_spawn_cross_family
}
criterion_main!(benches);
