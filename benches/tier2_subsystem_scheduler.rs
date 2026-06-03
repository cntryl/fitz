use criterion::{
    BatchSize, Criterion, SamplingMode, Throughput, black_box, criterion_group, criterion_main,
};
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::MailboxSink;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn make_registration_batch(
    prefix: &str,
    count: usize,
) -> (Vec<RouteAddress>, Vec<Arc<dyn MailboxSink>>) {
    let addresses = (0..count)
        .map(|i| test_address(1, &format!("{prefix}/{i}")))
        .collect();
    let sinks = (0..count)
        .map(|_| Arc::new(Mailbox::new(100)) as Arc<dyn MailboxSink>)
        .collect();
    (addresses, sinks)
}

fn register_all(scheduler: &Scheduler, addresses: &[RouteAddress], sinks: &[Arc<dyn MailboxSink>]) {
    let router = scheduler.router();
    for (address, sink) in addresses.iter().zip(sinks) {
        router.register(address.clone(), Arc::clone(sink));
    }
}

struct SpawnActor;

impl Actor for SpawnActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}

    fn started(&mut self, ctx: &mut Context<Self>) {
        ctx.stop();
    }
}

/// Full spawn cost (mailbox + router register + thread::spawn). Keep as a smoke baseline only.
fn bench_scheduler_spawn_smoke(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let scheduler = Arc::new(Scheduler::new(1));
    let addresses: Vec<_> = (0..16)
        .map(|i| test_address(1, &format!("/bench/spawn/{}", i)))
        .collect();

    scheduler.spawn(SpawnActor, addresses[0].clone(), 100);

    group.bench_function("spawn_single_actor_smoke", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let address = addresses[idx % addresses.len()].clone();
            idx = (idx + 1) % addresses.len();
            scheduler.spawn(SpawnActor, black_box(address), 100);
        })
    });

    group.finish();
}

/// Registration only (no thread). This is the primary Tier 2 scheduler signal.
fn bench_scheduler_register_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_scheduler");
    group.sampling_mode(SamplingMode::Flat);

    let (single_addresses, single_sinks) = make_registration_batch("/bench/reg/single", 1);
    let (batch_addresses, batch_sinks) = make_registration_batch("/bench/reg/batch", 64);

    group.throughput(Throughput::Elements(1));
    group.bench_function("register_single_fresh_primary", |b| {
        b.iter_batched(
            || Scheduler::new(1),
            |scheduler| {
                scheduler.router().register(
                    black_box(single_addresses[0].clone()),
                    black_box(Arc::clone(&single_sinks[0])),
                );
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("register_single_replace_primary", |b| {
        b.iter_batched(
            || {
                let scheduler = Scheduler::new(1);
                scheduler
                    .router()
                    .register(single_addresses[0].clone(), Arc::clone(&single_sinks[0]));
                scheduler
            },
            |scheduler| {
                scheduler.router().register(
                    black_box(single_addresses[0].clone()),
                    black_box(Arc::clone(&single_sinks[0])),
                );
            },
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Elements(batch_addresses.len() as u64));
    group.bench_function("register_64_fresh_primary", |b| {
        b.iter_batched(
            || Scheduler::new(1),
            |scheduler| {
                register_all(&scheduler, &batch_addresses, &batch_sinks);
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("register_64_replace_primary", |b| {
        b.iter_batched(
            || {
                let scheduler = Scheduler::new(1);
                register_all(&scheduler, &batch_addresses, &batch_sinks);
                scheduler
            },
            |scheduler| {
                register_all(&scheduler, &batch_addresses, &batch_sinks);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_scheduler_spawn_cross_family_smoke(c: &mut Criterion) {
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

    group.bench_function("spawn_different_family_smoke", |b| {
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
    targets =
        bench_scheduler_spawn_smoke,
        bench_scheduler_register_primary,
        bench_scheduler_spawn_cross_family_smoke
}
criterion_main!(benches);
