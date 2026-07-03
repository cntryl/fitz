#![allow(deprecated)]
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::MailboxSink;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
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
        );
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
        );
    });

    group.throughput(Throughput::Elements(batch_addresses.len() as u64));
    group.bench_function("register_64_fresh_primary", |b| {
        b.iter_batched(
            || Scheduler::new(1),
            |scheduler| {
                register_all(&scheduler, &batch_addresses, &batch_sinks);
            },
            BatchSize::SmallInput,
        );
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
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_scheduler_register_primary
}
criterion_main!(benches);
