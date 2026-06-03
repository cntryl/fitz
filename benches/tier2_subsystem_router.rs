use criterion::{
    BatchSize, Criterion, SamplingMode, Throughput, black_box, criterion_group, criterion_main,
};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::{DeliveryError, MailboxSink, RouteError, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

struct NoopSink;

impl MailboxSink for NoopSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Ok(())
    }

    fn deliver_high_priority(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        Ok(())
    }
}

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn make_exact_router(count: usize, sink: Arc<dyn MailboxSink>) -> (Router, Vec<RouteAddress>) {
    let router = Router::new();
    let addresses: Vec<_> = (0..count)
        .map(|i| test_address(1, &format!("rpc://acme/router/exact/{i}")))
        .collect();

    for address in &addresses {
        router.register(address.clone(), Arc::clone(&sink));
    }

    (router, addresses)
}

fn bench_route_exact_primary(c: &mut Criterion) {
    let (router, addresses) = make_exact_router(1, Arc::new(NoopSink));
    let address = addresses[0].clone();

    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_exact_noop_primary", |b| {
        let mut seq = 0_u64;
        b.iter(|| {
            router
                .route(Envelope::new(black_box(address.clone()), black_box(seq)))
                .expect("exact route should succeed");
            seq = seq.wrapping_add(1);
        })
    });

    group.finish();
}

fn bench_route_domain_fallback_primary(c: &mut Criterion) {
    let router = Router::new();
    router.register_domain_pattern("rpc", Arc::new(NoopSink));
    let address = test_address(1, "rpc://acme/router/fallback/target");

    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_domain_fallback_noop_primary", |b| {
        let mut seq = 0_u64;
        b.iter(|| {
            router
                .route(Envelope::new(black_box(address.clone()), black_box(seq)))
                .expect("domain fallback route should succeed");
            seq = seq.wrapping_add(1);
        })
    });

    group.finish();
}

fn bench_route_batch_exact_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);

    for route_count in [1usize, 64usize, 1024usize] {
        let (router, addresses) = make_exact_router(route_count, Arc::new(NoopSink));
        group.throughput(Throughput::Elements(route_count as u64));

        group.bench_function(
            format!("route_batch_exact_{}_noop_primary", route_count),
            |b| {
                let mut seq = 0_u64;
                b.iter(|| {
                    for address in &addresses {
                        router
                            .route(Envelope::new(address.clone(), black_box(seq)))
                            .expect("batched exact route should succeed");
                        seq = seq.wrapping_add(1);
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_route_mailbox_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_exact_mailbox_primary", |b| {
        b.iter_batched(
            || {
                let router = Router::new();
                let address = test_address(1, "rpc://acme/router/mailbox/target");
                router.register(address.clone(), Arc::new(Mailbox::new(1)));
                (router, address)
            },
            |(router, address)| {
                router
                    .route(Envelope::new(black_box(address), black_box(0_u64)))
                    .expect("mailbox route should succeed");
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_route_backpressure_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("route_exact_backpressure_mailbox_primary", |b| {
        b.iter_batched(
            || {
                let router = Router::new();
                let mailbox = Arc::new(Mailbox::new(1));
                let address = test_address(1, "rpc://acme/router/full/target");
                router.register(address.clone(), mailbox.clone());
                mailbox
                    .deliver(Envelope::new(address.clone(), 0_u64))
                    .expect("prefill should succeed");
                (router, address)
            },
            |(router, address)| match router
                .route(Envelope::new(black_box(address), black_box(1_u64)))
            {
                Err(RouteError::DeliveryFailed(_, DeliveryError::MailboxFull { .. })) => {
                    black_box(());
                }
                other => panic!("expected MailboxFull, got {other:?}"),
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets =
        bench_route_exact_primary,
        bench_route_domain_fallback_primary,
        bench_route_batch_exact_primary,
        bench_route_mailbox_primary,
        bench_route_backpressure_primary
}
criterion_main!(benches);
