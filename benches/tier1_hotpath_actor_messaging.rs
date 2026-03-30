use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::envelope::MessageId;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::{Actor, Context};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

struct BenchActor;

impl Actor for BenchActor {
    type Message = u64;

    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn make_context_with_registered_routes(
    route_count: usize,
) -> (Context<BenchActor>, Vec<RouteAddress>) {
    let router = Arc::new(Router::new());
    let addresses: Vec<_> = (0..route_count)
        .map(|i| test_address(1, &format!("/bench/resolve/{}", i)))
        .collect();

    for address in &addresses {
        router.register(address.clone(), Arc::new(Mailbox::new(64)));
    }

    let ctx = Context::<BenchActor>::new(test_address(1, "/bench/source"), Arc::clone(&router));
    (ctx, addresses)
}

fn bench_context_address(c: &mut Criterion) {
    let ctx = Context::<BenchActor>::new(test_address(1, "/bench/source"), Arc::new(Router::new()));

    let mut group = c.benchmark_group("hotpath_actor_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("context_address", |b| {
        b.iter(|| {
            black_box(ctx.address());
        })
    });

    group.finish();
}

fn bench_context_current_metadata_none(c: &mut Criterion) {
    let ctx = Context::<BenchActor>::new(test_address(1, "/bench/source"), Arc::new(Router::new()));

    let mut group = c.benchmark_group("hotpath_actor_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("context_current_metadata_none", |b| {
        b.iter(|| {
            black_box(ctx.current_metadata());
        })
    });

    group.finish();
}

fn bench_message_id_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_actor_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("message_id_new", |b| {
        b.iter(|| {
            black_box(MessageId::new());
        })
    });

    group.finish();
}

fn bench_resolve_sink_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_actor_context");
    group.sampling_mode(SamplingMode::Flat);

    for route_count in [16usize, 256usize, 4096usize] {
        let (ctx, addresses) = make_context_with_registered_routes(route_count);
        group.throughput(Throughput::Elements(1));

        group.bench_function(format!("resolve_sink_exact_{}_routes", route_count), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let sink = ctx.resolve_sink(black_box(&addresses[idx % addresses.len()]));
                idx = (idx + 1) % addresses.len();
                black_box(sink);
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_context_address,
        bench_context_current_metadata_none,
        bench_message_id_new,
        bench_resolve_sink_scaling
}
criterion_main!(benches);
