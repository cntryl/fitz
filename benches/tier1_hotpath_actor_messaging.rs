use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{DeliveryError, MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::{Actor, ActorRef, Context};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

struct BenchActor;

struct CountingSink {
    deliveries: AtomicU64,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            deliveries: AtomicU64::new(0),
        }
    }

    fn delivery_count(&self) -> u64 {
        self.deliveries.load(Ordering::Relaxed)
    }
}

impl MailboxSink for CountingSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        std::hint::black_box(envelope);
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        std::hint::black_box(envelope);
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl Actor for BenchActor {
    type Message = u64;

    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route))
}

fn make_router_with_registered_routes(
    route_count: usize,
) -> (Arc<Router>, Arc<CountingSink>, Vec<RouteAddress>) {
    let router = Arc::new(Router::new());
    let sink = Arc::new(CountingSink::new());
    let addresses: Vec<_> = (0..route_count)
        .map(|i| test_address(1, &format!("/bench/send/{}", i)))
        .collect();

    for address in &addresses {
        router.register(address.clone(), sink.clone());
    }

    (router, sink, addresses)
}

fn bench_actor_ref_send(c: &mut Criterion) {
    let (router, sink, addresses) = make_router_with_registered_routes(1);
    let actor_ref: ActorRef<u64> = ActorRef::new(addresses[0].clone(), router);

    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("actor_ref_send_exact", |b| {
        b.iter(|| {
            actor_ref.send(black_box(42_u64)).expect("actor ref send");
        })
    });

    black_box(sink.delivery_count());

    group.finish();
}

fn bench_context_send_untracked(c: &mut Criterion) {
    let (router, sink, addresses) = make_router_with_registered_routes(1);
    let ctx = Context::<BenchActor>::new(test_address(1, "/bench/source"), router);

    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("context_send_untracked_exact", |b| {
        b.iter(|| {
            ctx.send_untracked(addresses[0].clone(), black_box(42_u64))
                .expect("untracked send");
        })
    });

    black_box(sink.delivery_count());

    group.finish();
}

fn bench_context_send(c: &mut Criterion) {
    let (router, sink, addresses) = make_router_with_registered_routes(1);
    let ctx = Context::<BenchActor>::new(test_address(1, "/bench/source"), router);

    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("context_send_exact", |b| {
        b.iter(|| {
            ctx.send(addresses[0].clone(), black_box(42_u64))
                .expect("context send");
        })
    });

    black_box(sink.delivery_count());

    group.finish();
}

fn bench_context_send_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);

    for route_count in [16usize, 256usize, 4096usize] {
        let (router, sink, addresses) = make_router_with_registered_routes(route_count);
        let ctx = Context::<BenchActor>::new(test_address(1, "/bench/source"), router);
        group.throughput(Throughput::Elements(1));

        group.bench_function(format!("context_send_{}_routes", route_count), |b| {
            let mut idx = 0usize;
            b.iter(|| {
                ctx.send(addresses[idx % addresses.len()].clone(), black_box(42_u64))
                    .expect("context send scaling");
                idx = (idx + 1) % addresses.len();
            })
        });

        black_box(sink.delivery_count());
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_actor_ref_send,
        bench_context_send_untracked,
        bench_context_send,
        bench_context_send_scaling
}
criterion_main!(benches);
