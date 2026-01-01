use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};

#[path = "../config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

struct PingActor;

impl Actor for PingActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn bench_send_local(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - create scheduler and spawn actor
    let scheduler = Scheduler::new(1);
    let actor_ref = scheduler.spawn(PingActor, test_address(1, "/bench/ping"), 1000);

    let mut group = c.benchmark_group("hotpath_send_local");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_u64_message", |b| {
        b.iter(|| {
            // ONLY hot path - send a message
            actor_ref.send(black_box(42)).ok();
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_send_local
}
criterion_main!(benches);
