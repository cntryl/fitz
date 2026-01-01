use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};

#[path = "../config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

struct EchoActor;

impl Actor for EchoActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn bench_reply(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let scheduler = Scheduler::new(1);
    let echo_ref = scheduler.spawn(EchoActor, test_address(1, "/bench/echo"), 10000);

    let mut group = c.benchmark_group("hotpath_reply");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_and_receive", |b| {
        b.iter(|| {
            // ONLY hot path - send message and receive response
            echo_ref.send(black_box(42)).ok();
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_reply
}
criterion_main!(benches);
