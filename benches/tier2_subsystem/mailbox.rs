use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};

#[path = "../config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

struct MessageActor;

impl Actor for MessageActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn bench_mailbox_send(c: &mut Criterion) {
    let scheduler = Scheduler::new(1);
    let actor_ref = scheduler.spawn(MessageActor, test_address(1, "/bench/mailbox"), 10000);

    let mut group = c.benchmark_group("subsystem_mailbox");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_to_mailbox", |b| {
        b.iter(|| {
            actor_ref.send(black_box(42)).ok();
        })
    });

    group.finish();
}

fn bench_mailbox_capacity(c: &mut Criterion) {
    let scheduler = Scheduler::new(1);
    let actor_ref = scheduler.spawn(MessageActor, test_address(1, "/bench/capacity"), 1000);

    let mut group = c.benchmark_group("subsystem_mailbox_capacity");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("send_100_messages", |b| {
        b.iter(|| {
            for i in 0..100 {
                actor_ref.send(black_box(i)).ok();
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_mailbox_send, bench_mailbox_capacity
}
criterion_main!(benches);
