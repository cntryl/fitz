use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};

#[path = "criterion_config.rs"]
mod criterion_config;

struct MessageActor;

impl Actor for MessageActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

fn bench_mailbox_send(c: &mut Criterion) {
    let scheduler = Scheduler::new(1);
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("/bench/mailbox"));
    let actor_ref = scheduler.spawn(MessageActor, address, 10000);

    let mut group = c.benchmark_group("subsystem_mailbox");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_to_mailbox", |b| {
        let ref_clone = actor_ref.clone();
        b.iter(|| {
            ref_clone.send(black_box(42)).ok();
        })
    });

    group.finish();
}

/// Send 100 messages per iteration. Variance (rel_stddev ~0.10+) is expected from
/// scheduler/thread wakeups and channel contention; run with a single worker and no
/// other load to reduce noise.
fn bench_mailbox_capacity(c: &mut Criterion) {
    let scheduler = Scheduler::new(1);
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("/bench/capacity"));
    let actor_ref = scheduler.spawn(MessageActor, address, 1000);

    let mut group = c.benchmark_group("subsystem_mailbox");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("send_100_messages", |b| {
        let ref_clone = actor_ref.clone();
        b.iter(|| {
            for i in 0..100 {
                ref_clone.send(black_box(i)).ok();
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_mailbox_send, bench_mailbox_capacity
}
criterion_main!(benches);
