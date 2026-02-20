use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use parking_lot::Mutex;
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

struct PingActor;

impl Actor for PingActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
}

struct CounterActor {
    count: Arc<Mutex<u64>>,
}

impl Actor for CounterActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {
        *self.count.lock() += 1;
    }
}

fn bench_send_to_other(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - create scheduler and spawn actor
    let scheduler = Scheduler::new(1);
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("/bench/ping".to_string()));
    let actor_ref = scheduler.spawn(PingActor, address, 1000);

    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_to_other_actor", |b| {
        b.iter(|| {
            // ONLY hot path - send a message
            actor_ref.send(black_box(42)).ok();
        })
    });

    group.finish();
}

fn bench_send_to_self(c: &mut Criterion) {
    let count = Arc::new(Mutex::new(0u64));
    let count_clone = count.clone();

    let scheduler = Scheduler::new(1);
    let address = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("/bench/counter".to_string()),
    );
    let actor_ref = scheduler.spawn(CounterActor { count: count_clone }, address, 10000);

    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_to_self", |b| {
        b.iter(|| {
            // ONLY hot path - send a message to self
            actor_ref.send(black_box(1)).ok();
        })
    });

    group.finish();
}

fn bench_message_overhead(c: &mut Criterion) {
    // Measure pure ActorRef clone overhead
    let scheduler = Scheduler::new(1);
    let address = RouteAddress::new(RouteFamily::new(1), Route::new("/bench/ping".to_string()));
    let actor_ref = scheduler.spawn(PingActor, address, 1000);

    let mut group = c.benchmark_group("hotpath_actor_messaging");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("actorref_clone_overhead", |b| {
        b.iter(|| {
            // Measure clone cost
            let _cloned = black_box(actor_ref.clone());
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_send_to_other, bench_send_to_self, bench_message_overhead
}
criterion_main!(benches);
