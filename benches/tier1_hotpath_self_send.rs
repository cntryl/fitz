use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};
use parking_lot::Mutex;
use std::sync::Arc;

#[path = "config.rs"]
mod config;

struct CounterActor {
    count: Arc<Mutex<u64>>,
}

impl Actor for CounterActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {
        *self.count.lock() += 1;
    }
}

fn bench_self_send(c: &mut Criterion) {
    let count = Arc::new(Mutex::new(0u64));
    let count_clone = count.clone();
    
    let scheduler = Scheduler::new(1);
    let address = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("/bench/counter".to_string()),
    );
    let actor_ref = scheduler.spawn(
        CounterActor { count: count_clone },
        address,
        10000,
    );

    let mut group = c.benchmark_group("hotpath_self_send");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("send_to_self", |b| {
        let ref_clone = actor_ref.clone();
        b.iter(|| {
            // ONLY hot path - send a message to self
            ref_clone.send(black_box(1)).ok();
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_self_send
}
criterion_main!(benches);
