use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::scheduler::Scheduler;
use fitz::runtime::{Actor, Context};
use fitz::transport::routing::{Route, RouteAddress, RouteFamily};
use std::sync::{Arc, Mutex};

#[path = "../config.rs"]
mod config;

fn test_address(family: u64, route: &str) -> RouteAddress {
    RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
}

struct CounterActor {
    count: Arc<Mutex<u64>>,
}

impl Actor for CounterActor {
    type Message = u64;
    fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {
        *self.count.lock().unwrap() += 1;
    }
}

fn bench_self_send(c: &mut Criterion) {
    let count = Arc::new(Mutex::new(0u64));
    let count_clone = count.clone();
    
    let scheduler = Scheduler::new(1);
    let actor_ref = scheduler.spawn(
        CounterActor { count: count_clone },
        test_address(1, "/bench/counter"),
        10000,
    );

    let mut group = c.benchmark_group("hotpath_self_send");
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

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_self_send
}
criterion_main!(benches);
