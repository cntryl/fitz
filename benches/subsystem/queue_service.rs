// Moved from benches/hotpath/queue.rs — now a subsystem/service-level bench
use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::queue::QueueService;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

fn queue_service() -> QueueService {
    QueueService::new(fitz::storage::midge_adapter::create_memory_store().unwrap())
}

const MAX_ITERS: u64 = 1_000;

fn bench_queue_enqueue_reserve(c: &mut Criterion) {
    let svc = queue_service();

    c.bench_function("queue_enqueue_reserve_subsystem", |b| {
        b.iter_custom(|_| {
            let start = std::time::Instant::now();
            tokio::runtime::Runtime::new()
                .expect("rt")
                .block_on(async {
                    for i in 0..MAX_ITERS {
                        let _id = svc
                            .enqueue(
                                "realm1",
                                "area1",
                                "resource1",
                                format!("msg-{}", i).into_bytes(),
                                Some(60),
                                None,
                            )
                            .await
                            .expect("enqueue");

                        let _ = svc
                            .receive("realm1", "area1", "resource1", 1, 60)
                            .await
                            .expect("reserve");
                    }
                });
            start.elapsed()
        })
    });
}

criterion_group!(
    name = subsystem_queue_service;
    config = config::criterion_config();
    targets = bench_queue_enqueue_reserve
);
criterion_main!(subsystem_queue_service);