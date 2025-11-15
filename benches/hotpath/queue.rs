//! Hotpath benchmarks for queue service operations
//!
//! These benchmarks test the core queue service primitives that are performance-critical:
//! enqueue, reserve, complete operations on the QueueService directly.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::queue::service::QueueService;
use fitz::storage::traits::KvStore;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared runtime and services
// ---------------------------------------------------------
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

static QUEUE_SERVICE: OnceLock<Arc<QueueService>> = OnceLock::new();
fn queue_service() -> Arc<QueueService> {
    QUEUE_SERVICE.get_or_init(|| {
        rt().block_on(async {
            // Create a QueueService with in-memory store for benchmarking
            let store = fitz::storage::midge_adapter::create_memory_store().unwrap();
            Arc::new(QueueService::new(Arc::new(store)))
        })
    })
}

static TEST_MESSAGES: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
fn test_messages() -> &'static [Vec<u8>] {
    TEST_MESSAGES.get_or_init(|| {
        vec![
            vec![b'm'; 64],        // 64B message
            vec![b'm'; 1024],      // 1KB message
            vec![b'm'; 64 * 1024], // 64KB message
        ]
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_queue_enqueue(c: &mut Criterion) {
    let service = queue_service();
    let messages = test_messages();
    let mut counter = 0;

    c.bench_function("queue_enqueue", |b| {
        b.iter(|| {
            let message = &messages[counter % messages.len()];
            counter += 1;
            rt().block_on(async {
                let result = service.enqueue("test", "bench", "queue1", message.clone(), None, None).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_queue_reserve(c: &mut Criterion) {
    let service = queue_service();
    let messages = test_messages();

    // Pre-populate some messages
    rt().block_on(async {
        for (i, message) in messages.iter().enumerate() {
            if i >= 10 { break; } // Just a few messages
            let _ = service.enqueue("test", "bench", "reserve_queue", message.clone(), None, None).await;
        }
    });

    c.bench_function("queue_reserve", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.reserve("test", "bench", "reserve_queue", 1, 30).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_queue_complete(c: &mut Criterion) {
    let service = queue_service();
    let messages = test_messages();

    c.bench_function("queue_complete", |b| {
        b.iter_batched(
            || {
                // Setup: enqueue and reserve a message
                rt().block_on(async {
                    let _ = service.enqueue("test", "bench", "complete_queue", messages[0].clone(), None, None).await;
                    service.reserve("test", "bench", "complete_queue", 1, 30).await.unwrap()
                })
            },
            |reservation| {
                rt().block_on(async {
                    let result = service.complete("test", "bench", "complete_queue", &reservation.lease_id, &reservation.token).await;
                    criterion::black_box(result.ok());
                });
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_queue_round_trip(c: &mut Criterion) {
    let service = queue_service();
    let messages = test_messages();
    let mut counter = 0;

    c.bench_function("queue_round_trip", |b| {
        b.iter(|| {
            let message = &messages[counter % messages.len()];
            counter += 1;
            rt().block_on(async {
                // Enqueue
                let _ = service.enqueue("test", "bench", "round_trip_queue", message.clone(), None, None).await;
                // Reserve
                let reservation = service.reserve("test", "bench", "round_trip_queue", 1, 30).await.unwrap();
                // Complete
                let result = service.complete("test", "bench", "round_trip_queue", &reservation.lease_id, &reservation.token).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

criterion_group!(
    name = hotpath_queue;
    config = config::criterion_config();
    targets =
        bench_queue_enqueue,
        bench_queue_reserve,
        bench_queue_complete,
        bench_queue_round_trip
);

criterion_main!(hotpath_queue);