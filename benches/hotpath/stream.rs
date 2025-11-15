//! Hotpath benchmarks for stream service operations
//!
//! These benchmarks test the core stream service primitives that are performance-critical:
//! append, read, commit operations on the StreamService directly.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::stream::service::StreamService;
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

static STREAM_SERVICE: OnceLock<Arc<StreamService>> = OnceLock::new();
fn stream_service() -> Arc<StreamService> {
    STREAM_SERVICE.get_or_init(|| {
        rt().block_on(async {
            // Create a StreamService with in-memory store for benchmarking
            let store = fitz::storage::midge_adapter::create_memory_store().unwrap();
            Arc::new(StreamService::new(Arc::new(store)))
        })
    })
}

static TEST_EVENTS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
fn test_events() -> &'static [Vec<u8>] {
    TEST_EVENTS.get_or_init(|| {
        vec![
            vec![b'e'; 64],        // 64B event
            vec![b'e'; 1024],      // 1KB event
            vec![b'e'; 64 * 1024], // 64KB event
        ]
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_stream_append(c: &mut Criterion) {
    let service = stream_service();
    let events = test_events();
    let mut counter = 0;

    c.bench_function("stream_append", |b| {
        b.iter(|| {
            let event = &events[counter % events.len()];
            counter += 1;
            rt().block_on(async {
                let result = service.append("test", "bench", "stream1", vec![event.clone()], None).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_stream_read(c: &mut Criterion) {
    let service = stream_service();
    let events = test_events();

    // Pre-populate some events
    rt().block_on(async {
        for (i, event) in events.iter().enumerate() {
            if i >= 10 { break; } // Just a few events
            let _ = service.append("test", "bench", "read_stream", vec![event.clone()], None).await;
        }
    });

    c.bench_function("stream_read", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.read("test", "bench", "read_stream", 0, 10, 50).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_stream_commit(c: &mut Criterion) {
    let service = stream_service();
    let events = test_events();

    c.bench_function("stream_commit", |b| {
        b.iter_batched(
            || {
                // Setup: append some events
                rt().block_on(async {
                    service.append("test", "bench", "commit_stream", vec![events[0].clone()], None).await.unwrap()
                })
            },
            |append_result| {
                rt().block_on(async {
                    let result = service.commit("test", "bench", "commit_stream", append_result.first_seq, append_result.last_seq).await;
                    criterion::black_box(result.ok());
                });
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_stream_append_commit(c: &mut Criterion) {
    let service = stream_service();
    let events = test_events();
    let mut counter = 0;

    c.bench_function("stream_append_commit", |b| {
        b.iter(|| {
            let event = &events[counter % events.len()];
            counter += 1;
            rt().block_on(async {
                // Append
                let append_result = service.append("test", "bench", "append_commit_stream", vec![event.clone()], None).await.unwrap();
                // Commit
                let result = service.commit("test", "bench", "append_commit_stream", append_result.first_seq, append_result.last_seq).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

criterion_group!(
    name = hotpath_stream;
    config = config::criterion_config();
    targets =
        bench_stream_append,
        bench_stream_read,
        bench_stream_commit,
        bench_stream_append_commit
);

criterion_main!(hotpath_stream);