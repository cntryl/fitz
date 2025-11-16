// Moved from benches/hotpath/stream.rs — subsystem/service-level bench
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::stream::StreamService;
use fitz::core::stream::types::StreamEvent;
use fitz::routing::DEFAULT_RF;
use fitz::storage::traits::KvStore;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

static STREAM_SERVICE: OnceLock<Arc<StreamService>> = OnceLock::new();
fn stream_service() -> Arc<StreamService> {
    STREAM_SERVICE.get_or_init(|| {
        rt().block_on(async {
            let store = fitz::storage::midge_adapter::create_memory_store().unwrap();
            Arc::new(StreamService::new(store))
        })
    }).clone()
}

const MAX_ITERS: u64 = 2_000;

fn bench_stream_append_subsystem(c: &mut Criterion) {
    let svc = stream_service();
    let rf = DEFAULT_RF;

    c.bench_function("stream_append_subsystem", |b| {
        b.iter_custom(|_| {
            let start = std::time::Instant::now();
            rt().block_on(async {
                for i in 0..MAX_ITERS {
                    let txn = svc
                        .begin_append(rf, "realm1", "area1", "resource1")
                        .await
                        .expect("begin");

                    let event = StreamEvent {
                        sequence: 0,
                        resource: "resource1".to_string(),
                        area_seq: None,
                        body: format!("body_{:04}", i).into_bytes(),
                        metadata: None,
                        created_at: 0,
                        is_end: false,
                    };

                    svc.append_event(txn, rf, event)
                        .await
                        .expect("append");

                    let _ = svc.commit_append(txn, rf).await.expect("commit");
                }
            });
            start.elapsed()
        })
    });
}

criterion_group!(
    name = subsystem_stream_service;
    config = config::criterion_config();
    targets = bench_stream_append_subsystem
);
criterion_main!(subsystem_stream_service);