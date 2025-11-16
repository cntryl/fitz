// Moved from benches/hotpath/kv.rs — now classified as a subsystem/service benchmark
#![allow(dead_code)]
use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::kv::KvService;
use fitz::core::kv::KvOperation;
use std::sync::{Arc, OnceLock};

#[path = "../config.rs"]
mod config;

static KV_SERVICE: OnceLock<Arc<KvService>> = OnceLock::new();
fn kv_service() -> Arc<KvService> {
    KV_SERVICE.get_or_init(|| {
        // Create a KvService with in-memory store for benchmarking
        let store = fitz::storage::midge_adapter::create_memory_store().unwrap();
        Arc::new(KvService::new(store))
    }).clone()
}

const MAX_ITERS: u64 = 5_000;

fn bench_kv_put_get_subsystem(c: &mut Criterion) {
    let svc = kv_service();
    let route = "kv://realm1/area1".to_string();

    c.bench_function("kv_put_get_subsystem", |b| {
        b.iter_custom(|_| {
            let start = std::time::Instant::now();
            for i in 0..MAX_ITERS {
                let key = format!("key_{}", i % 1024);
                let val = format!("value_{}", i).into_bytes();
                let _ = svc
                    .handle_operation(KvOperation::Put, &route, Some(key.clone()), Some(val));

                let _ = svc
                    .handle_operation(KvOperation::Get, &route, Some(key.clone()), None);
            }
            start.elapsed()
        })
    });
}

criterion_group!(
    name = subsystem_kv_service;
    config = config::criterion_config();
    targets = bench_kv_put_get_subsystem
);
criterion_main!(subsystem_kv_service);