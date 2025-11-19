//! Hotpath benchmarks for KV domain.
//!
//! Measures ONLY the internal logic of the KV service:
//!   - Put: store key-value pairs
//!   - Get: retrieve values by key
//!   - Scan: list keys with prefix
//!
//! Zero frame parsing, zero engine, zero outbound delivery.
//! This is the true "business logic" bench.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::kv::{KvOperation, KvService};
use fitz::storage::midge_adapter;

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn bench_hot_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_hot_put");
    group.bench_function("put", |b| {
        b.iter_batched(
            || KvService::new(midge_adapter::create_memory_store().unwrap()),
            |svc| {
                svc.handle_operation(
                    KvOperation::Put,
                    "kv://realm/area/key1",
                    Some("key1".to_string()),
                    Some(b"value".to_vec()),
                )
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_get(c: &mut Criterion) {
    let kv_store = midge_adapter::create_memory_store().unwrap();
    let svc = KvService::new(kv_store);
    // Pre-populate
    svc.handle_operation(
        KvOperation::Put,
        "kv://realm/area/key1",
        Some("key1".to_string()),
        Some(b"value".to_vec()),
    )
    .unwrap();

    let mut group = c.benchmark_group("kv_hot_get");
    group.bench_function("get", |b| {
        b.iter(|| {
            svc.handle_operation(
                KvOperation::Get,
                "kv://realm/area/key1",
                Some("key1".to_string()),
                None,
            )
        })
    });
    group.finish();
}

fn bench_hot_scan(c: &mut Criterion) {
    let kv_store = midge_adapter::create_memory_store().unwrap();
    let svc = KvService::new(kv_store);
    // Pre-populate some keys
    for i in 0..10 {
        svc.handle_operation(
            KvOperation::Put,
            &format!("kv://realm/area/key{}", i),
            Some(format!("key{}", i)),
            Some(format!("value{}", i).into_bytes()),
        )
        .unwrap();
    }

    let mut group = c.benchmark_group("kv_hot_scan");
    group.bench_function("scan", |b| {
        b.iter(|| {
            svc.handle_operation(
                KvOperation::Scan,
                "kv://realm/area/",
                None,
                Some("key0\nkey9".as_bytes().to_vec()),
            )
        })
    });
    group.finish();
}

// -----------------------------------------------------------------------------
// Registration
// -----------------------------------------------------------------------------

criterion_group!(
    name = hotpath_kv;
    config = config::criterion_config();
    targets =
        bench_hot_put,
        bench_hot_get,
        bench_hot_scan
);
criterion_main!(hotpath_kv);
