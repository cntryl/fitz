//! Hotpath benchmarks for KV service operations
//!
//! These benchmarks test the core KV service primitives that are performance-critical:
//! put, get, delete, scan operations on the KvService directly.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::kv::service::KvService;
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

static KV_SERVICE: OnceLock<Arc<KvService>> = OnceLock::new();
fn kv_service() -> Arc<KvService> {
    KV_SERVICE.get_or_init(|| {
        rt().block_on(async {
            // Create a KvService with in-memory store for benchmarking
            let store = fitz::storage::midge_adapter::create_memory_store().unwrap();
            Arc::new(KvService::new(store))
        })
    })
}

static TEST_KEYS: OnceLock<Vec<String>> = OnceLock::new();
fn test_keys() -> &'static [String] {
    TEST_KEYS.get_or_init(|| {
        (0..1000)
            .map(|i| format!("bench_key_{:04}", i))
            .collect()
    })
}

static TEST_VALUES: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
fn test_values() -> &'static [Vec<u8>] {
    TEST_VALUES.get_or_init(|| {
        vec![
            vec![b'x'; 64],        // 64B
            vec![b'x'; 1024],      // 1KB
            vec![b'x'; 64 * 1024], // 64KB
        ]
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_kv_put(c: &mut Criterion) {
    let service = kv_service();
    let keys = test_keys();
    let values = test_values();
    let mut counter = 0;

    c.bench_function("kv_put", |b| {
        b.iter(|| {
            let key = &keys[counter % keys.len()];
            let value = &values[counter % values.len()];
            counter += 1;
            rt().block_on(async {
                let result = service.put("test", "bench", key, value.clone()).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_kv_get(c: &mut Criterion) {
    let service = kv_service();
    let keys = test_keys();
    let values = test_values();

    // Pre-populate some data
    rt().block_on(async {
        for (i, key) in keys.iter().enumerate() {
            let value = &values[i % values.len()];
            let _ = service.put("test", "bench", key, value.clone()).await;
        }
    });

    let mut counter = 0;
    c.bench_function("kv_get", |b| {
        b.iter(|| {
            let key = &keys[counter % keys.len()];
            counter += 1;
            rt().block_on(async {
                let result = service.get("test", "bench", key).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_kv_delete(c: &mut Criterion) {
    let service = kv_service();
    let keys = test_keys();
    let values = test_values();
    let mut counter = 0;

    c.bench_function("kv_delete", |b| {
        b.iter_batched(
            || {
                // Setup: put a value
                let key = keys[counter % keys.len()].clone();
                let value = &values[counter % values.len()];
                counter += 1;
                rt().block_on(async {
                    let _ = service.put("test", "bench", &key, value.clone()).await;
                });
                key
            },
            |key| {
                rt().block_on(async {
                    let result = service.delete("test", "bench", &key).await;
                    criterion::black_box(result.ok());
                });
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_kv_scan(c: &mut Criterion) {
    let service = kv_service();
    let values = test_values();

    // Pre-populate a range of keys
    rt().block_on(async {
        for i in 0..100 {
            let key = format!("scan_key_{:04}", i);
            let value = &values[i % values.len()];
            let _ = service.put("test", "bench", &key, value.clone()).await;
        }
    });

    c.bench_function("kv_scan", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.scan("test", "bench", "scan_key_0000", "scan_key_0099", 50).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

criterion_group!(
    name = hotpath_kv;
    config = config::criterion_config();
    targets =
        bench_kv_put,
        bench_kv_get,
        bench_kv_delete,
        bench_kv_scan
);

criterion_main!(hotpath_kv);