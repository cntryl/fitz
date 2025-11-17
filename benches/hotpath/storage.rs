//! Hotpath benchmarks for storage operations
//!
//! Storage operations are performance-critical, especially for KV and queue operations.
//! These benchmarks focus on the core storage primitives that are called frequently.

use cntryl_midge::ColumnFamilyId;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::storage::midge_adapter;
use fitz::storage::traits::KvStore;
use std::sync::{Arc, OnceLock};

#[path = "../config.rs"]
mod config;

// Default column family for storage operations
const DEFAULT_CF: ColumnFamilyId = ColumnFamilyId(0);

// ---------------------------------------------------------
// Shared test data and stores
// ---------------------------------------------------------
static MIDGE_STORE: OnceLock<Arc<dyn KvStore>> = OnceLock::new();
fn midge_store() -> Arc<dyn KvStore> {
    MIDGE_STORE
        .get_or_init(|| {
            // Use in-memory Midge store for benches — avoids filesystem effects
            midge_adapter::create_memory_store().expect("create memory store")
        })
        .clone()
}

#[allow(dead_code)]
static TEST_KEYS: OnceLock<Vec<String>> = OnceLock::new();
#[allow(dead_code)]
fn test_keys() -> &'static [String] {
    TEST_KEYS.get_or_init(|| (0..1000).map(|i| format!("key_{:04}", i)).collect())
}

static TEST_VALUES: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
fn test_values() -> &'static [Vec<u8>] {
    TEST_VALUES.get_or_init(|| {
        vec![
            vec![b'x'; 64],          // 64B
            vec![b'x'; 1024],        // 1KB
            vec![b'x'; 64 * 1024],   // 64KB
            vec![b'x'; 1024 * 1024], // 1MB
        ]
    })
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_midge_put_small(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_small";
    let value = &test_values()[0]; // 64B

    c.bench_function("midge_put_64b", |b| {
        b.iter(|| {
            store.put(DEFAULT_CF, key, value).unwrap();
        })
    });
}

fn bench_midge_put_medium(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_medium";
    let value = &test_values()[1]; // 1KB

    c.bench_function("midge_put_1kb", |b| {
        b.iter(|| {
            store.put(DEFAULT_CF, key, value).unwrap();
        })
    });
}

fn bench_midge_put_large(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_large";
    let value = &test_values()[2]; // 64KB

    c.bench_function("midge_put_64kb", |b| {
        b.iter(|| {
            store.put(DEFAULT_CF, key, value).unwrap();
        })
    });
}

fn bench_midge_get_small(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_small";
    let value = &test_values()[0];
    store.put(DEFAULT_CF, key, value).unwrap(); // Setup

    c.bench_function("midge_get_64b", |b| {
        b.iter(|| {
            let _result = store.get(DEFAULT_CF, key).unwrap();
        })
    });
}

fn bench_midge_get_medium(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_medium";
    let value = &test_values()[1];
    store.put(DEFAULT_CF, key, value).unwrap(); // Setup

    c.bench_function("midge_get_1kb", |b| {
        b.iter(|| {
            let _result = store.get(DEFAULT_CF, key).unwrap();
        })
    });
}

fn bench_midge_get_large(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_large";
    let value = &test_values()[2];
    store.put(DEFAULT_CF, key, value).unwrap(); // Setup

    c.bench_function("midge_get_64kb", |b| {
        b.iter(|| {
            let _result = store.get(DEFAULT_CF, key).unwrap();
        })
    });
}

fn bench_midge_get_missing(c: &mut Criterion) {
    let store = midge_store();
    let key = b"bench_key_missing";

    c.bench_function("midge_get_missing", |b| {
        b.iter(|| {
            let _result = store.get(DEFAULT_CF, key);
        })
    });
}

// fn bench_midge_scan_prefix(c: &mut Criterion) {
//     let store = midge_store();

//     // Setup: insert keys with common prefix
//     for i in 0..100 {
//         let key = format!("prefix_key_{:04}", i);
//         let value = &test_values()[0];
//         store.put(DEFAULT_CF, key.as_bytes(), value).unwrap();
//     }

//     c.bench_function("midge_scan_prefix_100", |b| {
//         b.iter(|| {
//             let mut count = 0;
//             let mut iter = store.scan_prefix(b"prefix_key_");
//             while let Some(_) = iter.next() {
//                 count += 1;
//                 if count >= 10 { break; } // Limit for benchmark
//             }
//         })
//     });
// }

fn bench_midge_batch_put(c: &mut Criterion) {
    let store = midge_store();

    c.bench_function("midge_batch_put_10", |b| {
        b.iter_batched(
            || {
                let mut batch = Vec::new();
                for i in 0..10 {
                    batch.push((
                        format!("batch_key_{}", i).into_bytes(),
                        test_values()[0].clone(),
                    ));
                }
                batch
            },
            |batch| {
                for (key, value) in batch {
                    store.put(DEFAULT_CF, &key, &value).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_midge_delete(c: &mut Criterion) {
    let store = midge_store();

    c.bench_function("midge_delete", |b| {
        b.iter_batched(
            || {
                let key = b"delete_key";
                let value = &test_values()[0];
                store.put(DEFAULT_CF, key, value).unwrap();
                key
            },
            |key| {
                store.delete(DEFAULT_CF, key).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    name = storage_hotpath;
    config = config::criterion_config();
    targets =
        bench_midge_put_small,
        bench_midge_put_medium,
        bench_midge_put_large,
        bench_midge_get_small,
        bench_midge_get_medium,
        bench_midge_get_large,
        bench_midge_get_missing,
        bench_midge_batch_put,
        bench_midge_delete
);

criterion_main!(storage_hotpath);
