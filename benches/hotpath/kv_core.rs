//! Hotpath microbenchmarks for small KV map operations.
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;

#[path = "../config.rs"]
mod config;

fn bench_hashmap_insert(c: &mut Criterion) {
    c.bench_function("kv_hashmap_insert", |b| {
        b.iter(|| {
            let mut m: HashMap<String, Vec<u8>> = HashMap::with_capacity(256);
            for i in 0..128u32 {
                let k = format!("key{:08}", i);
                m.insert(k, vec![0u8; 64]);
            }
        })
    });
}

fn bench_hashmap_get(c: &mut Criterion) {
    let mut m: HashMap<String, Vec<u8>> = HashMap::with_capacity(512);
    for i in 0..1000u32 {
        let k = format!("key{:08}", i);
        m.insert(k, vec![0u8; 32]);
    }

    let keys: Vec<String> = (0..1000u32).map(|i| format!("key{:08}", i)).collect();

    c.bench_function("kv_hashmap_get", |b| {
        b.iter(|| {
            for k in &keys {
                let _ = m.get(k);
            }
        })
    });
}

criterion_group!(
    name = hotpath_kv_core;
    config = config::criterion_config();
    targets = bench_hashmap_insert, bench_hashmap_get
);
criterion_main!(hotpath_kv_core);
