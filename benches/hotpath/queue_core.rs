//! Hotpath microbenchmarks for queue core operations (in-memory).
use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::VecDeque;

#[path = "../config.rs"]
mod config;

fn bench_queue_push_pop(c: &mut Criterion) {
    c.bench_function("queue_push_pop", |b| {
        b.iter(|| {
            let mut q: VecDeque<u64> = VecDeque::with_capacity(1024);
            for i in 0..1000u64 {
                q.push_back(i);
            }
            while let Some(_v) = q.pop_front() {
                // remove
            }
        })
    });
}

criterion_group!(
    name = hotpath_queue_core;
    config = config::criterion_config();
    targets = bench_queue_push_pop
);
criterion_main!(hotpath_queue_core);
