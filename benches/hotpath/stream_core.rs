//! Hotpath microbenchmarks for stream core operations.
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "../config.rs"]
mod config;

fn bench_stream_append(c: &mut Criterion) {
    c.bench_function("stream_append", |b| {
        b.iter(|| {
            let mut log: Vec<u64> = Vec::with_capacity(1024);
            for i in 0..1000u64 {
                log.push(i);
            }
        })
    });
}

fn bench_stream_read_range(c: &mut Criterion) {
    let mut log: Vec<u64> = Vec::with_capacity(1024);
    for i in 0..1000u64 {
        log.push(i);
    }

    c.bench_function("stream_read_range", |b| {
        b.iter(|| {
            let _slice = &log[100..200];
            std::hint::black_box(_slice);
        })
    });
}

criterion_group!(
    name = hotpath_stream_core;
    config = config::criterion_config();
    targets = bench_stream_append, bench_stream_read_range
);
criterion_main!(hotpath_stream_core);
