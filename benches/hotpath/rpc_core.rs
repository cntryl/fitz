//! Hotpath microbenchmarks for RPC correlation id / micro-routes.
use criterion::{criterion_group, criterion_main, Criterion};
use uuid::Uuid;

#[path = "../config.rs"]
mod config;

fn bench_generate_correlation_id(c: &mut Criterion) {
    c.bench_function("rpc_correlation_id", |b| {
        b.iter(|| {
            let _ = Uuid::new_v4().to_string();
        })
    });
}

criterion_group!(
    name = hotpath_rpc_core;
    config = config::criterion_config();
    targets = bench_generate_correlation_id
);
criterion_main!(hotpath_rpc_core);
