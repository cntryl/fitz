//! KV domain tier 3 system benchmarks
//!
//! Concurrent realm/column family contention
//! Tests impact of state isolation and sharding on performance
//! Measures concurrent access patterns and lock contention
//! Target: Compare against tier 1/2 baselines to measure overhead

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::kv::{KvActor, KvMessage, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_single_family_intensive(c: &mut Criterion) {
    // Repeated operations on same family - measures lock contention
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_system_single_family_intensive");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(10));

    group.bench_function("10_puts_same_family", |b| {
        b.iter(|| {
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "system".to_string(),
                area: "kv".to_string(),
                resource: "intensive".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            for i in 0..10 {
                actor.handle(KvMessage::Put {
                    route_family: RouteFamily::new(1),
                    resource: "intensive".to_string(),
                    key: Bytes::from(format!("key{}", i).into_bytes()),
                    value: Bytes::from(format!("value{}", i).into_bytes()),
                });
            }

            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

fn bench_dual_family_concurrent(c: &mut Criterion) {
    // Interleaved operations on two families - measures isolation
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_system_dual_family_concurrent");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(20)); // 10 per family

    group.bench_function("interleaved_puts_2_families", |b| {
        b.iter(|| {
            // Begin on family 1
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "system".to_string(),
                area: "kv".to_string(),
                resource: "f1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            // Begin on family 2
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(2),
                realm: "system".to_string(),
                area: "kv".to_string(),
                resource: "f2".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            // Interleaved puts
            for i in 0..10 {
                // Put to family 1
                actor.handle(KvMessage::Put {
                    route_family: RouteFamily::new(1),
                    resource: "f1".to_string(),
                    key: Bytes::from(format!("k1_{}", i).into_bytes()),
                    value: Bytes::from_static(b"v1"),
                });

                // Put to family 2
                actor.handle(KvMessage::Put {
                    route_family: RouteFamily::new(2),
                    resource: "f2".to_string(),
                    key: Bytes::from(format!("k2_{}", i).into_bytes()),
                    value: Bytes::from_static(b"v2"),
                });
            }

            // Rollback both
            actor.handle(KvMessage::Rollback);
            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

fn bench_triple_family_contention(c: &mut Criterion) {
    // Three families accessed sequentially - measures state isolation overhead
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_system_triple_family_contention");
    group.sample_size(15);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(30)); // 10 per family

    group.bench_function("10_puts_per_3_families", |b| {
        b.iter(|| {
            for family_id in 1..=3 {
                actor.handle(KvMessage::Begin {
                    route_family: RouteFamily::new(family_id),
                    realm: "system".to_string(),
                    area: "kv".to_string(),
                    resource: format!("f{}", family_id),
                    mode: TxMode::ReadWrite,
                    write_options: cntryl_midge::WriteOptions::buffered(),
                });

                for i in 0..10 {
                    actor.handle(KvMessage::Put {
                        route_family: RouteFamily::new(family_id),
                        resource: format!("f{}", family_id),
                        key: Bytes::from(format!("k{}", i).into_bytes()),
                        value: Bytes::from_static(b"v"),
                    });
                }

                actor.handle(KvMessage::Rollback);
            }
        })
    });

    group.finish();
}

fn bench_mixed_read_write_families(c: &mut Criterion) {
    // Reads and writes on different families - isolates read vs write overhead
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);

    // Populate some data first
    actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "system".to_string(),
        area: "kv".to_string(),
        resource: "setup".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    for i in 0..5 {
        actor.handle(KvMessage::Put {
            route_family: RouteFamily::new(1),
            resource: "setup".to_string(),
            key: Bytes::from(format!("setup_k{}", i).into_bytes()),
            value: Bytes::from_static(b"setup_v"),
        });
    }

    actor.handle(KvMessage::Rollback);

    let mut group = c.benchmark_group("kv_system_mixed_read_write_families");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(15)); // 5 reads + 5 writes + 5 deletes

    group.bench_function("5_reads_on_f1_5_writes_on_f2", |b| {
        b.iter(|| {
            // Read-only transaction on family 1
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "system".to_string(),
                area: "kv".to_string(),
                resource: "read_f1".to_string(),
                mode: TxMode::ReadOnly,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            for i in 0..5 {
                actor.handle(KvMessage::Get {
                    route_family: RouteFamily::new(1),
                    resource: "setup".to_string(),
                    key: Bytes::from(format!("setup_k{}", i).into_bytes()),
                });
            }

            actor.handle(KvMessage::Rollback);

            // Write transaction on family 2
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(2),
                realm: "system".to_string(),
                area: "kv".to_string(),
                resource: "write_f2".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            for i in 0..5 {
                actor.handle(KvMessage::Put {
                    route_family: RouteFamily::new(2),
                    resource: "write_f2".to_string(),
                    key: Bytes::from(format!("new_k{}", i).into_bytes()),
                    value: Bytes::from_static(b"new_v"),
                });
            }

            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_single_family_intensive,
        bench_dual_family_concurrent,
        bench_triple_family_contention,
        bench_mixed_read_write_families
}
criterion_main!(benches);
