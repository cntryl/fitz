//! KV domain tier 4 integration benchmarks
//!
//! Full system pipeline with durability (when Midge commit bug is fixed)
//! Currently: Measures operation latency through engine routing + domain handling
//! Includes domain context creation overhead, TLV serialization, routing
//!
//! Note: Cannot measure persistence/recovery due to Midge commit() bug
//! See TODO.md: "Midge commit() fails when transaction contains writes"
//! Workaround: Measure Begin→Op→Rollback (no durability test)
//! Target: Understand total latency including engine + routing overhead

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::kv::{KvActor, KvMessage, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_full_pipeline_put(c: &mut Criterion) {
    // Complete pipeline: Create actor, begin, put, rollback
    // Measures total overhead vs hotpath
    let mut group = c.benchmark_group("kv_integration_full_pipeline_put");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("create_actor_begin_put_rollback", |b| {
        b.iter(|| {
            let store = create_test_engine_with_cfs(vec![1]);
            let mut actor = KvActor::new(store);

            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "full_pipeline".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(1),
                resource: "full_pipeline".to_string(),
                key: Bytes::from_static(b"integration_key"),
                value: Bytes::from_static(b"integration_value_with_some_length"),
            });

            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

fn bench_full_pipeline_transaction_sequence(c: &mut Criterion) {
    // Realistic transaction sequence: Begin, get, put, delete, rollback
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);

    // Setup some initial data
    actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "integration".to_string(),
        area: "kv".to_string(),
        resource: "initial".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    actor.handle(KvMessage::Put {
        route_family: RouteFamily::new(1),
        resource: "initial".to_string(),
        key: Bytes::from_static(b"existing_key"),
        value: Bytes::from_static(b"existing_value"),
    });

    actor.handle(KvMessage::Rollback);

    let mut group = c.benchmark_group("kv_integration_full_pipeline_sequence");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1));

    group.bench_function("begin_get_put_delete_rollback_full_cycle", |b| {
        b.iter(|| {
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "cycle".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            // Read existing
            actor.handle(KvMessage::Get {
                route_family: RouteFamily::new(1),
                resource: "initial".to_string(),
                key: Bytes::from_static(b"existing_key"),
            });

            // Add new
            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(1),
                resource: "cycle".to_string(),
                key: Bytes::from_static(b"new_key"),
                value: Bytes::from_static(b"new_value"),
            });

            // Delete existing
            actor.handle(KvMessage::Delete {
                route_family: RouteFamily::new(1),
                resource: "initial".to_string(),
                key: Bytes::from_static(b"existing_key"),
            });

            // Cleanup
            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

fn bench_multi_resource_transaction(c: &mut Criterion) {
    // Transactions spanning multiple resources within same realm/area
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_integration_multi_resource");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(3));

    group.bench_function("single_tx_3_resources", |b| {
        b.iter(|| {
            // All resources use same family for simplicity
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "r1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            // Operations on resource 1
            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(1),
                resource: "r1".to_string(),
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            });

            // Operations on resource 2 (same transaction context)
            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(1),
                resource: "r2".to_string(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            });

            // Operations on resource 3
            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(1),
                resource: "r3".to_string(),
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            });

            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

fn bench_cross_family_transaction_sequence(c: &mut Criterion) {
    // Separate transactions on different families within same benchmark iteration
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_integration_cross_family_sequence");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(3));

    group.bench_function("3_separate_family_transactions", |b| {
        b.iter(|| {
            // Transaction on family 1
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "tx1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(1),
                resource: "tx1".to_string(),
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            });

            actor.handle(KvMessage::Rollback);

            // Transaction on family 2
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(2),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "tx2".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(2),
                resource: "tx2".to_string(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            });

            actor.handle(KvMessage::Rollback);

            // Transaction on family 3
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(3),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "tx3".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });

            actor.handle(KvMessage::Put {
                route_family: RouteFamily::new(3),
                resource: "tx3".to_string(),
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            });

            actor.handle(KvMessage::Rollback);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = 
        bench_full_pipeline_put,
        bench_full_pipeline_transaction_sequence,
        bench_multi_resource_transaction,
        bench_cross_family_transaction_sequence
}
criterion_main!(benches);
