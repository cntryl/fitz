//! KV domain tier 2 subsystem benchmarks
//!
//! Transaction lifecycle stress: Begin → multi-op → Rollback cycles
//! Tests transaction coordination, locking, isolation
//! Note: Uses Rollback workaround due to Midge commit() bug
//! Target: ~50-100 µs per transaction

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::kv::{KvActor, KvMessage, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_transaction_lifecycle(c: &mut Criterion) {
    // Setup store once
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_subsystem_tx_lifecycle");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("begin_put_rollback_cycle", |b| {
        b.iter(|| {
            // Begin
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            // Put
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"key1"),
                value: Bytes::from_static(b"value1"),
            });

            // Rollback (workaround for Midge commit bug)
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

fn bench_multi_operation_transaction(c: &mut Criterion) {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_subsystem_multi_op_tx");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("begin_5xput_rollback", |b| {
        b.iter(|| {
            // Begin
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            // Multiple puts
            for i in 0..5 {
                actor.handle(KvMessage::Put {
                    tx_id,
                    route_family: RouteFamily::new(1),
                    resource: "table".to_string(),
                    key: Bytes::from_static(b"key1"),
                    value: Bytes::from(format!("value{}", i).into_bytes()),
                });
            }

            // Rollback
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

fn bench_read_write_mixed_transaction(c: &mut Criterion) {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    // Setup: do initial put to have data
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "subsys".to_string(),
        area: "kv".to_string(),
        resource: "table".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id_setup = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => return,
    };

    actor.handle(KvMessage::Put {
        tx_id: tx_id_setup,
        route_family: RouteFamily::new(1),
        resource: "table".to_string(),
        key: Bytes::from_static(b"setup_key"),
        value: Bytes::from_static(b"setup_value"),
    });

    actor.handle(KvMessage::Rollback { tx_id: tx_id_setup });

    let mut group = c.benchmark_group("kv_subsystem_mixed_read_write");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("begin_get_put_delete_rollback", |b| {
        b.iter(|| {
            // Begin
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            // Get
            actor.handle(KvMessage::Get {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"setup_key"),
            });

            // Put
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"new_key"),
                value: Bytes::from_static(b"new_value"),
            });

            // Delete
            actor.handle(KvMessage::Delete {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"setup_key"),
            });

            // Rollback
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

fn bench_sequential_transactions(c: &mut Criterion) {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_subsystem_sequential_tx");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1));

    group.bench_function("3_sequential_transactions", |b| {
        b.iter(|| {
            // Transaction 1
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            });
            actor.handle(KvMessage::Rollback { tx_id });

            // Transaction 2
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            });
            actor.handle(KvMessage::Rollback { tx_id });

            // Transaction 3
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            });
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

fn bench_cross_family_stress(c: &mut Criterion) {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_subsystem_cross_family");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(3));

    group.bench_function("ops_on_3_families", |b| {
        b.iter(|| {
            // Family 1
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "t1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "t1".to_string(),
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            });
            actor.handle(KvMessage::Rollback { tx_id });

            // Family 2
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(2),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "t2".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(2),
                resource: "t2".to_string(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            });
            actor.handle(KvMessage::Rollback { tx_id });

            // Family 3
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(3),
                realm: "subsys".to_string(),
                area: "kv".to_string(),
                resource: "t3".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(3),
                resource: "t3".to_string(),
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            });
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_transaction_lifecycle,
        bench_multi_operation_transaction,
        bench_read_write_mixed_transaction,
        bench_sequential_transactions,
        bench_cross_family_stress
}
criterion_main!(benches);
