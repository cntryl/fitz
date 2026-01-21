//! KV domain tier 1 hotpath benchmarks
//!
//! Pure operation latency: Get, Put, Insert, Delete, Scan
//! Measures single-operation performance without transaction overhead
//! Target: <10 µs per operation

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use fitz::domains::kv::{KvActor, KvMessage, ScanQuery, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

#[path = "config.rs"]
mod config;

fn bench_put_operation(c: &mut Criterion) {
    // Setup
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    // Begin transaction outside the loop
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "kv".to_string(),
        resource: "table".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => return,
    };

    let key = Bytes::from_static(b"bench_key");
    let value = Bytes::from_static(b"bench_value_0123456789");

    let mut group = c.benchmark_group("kv_hotpath_put");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("put_single_key", |b| {
        b.iter(|| {
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: black_box(key.clone()),
                value: black_box(value.clone()),
            })
        })
    });
    group.finish();
}

fn bench_get_operation(c: &mut Criterion) {
    // Setup
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    // Begin transaction
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "kv".to_string(),
        resource: "table".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => return,
    };

    let key = Bytes::from_static(b"bench_key");

    let mut group = c.benchmark_group("kv_hotpath_get");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("get_single_key", |b| {
        b.iter(|| {
            actor.handle(KvMessage::Get {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: black_box(key.clone()),
            })
        })
    });
    group.finish();
}

fn bench_delete_operation(c: &mut Criterion) {
    // Setup
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    // Begin transaction
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "kv".to_string(),
        resource: "table".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => return,
    };

    let key = Bytes::from_static(b"bench_key");

    let mut group = c.benchmark_group("kv_hotpath_delete");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("delete_single_key", |b| {
        b.iter(|| {
            actor.handle(KvMessage::Delete {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                key: black_box(key.clone()),
            })
        })
    });
    group.finish();
}

fn bench_scan_operation(c: &mut Criterion) {
    // Setup
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let mut actor = KvActor::new(store);

    // Begin transaction
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "kv".to_string(),
        resource: "table".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => return,
    };

    let query = ScanQuery {
        start: None,
        end: None,
        limit: Some(10),
        reverse: false,
    };

    let mut group = c.benchmark_group("kv_hotpath_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("scan_with_limit_10", |b| {
        b.iter(|| {
            actor.handle(KvMessage::Scan {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "table".to_string(),
                query: black_box(query.clone()),
            })
        })
    });
    group.finish();
}

fn bench_begin_operation(c: &mut Criterion) {
    // Setup - new actor per iteration to measure fresh begin
    let mut group = c.benchmark_group("kv_hotpath_begin");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("begin_read_write_transaction", |b| {
        b.iter(|| {
            let store = create_test_engine_with_cfs(vec![1]);
            let mut actor = KvActor::new(store);
            actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: black_box("bench".to_string()),
                area: black_box("kv".to_string()),
                resource: black_box("table".to_string()),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            })
        })
    });
    group.finish();
}

fn bench_rollback_operation(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_hotpath_rollback");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("rollback_transaction", |b| {
        b.iter(|| {
            // Create a fresh transaction for each rollback
            let store = create_test_engine_with_cfs(vec![1]);
            let mut actor = KvActor::new(store);
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "bench".to_string(),
                area: "kv".to_string(),
                resource: "table".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_put_operation,
        bench_get_operation,
        bench_delete_operation,
        bench_scan_operation,
        bench_begin_operation,
        bench_rollback_operation
}
criterion_main!(benches);
