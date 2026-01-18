use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::kv::actor::KvActor;
use fitz::domains::kv::protocol::{KvMessage, KvRequest};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use uuid::Uuid;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 2: SUBSYSTEM BENCHMARKS
//
// Target: Measure TRANSACTION LIFECYCLE including begin/commit overhead
// Goal: <10µs p50 for transaction lifecycle
// Throughput: 100k+ tx/sec
//
// These benchmarks include transaction setup/teardown costs.
// ============================================================================

fn make_kv_ctx(family: RouteFamily) -> Context<KvActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(family, Route::new("kv://test/area/bench"));
    Context::new(addr, router)
}

fn make_store() -> Arc<cntryl_midge::Engine> {
    Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    )
}

fn bench_transaction_lifecycle(c: &mut Criterion) {
    // Arrange: Setup actor
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    let mut group = c.benchmark_group("kv_subsystem_transaction_lifecycle");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("begin_commit_cycle", |b| {
        b.iter(|| {
            // Begin
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx,
            );

            // Single operation
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Put {
                        key: black_box(Bytes::from("key")),
                        value: black_box(Bytes::from("value")),
                    },
                },
                &mut ctx,
            );

            // Commit
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Commit,
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

fn bench_multi_operation_transaction(c: &mut Criterion) {
    // Arrange: Setup actor
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Pre-generate keys/values outside loop
    let keys: Vec<Bytes> = (0..10)
        .map(|i| Bytes::from(format!("key{:02}", i)))
        .collect();
    let values: Vec<Bytes> = (0..10)
        .map(|_| Bytes::from(vec![0u8; 256]))
        .collect();

    let mut group = c.benchmark_group("kv_subsystem_multi_operation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("10_puts_per_transaction", |b| {
        b.iter(|| {
            // Begin
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx,
            );

            // 10 puts
            for i in 0..10 {
                actor.handle(
                    KvRequest {
                        id: Uuid::new_v4(),
                        route: Route::new("kv://test/area/bench"),
                        payload: KvMessage::Put {
                            key: black_box(keys[i % keys.len()].clone()),
                            value: black_box(values[i % values.len()].clone()),
                        },
                    },
                    &mut ctx,
                );
            }

            // Commit
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Commit,
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

fn bench_scan_ordered_results(c: &mut Criterion) {
    // Arrange: Setup actor with pre-populated data
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Pre-populate with ordered keys
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    for i in 0..100 {
        actor.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/bench"),
                payload: KvMessage::Put {
                    key: Bytes::from(format!("key{:03}", i)),
                    value: Bytes::from(vec![0u8; 256]),
                },
            },
            &mut ctx,
        );
    }

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Commit,
        },
        &mut ctx,
    );

    let mut group = c.benchmark_group("kv_subsystem_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("scan_100_keys_no_limit", |b| {
        b.iter(|| {
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx,
            );

            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Scan {
                        start: black_box(Bytes::from("key000")),
                        end: black_box(Bytes::from("key999")),
                        limit: None,
                        reverse: false,
                    },
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

fn bench_rollback_discard(c: &mut Criterion) {
    // Arrange: Setup actor
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    let mut group = c.benchmark_group("kv_subsystem_rollback");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("begin_write_rollback_cycle", |b| {
        b.iter(|| {
            // Begin
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx,
            );

            // Write data
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Put {
                        key: black_box(Bytes::from("key")),
                        value: black_box(Bytes::from("value")),
                    },
                },
                &mut ctx,
            );

            // Rollback
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Rollback,
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_transaction_lifecycle, bench_multi_operation_transaction, bench_scan_ordered_results, bench_rollback_discard
}
criterion_main!(benches);
