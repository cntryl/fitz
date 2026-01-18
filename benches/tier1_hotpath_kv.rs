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
// TIER 1: HOT PATH MICROBENCHMARKS
//
// Target: Measure PURE actor operations WITHOUT scheduler overhead
// Goal: <1µs p50 for get/put, <5µs p50 for insert (with existence check)
// Throughput: 1M+ ops/sec for simple get/put
//
// These benchmarks call actor methods directly to measure the hot path.
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

fn bench_get_hotpath(c: &mut Criterion) {
    // Arrange: Setup actor with pre-populated transaction
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Pre-populate with data
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

    let mut group = c.benchmark_group("kv_hotpath_get");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_existing_key", |b| {
        b.iter(|| {
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Get {
                        key: black_box(Bytes::from("key050")),
                    },
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

fn bench_put_hotpath(c: &mut Criterion) {
    // Arrange: Setup with active transaction
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    // Pre-generate keys/values outside loop
    let keys: Vec<Bytes> = (0..100)
        .map(|i| Bytes::from(format!("key{:03}", i)))
        .collect();
    let values: Vec<Bytes> = (0..100)
        .map(|_| Bytes::from(vec![0u8; 256]))
        .collect();

    let mut key_idx = 0;

    let mut group = c.benchmark_group("kv_hotpath_put");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("put_new_key", |b| {
        b.iter(|| {
            let key = black_box(keys[key_idx % keys.len()].clone());
            let value = black_box(values[key_idx % values.len()].clone());
            key_idx = (key_idx + 1) % keys.len();

            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Put { key, value },
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

fn bench_insert_hotpath(c: &mut Criterion) {
    // Arrange: Setup with active transaction
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    // Generate keys outside loop
    let keys: Vec<Bytes> = (1000..1100)
        .map(|i| Bytes::from(format!("key{:04}", i)))
        .collect();

    let mut key_idx = 0;

    let mut group = c.benchmark_group("kv_hotpath_insert");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("insert_first_time", |b| {
        b.iter(|| {
            let key = black_box(keys[key_idx % keys.len()].clone());
            key_idx = (key_idx + 1) % keys.len();

            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Insert {
                        key,
                        value: Bytes::from(vec![0u8; 256]),
                    },
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
    targets = bench_get_hotpath, bench_put_hotpath, bench_insert_hotpath
}
criterion_main!(benches);
