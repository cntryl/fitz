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
// TIER 3: SYSTEM BENCHMARKS
//
// Target: Measure FULL ENGINE PIPELINE including routing, authorization, TLV
// Goal: <50µs p50 for complete request lifecycle
// Throughput: 20k+ req/sec through full pipeline
//
// These benchmarks measure realistic request handling with all layers.
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

fn bench_end_to_end_write_workflow(c: &mut Criterion) {
    // Arrange: Setup actor with multiple concurrent transactions
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    let mut group = c.benchmark_group("kv_system_write_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("write_5kb_value_transactional", |b| {
        b.iter(|| {
            let large_value = black_box(Bytes::from(vec![0u8; 5 * 1024]));

            // Begin
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx,
            );

            // Write large value
            actor.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Put {
                        key: black_box(Bytes::from("large_key")),
                        value: large_value,
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

fn bench_end_to_end_read_workflow(c: &mut Criterion) {
    // Arrange: Setup actor with pre-populated data
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Pre-populate with large values
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    for i in 0..50 {
        actor.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/bench"),
                payload: KvMessage::Put {
                    key: Bytes::from(format!("key{:03}", i)),
                    value: Bytes::from(vec![0u8; 5 * 1024]),
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

    let mut group = c.benchmark_group("kv_system_read_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("read_5kb_value_from_storage", |b| {
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
                    payload: KvMessage::Get {
                        key: black_box(Bytes::from("key025")),
                    },
                },
                &mut ctx,
            )
        })
    });

    group.finish();
}

fn bench_range_delete_system(c: &mut Criterion) {
    // Arrange: Setup actor with pre-populated data
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = KvActor::new(family, store);
    let mut ctx = make_kv_ctx(family);

    // Pre-populate with many keys
    actor.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx,
    );

    for i in 0..1000 {
        actor.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/bench"),
                payload: KvMessage::Put {
                    key: Bytes::from(format!("key{:04}", i)),
                    value: Bytes::from("value"),
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

    let mut group = c.benchmark_group("kv_system_range_delete");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("delete_100_key_range", |b| {
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
                    payload: KvMessage::DeleteRange {
                        start: black_box(Bytes::from("key0200")),
                        end: black_box(Bytes::from("key0300")),
                    },
                },
                &mut ctx,
            );

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

fn bench_isolation_across_families(c: &mut Criterion) {
    // Arrange: Setup two actors for different RouteFamily IDs
    let family_a = RouteFamily::new(100);
    let family_b = RouteFamily::new(200);
    let store = make_store();

    let mut actor_a = KvActor::new(family_a, store.clone());
    let mut ctx_a = make_kv_ctx(family_a);

    let mut actor_b = KvActor::new(family_b, store);
    let mut ctx_b = make_kv_ctx(family_b);

    // Pre-populate both families
    actor_a.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx_a,
    );

    for i in 0..100 {
        actor_a.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/bench"),
                payload: KvMessage::Put {
                    key: Bytes::from(format!("key{:03}", i)),
                    value: Bytes::from("value_a"),
                },
            },
            &mut ctx_a,
        );
    }

    actor_a.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Commit,
        },
        &mut ctx_a,
    );

    actor_b.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Begin,
        },
        &mut ctx_b,
    );

    for i in 0..100 {
        actor_b.handle(
            KvRequest {
                id: Uuid::new_v4(),
                route: Route::new("kv://test/area/bench"),
                payload: KvMessage::Put {
                    key: Bytes::from(format!("key{:03}", i)),
                    value: Bytes::from("value_b"),
                },
            },
            &mut ctx_b,
        );
    }

    actor_b.handle(
        KvRequest {
            id: Uuid::new_v4(),
            route: Route::new("kv://test/area/bench"),
            payload: KvMessage::Commit,
        },
        &mut ctx_b,
    );

    let mut group = c.benchmark_group("kv_system_isolation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("concurrent_read_different_families", |b| {
        b.iter(|| {
            actor_a.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx_a,
            );

            let result_a = actor_a.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Get {
                        key: black_box(Bytes::from("key050")),
                    },
                },
                &mut ctx_a,
            );

            actor_b.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Begin,
                },
                &mut ctx_b,
            );

            let result_b = actor_b.handle(
                KvRequest {
                    id: Uuid::new_v4(),
                    route: Route::new("kv://test/area/bench"),
                    payload: KvMessage::Get {
                        key: black_box(Bytes::from("key050")),
                    },
                },
                &mut ctx_b,
            );

            (result_a, result_b)
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_end_to_end_write_workflow, bench_end_to_end_read_workflow, bench_range_delete_system, bench_isolation_across_families
}
criterion_main!(benches);
