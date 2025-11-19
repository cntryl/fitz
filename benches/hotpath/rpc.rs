//! Hotpath benchmarks for RPC domain.
//!
//! Measures ONLY the internal logic of the RPC service:
//!   - subscribe/unsubscribe handler
//!   - route request lookup
//!   - route reply delivery
//!
//! Zero frame parsing, zero engine, zero allocations per op.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::rpc::RpcService;
use fitz::routing::{GlobalInternTable, DEFAULT_RF};
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn intern_route(table: &Arc<GlobalInternTable>, s: &str) -> String {
    // NOTE: RpcService currently expects &str / String for handler registration.
    // Interning still helps because parsing is cheap + repeated strings dedupe.
    table.intern(s);
    s.to_string()
}

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn bench_hot_subscribe(c: &mut Criterion) {
    let routes = Arc::new(GlobalInternTable::new());
    let route = intern_route(&routes, "rpc://realm/area/handler");

    let mut group = c.benchmark_group("rpc_hot_subscribe");
    group.bench_function("subscribe", |b| {
        b.iter_batched(
            || RpcService::new(routes.clone()),
            |mut svc| {
                let _ = svc.subscribe_handler(DEFAULT_RF, route.clone(), 1);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_unsubscribe(c: &mut Criterion) {
    let routes = Arc::new(GlobalInternTable::new());
    let route = intern_route(&routes, "rpc://realm/area/handler");

    let mut group = c.benchmark_group("rpc_hot_unsubscribe");
    group.bench_function("unsubscribe", |b| {
        b.iter_batched(
            || {
                let mut svc = RpcService::new(routes.clone());
                let id = svc.subscribe_handler(DEFAULT_RF, route.clone(), 1);
                (svc, id)
            },
            |(mut svc, id)| {
                svc.unsubscribe(DEFAULT_RF, black_box(id));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_route_request(c: &mut Criterion) {
    let routes = Arc::new(GlobalInternTable::new());
    let route = intern_route(&routes, "rpc://realm/area/handler");

    let mut svc = RpcService::new(routes.clone());
    svc.subscribe_handler(DEFAULT_RF, route.clone(), 1);
    let svc = Arc::new(svc);

    let mut group = c.benchmark_group("rpc_hot_route_request");
    group.bench_function("route_request", |b| {
        b.iter(|| {
            let _ = svc.route_request(
                DEFAULT_RF,
                black_box(route.as_str()),
                Some("corr123"),
                Some("inbox://reply"),
                black_box(b"body"),
            );
        })
    });
    group.finish();
}

fn bench_hot_route_reply(c: &mut Criterion) {
    let routes = Arc::new(GlobalInternTable::new());
    let route = intern_route(&routes, "rpc://realm/area/handler");

    let mut svc = RpcService::new(routes.clone());

    // Setup inbox
    let inbox = svc.allocate_inbox(2);
    svc.subscribe_inbox(DEFAULT_RF, inbox.clone(), 2);

    // Register outstanding request
    svc.register_request("corr123".into(), route.clone(), inbox.clone());

    let svc = Arc::new(svc);

    let mut group = c.benchmark_group("rpc_hot_route_reply");
    group.bench_function("route_reply", |b| {
        b.iter(|| {
            let _ = svc.route_reply(
                DEFAULT_RF,
                black_box(&inbox),
                Some("corr123"),
                black_box(b"reply"),
                None,
                false,
            );
        })
    });
    group.finish();
}

criterion_group!(
    name = hotpath_rpc;
    config = config::criterion_config();
    targets =
        bench_hot_subscribe,
        bench_hot_unsubscribe,
        bench_hot_route_request,
        bench_hot_route_reply
);
criterion_main!(hotpath_rpc);
