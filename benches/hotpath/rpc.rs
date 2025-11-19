//! Hotpath benchmarks for RPC domain.
//!
//! Measures ONLY the internal logic of the RPC service:
//!   - Subscribe: register handler for RPC routes
//!   - Unsubscribe: remove handler registration
//!   - Route request: find and deliver to handlers
//!   - Route reply: deliver to inbox subscribers
//!
//! Zero frame parsing, zero engine, zero outbound delivery.
//! This is the true "business logic" bench.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::rpc::RpcService;
use fitz::routing::GlobalInternTable;
use parking_lot::RwLock;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn bench_hot_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hot_subscribe");
    group.bench_function("subscribe", |b| {
        b.iter_batched(
            || Arc::new(RwLock::new(RpcService::new(Arc::new(GlobalInternTable::new())))),
            |svc| {
                let mut service = svc.write();
                let _sub_id =
                    service.subscribe_handler(0, "rpc://realm/area/handler".to_string(), 1);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_unsubscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hot_unsubscribe");
    group.bench_function("unsubscribe", |b| {
        b.iter_batched(
            || {
                let svc = Arc::new(RwLock::new(RpcService::new(Arc::new(GlobalInternTable::new()))));
                let sub_id =
                    svc.write()
                        .subscribe_handler(0, "rpc://realm/area/handler".to_string(), 1);
                (svc, sub_id)
            },
            |(svc, sub_id)| {
                let mut service = svc.write();
                service.unsubscribe(0, sub_id);
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_route_request(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hot_route_request");
    group.bench_function("route_request", |b| {
        b.iter_batched(
            || {
                let svc = Arc::new(RwLock::new(RpcService::new(Arc::new(GlobalInternTable::new()))));
                {
                    let mut service = svc.write();
                    service.subscribe_handler(0, "rpc://realm/area/handler".to_string(), 1);
                }
                svc
            },
            |svc| {
                let service = svc.read();
                let _result = service.route_request(
                    0,
                    "rpc://realm/area/handler",
                    Some("corr123"),
                    Some("inbox://reply"),
                    b"test body",
                );
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_route_reply(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hot_route_reply");
    group.bench_function("route_reply", |b| {
        b.iter_batched(
            || {
                let svc = Arc::new(RwLock::new(RpcService::new(Arc::new(GlobalInternTable::new()))));
                {
                    let mut service = svc.write();
                    let _ = service.subscribe_inbox(0, "inbox://client/inbox".to_string(), 2);
                    service.register_request(
                        "corr123".to_string(),
                        "rpc://realm/area/handler".to_string(),
                        "inbox://client/inbox".to_string(),
                    );
                }
                svc
            },
            |svc| {
                let service = svc.read();
                let _result = service.route_reply(
                    0,
                    "inbox://client/inbox",
                    Some("corr123"),
                    b"reply body",
                    None,
                    false,
                );
            },
            BatchSize::SmallInput,
        )
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
