use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use uuid::Uuid;
use bytes::Bytes;

#[path = "config.rs"]
mod config;

fn make_ctx() -> Context<RpcRouteActor> {
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://realm/service/operation"),
    );
    Context::new(addr, router)
}

fn bench_worker_subscribe(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_subscribe");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Precompute worker addresses outside the hot path
    let workers: Vec<RouteAddress> = (0..64)
        .map(|i| {
            RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://realm/service/worker{}", i)),
            )
        })
        .collect();

    group.bench_function("subscribe_single_worker", |b| {
        b.iter(|| {
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = make_ctx();
            let msg = RpcMessage::Subscribe {
                worker_addr: workers[0].clone(),
            };
            actor.receive(black_box(msg), &mut ctx);
        })
    });

    group.bench_function("subscribe_64_workers", |b| {
        b.iter(|| {
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = make_ctx();
            for worker in &workers {
                let msg = RpcMessage::Subscribe {
                    worker_addr: worker.clone(),
                };
                actor.receive(msg, &mut ctx);
            }
        })
    });

    group.finish();
}

fn bench_request_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_dispatch");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Precompute requests outside the hot path
    let requests: Vec<RpcRequest> = (0..256)
        .map(|_| RpcRequest {
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64])
        })
        .collect();

    // Setup actor with single worker
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();
    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    group.bench_function("dispatch_request_to_worker", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let req = requests[idx % requests.len()].clone();
            let msg = RpcMessage::Request(req);
            actor.receive(black_box(msg), &mut ctx);
            idx += 1;
        })
    });

    group.finish();
}

fn bench_request_enqueue(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_enqueue");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Precompute requests outside the hot path
    let requests: Vec<RpcRequest> = (0..256)
        .map(|_| RpcRequest {
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64])
        })
        .collect();

    group.bench_function("enqueue_request_no_workers", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = make_ctx();
            let req = requests[idx % requests.len()].clone();
            let msg = RpcMessage::Request(req);
            actor.receive(black_box(msg), &mut ctx);
            idx += 1;
        })
    });

    group.finish();
}

fn bench_response_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_response");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Setup: actor with worker and dispatched request
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();
    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Dispatch initial request to establish lease
    let initial_cid = Uuid::new_v4();
    let initial_req = RpcRequest {
        correlation_id: initial_cid,
        route: Route::new("rpc://realm/service/operation"),
        reply_route: Route::new("inbox://session/1"),
        body: Bytes::from(vec![0u8; 64]),
    };
    actor.receive(RpcMessage::Request(initial_req), &mut ctx);

    // Precompute responses outside the hot path
    let responses: Vec<RpcResponse> = (0..256)
        .map(|i| {
            let cid = if i == 0 { initial_cid } else { Uuid::new_v4() };
            RpcResponse {
                correlation_id: cid,
                seq: 0,
                stream_end: true,
                body: Bytes::from(vec![0u8; 64]),
            }
        })
        .collect();

    group.bench_function("handle_single_chunk_response", |b| {
        b.iter(|| {
            let resp = responses[0].clone();
            let msg = RpcMessage::Response(resp);
            actor.receive(black_box(msg), &mut ctx);
            
            // Re-establish lease for next iteration
            let req = RpcRequest {
                correlation_id: initial_cid,
                route: Route::new("rpc://realm/service/operation"),
                reply_route: Route::new("inbox://session/1"),
                body: Bytes::from(vec![0u8; 64]),
            };
            actor.receive(RpcMessage::Request(req), &mut ctx);
        })
    });

    group.finish();
}

fn bench_lease_tracking(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_lease");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Setup actor with worker
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();
    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Precompute requests
    let requests: Vec<RpcRequest> = (0..256)
        .map(|_| RpcRequest {
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64])
        })
        .collect();

    group.bench_function("track_active_leases", |b| {
        b.iter(|| {
            let req = requests[0].clone();
            let msg = RpcMessage::Request(req);
            actor.receive(black_box(msg), &mut ctx);
            let _count = actor.active_leases();
        })
    });

    group.finish();
}

fn bench_round_robin_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_round_robin");
    group.sampling_mode(SamplingMode::Flat);

    let worker_counts = [1usize, 4usize, 16usize, 64usize];

    for &worker_count in &worker_counts {
        // Setup actor with N workers
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();
        
        for i in 0..worker_count {
            let worker_addr = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://realm/service/worker{}", i)),
            );
            actor.receive(
                RpcMessage::Subscribe {
                    worker_addr: worker_addr.clone(),
                },
                &mut ctx,
            );
        }

        // Precompute requests
        let requests: Vec<RpcRequest> = (0..256)
            .map(|_| RpcRequest {
                correlation_id: Uuid::new_v4(),
                route: Route::new("rpc://realm/service/operation"),
                reply_route: Route::new("inbox://session/1"),
                body: Bytes::from(vec![0u8; 64])
            })
            .collect();

        let name = format!("distribute_to_{}_workers", worker_count);
        group.throughput(Throughput::Elements(worker_count as u64));
        
        group.bench_function(&name, |b| {
            let mut idx = 0usize;
            b.iter(|| {
                for _ in 0..worker_count {
                    let req = requests[idx % requests.len()].clone();
                    let msg = RpcMessage::Request(req);
                    actor.receive(msg, &mut ctx);
                    idx += 1;
                }
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_worker_subscribe,
        bench_request_dispatch,
        bench_request_enqueue,
        bench_response_routing,
        bench_lease_tracking,
        bench_round_robin_distribution
}
criterion_main!(benches);



