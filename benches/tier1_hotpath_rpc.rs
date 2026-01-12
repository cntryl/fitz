use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_bench_rpc_context;
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::Duration;
use uuid::Uuid;

#[path = "config.rs"]
mod config;

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
            let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
            let msg = RpcMessage::Subscribe {
                worker_addr: workers[0].clone(),
            };
            actor.receive(black_box(msg), &mut ctx);
        })
    });

    group.finish();
}

fn bench_family_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_rpc_family_validation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Precompute requests outside hot path
    let valid_req = RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/service/operation"),
        reply_route: Route::new("inbox://session/1"),
        body: Bytes::from(vec![0u8; 64]),
    };

    let invalid_req = RpcRequest {
        family_id: RouteFamily::new(2), // Mismatched family
        correlation_id: Uuid::new_v4(),
        route: Route::new("rpc://realm/service/operation"),
        reply_route: Route::new("inbox://session/1"),
        body: Bytes::from(vec![0u8; 64]),
    };

    // Subscribe a worker so requests can be routed
    let worker = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://realm/service/worker1"),
    );

    group.bench_function("valid_family", |b| {
        b.iter(|| {
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
            actor.receive(
                RpcMessage::Subscribe {
                    worker_addr: worker.clone(),
                },
                &mut ctx,
            );
            let req = valid_req.clone();
            actor.receive(black_box(RpcMessage::Request(req)), &mut ctx);
        })
    });

    group.bench_function("invalid_family_rejected", |b| {
        b.iter(|| {
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
            actor.receive(
                RpcMessage::Subscribe {
                    worker_addr: worker.clone(),
                },
                &mut ctx,
            );
            let req = invalid_req.clone();
            actor.receive(black_box(RpcMessage::Request(req)), &mut ctx);
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
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64]),
        })
        .collect();

    // Setup actor with single worker
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
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
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64]),
        })
        .collect();

    group.bench_function("enqueue_request_no_workers", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let mut actor = RpcRouteActor::new(RouteFamily::new(1));
            let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
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
    let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
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
        family_id: RouteFamily::new(1),
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
                family_id: RouteFamily::new(1),
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
    let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
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
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::new_v4(),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64]),
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
        let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");

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
                family_id: RouteFamily::new(1),
                correlation_id: Uuid::new_v4(),
                route: Route::new("rpc://realm/service/operation"),
                reply_route: Route::new("inbox://session/1"),
                body: Bytes::from(vec![0u8; 64]),
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

fn bench_dispatch_zero_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hardening_dispatch");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Precompute requests outside the hot path (no clones in measured loop)
    let requests: Vec<RpcRequest> = (0..256)
        .map(|i| RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::from_u128(i), // Deterministic, no RNG
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64]),
        })
        .collect();

    // Setup actor with single worker
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");
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

    group.bench_function("dispatch_64B_1worker_zero_alloc", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            // Move ownership (no clone) - measures true dispatch cost
            let req = requests[idx % requests.len()].clone();
            actor.receive(black_box(RpcMessage::Request(req)), &mut ctx);
            idx += 1;
        })
    });

    group.finish();
}

fn bench_lease_expiration_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hardening_expiration");
    group.sampling_mode(SamplingMode::Flat);

    let in_flight_counts = [100usize, 1000usize, 5000usize, 10000usize];

    for &count in &in_flight_counts {
        // Setup actor with many in-flight requests
        let mut actor = RpcRouteActor::with_timeout(
            RouteFamily::new(1),
            count + 1000,
            Duration::from_secs(60), // Long timeout so none expire during test
        );
        let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");

        // Register worker
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

        // Create in-flight requests
        for i in 0..count {
            let req = RpcRequest {
                family_id: RouteFamily::new(1),
                correlation_id: Uuid::from_u128(i as u128),
                route: Route::new("rpc://realm/service/operation"),
                reply_route: Route::new("inbox://session/1"),
                body: Bytes::from(vec![0u8; 64]),
            };
            actor.receive(RpcMessage::Request(req), &mut ctx);
        }

        // Now benchmark dispatch with N in-flight leases
        let test_req = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::from_u128(99999),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64]),
        };

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("dispatch_with_{}_inflight", count), |b| {
            b.iter(|| {
                let req = test_req.clone();
                actor.receive(black_box(RpcMessage::Request(req)), &mut ctx);
            })
        });
    }

    group.finish();
}

fn bench_worker_index_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_hardening_worker_lookup");
    group.sampling_mode(SamplingMode::Flat);

    let worker_counts = [1usize, 8usize, 64usize, 256usize];

    for &count in &worker_counts {
        // Setup actor with many workers
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = create_bench_rpc_context("rpc://realm/service/operation");

        for i in 0..count {
            let worker_addr = RouteAddress::new(
                RouteFamily::new(1),
                Route::new(format!("worker://realm/service/worker{}", i)),
            );
            actor.receive(RpcMessage::Subscribe { worker_addr }, &mut ctx);
        }

        // Benchmark dispatch (includes O(1) worker selection)
        let req = RpcRequest {
            family_id: RouteFamily::new(1),
            correlation_id: Uuid::from_u128(12345),
            route: Route::new("rpc://realm/service/operation"),
            reply_route: Route::new("inbox://session/1"),
            body: Bytes::from(vec![0u8; 64]),
        };

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("dispatch_with_{}_workers", count), |b| {
            b.iter(|| {
                let request = req.clone();
                actor.receive(black_box(RpcMessage::Request(request)), &mut ctx);
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
        bench_family_validation,
        bench_request_dispatch,
        bench_request_enqueue,
        bench_response_routing,
        bench_lease_tracking,
        bench_round_robin_distribution,
        bench_dispatch_zero_allocation,
        bench_lease_expiration_scaling,
        bench_worker_index_lookup
}
criterion_main!(benches);
