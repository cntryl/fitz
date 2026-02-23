//! RPC domain tier 3 system benchmarks using stress.
//!
//! Measures the **real in-proc RPC path**: request → route actor (correlation, dispatch)
//! → worker (Deliver) → response + ack → route actor (release lease).
//! Uses router + mailboxes so ctx.send() and message delivery are included.

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use criterion::black_box;
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use fitz::domains::rpc::RpcRouteActor;
use fitz::runtime::actor::{Actor, Context};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::mailbox::Mailbox;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use uuid::Uuid;

/// Bench worker: receives Deliver, immediately sends Response + Ack to route.
/// Exercises correlation map and mailbox delivery.
struct BenchRpcWorker {
    route_addr: RouteAddress,
}

impl Actor for BenchRpcWorker {
    type Message = RpcMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        if let RpcMessage::Deliver(work_item) = msg {
            let resp = RpcResponse::single(work_item.correlation_id, work_item.body.clone());
            let _ = ctx.send(self.route_addr.clone(), RpcMessage::Response(resp));
            let _ = ctx.send(
                self.route_addr.clone(),
                RpcMessage::Ack {
                    correlation_id: work_item.correlation_id,
                },
            );
        }
    }
}

const ROUTE_STR: &str = "rpc://bench/system/route";
const WORKER_STR_PREFIX: &str = "worker://bench/system/w";
const CLIENT_ADDR_STR: &str = "inbox://session/bench";
const MAILBOX_CAP: usize = 10_000;

/// Result of setting up one RPC route actor and one worker (actors, contexts, router, mailboxes, addresses).
type SetupOneWorkerResult = (
    RpcRouteActor,
    BenchRpcWorker,
    Context<RpcRouteActor>,
    Context<BenchRpcWorker>,
    Arc<Router>,
    Mailbox,
    Mailbox,
    RouteAddress,
    RouteAddress,
    RouteAddress,
);

/// Setup router, route actor, one worker, mailboxes; subscribe worker.
/// Returns route/worker mailboxes (clones used for receiving; originals are in the router).
fn setup_one_worker() -> SetupOneWorkerResult {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let route_addr = RouteAddress::new(family, Route::new(ROUTE_STR));
    let worker_addr = RouteAddress::new(family, Route::new(format!("{WORKER_STR_PREFIX}0")));
    let client_addr = RouteAddress::new(family, Route::new(CLIENT_ADDR_STR));

    let route_mb = Mailbox::new(MAILBOX_CAP);
    let worker_mb = Mailbox::new(MAILBOX_CAP);
    let route_mb_rx = route_mb.clone();
    let worker_mb_rx = worker_mb.clone();

    router.register(route_addr.clone(), Arc::new(route_mb));
    router.register(worker_addr.clone(), Arc::new(worker_mb));

    let mut route_actor = RpcRouteActor::new(family);
    let mut worker_actor = BenchRpcWorker {
        route_addr: route_addr.clone(),
    };
    let mut route_ctx = Context::new(route_addr.clone(), router.clone());
    let mut worker_ctx = Context::new(worker_addr.clone(), router.clone());

    // Subscribe worker
    let subscribe = RpcMessage::Subscribe {
        worker_addr: worker_addr.clone(),
    };
    let env = Envelope::from_route(client_addr.clone(), route_addr.clone(), subscribe);
    let _ = router.route(env);
    drain_rpc_loop(&mut DrainRpcLoopParams {
        route_mb: &route_mb_rx,
        worker_mb: &worker_mb_rx,
        route_actor: &mut route_actor,
        worker_actor: &mut worker_actor,
        route_ctx: &mut route_ctx,
        worker_ctx: &mut worker_ctx,
    });

    (
        route_actor,
        worker_actor,
        route_ctx,
        worker_ctx,
        router,
        route_mb_rx,
        worker_mb_rx,
        route_addr,
        worker_addr,
        client_addr,
    )
}

/// Parameters for draining both RPC mailboxes and dispatching to route and worker.
struct DrainRpcLoopParams<'a> {
    route_mb: &'a Mailbox,
    worker_mb: &'a Mailbox,
    route_actor: &'a mut RpcRouteActor,
    worker_actor: &'a mut BenchRpcWorker,
    route_ctx: &'a mut Context<RpcRouteActor>,
    worker_ctx: &'a mut Context<BenchRpcWorker>,
}

/// Drain both mailboxes until empty, dispatching to route or worker.
fn drain_rpc_loop(params: &mut DrainRpcLoopParams<'_>) {
    loop {
        let mut progress = false;
        while let Ok(env) = params.route_mb.receiver().try_recv() {
            progress = true;
            if let Some(msg) = env.into_payload::<RpcMessage>() {
                params.route_actor.receive(msg, params.route_ctx);
            }
        }
        while let Ok(env) = params.worker_mb.receiver().try_recv() {
            progress = true;
            if let Some(msg) = env.into_payload::<RpcMessage>() {
                params.worker_actor.receive(msg, params.worker_ctx);
            }
        }
        if !progress {
            break;
        }
    }
}

#[stress_test]
fn should_complete_request_dispatch_sustained(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.set_elements(ITERS);
    ctx.tag("scenario", "sustained_dispatch");

    let (
        mut route_actor,
        mut worker_actor,
        mut route_ctx,
        mut worker_ctx,
        router,
        route_mb,
        worker_mb,
        route_addr,
        _worker_addr,
        client_addr,
    ) = setup_one_worker();

    let payload = Bytes::from_static(b"rpc request payload");

    ctx.measure(|| {
        for _ in 0..ITERS {
            let request = RpcRequest {
                family_id: RouteFamily::new(1),
                correlation_id: Uuid::new_v4(),
                route: Route::new(ROUTE_STR),
                reply_route: Route::new(CLIENT_ADDR_STR),
                body: payload.clone(),
            };
            let env = Envelope::from_route(
                client_addr.clone(),
                route_addr.clone(),
                RpcMessage::Request(request),
            );
            let _ = router.route(env);
            drain_rpc_loop(&mut DrainRpcLoopParams {
                route_mb: &route_mb,
                worker_mb: &worker_mb,
                route_actor: &mut route_actor,
                worker_actor: &mut worker_actor,
                route_ctx: &mut route_ctx,
                worker_ctx: &mut worker_ctx,
            });
        }
        black_box(&route_actor);
    });
}

#[stress_test]
fn should_complete_response_streaming_throughput(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.set_elements(ITERS);
    ctx.tag("scenario", "response_streaming");

    let (
        mut route_actor,
        mut worker_actor,
        mut route_ctx,
        mut worker_ctx,
        router,
        route_mb,
        worker_mb,
        route_addr,
        _worker_addr,
        client_addr,
    ) = setup_one_worker();

    let payload = Bytes::from_static(b"streaming request");

    ctx.measure(|| {
        for _ in 0..ITERS {
            let request = RpcRequest {
                family_id: RouteFamily::new(1),
                correlation_id: Uuid::new_v4(),
                route: Route::new(ROUTE_STR),
                reply_route: Route::new(CLIENT_ADDR_STR),
                body: payload.clone(),
            };
            let env = Envelope::from_route(
                client_addr.clone(),
                route_addr.clone(),
                RpcMessage::Request(request),
            );
            let _ = router.route(env);
            drain_rpc_loop(&mut DrainRpcLoopParams {
                route_mb: &route_mb,
                worker_mb: &worker_mb,
                route_actor: &mut route_actor,
                worker_actor: &mut worker_actor,
                route_ctx: &mut route_ctx,
                worker_ctx: &mut worker_ctx,
            });
        }
        black_box(&route_actor);
    });
}

#[stress_test]
fn should_complete_worker_pool_scaling_64_workers(ctx: &mut StressContext) {
    const ITERS: u64 = 500;
    ctx.set_elements(ITERS);
    ctx.tag("scenario", "scaling_64");

    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let route_addr = RouteAddress::new(family, Route::new(ROUTE_STR));
    let client_addr = RouteAddress::new(family, Route::new(CLIENT_ADDR_STR));

    let route_mb = Mailbox::new(MAILBOX_CAP);
    let route_mb_rx = route_mb.clone();
    router.register(route_addr.clone(), Arc::new(route_mb));

    let mut route_actor = RpcRouteActor::new(family);
    let mut route_ctx = Context::new(route_addr.clone(), router.clone());

    let worker_addrs: Vec<RouteAddress> = (0..64)
        .map(|i| RouteAddress::new(family, Route::new(format!("{WORKER_STR_PREFIX}{}", i))))
        .collect();

    let mut worker_mailboxes: Vec<(Mailbox, BenchRpcWorker, Context<BenchRpcWorker>)> =
        worker_addrs
            .iter()
            .map(|addr| {
                let mb = Mailbox::new(MAILBOX_CAP);
                let mb_rx = mb.clone();
                router.register(addr.clone(), Arc::new(mb));
                (
                    mb_rx,
                    BenchRpcWorker {
                        route_addr: route_addr.clone(),
                    },
                    Context::new(addr.clone(), router.clone()),
                )
            })
            .collect();

    for addr in &worker_addrs {
        let env = Envelope::from_route(
            client_addr.clone(),
            route_addr.clone(),
            RpcMessage::Subscribe {
                worker_addr: addr.clone(),
            },
        );
        let _ = router.route(env);
    }

    // Drain route only (subscribe msgs)
    for _ in 0..64 {
        if let Ok(env) = route_mb_rx.receiver().try_recv() {
            if let Some(msg) = env.into_payload::<RpcMessage>() {
                route_actor.receive(msg, &mut route_ctx);
            }
        }
    }

    let payload = Bytes::from_static(b"scaling payload");

    ctx.measure(|| {
        for _ in 0..ITERS {
            let request = RpcRequest {
                family_id: family,
                correlation_id: Uuid::new_v4(),
                route: Route::new(ROUTE_STR),
                reply_route: Route::new(CLIENT_ADDR_STR),
                body: payload.clone(),
            };
            let env = Envelope::from_route(
                client_addr.clone(),
                route_addr.clone(),
                RpcMessage::Request(request),
            );
            let _ = router.route(env);
            drain_rpc_loop_multi(
                &route_mb_rx,
                &mut worker_mailboxes,
                &mut route_actor,
                &mut route_ctx,
            );
        }
        black_box(&route_actor);
    });
}

#[stress_test]
fn should_complete_worker_pool_scaling_256_workers(ctx: &mut StressContext) {
    const ITERS: u64 = 200;
    ctx.set_elements(ITERS);
    ctx.tag("scenario", "scaling_256");

    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let route_addr = RouteAddress::new(family, Route::new(ROUTE_STR));
    let client_addr = RouteAddress::new(family, Route::new(CLIENT_ADDR_STR));

    let route_mb = Mailbox::new(MAILBOX_CAP);
    let route_mb_rx = route_mb.clone();
    router.register(route_addr.clone(), Arc::new(route_mb));

    let mut route_actor = RpcRouteActor::new(family);
    let mut route_ctx = Context::new(route_addr.clone(), router.clone());

    let worker_addrs: Vec<RouteAddress> = (0..256)
        .map(|i| RouteAddress::new(family, Route::new(format!("{WORKER_STR_PREFIX}256_{}", i))))
        .collect();

    let mut worker_mailboxes: Vec<(Mailbox, BenchRpcWorker, Context<BenchRpcWorker>)> =
        worker_addrs
            .iter()
            .map(|addr| {
                let mb = Mailbox::new(MAILBOX_CAP);
                let mb_rx = mb.clone();
                router.register(addr.clone(), Arc::new(mb));
                (
                    mb_rx,
                    BenchRpcWorker {
                        route_addr: route_addr.clone(),
                    },
                    Context::new(addr.clone(), router.clone()),
                )
            })
            .collect();

    for addr in &worker_addrs {
        let env = Envelope::from_route(
            client_addr.clone(),
            route_addr.clone(),
            RpcMessage::Subscribe {
                worker_addr: addr.clone(),
            },
        );
        let _ = router.route(env);
    }

    for _ in 0..256 {
        if let Ok(env) = route_mb_rx.receiver().try_recv() {
            if let Some(msg) = env.into_payload::<RpcMessage>() {
                route_actor.receive(msg, &mut route_ctx);
            }
        }
    }

    let payload = Bytes::from_static(b"scaling payload");

    ctx.measure(|| {
        for _ in 0..ITERS {
            let request = RpcRequest {
                family_id: family,
                correlation_id: Uuid::new_v4(),
                route: Route::new(ROUTE_STR),
                reply_route: Route::new(CLIENT_ADDR_STR),
                body: payload.clone(),
            };
            let env = Envelope::from_route(
                client_addr.clone(),
                route_addr.clone(),
                RpcMessage::Request(request),
            );
            let _ = router.route(env);
            drain_rpc_loop_multi(
                &route_mb_rx,
                &mut worker_mailboxes,
                &mut route_actor,
                &mut route_ctx,
            );
        }
        black_box(&route_actor);
    });
}

/// Drain route and all worker mailboxes until quiet (multi-worker).
///
/// Cost is O(workers) per call: we iterate over every worker mailbox each round.
/// The route actor itself uses O(1) worker selection (ready_queue); the scaling_256
/// vs scaling_64 gap is from this drain loop, not from dispatch.
fn drain_rpc_loop_multi(
    route_mb: &Mailbox,
    worker_mailboxes: &mut [(Mailbox, BenchRpcWorker, Context<BenchRpcWorker>)],
    route_actor: &mut RpcRouteActor,
    route_ctx: &mut Context<RpcRouteActor>,
) {
    loop {
        let mut progress = false;
        while let Ok(env) = route_mb.receiver().try_recv() {
            progress = true;
            if let Some(msg) = env.into_payload::<RpcMessage>() {
                route_actor.receive(msg, route_ctx);
            }
        }
        for (_mb, worker_actor, worker_ctx) in worker_mailboxes.iter_mut() {
            while let Ok(env) = _mb.receiver().try_recv() {
                progress = true;
                if let Some(msg) = env.into_payload::<RpcMessage>() {
                    worker_actor.receive(msg, worker_ctx);
                }
            }
        }
        if !progress {
            break;
        }
    }
}

#[stress_test]
fn should_complete_concurrent_request_tracking(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.set_elements(ITERS);
    ctx.tag("scenario", "concurrent_tracking");

    let (
        mut route_actor,
        mut worker_actor,
        mut route_ctx,
        mut worker_ctx,
        router,
        route_mb,
        worker_mb,
        route_addr,
        _worker_addr,
        client_addr,
    ) = setup_one_worker();

    let payload = Bytes::from_static(b"concurrent request");

    ctx.measure(|| {
        for _ in 0..ITERS {
            let request = RpcRequest {
                family_id: RouteFamily::new(1),
                correlation_id: Uuid::new_v4(),
                route: Route::new(ROUTE_STR),
                reply_route: Route::new(CLIENT_ADDR_STR),
                body: payload.clone(),
            };
            let env = Envelope::from_route(
                client_addr.clone(),
                route_addr.clone(),
                RpcMessage::Request(request),
            );
            let _ = router.route(env);
            drain_rpc_loop(&mut DrainRpcLoopParams {
                route_mb: &route_mb,
                worker_mb: &worker_mb,
                route_actor: &mut route_actor,
                worker_actor: &mut worker_actor,
                route_ctx: &mut route_ctx,
                worker_ctx: &mut worker_ctx,
            });
        }
        black_box(&route_actor);
    });
}

#[stress_test]
fn should_complete_mixed_request_response_workflow(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("scenario", "mixed_workload");

    let (
        mut route_actor,
        mut worker_actor,
        mut route_ctx,
        mut worker_ctx,
        router,
        route_mb,
        worker_mb,
        route_addr,
        _worker_addr,
        client_addr,
    ) = setup_one_worker();

    let payload = Bytes::from_static(b"mixed workload");

    ctx.measure(|| {
        for _ in 0..10 {
            let request = RpcRequest {
                family_id: RouteFamily::new(1),
                correlation_id: Uuid::new_v4(),
                route: Route::new(ROUTE_STR),
                reply_route: Route::new(CLIENT_ADDR_STR),
                body: payload.clone(),
            };
            let env = Envelope::from_route(
                client_addr.clone(),
                route_addr.clone(),
                RpcMessage::Request(request),
            );
            let _ = router.route(env);
            drain_rpc_loop(&mut DrainRpcLoopParams {
                route_mb: &route_mb,
                worker_mb: &worker_mb,
                route_actor: &mut route_actor,
                worker_actor: &mut worker_actor,
                route_ctx: &mut route_ctx,
                worker_ctx: &mut worker_ctx,
            });
        }
        black_box(&route_actor);
    });
}

stress_main!();
