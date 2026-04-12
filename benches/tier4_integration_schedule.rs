//! Schedule domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **MultiClient** - N concurrent WS clients (real concurrency)

#[macro_use]
#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_test, StressContext};
use fitz::benchkit::{
    build_schedule_create, build_schedule_create_batch, create_local_bench_store,
    ensure_schedule_ok, shared_bench_runtime,
};
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::ScheduleMessage;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

const DIRECT_ROUTE_RING_SIZE: usize = 65_536;
const TRANSPORT_FRAME_RING_SIZE: usize = 8_192;
const CREATE_BATCH_WIDTH: usize = 32;

fn valid_schedule_route(prefix: &str, index: usize) -> String {
    format!("schedule://tier4/{prefix}/resource-{index}/run")
}

fn make_schedule_ctx() -> Context<ScheduleActor> {
    let router = Router::new();
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new(valid_schedule_route("direct", 0)),
    );
    Context::new(addr, Arc::new(router))
}

#[stress_test]
fn should_complete_direct_create(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "create");
    ctx.tag("measurement_scope", "direct_inproc");
    ctx.tag("batch_size", "single_create");

    let (db, _temp_dir) = create_local_bench_store();
    let mut actor = ScheduleActor::new(
        RouteFamily::new(1),
        db,
        cntryl_midge::WriteOptions::buffered(),
    );
    let mut actor_ctx = make_schedule_ctx();
    let route_ring: Vec<String> = (0..DIRECT_ROUTE_RING_SIZE)
        .map(|index| valid_schedule_route("direct", index))
        .collect();

    // Warmup direct actor path outside measurement.
    actor.receive(
        ScheduleMessage::Create {
            route: route_ring[0].clone(),
            cron: "0 * * * *".to_string(),
            payload: Bytes::from_static(b"payload"),
        },
        &mut actor_ctx,
    );

    let mut next_index = 1usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let route = &route_ring[next_index % route_ring.len()];
        next_index += 1;
        actor.receive(
            ScheduleMessage::Create {
                route: route.clone(),
                cron: "0 * * * *".to_string(),
                payload: Bytes::from_static(b"payload"),
            },
            &mut actor_ctx,
        );
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_tcp_create(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "create");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "single_create");

    let frame_ring: Vec<Vec<u8>> = (0..TRANSPORT_FRAME_RING_SIZE)
        .map(|index| {
            build_schedule_create(&valid_schedule_route("tcp", index), "0 * * * *", b"payload")
        })
        .collect();

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    // Warmup one request to reduce transport/session cold-start variance.
    let warmup = runtime
        .block_on(client.request(&frame_ring[0], 2000))
        .expect("warmup create response");
    ensure_schedule_ok(&warmup).expect("warmup create should succeed");

    let mut next_index = 1usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let frame = &frame_ring[next_index % frame_ring.len()];
        next_index += 1;
        let response = runtime
            .block_on(client.request(frame, 2000))
            .expect("create response");
        ensure_schedule_ok(&response).expect("create should succeed");
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_ws_create(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "create");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "single_create");

    let frame_ring: Vec<Vec<u8>> = (0..TRANSPORT_FRAME_RING_SIZE)
        .map(|index| {
            build_schedule_create(&valid_schedule_route("ws", index), "0 * * * *", b"payload")
        })
        .collect();

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    // Warmup one request to reduce transport/session cold-start variance.
    let warmup = runtime
        .block_on(client.request(&frame_ring[0], 2000))
        .expect("warmup create response");
    ensure_schedule_ok(&warmup).expect("warmup create should succeed");

    let mut next_index = 1usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let frame = &frame_ring[next_index % frame_ring.len()];
        next_index += 1;
        let response = runtime
            .block_on(client.request(frame, 2000))
            .expect("create response");
        ensure_schedule_ok(&response).expect("create should succeed");
    });
    ctx.set_elements(iterations as u64);

    runtime
        .block_on(client.close())
        .expect("close ws client gracefully");
}

#[stress_test]
fn should_complete_ws_batch_create(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "batch_create");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "32_creates_per_request");

    let frame_ring: Vec<Vec<u8>> = (0..TRANSPORT_FRAME_RING_SIZE)
        .map(|batch_index| {
            let routes: Vec<String> = (0..CREATE_BATCH_WIDTH)
                .map(|entry_index| {
                    valid_schedule_route(
                        "ws-batch",
                        (batch_index * CREATE_BATCH_WIDTH) + entry_index,
                    )
                })
                .collect();
            let entries: Vec<_> = routes
                .iter()
                .map(|route| (route.as_str(), "0 * * * *", b"payload".as_slice()))
                .collect();
            build_schedule_create_batch(&entries)
        })
        .collect();

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let warmup = runtime
        .block_on(client.request(&frame_ring[0], 2000))
        .expect("warmup batch create response");
    ensure_schedule_ok(&warmup).expect("warmup batch create should succeed");

    let mut next_index = 1usize;
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let frame = &frame_ring[next_index % frame_ring.len()];
        next_index += 1;
        let response = runtime
            .block_on(client.request(frame, 2000))
            .expect("batch create response");
        ensure_schedule_ok(&response).expect("batch create should succeed");
    });
    ctx.set_elements((CREATE_BATCH_WIDTH as u64) * iterations as u64);

    runtime
        .block_on(client.close())
        .expect("close ws client gracefully");
}

#[stress_test]
fn should_complete_multiclient_creates(ctx: &mut StressContext) {
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_creates");
    ctx.tag("measurement_scope", "ws_multiclient_e2e");
    ctx.tag("batch_size", "10_clients_1_create_each");
    ctx.tag("client_count", "10");
    let frame_rings: Vec<Vec<Vec<u8>>> = (0..10)
        .map(|client_index| {
            (0..TRANSPORT_FRAME_RING_SIZE)
                .map(|frame_index| {
                    build_schedule_create(
                        &valid_schedule_route(&format!("multi-{client_index}"), frame_index),
                        "0 * * * *",
                        b"payload",
                    )
                })
                .collect()
        })
        .collect();

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let clients: Vec<Arc<Mutex<TestWebSocketClient>>> = (0..10)
        .map(|_| {
            let c = runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws");
            Arc::new(Mutex::new(c))
        })
        .collect();

    // Warmup each client once outside measurement to reduce connection/setup skew.
    let _warmup: Vec<_> = runtime.block_on(futures::future::join_all(
        clients.iter().enumerate().map(|(client_index, arc)| {
            let arc = arc.clone();
            let frame = frame_rings[client_index][0].clone();
            async move {
                let mut c = arc.lock().await;
                let response = c.request(&frame, 2000).await.expect("warmup create");
                ensure_schedule_ok(&response).expect("warmup create should succeed");
            }
        }),
    ));

    let next_indices = Arc::new(std::sync::Mutex::new(vec![1usize; clients.len()]));
    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let next_indices = next_indices.clone();
        let _results: Vec<_> = runtime.block_on(futures::future::join_all(
            clients.iter().enumerate().map(|(client_index, arc)| {
                let arc = arc.clone();
                let frame = {
                    let mut indices = next_indices.lock().unwrap();
                    let index = indices[client_index] % frame_rings[client_index].len();
                    indices[client_index] += 1;
                    frame_rings[client_index][index].clone()
                };
                async move {
                    let mut c = arc.lock().await;
                    let response = c.request(&frame, 2000).await.expect("create");
                    ensure_schedule_ok(&response).expect("create should succeed");
                }
            }),
        ));
    });
    ctx.set_elements(10 * iterations as u64);

    let _closed: Vec<_> = runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
        let arc = arc.clone();
        async move {
            let mut c = arc.lock().await;
            c.close().await.expect("close ws client gracefully");
        }
    })));
}

stress_main_with_env!();
