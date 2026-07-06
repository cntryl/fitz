//! Queue domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor in-process using the same in-memory storage mode as `TestServer`
//! 2. **Encoded** - Same as direct but with TLV codec (measures serialization cost)
//! 3. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 4. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 5. **`MultiClient`** - N concurrent WS clients hitting domain concurrently

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_queue_enqueue, create_bench_queue_actor, parse_queue_response, shared_bench_runtime,
};
use fitz::domains::queue::protocol::QueueMessage;
use fitz::protocol::queue_codec::parse_request as queue_parse_request;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

const DIRECT_ENQUEUE_ROUNDS_PER_ITERATION: usize = 64;
const ENCODED_ENQUEUE_ROUNDS_PER_ITERATION: usize = 4;
const MULTICLIENT_ENQUEUE_ROUNDS_PER_ITERATION: usize = 32;
const TCP_ENQUEUE_ROUNDS_PER_ITERATION: usize = 4;
const WS_ENQUEUE_ROUNDS_PER_ITERATION: usize = 16;

fn setup_queue_actor() -> fitz::domains::queue::QueueActor {
    create_bench_queue_actor("tier4", "queue", "main", None)
}

#[stress(tier = 4)]
fn should_complete_direct_enqueue(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "enqueue");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter(
        "batch_size",
        format!("{DIRECT_ENQUEUE_ROUNDS_PER_ITERATION}_enqueues"),
    );

    let mut actor = setup_queue_actor();

    let iterations = ctx.measure_workload("complete_direct_enqueue", || {
        for _ in 0..DIRECT_ENQUEUE_ROUNDS_PER_ITERATION {
            let response = actor.handle_send(Bytes::from_static(b"msg"), None);
            assert!(matches!(
                response,
                fitz::domains::queue::QueueResponse::Sent { .. }
            ));
        }
    });
    stress_config::record_completed(ctx, DIRECT_ENQUEUE_ROUNDS_PER_ITERATION as u64 * iterations);
}

#[stress(tier = 4)]
fn should_complete_encoded_enqueue(ctx: &mut StressContext) {
    ctx.parameter("layer", "encoded");
    ctx.parameter("scenario", "enqueue");
    ctx.parameter("measurement_scope", "encoded_inproc");
    ctx.parameter("batch_size", "4_enqueues");

    let route = "queue://tier4/queue/main/enqueue";
    let mut actor = setup_queue_actor();
    let enqueue_frame = build_queue_enqueue(route, b"msg");
    let family = RouteFamily::new(1);

    let iterations = ctx.measure_workload("complete_encoded_enqueue", || {
        for _ in 0..ENCODED_ENQUEUE_ROUNDS_PER_ITERATION {
            let mut parser = TlvFrameParser::new(&enqueue_frame);
            let (msg_type, payload) = parser.next_field_ref().expect("enqueue field");
            let msg = queue_parse_request(msg_type, family, payload).expect("parse enqueue");
            let QueueMessage::Send {
                body,
                delay_seconds,
                ..
            } = msg
            else {
                panic!("expected queue send message");
            };
            let response = actor.handle_send(body, delay_seconds);
            assert!(matches!(
                response,
                fitz::domains::queue::QueueResponse::Sent { .. }
            ));
        }
    });
    stress_config::record_completed(
        ctx,
        ENCODED_ENQUEUE_ROUNDS_PER_ITERATION as u64 * iterations,
    );
}

#[stress(tier = 4)]
fn should_complete_tcp_enqueue(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "enqueue");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("batch_size", "4_enqueues");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let iterations = ctx.measure_workload("complete_tcp_enqueue", || {
        for _ in 0..TCP_ENQUEUE_ROUNDS_PER_ITERATION {
            let response = runtime
                .block_on(client.request(&enqueue_frame, 2000))
                .expect("enqueue response");
            let (_msg_type, _status, _data) = parse_queue_response(&response);
        }
    });
    stress_config::record_completed(ctx, TCP_ENQUEUE_ROUNDS_PER_ITERATION as u64 * iterations);
}

#[stress(tier = 4)]
fn should_complete_ws_enqueue(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "enqueue");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter(
        "batch_size",
        format!("{WS_ENQUEUE_ROUNDS_PER_ITERATION}_enqueues"),
    );

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let iterations = ctx.measure_workload("complete_ws_enqueue", || {
        for _ in 0..WS_ENQUEUE_ROUNDS_PER_ITERATION {
            let response = runtime
                .block_on(client.request(&enqueue_frame, 2000))
                .expect("enqueue response");
            let (_msg_type, _status, _data) = parse_queue_response(&response);
        }
    });
    stress_config::record_completed(ctx, WS_ENQUEUE_ROUNDS_PER_ITERATION as u64 * iterations);
}

#[stress(tier = 4)]
fn should_complete_multiclient_concurrent_enqueues(ctx: &mut StressContext) {
    measure_multiclient_concurrent_enqueues(
        ctx,
        "complete_multiclient_concurrent_enqueues",
        "concurrent_enqueues",
        10,
    );
}

fn measure_multiclient_concurrent_enqueues(
    ctx: &mut StressContext,
    name: &str,
    scenario: &'static str,
    client_count: usize,
) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "ws_multiclient_e2e");
    let batch_size =
        format!("{client_count}_clients_{MULTICLIENT_ENQUEUE_ROUNDS_PER_ITERATION}_rounds");
    ctx.parameter("batch_size", batch_size.as_str());
    let client_count_tag = client_count.to_string();
    ctx.parameter("client_count", client_count_tag.as_str());

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let clients: Vec<Arc<Mutex<TestWebSocketClient>>> = (0..client_count)
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

    let iterations = ctx.measure_workload(name, || {
        for _ in 0..MULTICLIENT_ENQUEUE_ROUNDS_PER_ITERATION {
            let _results: Vec<_> =
                runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                    let arc = arc.clone();
                    let frame = enqueue_frame.clone();
                    async move {
                        let mut c = arc.lock().await;
                        let response = c.request(&frame, 2000).await.expect("enqueue");
                        let _ = parse_queue_response(&response);
                    }
                })));
        }
    });
    stress_config::record_completed(
        ctx,
        (client_count * MULTICLIENT_ENQUEUE_ROUNDS_PER_ITERATION) as u64 * iterations,
    );
}

#[stress(tier = 4)]
fn should_complete_multiclient_concurrent_enqueues_client_scaling_4(ctx: &mut StressContext) {
    measure_multiclient_concurrent_enqueues(
        ctx,
        "complete_multiclient_concurrent_enqueues_client_scaling_4",
        "concurrent_enqueues_client_scaling",
        4,
    );
}

stress_main!();
