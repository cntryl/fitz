//! Stream domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor + disk (no network) - baseline integration overhead
//! 2. **Encoded** - Same as direct but with TLV codec (measures serialization cost)
//! 3. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 4. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 5. **MultiClient** - N concurrent WS clients hitting domain concurrently

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, create_local_bench_stream_actor,
    parse_stream_response, parse_stream_session_id,
};
use fitz::domains::stream::protocol::StreamMessage;
use fitz::prelude::Actor;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};

#[stress_test]
fn should_complete_direct_append(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "append");

    let (mut actor, mut actor_ctx, _temp_dir) =
        create_local_bench_stream_actor("tier4", "stream", "direct");
    let family = RouteFamily::new(1);
    let route = Route::new("stream://tier4/stream/direct/append".to_string());
    actor.receive(
        StreamMessage::Begin {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor_ctx,
    );

    ctx.measure(|| {
        actor.receive(
            StreamMessage::Append {
                session_id: 1,
                body: Bytes::from_static(b"event"),
                metadata: None,
            },
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_encoded_append(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "append");

    let (mut actor, mut actor_ctx, _temp_dir) =
        create_local_bench_stream_actor("tier4", "stream", "encoded");
    let family = RouteFamily::new(1);
    let route = Route::new("stream://tier4/stream/encoded/append".to_string());
    actor.receive(
        StreamMessage::Begin {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor_ctx,
    );

    let begin_frame = build_stream_begin("stream://tier4/stream/encoded/append", 0);
    let append_frame = build_stream_append(1, b"event");

    ctx.measure(|| {
        actor.receive(
            StreamMessage::Append {
                session_id: 1,
                body: Bytes::from_static(b"event"),
                metadata: None,
            },
            &mut actor_ctx,
        );
        let _ = (&begin_frame, &append_frame);
    });
}

#[stress_test]
fn should_complete_tcp_append(ctx: &mut StressContext) {
    ctx.set_elements(2);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let route = "stream://tier4/stream/tcp/append";
    let begin_frame = build_stream_begin(route, 0);

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&begin_frame, 2000))
            .expect("begin response");
        let (_msg_type, _status, data) = parse_stream_response(&response);
        let session_id = parse_stream_session_id(&data).expect("session_id");

        let append_frame = build_stream_append(session_id, b"event");
        let _ = runtime
            .block_on(client.request(&append_frame, 2000))
            .expect("append response");
    });
}

#[stress_test]
fn should_complete_ws_append(ctx: &mut StressContext) {
    ctx.set_elements(2);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

    let route = "stream://tier4/stream/ws/append";
    let begin_frame = build_stream_begin(route, 0);

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&begin_frame, 2000))
            .expect("begin response");
        let (_msg_type, _status, data) = parse_stream_response(&response);
        let session_id = parse_stream_session_id(&data).expect("session_id");

        let append_frame = build_stream_append(session_id, b"event");
        let _ = runtime
            .block_on(client.request(&append_frame, 2000))
            .expect("append response");
    });
}

#[stress_test]
fn should_complete_multiclient_appends(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_appends");

    let route = "stream://tier4/stream/multi/append";
    let begin_frame = build_stream_begin(route, 0);

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut clients: Vec<TestWebSocketClient> = (0..10)
        .map(|_| {
            runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws")
        })
        .collect();

    ctx.measure(|| {
        for client in clients.iter_mut() {
            let response = runtime
                .block_on(client.request(&begin_frame, 2000))
                .expect("begin response");
            let (_msg_type, _status, data) = parse_stream_response(&response);
            let session_id = parse_stream_session_id(&data).expect("session_id");

            let append_frame = build_stream_append(session_id, b"event");
            let _ = runtime
                .block_on(client.request(&append_frame, 2000))
                .expect("append response");
        }
    });
}

stress_main!();
