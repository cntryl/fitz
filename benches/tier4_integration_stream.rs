//! Stream domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **MultiClient** - N concurrent WS clients (real concurrency)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, create_bench_stream_sink, extract_single_tlv_field,
    parse_stream_response, parse_stream_session_id, register_session_queue_sink, route_frame,
    shared_bench_runtime,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

#[stress_test]
fn should_complete_direct_append(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "append");

    let family = RouteFamily::new(1);
    let route = "stream://tier4/stream/direct/append";
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, 1);

    let begin_frame = build_stream_begin(route, 0);
    let (begin_msg_type, begin_payload) = extract_single_tlv_field(&begin_frame);
    route_frame(
        router.as_ref(),
        &source,
        route,
        1,
        ChannelId::Pub,
        begin_msg_type,
        begin_payload,
        family,
    )
    .expect("stream begin");
    let begin_responses = inbox.drain();
    let session_id = parse_stream_session_id(
        begin_responses
            .last()
            .expect("begin response")
            .payload
            .as_ref(),
    )
    .expect("session_id");

    let append_frame = build_stream_append(session_id, Bytes::from_static(b"event").as_ref());
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);

    ctx.measure(|| {
        route_frame(
            router.as_ref(),
            &source,
            route,
            1,
            ChannelId::Pub,
            append_msg_type,
            append_payload.clone(),
            family,
        )
        .expect("stream append");
        let _ = inbox.drain();
    });
}

#[stress_test]
fn should_complete_tcp_append(ctx: &mut StressContext) {
    ctx.set_elements(2);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let route = "stream://tier4/stream/tcp/append";
    let begin_frame = build_stream_begin(route, 0);

    let runtime = shared_bench_runtime();
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

    let runtime = shared_bench_runtime();
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

    ctx.measure(|| {
        let _results: Vec<_> =
            runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                let arc = arc.clone();
                let begin = begin_frame.clone();
                async move {
                    let mut c = arc.lock().await;
                    let response = c.request(&begin, 2000).await.expect("begin");
                    let (_msg_type, _status, data) = parse_stream_response(&response);
                    let session_id = parse_stream_session_id(&data).expect("session_id");
                    let append_frame = build_stream_append(session_id, b"event");
                    c.request(&append_frame, 2000).await.expect("append");
                }
            })));
    });
}

stress_main!();
