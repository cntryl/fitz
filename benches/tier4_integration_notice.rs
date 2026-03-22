//! Notice domain tier 4 integration benchmarks using stress
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
    build_notice_publish, build_notice_subscribe, create_bench_notice_sink,
    extract_single_tlv_field, parse_notice_response, register_session_counting_sink, route_frame,
    shared_bench_runtime,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

#[stress_test]
fn should_complete_direct_publish(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "publish");

    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_notice_sink(router.clone());
    router.register_domain_pattern("notice", sink as Arc<dyn MailboxSink>);

    let (subscriber_source, _subscriber_sink) = register_session_counting_sink(&router, family, 1);
    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        router.as_ref(),
        &subscriber_source,
        "notice://test/events",
        1,
        ChannelId::Pub,
        subscribe_msg_type,
        subscribe_payload,
        family,
    )
    .expect("notice subscribe");

    let (publisher_source, _publisher_sink) = register_session_counting_sink(&router, family, 2);
    let publish_frame = build_notice_publish("notice://test/events", Bytes::from_static(b"event").as_ref());
    let (publish_msg_type, publish_payload) = extract_single_tlv_field(&publish_frame);

    ctx.measure(|| {
        route_frame(
            router.as_ref(),
            &publisher_source,
            "notice://test/events",
            2,
            ChannelId::Pub,
            publish_msg_type,
            publish_payload.clone(),
            family,
        )
        .expect("notice publish");
    });
}

#[stress_test]
fn should_complete_tcp_publish(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    runtime
        .block_on(client.request(&subscribe_frame, 2000))
        .expect("subscribe response");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&publish_frame, 2000))
            .expect("publish response");
        let (_msg_type, _status, _data) = parse_notice_response(&response);
    });
}

#[stress_test]
fn should_complete_ws_publish(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    runtime
        .block_on(client.request(&subscribe_frame, 2000))
        .expect("subscribe response");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&publish_frame, 2000))
            .expect("publish response");
        let (_msg_type, _status, _data) = parse_notice_response(&response);
    });
}

#[stress_test]
fn should_complete_multiclient_concurrent_publishes(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_publishers");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

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

    let sub = subscribe_frame.clone();
    runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
        let arc = arc.clone();
        let f = sub.clone();
        async move {
            let mut c = arc.lock().await;
            c.request(&f, 2000).await.expect("subscribe")
        }
    })));

    ctx.measure(|| {
        let publish_frame = publish_frame.clone();
        let _results: Vec<_> =
            runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                let arc = arc.clone();
                let frame = publish_frame.clone();
                async move {
                    let mut c = arc.lock().await;
                    let response = c.request(&frame, 2000).await.expect("publish");
                    let _ = parse_notice_response(&response);
                }
            })));
    });
}

stress_main!();
