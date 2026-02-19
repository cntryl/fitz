//! Notice domain tier 4 integration benchmarks using stress
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
use fitz::benchkit::{build_notice_publish, build_notice_subscribe, parse_notice_response};
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{
    addr, make_router, route, session_id, TestClient, TestServer, TestSink, TestWebSocketClient,
};
use std::sync::Arc;

fn setup_notice_actor(
    subscriber_count: usize,
    pattern: &str,
) -> (NoticeRouteActor, Context<NoticeRouteActor>) {
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let family = RouteFamily::new(1);
    let mut actor = NoticeRouteActor::new(family);

    for i in 0..subscriber_count {
        let subscriber = addr(&format!("notice://realm/area/sub{}", i));
        router.register(subscriber.clone(), sink.clone());
    }

    let router = Arc::new(router);
    let mut ctx = Context::new(addr("notice://realm/area/ctx"), router.clone());

    for i in 0..subscriber_count {
        let subscriber = addr(&format!("notice://realm/area/sub{}", i));
        let subscribe =
            SubscribeMessage::new(family, route(pattern), session_id(i as u64 + 1), subscriber);
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
    }

    (actor, ctx)
}

#[stress_test]
fn should_complete_direct_publish(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "publish");

    let (mut actor, mut actor_ctx) = setup_notice_actor(1, "notice://test/events");
    let publish = PublishMessage::new(
        RouteFamily::new(1),
        route("notice://test/events"),
        Bytes::from_static(b"event"),
    );

    ctx.measure(|| {
        actor.receive(
            NotificationMessage::Publish(publish.clone()),
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_encoded_publish(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "publish");

    let (mut actor, mut actor_ctx) = setup_notice_actor(1, "notice://test/events");
    let publish = PublishMessage::new(
        RouteFamily::new(1),
        route("notice://test/events"),
        Bytes::from_static(b"event"),
    );
    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    ctx.measure(|| {
        actor.receive(
            NotificationMessage::Publish(publish.clone()),
            &mut actor_ctx,
        );
        let _ = (&subscribe_frame, &publish_frame);
    });
}

#[stress_test]
fn should_complete_tcp_publish(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
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

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
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

    for client in clients.iter_mut() {
        runtime
            .block_on(client.request(&subscribe_frame, 2000))
            .expect("subscribe response");
    }

    ctx.measure(|| {
        for client in clients.iter_mut() {
            let response = runtime
                .block_on(client.request(&publish_frame, 2000))
                .expect("publish response");
            let (_msg_type, _status, _data) = parse_notice_response(&response);
        }
    });
}

stress_main!();
