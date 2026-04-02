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
    build_notice_publish, build_notice_subscribe, build_notice_unsubscribe,
    create_bench_notice_sink, extract_single_tlv_field, parse_notice_delivery,
    parse_notice_response, parse_notice_subscription_id, register_session_counting_sink,
    route_frame, shared_bench_runtime,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

#[stress_test]
fn should_complete_direct_publish(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "publish");
    ctx.tag("measurement_scope", "direct_inproc");
    ctx.tag("batch_size", "single_publish");
    ctx.tag("subscriber_count", "1");

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
    let publish_frame = build_notice_publish(
        "notice://test/events",
        Bytes::from_static(b"event").as_ref(),
    );
    let (publish_msg_type, publish_payload) = extract_single_tlv_field(&publish_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
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
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_tcp_publish(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "publish");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "single_publish");
    ctx.tag("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut subscriber = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp subscriber");
    let mut publisher = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp publisher");

    runtime
        .block_on(subscriber.request(&subscribe_frame, 2000))
        .expect("subscribe response");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        runtime
            .block_on(publisher.send_frame(&publish_frame))
            .expect("publish frame");

        let notification = runtime
            .block_on(subscriber.recv_frame(2000))
            .expect("publish notification");
        let _ = parse_notice_delivery(&notification);
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_ws_publish(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "publish");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "single_publish");
    ctx.tag("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut subscriber = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws subscriber");
    let mut publisher = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws publisher");

    runtime
        .block_on(subscriber.request(&subscribe_frame, 2000))
        .expect("subscribe response");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        runtime
            .block_on(publisher.send_frame(&publish_frame))
            .expect("publish frame");

        let notification = runtime
            .block_on(subscriber.recv_frame(2000))
            .expect("publish notification");
        let _ = parse_notice_delivery(&notification);
    });
    ctx.set_elements(iterations as u64);

    runtime
        .block_on(publisher.close())
        .expect("close ws publisher gracefully");
    runtime
        .block_on(subscriber.close())
        .expect("close ws subscriber gracefully");
}

#[stress_test]
fn should_complete_tcp_subscribe_unsubscribe_cycle(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "subscribe_unsubscribe_cycle");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "1_subscribe_1_unsubscribe");
    ctx.tag("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/lifecycle/events");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let subscribe_response = runtime
            .block_on(client.request(&subscribe_frame, 2000))
            .expect("subscribe response");
        let (_msg_type, status, data) = parse_notice_response(&subscribe_response);
        assert_eq!(status, 0, "expected notice subscribe success");
        let subscription_id = parse_notice_subscription_id(&data)
            .expect("parse subscribe response")
            .expect("subscription id");

        let unsubscribe_frame = build_notice_unsubscribe(subscription_id);
        let unsubscribe_response = runtime
            .block_on(client.request(&unsubscribe_frame, 2000))
            .expect("unsubscribe response");
        let (_msg_type, status, _data) = parse_notice_response(&unsubscribe_response);
        assert_eq!(status, 0, "expected notice unsubscribe success");
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_ws_subscribe_unsubscribe_cycle(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "subscribe_unsubscribe_cycle");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "1_subscribe_1_unsubscribe");
    ctx.tag("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/lifecycle/events");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let subscribe_response = runtime
            .block_on(client.request(&subscribe_frame, 2000))
            .expect("subscribe response");
        let (_msg_type, status, data) = parse_notice_response(&subscribe_response);
        assert_eq!(status, 0, "expected notice subscribe success");
        let subscription_id = parse_notice_subscription_id(&data)
            .expect("parse subscribe response")
            .expect("subscription id");

        let unsubscribe_frame = build_notice_unsubscribe(subscription_id);
        let unsubscribe_response = runtime
            .block_on(client.request(&unsubscribe_frame, 2000))
            .expect("unsubscribe response");
        let (_msg_type, status, _data) = parse_notice_response(&unsubscribe_response);
        assert_eq!(status, 0, "expected notice unsubscribe success");
    });
    ctx.set_elements(iterations as u64);

    runtime
        .block_on(client.close())
        .expect("close ws client gracefully");
}

#[stress_test]
fn should_complete_multiclient_fanout_publish(ctx: &mut StressContext) {
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "fanout_publish");
    ctx.tag("measurement_scope", "ws_multiclient_e2e");
    ctx.tag("batch_size", "1_publish_10_notifications");
    ctx.tag("publisher_count", "1");
    ctx.tag("subscriber_count", "10");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame = build_notice_publish("notice://test/events", b"event");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let subscribers: Vec<Arc<Mutex<TestWebSocketClient>>> = (0..10)
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

    let mut publisher = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws publisher");

    let sub = subscribe_frame.clone();
    runtime.block_on(futures::future::join_all(subscribers.iter().map(|arc| {
        let arc = arc.clone();
        let f = sub.clone();
        async move {
            let mut c = arc.lock().await;
            c.request(&f, 2000).await.expect("subscribe")
        }
    })));

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let publish_frame = publish_frame.clone();
        runtime
            .block_on(publisher.send_frame(&publish_frame))
            .expect("publish frame");

        let _results: Vec<_> =
            runtime.block_on(futures::future::join_all(subscribers.iter().map(|arc| {
                let arc = arc.clone();
                async move {
                    let mut c = arc.lock().await;
                    let response = c.recv_frame(2000).await.expect("publish notification");
                    let _ = parse_notice_delivery(&response);
                }
            })));
    });
    ctx.set_elements(10 * iterations as u64);

    runtime
        .block_on(publisher.close())
        .expect("close ws publisher gracefully");
    let _closed: Vec<_> =
        runtime.block_on(futures::future::join_all(subscribers.iter().map(|arc| {
            let arc = arc.clone();
            async move {
                let mut c = arc.lock().await;
                c.close().await.expect("close ws subscriber gracefully");
            }
        })));
}

stress_main!();
