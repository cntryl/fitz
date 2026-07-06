//! Notice domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **`MultiClient`** - N concurrent WS clients (real concurrency)

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_notice_publish, build_notice_subscribe, build_notice_unsubscribe,
    create_bench_notice_sink, extract_single_tlv_field, parse_notice_response,
    parse_notice_subscription_id, register_session_counting_sink, route_frame,
    shared_bench_runtime,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const DELIVERY_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECT_PUBLISHES_PER_ITERATION: u64 = 32;
const TCP_PUBLISHES_PER_ITERATION: u64 = 16;
const TCP_SUBSCRIBE_CYCLES_PER_ITERATION: u64 = 16;
const WS_PUBLISHES_PER_ITERATION: u64 = 16;
const WS_SUBSCRIBE_CYCLES_PER_ITERATION: u64 = 16;
const MULTICLIENT_FANOUT_PUBLISHES_PER_ITERATION: u64 = 4;

fn is_recv_timeout(error: &dyn std::error::Error) -> bool {
    error.to_string().contains("timeout waiting for response")
}

fn wait_for_delivery_count(
    runtime: &'static Runtime,
    delivered: &Arc<AtomicU64>,
    expected: u64,
    description: &str,
) {
    runtime.block_on(async {
        tokio::time::timeout(DELIVERY_WAIT_TIMEOUT, async {
            while delivered.load(Ordering::Relaxed) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out waiting for {description}: expected {expected}, observed {}",
                delivered.load(Ordering::Relaxed)
            )
        });
    });
}

fn wait_for_all_delivery_counts(
    runtime: &'static Runtime,
    delivered: &[Arc<AtomicU64>],
    expected_per_subscriber: u64,
    description: &str,
) {
    runtime.block_on(async {
        tokio::time::timeout(DELIVERY_WAIT_TIMEOUT, async {
            loop {
                if delivered
                    .iter()
                    .all(|counter| counter.load(Ordering::Relaxed) >= expected_per_subscriber)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let observed = delivered
                .iter()
                .map(|counter| counter.load(Ordering::Relaxed).to_string())
                .collect::<Vec<_>>()
                .join(",");
            panic!(
                "timed out waiting for {description}: expected {expected_per_subscriber} per subscriber, observed [{observed}]"
            )
        });
    });
}

fn spawn_tcp_subscriber_counter(
    runtime: &'static Runtime,
    mut subscriber: TestClient,
    delivered: Arc<AtomicU64>,
) -> (watch::Sender<bool>, JoinHandle<()>) {
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let subscriber_handle = runtime.spawn(async move {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                result = subscriber.recv_frame(5000) => {
                    match result {
                        Ok(_frame) => {
                            delivered.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            if *stop_rx.borrow() {
                                break;
                            }
                            if is_recv_timeout(error.as_ref()) {
                                continue;
                            }
                            panic!("publish notification: {error}");
                        }
                    }
                }
            }
        }
    });
    (stop_tx, subscriber_handle)
}

fn spawn_ws_subscriber_counter(
    runtime: &'static Runtime,
    mut subscriber: TestWebSocketClient,
    delivered: Arc<AtomicU64>,
) -> (watch::Sender<bool>, JoinHandle<()>) {
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let subscriber_handle = runtime.spawn(async move {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                result = subscriber.recv_frame(5000) => {
                    match result {
                        Ok(_frame) => {
                            delivered.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            if *stop_rx.borrow() {
                                break;
                            }
                            if is_recv_timeout(error.as_ref()) {
                                continue;
                            }
                            panic!("publish notification: {error}");
                        }
                    }
                }
            }
        }

        subscriber
            .close()
            .await
            .expect("close ws subscriber gracefully");
    });
    (stop_tx, subscriber_handle)
}

#[stress(tier = 4)]
fn should_complete_direct_publish(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "publish");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter(
        "batch_size",
        format!("{DIRECT_PUBLISHES_PER_ITERATION}_publishes"),
    );
    ctx.parameter("subscriber_count", "1");

    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_notice_sink(router.clone());
    router.register_domain_pattern("notice", sink as Arc<dyn MailboxSink>);

    let (subscriber_source, subscriber_sink) = register_session_counting_sink(&router, family, 1);
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
    let subscribe_count = subscriber_sink.wait_for_count(1, Duration::from_secs(1));
    assert_eq!(
        subscribe_count, 1,
        "notice subscribe should ack before publish benchmark"
    );
    subscriber_sink.reset();

    let (publisher_source, _publisher_sink) = register_session_counting_sink(&router, family, 2);
    let publish_frame = build_notice_publish(
        "notice://test/events",
        Bytes::from_static(b"event").as_ref(),
    );
    let (publish_msg_type, publish_payload) = extract_single_tlv_field(&publish_frame);
    let expected_deliveries_per_iteration =
        usize::try_from(DIRECT_PUBLISHES_PER_ITERATION).expect("publish count fits usize");
    let mut expected_deliveries = 0usize;

    let iterations = ctx.measure_workload("complete_direct_publish", || {
        for _ in 0..DIRECT_PUBLISHES_PER_ITERATION {
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
        }
        expected_deliveries += expected_deliveries_per_iteration;
        let delivered = subscriber_sink.wait_for_count(expected_deliveries, Duration::from_secs(1));
        assert_eq!(
            delivered, expected_deliveries,
            "notice direct publish should deliver exactly one notification"
        );
    });
    stress_config::record_completed(ctx, DIRECT_PUBLISHES_PER_ITERATION * iterations);
}

#[stress(tier = 4)]
fn should_complete_tcp_publish(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "publish");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter(
        "batch_size",
        format!("{TCP_PUBLISHES_PER_ITERATION}_publishes"),
    );
    ctx.parameter("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame: Arc<[u8]> = build_notice_publish("notice://test/events", b"event").into();

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

    let delivered = Arc::new(AtomicU64::new(0));
    let (stop_tx, subscriber_handle) =
        spawn_tcp_subscriber_counter(runtime, subscriber, delivered.clone());

    let mut expected_deliveries = 0_u64;
    let iterations = ctx.measure_workload("complete_tcp_publish", || {
        for _ in 0..TCP_PUBLISHES_PER_ITERATION {
            runtime
                .block_on(publisher.send_frame(publish_frame.as_ref()))
                .expect("publish frame");
        }
        expected_deliveries += TCP_PUBLISHES_PER_ITERATION;
        wait_for_delivery_count(
            runtime,
            &delivered,
            expected_deliveries,
            "tcp notice deliveries",
        );
    });
    stop_tx.send(true).expect("stop tcp subscriber");
    runtime.block_on(async {
        subscriber_handle.await.expect("subscriber task");
    });
    stress_config::record_completed(ctx, TCP_PUBLISHES_PER_ITERATION * iterations);
}

#[stress(tier = 4)]
fn should_complete_ws_publish(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "publish");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter(
        "batch_size",
        format!("{WS_PUBLISHES_PER_ITERATION}_publishes"),
    );
    ctx.parameter("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame: Arc<[u8]> = build_notice_publish("notice://test/events", b"event").into();

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

    let delivered = Arc::new(AtomicU64::new(0));
    let (stop_tx, subscriber_handle) =
        spawn_ws_subscriber_counter(runtime, subscriber, delivered.clone());

    let mut expected_deliveries = 0_u64;
    let iterations = ctx.measure_workload("complete_ws_publish", || {
        for _ in 0..WS_PUBLISHES_PER_ITERATION {
            runtime
                .block_on(publisher.send_frame(publish_frame.as_ref()))
                .expect("publish frame");
        }
        expected_deliveries += WS_PUBLISHES_PER_ITERATION;
        wait_for_delivery_count(
            runtime,
            &delivered,
            expected_deliveries,
            "ws notice deliveries",
        );
    });
    stop_tx.send(true).expect("stop ws subscriber");
    runtime.block_on(async {
        publisher
            .close()
            .await
            .expect("close ws publisher gracefully");
        subscriber_handle.await.expect("subscriber task");
    });
    stress_config::record_completed(ctx, WS_PUBLISHES_PER_ITERATION * iterations);
}

#[stress(tier = 4)]
fn should_complete_tcp_subscribe_unsubscribe_cycle(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "subscribe_unsubscribe_cycle");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter(
        "batch_size",
        format!("{TCP_SUBSCRIBE_CYCLES_PER_ITERATION}_subscribe_unsubscribe_cycles"),
    );
    ctx.parameter("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/lifecycle/events");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let iterations = ctx.measure_workload("complete_tcp_subscribe_unsubscribe_cycle", || {
        for _ in 0..TCP_SUBSCRIBE_CYCLES_PER_ITERATION {
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
        }
    });
    stress_config::record_completed(ctx, 2 * TCP_SUBSCRIBE_CYCLES_PER_ITERATION * iterations);
}

#[stress(tier = 4)]
fn should_complete_ws_subscribe_unsubscribe_cycle(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "subscribe_unsubscribe_cycle");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter(
        "batch_size",
        format!("{WS_SUBSCRIBE_CYCLES_PER_ITERATION}_subscribe_unsubscribe_cycles"),
    );
    ctx.parameter("subscriber_count", "1");

    let subscribe_frame = build_notice_subscribe("notice://test/lifecycle/events");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let iterations = ctx.measure_workload("complete_ws_subscribe_unsubscribe_cycle", || {
        for _ in 0..WS_SUBSCRIBE_CYCLES_PER_ITERATION {
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
        }
    });
    stress_config::record_completed(ctx, 2 * WS_SUBSCRIBE_CYCLES_PER_ITERATION * iterations);

    runtime
        .block_on(client.close())
        .expect("close ws client gracefully");
}

#[stress(tier = 4)]
fn should_complete_multiclient_fanout_publish(ctx: &mut StressContext) {
    measure_multiclient_fanout_publish(
        ctx,
        "complete_multiclient_fanout_publish",
        "fanout_publish",
        10,
    );
}

fn measure_multiclient_fanout_publish(
    ctx: &mut StressContext,
    name: &str,
    scenario: &'static str,
    subscriber_count: usize,
) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "ws_multiclient_e2e");
    ctx.parameter("completed_unit", "delivered_notifications");
    ctx.parameter("publisher_count", "1");
    let batch_size = format!("1_publish_{subscriber_count}_notifications");
    ctx.parameter("batch_size", batch_size.as_str());
    let subscriber_count_tag = subscriber_count.to_string();
    ctx.parameter("subscriber_count", subscriber_count_tag.as_str());

    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let publish_frame: Arc<[u8]> = build_notice_publish("notice://test/events", b"event").into();

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut publisher = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws publisher");

    let mut subscriber_deliveries = Vec::with_capacity(subscriber_count);
    let mut subscriber_stops = Vec::new();
    let mut subscriber_handles = Vec::new();
    for _ in 0..subscriber_count {
        let mut subscriber = runtime
            .block_on(TestWebSocketClient::connect(&format!(
                "ws://{}",
                server.ws_addr
            )))
            .expect("connect ws subscriber");
        runtime
            .block_on(subscriber.request(&subscribe_frame, 2000))
            .expect("subscribe response");
        let delivered = Arc::new(AtomicU64::new(0));
        let (stop_tx, handle) = spawn_ws_subscriber_counter(runtime, subscriber, delivered.clone());
        subscriber_deliveries.push(delivered);
        subscriber_stops.push(stop_tx);
        subscriber_handles.push(handle);
    }
    let mut expected_per_subscriber = 0_u64;
    let iterations = ctx.measure_workload(name, || {
        for _ in 0..MULTICLIENT_FANOUT_PUBLISHES_PER_ITERATION {
            runtime
                .block_on(publisher.send_frame(publish_frame.as_ref()))
                .expect("publish frame");
        }

        expected_per_subscriber += MULTICLIENT_FANOUT_PUBLISHES_PER_ITERATION;
        // Aggregate delivery counts can hide one lagging subscriber and fill its mailbox.
        wait_for_all_delivery_counts(
            runtime,
            &subscriber_deliveries,
            expected_per_subscriber,
            "multiclient notice deliveries",
        );
    });

    for stop_tx in subscriber_stops {
        stop_tx.send(true).expect("stop ws subscriber");
    }
    runtime.block_on(async {
        publisher
            .close()
            .await
            .expect("close ws publisher gracefully");
        for handle in subscriber_handles {
            handle.await.expect("subscriber task");
        }
    });
    stress_config::record_completed(
        ctx,
        subscriber_count as u64 * MULTICLIENT_FANOUT_PUBLISHES_PER_ITERATION * iterations,
    );
}

#[stress(tier = 4)]
fn should_complete_multiclient_fanout_publish_subscriber_scaling_1(ctx: &mut StressContext) {
    measure_multiclient_fanout_publish(
        ctx,
        "complete_multiclient_fanout_publish_subscriber_scaling_1",
        "fanout_publish_subscriber_scaling",
        1,
    );
}

#[stress(tier = 4)]
fn should_complete_multiclient_fanout_publish_subscriber_scaling_16(ctx: &mut StressContext) {
    measure_multiclient_fanout_publish(
        ctx,
        "complete_multiclient_fanout_publish_subscriber_scaling_16",
        "fanout_publish_subscriber_scaling",
        16,
    );
}

#[stress(tier = 4)]
fn should_complete_multiclient_fanout_publish_subscriber_scaling_64(ctx: &mut StressContext) {
    measure_multiclient_fanout_publish(
        ctx,
        "complete_multiclient_fanout_publish_subscriber_scaling_64",
        "fanout_publish_subscriber_scaling",
        64,
    );
}

stress_main!();
