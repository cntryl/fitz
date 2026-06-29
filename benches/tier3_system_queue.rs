#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_queue_complete, build_queue_dequeue, build_queue_dequeue_batch, build_queue_enqueue,
    build_queue_watch, create_bench_queue_actor, create_bench_queue_sink, extract_single_tlv_field,
    register_session_counting_sink, register_session_queue_sink, route_frame, CountingSink,
    FrameQueueSink,
};
use fitz::domains::queue::{Clock, QueueActor, QueueKey, QueueResponse, ReservedMessage};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CLIENT_SESSION_ID: u64 = 1;
const RECEIVE_BATCH_SIZE: usize = 50;

#[derive(Clone)]
struct BenchClock {
    start_instant: Instant,
    base_epoch_ms: u64,
    elapsed_ns: Arc<AtomicU64>,
}

impl BenchClock {
    fn new() -> Self {
        Self {
            start_instant: Instant::now(),
            base_epoch_ms: 1_700_000_000_000,
            elapsed_ns: Arc::new(AtomicU64::new(0)),
        }
    }

    fn advance(&self, duration: Duration) {
        self.elapsed_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
}

impl Clock for BenchClock {
    fn now_instant(&self) -> Instant {
        self.start_instant + Duration::from_nanos(self.elapsed_ns.load(Ordering::Relaxed))
    }

    fn now_epoch_ms(&self) -> u64 {
        self.base_epoch_ms + (self.elapsed_ns.load(Ordering::Relaxed) / 1_000_000)
    }
}

fn setup_queue_request_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_queue_sink(router.clone());
    router.register_domain_pattern("queue", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    (router, family, source, inbox)
}

fn request_queue_response(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        router.as_ref(),
        source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Sub,
        msg_type,
        payload,
        family,
    )
    .expect("queue request");
    inbox
        .drain()
        .last()
        .map(|frame| frame.payload.clone())
        .expect("queue response")
}

fn register_queue_watch(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
) {
    let watch_frame = build_queue_watch(destination);
    let (msg_type, payload) = extract_single_tlv_field(&watch_frame);
    route_frame(
        router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Sub,
        msg_type,
        payload,
        family,
    )
    .expect("queue wait registration");
}

fn assert_queue_success(response: &[u8]) {
    assert_eq!(
        response.first().copied(),
        Some(0),
        "expected queue success response"
    );
}

fn parse_received_messages(response: &[u8]) -> Vec<(u64, u64)> {
    assert_queue_success(response);
    assert!(response.len() >= 5, "queue receive response too short");

    let message_count = u32::from_be_bytes([response[1], response[2], response[3], response[4]]);
    let mut offset = 5usize;
    let mut messages = Vec::with_capacity(message_count as usize);

    for _ in 0..message_count {
        assert!(
            response.len() >= offset + 20,
            "queue receive message metadata too short"
        );
        let id = u64::from_be_bytes([
            response[offset],
            response[offset + 1],
            response[offset + 2],
            response[offset + 3],
            response[offset + 4],
            response[offset + 5],
            response[offset + 6],
            response[offset + 7],
        ]);
        offset += 8;

        let token = u64::from_be_bytes([
            response[offset],
            response[offset + 1],
            response[offset + 2],
            response[offset + 3],
            response[offset + 4],
            response[offset + 5],
            response[offset + 6],
            response[offset + 7],
        ]);
        offset += 8;

        let body_len = u32::from_be_bytes([
            response[offset],
            response[offset + 1],
            response[offset + 2],
            response[offset + 3],
        ]) as usize;
        offset += 4;

        assert!(
            response.len() >= offset + body_len,
            "queue receive body truncated"
        );
        offset += body_len;
        messages.push((id, token));
    }

    messages
}

fn parse_single_received_message(response: &[u8]) -> (u64, u64) {
    let mut messages = parse_received_messages(response);
    assert_eq!(
        messages.len(),
        1,
        "expected exactly one received queue message"
    );
    messages.pop().expect("single queue message")
}

fn receive_single_message(actor: &mut QueueActor) -> ReservedMessage {
    match actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { mut messages } => {
            assert_eq!(messages.len(), 1, "expected a single reserved message");
            messages.pop().expect("reserved message")
        }
        other => panic!("expected received message, got {other:?}"),
    }
}

fn receive_batch_messages(actor: &mut QueueActor, batch_size: usize) -> Vec<ReservedMessage> {
    match actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(batch_size)) {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), batch_size, "expected a full receive batch");
            messages
        }
        other => panic!("expected received batch, got {other:?}"),
    }
}

fn ack_reserved_messages(actor: &mut QueueActor, messages: Vec<ReservedMessage>) {
    for message in messages {
        let response = actor.handle_ack_for_session(CLIENT_SESSION_ID, message.id, message.token);
        assert_eq!(response, QueueResponse::Acked);
    }
}

fn measure_backlog_depth_steady_state(
    ctx: &mut StressContext,
    backlog_depth: usize,
    per_iteration_cycles: u64,
) {
    assert!(backlog_depth > 0, "backlog_depth must be at least one");

    ctx.tag("scenario", "backlog_depth_steady_state");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "dequeue_ack_replenish");
    ctx.tag("batch_size", format!("{per_iteration_cycles}_cycles"));
    ctx.tag("backlog_depth", backlog_depth.to_string());

    let mut actor = create_bench_queue_actor("bench", "depth", "queue", None);
    let payload = Bytes::from_static(b"backlog depth message");

    for _ in 0..backlog_depth {
        let response = actor.handle_send(payload.clone(), None);
        assert!(matches!(response, QueueResponse::Sent { .. }));
    }

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for _ in 0..per_iteration_cycles {
                let message = receive_single_message(&mut actor);
                let response =
                    actor.handle_ack_for_session(CLIENT_SESSION_ID, message.id, message.token);
                assert_eq!(response, QueueResponse::Acked);

                let response = actor.handle_send(payload.clone(), None);
                assert!(matches!(response, QueueResponse::Sent { .. }));
            }
        },
    );
    ctx.set_elements((per_iteration_cycles * 3) * iterations as u64);
}

#[stress_test]
fn should_complete_capacity_enqueue_isolated(ctx: &mut StressContext) {
    ctx.tag("scenario", "enqueue_isolated");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "enqueue");
    ctx.tag("batch_size", "single_enqueue");

    let payload = Bytes::from_static(b"enqueue isolated message");
    let mut actors: Vec<QueueActor> = (0..64)
        .map(|queue| create_bench_queue_actor("bench", "enqueue", &format!("queue{queue}"), None))
        .collect();
    let mut actor_index = 0usize;

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let response = actors[actor_index].handle_send(payload.clone(), None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
            actor_index = (actor_index + 1) % actors.len();
        },
    );
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_capacity_receive_batch_cleanup(ctx: &mut StressContext) {
    ctx.tag("scenario", "receive_batch_cleanup");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "receive");
    ctx.tag("cache_state", "warm");
    ctx.tag("batch_size", "50_enqueue_1_receive_50_ack_cleanup");

    let mut actor = create_bench_queue_actor("bench", "receive", "queue", None);
    let payload = Bytes::from_static(b"receive batch cleanup message");
    let batch: Vec<(Bytes, Option<u64>)> = (0..RECEIVE_BATCH_SIZE)
        .map(|_| (payload.clone(), None))
        .collect();

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let response = actor.handle_send_batch(&batch);
            assert!(matches!(response, QueueResponse::SentBatch { .. }));

            let messages = receive_batch_messages(&mut actor, RECEIVE_BATCH_SIZE);
            ack_reserved_messages(&mut actor, messages);
        },
    );
    ctx.set_elements(((RECEIVE_BATCH_SIZE as u64) * 2 + 1) * iterations as u64);
}

#[stress_test]
fn should_complete_capacity_ack_roundtrip(ctx: &mut StressContext) {
    ctx.tag("scenario", "ack_roundtrip");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "ack");
    ctx.tag("cache_state", "warm");
    ctx.tag("batch_size", "1_enqueue_1_receive_1_ack");

    let mut actor = create_bench_queue_actor("bench", "ack", "queue", None);
    let payload = Bytes::from_static(b"ack roundtrip message");

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let response = actor.handle_send(payload.clone(), None);
            assert!(matches!(response, QueueResponse::Sent { .. }));

            let message = receive_single_message(&mut actor);
            let response =
                actor.handle_ack_for_session(CLIENT_SESSION_ID, message.id, message.token);
            assert_eq!(response, QueueResponse::Acked);
        },
    );
    ctx.set_elements(3 * iterations as u64);
}

#[stress_test]
fn should_complete_capacity_extend_roundtrip(ctx: &mut StressContext) {
    ctx.tag("scenario", "extend_roundtrip");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "extend");
    ctx.tag("cache_state", "warm");
    ctx.tag("batch_size", "1_enqueue_1_receive_1_extend_1_ack");

    let mut actor = create_bench_queue_actor("bench", "extend", "queue", None);
    let payload = Bytes::from_static(b"extend roundtrip message");

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let response = actor.handle_send(payload.clone(), None);
            assert!(matches!(response, QueueResponse::Sent { .. }));

            let message = receive_single_message(&mut actor);
            let response =
                actor.handle_extend_for_session(CLIENT_SESSION_ID, message.id, message.token, 60);
            assert_eq!(response, QueueResponse::Extended);

            let response =
                actor.handle_ack_for_session(CLIENT_SESSION_ID, message.id, message.token);
            assert_eq!(response, QueueResponse::Acked);
        },
    );
    ctx.set_elements(4 * iterations as u64);
}

#[stress_test]
fn should_complete_capacity_sustained_load(ctx: &mut StressContext) {
    ctx.tag("scenario", "sustained_load");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "enqueue_receive");
    ctx.tag("batch_size", "50_enqueue_50_receive");

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"sustained load message");
    let payloads: Vec<Bytes> = (0..50).map(|_| payload.clone()).collect();
    let batch_50: Vec<(Bytes, Option<u64>)> = payloads
        .iter()
        .take(50)
        .map(|p| (p.clone(), None))
        .collect();

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = actor.handle_send_batch(&batch_50);
            for _ in 0..50 {
                let _ = actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(1));
            }
        },
    );
    ctx.set_elements(100 * iterations as u64);
}

#[stress_test]
fn should_complete_capacity_mixed_workload(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_steady_state");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "enqueue_receive_ack");
    ctx.tag("batch_size", "100_enqueue_100_receive_100_ack");

    let clock = BenchClock::new();
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "system".to_string(),
        resource: "queue".to_string(),
    };
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = QueueActor::with_clock_and_write_options(
        RouteFamily::new(1),
        queue_key,
        store,
        Box::new(clock.clone()),
        Some(3),
        fitz::utils::idempotency::default_dedup_store(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let payload = Bytes::from_static(b"mixed workload message");

    let payloads: Vec<Bytes> = (0..100).map(|_| payload.clone()).collect();
    let batch_mixed: Vec<(Bytes, Option<u64>)> = payloads
        .iter()
        .take(70)
        .map(|p| (p.clone(), None))
        .chain(
            payloads
                .iter()
                .skip(70)
                .take(20)
                .map(|p| (p.clone(), Some(5))),
        )
        .chain(payloads.iter().skip(90).take(10).map(|p| (p.clone(), None)))
        .collect();

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = actor.handle_send_batch(&batch_mixed);

            let immediate = match actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(80))
            {
                QueueResponse::Received { messages } => messages,
                other => panic!("expected received immediate batch, got {other:?}"),
            };
            assert_eq!(immediate.len(), 80);
            for message in immediate {
                let response =
                    actor.handle_ack_for_session(CLIENT_SESSION_ID, message.id, message.token);
                assert_eq!(response, QueueResponse::Acked);
            }

            clock.advance(Duration::from_secs(6));
            actor.process_delayed_messages();

            let delayed = match actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(20)) {
                QueueResponse::Received { messages } => messages,
                other => panic!("expected received delayed batch, got {other:?}"),
            };
            assert_eq!(delayed.len(), 20);
            for message in delayed {
                let response =
                    actor.handle_ack_for_session(CLIENT_SESSION_ID, message.id, message.token);
                assert_eq!(response, QueueResponse::Acked);
            }
        },
    );
    ctx.set_elements(300 * iterations as u64);
}

#[stress_test]
fn should_complete_backlog_depth_steady_state_1(ctx: &mut StressContext) {
    measure_backlog_depth_steady_state(ctx, 1, 100);
}

#[stress_test]
fn should_complete_backlog_depth_steady_state_64(ctx: &mut StressContext) {
    measure_backlog_depth_steady_state(ctx, 64, 100);
}

#[stress_test]
fn should_complete_backlog_depth_steady_state_256(ctx: &mut StressContext) {
    measure_backlog_depth_steady_state(ctx, 256, 100);
}

#[stress_test]
fn should_complete_backlog_depth_steady_state_1024(ctx: &mut StressContext) {
    measure_backlog_depth_steady_state(ctx, 1024, 100);
}

#[stress_test]
fn should_complete_capacity_cold_start_recovery(ctx: &mut StressContext) {
    ctx.tag("scenario", "cold_start_recovery");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "recover");
    ctx.tag("cache_state", "recovered");
    ctx.tag("batch_size", "100_recovered_messages");

    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "recovery".to_string(),
        resource: "queue".to_string(),
    };

    let store = create_test_engine_with_cfs(vec![1]);

    let mut pre_actor = QueueActor::new(
        RouteFamily::new(1),
        queue_key.clone(),
        store.clone(),
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    let payload = Bytes::from_static(b"recovery message");
    for _ in 0..100 {
        let _ = pre_actor.handle_send(payload.clone(), None);
    }
    drop(pre_actor);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _actor = QueueActor::new(
                RouteFamily::new(1),
                queue_key.clone(),
                store.clone(),
                None,
                fitz::utils::idempotency::default_dedup_store(),
            );
        },
    );
    ctx.set_elements(100 * iterations as u64);
}

#[stress_test]
fn should_complete_capacity_high_contention(ctx: &mut StressContext) {
    ctx.tag("scenario", "high_contention");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("operation", "enqueue_receive");
    ctx.tag("batch_size", "50_enqueue_50_receive");

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"contention message");
    let payloads: Vec<Bytes> = (0..50).map(|_| payload.clone()).collect();
    let batch_50: Vec<(Bytes, Option<u64>)> = payloads.iter().map(|p| (p.clone(), None)).collect();

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = actor.handle_send_batch(&batch_50);
            for _ in 0..50 {
                let _ = actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(1));
            }
        },
    );
    ctx.set_elements(100 * iterations as u64);
}

#[stress_test]
fn should_complete_routed_enqueue_sustained(ctx: &mut StressContext) {
    ctx.tag("scenario", "routed_enqueue_sustained");
    ctx.tag("measurement_scope", "routed_sink");
    ctx.tag("operation", "enqueue");
    ctx.tag("batch_size", "single_enqueue");

    let (router, family, source, inbox) = setup_queue_request_sink();
    let queue_routes: Vec<String> = (0..64)
        .map(|queue| format!("queue://bench/system/enqueue{queue}"))
        .collect();
    let enqueue_frames: Vec<(u16, Bytes)> = queue_routes
        .iter()
        .map(|route| {
            let frame = build_queue_enqueue(route, b"routed enqueue payload");
            extract_single_tlv_field(&frame)
        })
        .collect();
    let mut route_index = 0usize;

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let route = &queue_routes[route_index];
            let (msg_type, payload) = &enqueue_frames[route_index];
            let response = request_queue_response(
                &router,
                family,
                &source,
                &inbox,
                route,
                *msg_type,
                payload.clone(),
            );
            assert_queue_success(&response);
            route_index = (route_index + 1) % queue_routes.len();
        },
    );
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_routed_receive_batch_cleanup(ctx: &mut StressContext) {
    ctx.tag("scenario", "routed_receive_batch_cleanup");
    ctx.tag("measurement_scope", "routed_sink");
    ctx.tag("operation", "receive");
    ctx.tag("batch_size", "50_enqueue_1_receive_50_ack_cleanup");

    let (router, family, source, inbox) = setup_queue_request_sink();
    let route = "queue://bench/system/receive";
    let enqueue_frame = build_queue_enqueue(route, b"routed receive payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let receive_frame = build_queue_dequeue_batch(route, RECEIVE_BATCH_SIZE as u32);
    let (receive_msg_type, receive_payload) = extract_single_tlv_field(&receive_frame);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for _ in 0..RECEIVE_BATCH_SIZE {
                let response = request_queue_response(
                    &router,
                    family,
                    &source,
                    &inbox,
                    route,
                    enqueue_msg_type,
                    enqueue_payload.clone(),
                );
                assert_queue_success(&response);
            }

            let receive_response = request_queue_response(
                &router,
                family,
                &source,
                &inbox,
                route,
                receive_msg_type,
                receive_payload.clone(),
            );
            let messages = parse_received_messages(&receive_response);
            assert_eq!(
                messages.len(),
                RECEIVE_BATCH_SIZE,
                "expected a full routed receive batch"
            );

            for (message_id, token) in messages {
                let ack_frame = build_queue_complete(route, message_id, token);
                let (ack_msg_type, ack_payload) = extract_single_tlv_field(&ack_frame);
                let ack_response = request_queue_response(
                    &router,
                    family,
                    &source,
                    &inbox,
                    route,
                    ack_msg_type,
                    ack_payload,
                );
                assert_queue_success(&ack_response);
            }
        },
    );
    ctx.set_elements(((RECEIVE_BATCH_SIZE as u64) * 2 + 1) * iterations as u64);
}

#[stress_test]
fn should_complete_routed_ack_roundtrip(ctx: &mut StressContext) {
    ctx.tag("scenario", "routed_ack_roundtrip");
    ctx.tag("measurement_scope", "routed_sink");
    ctx.tag("operation", "ack");
    ctx.tag("batch_size", "1_enqueue_1_receive_1_ack");

    let (router, family, source, inbox) = setup_queue_request_sink();
    let route = "queue://bench/system/ack";
    let enqueue_frame = build_queue_enqueue(route, b"routed ack payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let dequeue_frame = build_queue_dequeue(route);
    let (dequeue_msg_type, dequeue_payload) = extract_single_tlv_field(&dequeue_frame);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let enqueue_response = request_queue_response(
                &router,
                family,
                &source,
                &inbox,
                route,
                enqueue_msg_type,
                enqueue_payload.clone(),
            );
            assert_queue_success(&enqueue_response);

            let dequeue_response = request_queue_response(
                &router,
                family,
                &source,
                &inbox,
                route,
                dequeue_msg_type,
                dequeue_payload.clone(),
            );
            let (message_id, token) = parse_single_received_message(&dequeue_response);

            let ack_frame = build_queue_complete(route, message_id, token);
            let (ack_msg_type, ack_payload) = extract_single_tlv_field(&ack_frame);
            let ack_response = request_queue_response(
                &router,
                family,
                &source,
                &inbox,
                route,
                ack_msg_type,
                ack_payload,
            );
            assert_queue_success(&ack_response);
        },
    );
    ctx.set_elements(3 * iterations as u64);
}

#[stress_test]
fn should_complete_wait_wakeup_with_waiters(ctx: &mut StressContext) {
    ctx.tag("scenario", "wait_wakeup");
    ctx.tag("measurement_scope", "routed_waiters");
    ctx.tag("operation", "watch_wakeup_roundtrip");
    ctx.tag("batch_size", "16_watch_16_enqueue_1_receive_16_ack_cleanup");
    ctx.tag("waiter_count", "16");

    let waiter_count = 16u64;
    let route = "queue://bench/system/wait";
    let (router, family, sender_source, sender_inbox) = setup_queue_request_sink();
    let enqueue_frame = build_queue_enqueue(route, b"queue wait wake payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let dequeue_frame = build_queue_dequeue_batch(route, waiter_count as u32);
    let (dequeue_msg_type, dequeue_payload) = extract_single_tlv_field(&dequeue_frame);
    let waiters: Vec<(u64, RouteAddress, Arc<CountingSink>)> = (0..waiter_count)
        .map(|index| {
            let session_id = CLIENT_SESSION_ID + 1 + index;
            let (source, sink) = register_session_counting_sink(&router, family, session_id);
            (session_id, source, sink)
        })
        .collect();

    for (session_id, source, sink) in &waiters {
        register_queue_watch(&router, family, source, route, *session_id);
        sink.reset();
    }

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for (_, _, sink) in &waiters {
                sink.reset();
            }

            for _ in 0..waiter_count {
                let response = request_queue_response(
                    &router,
                    family,
                    &sender_source,
                    &sender_inbox,
                    route,
                    enqueue_msg_type,
                    enqueue_payload.clone(),
                );
                assert_queue_success(&response);
            }

            let deliveries: usize = waiters.iter().map(|(_, _, sink)| sink.count()).sum();
            assert_eq!(
                deliveries, waiter_count as usize,
                "expected queue sends to wake all waiting receivers"
            );

            let dequeue_response = request_queue_response(
                &router,
                family,
                &sender_source,
                &sender_inbox,
                route,
                dequeue_msg_type,
                dequeue_payload.clone(),
            );
            let reserved_messages = parse_received_messages(&dequeue_response);
            assert_eq!(
                reserved_messages.len(),
                waiter_count as usize,
                "expected cleanup receive to drain the ready queue"
            );

            for (message_id, token) in reserved_messages {
                let ack_frame = build_queue_complete(route, message_id, token);
                let (ack_msg_type, ack_payload) = extract_single_tlv_field(&ack_frame);
                let ack_response = request_queue_response(
                    &router,
                    family,
                    &sender_source,
                    &sender_inbox,
                    route,
                    ack_msg_type,
                    ack_payload,
                );
                assert_queue_success(&ack_response);
            }
        },
    );
    ctx.set_elements(((waiter_count * 2) + waiter_count + 1) * iterations as u64);
}

stress_main!();
