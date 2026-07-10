#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_queue_complete, build_queue_dequeue, build_queue_dequeue_batch, build_queue_enqueue,
    build_queue_watch, create_bench_queue_sink, create_write_heavy_bench_store,
    extract_single_tlv_field, register_session_counting_sink, register_session_queue_sink,
    route_frame, CountingSink, FrameQueueSink,
};
use fitz::domains::queue::{Clock, QueueActor, QueueKey, QueueResponse};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CLIENT_SESSION_ID: u64 = 1;
const ROUTED_ACK_ROUNDTRIP_REPEAT_COUNT: u64 = 16;
const ROUTED_CONCURRENT_ENQUEUE_REPEAT_COUNT: usize = 16;

fn configure_queue_operation_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "queue_operations");
    ctx.parameter("logical_unit", "queue_operation");
}

fn configure_enqueue_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "enqueued_messages");
    ctx.parameter("logical_unit", "enqueue");
}

#[inline]
fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[inline]
fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[inline]
fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

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
        self.elapsed_ns.fetch_add(
            u128_to_u64_saturating(duration.as_nanos()),
            Ordering::Relaxed,
        );
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
        .drain_after_count(1, Duration::from_secs(1))
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

#[stress(tier = 3)]
fn should_complete_capacity_mixed_workload(ctx: &mut StressContext) {
    ctx.parameter("scenario", "mixed_steady_state");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("operation", "enqueue_receive_ack");
    ctx.parameter("batch_size", "100_enqueue_100_receive_100_ack");
    configure_queue_operation_measurement(ctx);

    let clock = BenchClock::new();
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "system".to_string(),
        resource: "queue".to_string(),
    };
    let store = create_write_heavy_bench_store();
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

    let iterations = ctx.measure_workload("complete_capacity_mixed_workload", || {
        let _ = actor.handle_send_batch(&batch_mixed);

        let immediate = match actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(80)) {
            QueueResponse::Received { messages } => messages,
            other => panic!("expected received immediate batch, got {other:?}"),
        };
        assert_eq!(immediate.len(), 80);
        let immediate_acks: Vec<_> = immediate
            .iter()
            .map(|message| (message.id, message.token))
            .collect();
        let responses = actor.handle_ack_batch_for_session(CLIENT_SESSION_ID, &immediate_acks);
        assert_eq!(responses, vec![QueueResponse::Acked; 80]);

        clock.advance(Duration::from_secs(6));
        actor.process_delayed_messages();

        let delayed = match actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(20)) {
            QueueResponse::Received { messages } => messages,
            other => panic!("expected received delayed batch, got {other:?}"),
        };
        assert_eq!(delayed.len(), 20);
        let delayed_acks: Vec<_> = delayed
            .iter()
            .map(|message| (message.id, message.token))
            .collect();
        let responses = actor.handle_ack_batch_for_session(CLIENT_SESSION_ID, &delayed_acks);
        assert_eq!(responses, vec![QueueResponse::Acked; 20]);
    });
    stress_config::record_completed(ctx, 300 * iterations);
}

#[stress(tier = 3)]
fn should_complete_routed_enqueue_sustained(ctx: &mut StressContext) {
    ctx.parameter("scenario", "routed_enqueue_sustained");
    ctx.parameter("measurement_scope", "routed_sink");
    ctx.parameter("operation", "enqueue");
    ctx.parameter("batch_size", "single_enqueue");
    configure_enqueue_measurement(ctx);

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

    let iterations = ctx.measure_workload("complete_routed_enqueue_sustained", || {
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
    });
    stress_config::record_completed(ctx, iterations);
}

fn measure_routed_concurrent_enqueues(ctx: &mut StressContext, name: &str, client_count: usize) {
    ctx.parameter("scenario", "concurrent_enqueues_client_scaling");
    ctx.parameter("measurement_scope", "routed_sink_concurrent");
    ctx.parameter("operation", "enqueue");
    let batch_size =
        format!("{client_count}_sessions_{ROUTED_CONCURRENT_ENQUEUE_REPEAT_COUNT}_enqueues_each");
    ctx.parameter("batch_size", batch_size.as_str());
    let client_count_tag = client_count.to_string();
    ctx.parameter("client_count", client_count_tag.as_str());
    ctx.parameter(
        "enqueues_per_client",
        ROUTED_CONCURRENT_ENQUEUE_REPEAT_COUNT.to_string(),
    );
    configure_enqueue_measurement(ctx);

    let (router, family, _, _) = setup_queue_request_sink();
    let route = "queue://bench/system/concurrent-enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"routed concurrent enqueue payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let clients: Vec<(u64, RouteAddress, Arc<CountingSink>)> = (0..client_count)
        .map(|index| {
            let session_id = 30_000 + u64::try_from(index).expect("client index should fit u64");
            let (source, sink) = register_session_counting_sink(&router, family, session_id);
            (session_id, source, sink)
        })
        .collect();

    let iterations = std::thread::scope(|scope| {
        let (done_tx, done_rx) = crossbeam_channel::bounded(client_count);
        let start_txs: Vec<_> = clients
            .iter()
            .map(|(session_id, source, _)| {
                let (start_tx, start_rx) = crossbeam_channel::bounded(0);
                let done_tx = done_tx.clone();
                let router = router.clone();
                let source = source.clone();
                let payload = enqueue_payload.clone();
                let session_id = *session_id;
                scope.spawn(move || {
                    while let Ok(enqueue_count) = start_rx.recv() {
                        for _ in 0..enqueue_count {
                            route_frame(
                                router.as_ref(),
                                &source,
                                route,
                                session_id,
                                ChannelId::Sub,
                                enqueue_msg_type,
                                payload.clone(),
                                family,
                            )
                            .expect("routed concurrent enqueue");
                        }
                        done_tx
                            .send(())
                            .expect("completion receiver should stay open");
                    }
                });
                start_tx
            })
            .collect();
        drop(done_tx);

        let iterations = ctx.measure_workload(name, || {
            for (_, _, sink) in &clients {
                sink.reset();
            }

            for start_tx in &start_txs {
                start_tx
                    .send(ROUTED_CONCURRENT_ENQUEUE_REPEAT_COUNT)
                    .expect("enqueue worker should stay active");
            }

            for _ in 0..client_count {
                done_rx.recv().expect("enqueue worker should complete");
            }

            let response_count: usize = clients.iter().map(|(_, _, sink)| sink.count()).sum();
            let expected_response_count = client_count * ROUTED_CONCURRENT_ENQUEUE_REPEAT_COUNT;
            assert_eq!(
                response_count, expected_response_count,
                "expected every concurrent routed enqueue to receive a response"
            );
        });

        drop(start_txs);
        iterations
    });
    stress_config::record_completed(
        ctx,
        (client_count as u64) * (ROUTED_CONCURRENT_ENQUEUE_REPEAT_COUNT as u64) * iterations,
    );
}

#[stress(tier = 3)]
fn should_complete_routed_concurrent_enqueues_client_scaling_16(ctx: &mut StressContext) {
    measure_routed_concurrent_enqueues(
        ctx,
        "complete_routed_concurrent_enqueues_client_scaling_16",
        16,
    );
}

#[stress(tier = 3)]
fn should_complete_routed_ack_roundtrip(ctx: &mut StressContext) {
    ctx.parameter("scenario", "routed_ack_roundtrip");
    ctx.parameter("measurement_scope", "routed_sink");
    ctx.parameter("operation", "ack");
    ctx.parameter(
        "batch_size",
        format!("{ROUTED_ACK_ROUNDTRIP_REPEAT_COUNT}_enqueue_receive_ack_roundtrips"),
    );
    configure_queue_operation_measurement(ctx);

    let (router, family, source, inbox) = setup_queue_request_sink();
    let route = "queue://bench/system/ack";
    let enqueue_frame = build_queue_enqueue(route, b"routed ack payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let dequeue_frame = build_queue_dequeue(route);
    let (dequeue_msg_type, dequeue_payload) = extract_single_tlv_field(&dequeue_frame);

    let iterations = ctx.measure_workload("complete_routed_ack_roundtrip", || {
        for _ in 0..ROUTED_ACK_ROUNDTRIP_REPEAT_COUNT {
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
        }
    });
    stress_config::record_completed(ctx, 3 * ROUTED_ACK_ROUNDTRIP_REPEAT_COUNT * iterations);
}

#[stress(tier = 3)]
fn should_complete_wait_wakeup_with_waiters(ctx: &mut StressContext) {
    ctx.parameter("scenario", "wait_wakeup");
    ctx.parameter("measurement_scope", "routed_waiters");
    ctx.parameter("operation", "watch_wakeup_roundtrip");
    ctx.parameter("batch_size", "16_watch_16_enqueue_1_receive_16_ack_cleanup");
    ctx.parameter("waiter_count", "16");
    configure_queue_operation_measurement(ctx);

    let waiter_count = 16u64;
    let route = "queue://bench/system/wait";
    let (router, family, sender_source, sender_inbox) = setup_queue_request_sink();
    let enqueue_frame = build_queue_enqueue(route, b"queue wait wake payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let dequeue_frame = build_queue_dequeue_batch(route, u64_to_u32_saturating(waiter_count));
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

    let iterations = ctx.measure_workload("complete_wait_wakeup_with_waiters", || {
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
            deliveries,
            u64_to_usize_saturating(waiter_count),
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
            u64_to_usize_saturating(waiter_count),
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
    });
    stress_config::record_completed(ctx, ((waiter_count * 2) + waiter_count + 1) * iterations);
}

stress_main!();
