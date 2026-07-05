#![allow(deprecated)]
use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_queue_complete, build_queue_dequeue, build_queue_enqueue, build_queue_watch,
    create_bench_queue_sink, extract_single_tlv_field, register_session_counting_sink,
    register_session_queue_sink, route_frame, CountingSink, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CLIENT_SESSION_ID: u64 = 1;
const SETUP_SESSION_ID: u64 = 2;
const ROUTE_STR: &str = "queue://bench/subsystem/queue";
const SINGLE_QUEUE_ENQUEUE_BATCH_SIZE: usize = 1024;
const MULTI_QUEUE_MESSAGES_PER_QUEUE: usize = 1;
const DEQUEUE_OPERATION_BATCH_SIZE: usize = 256;
const ACK_OPERATION_BATCH_SIZE: usize = 256;
const WATCH_REGISTER_BATCH_SIZE: usize = 64;
const QUEUE_MEASUREMENT_REPEAT_COUNT: u32 = 2;

struct PreparedDequeueCase {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
    dequeue_msg_type: u16,
    dequeue_payload: Bytes,
}

struct PreparedAckCase {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
    route: &'static str,
    ack_requests: Vec<(u16, Bytes)>,
}

struct PreparedWatchRegisterCase {
    router: Arc<Router>,
    family: RouteFamily,
    registrations: Vec<(u64, RouteAddress, Arc<CountingSink>, u16, Bytes)>,
}

fn setup_queue_domain() -> (Arc<Router>, RouteFamily) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_queue_sink(router.clone());
    router.register_domain_pattern("queue", sink as Arc<dyn MailboxSink>);
    (router, family)
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
    send_queue_request(
        router.as_ref(),
        source,
        destination,
        CLIENT_SESSION_ID,
        msg_type,
        payload,
        family,
    );

    inbox
        .drain_after_count(1, Duration::from_secs(1))
        .last()
        .map(|frame| frame.payload.clone())
        .expect("queue response")
}

fn send_queue_request(
    router: &Router,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    msg_type: u16,
    payload: Bytes,
    family: RouteFamily,
) {
    route_frame(
        router,
        source,
        destination,
        session_id,
        ChannelId::Sub,
        msg_type,
        payload,
        family,
    )
    .expect("queue request");
}

fn assert_queue_success(response: &[u8]) {
    assert_eq!(
        response.first().copied(),
        Some(0),
        "expected queue success response"
    );
}

fn assert_queue_success_frames(
    frames: Vec<fitz::protocol::frame_context::FrameContext>,
    expected_count: usize,
) {
    assert_eq!(
        frames.len(),
        expected_count,
        "expected {expected_count} queue responses"
    );

    for frame in frames {
        assert_queue_success(&frame.payload);
    }
}

fn queue_response_message_count(response: &[u8]) -> usize {
    assert_queue_success(response);
    assert!(response.len() >= 5, "queue receive response too short");
    u32::from_be_bytes([response[1], response[2], response[3], response[4]]) as usize
}

fn parse_single_received_message(response: &[u8]) -> (u64, u64) {
    assert_queue_success(response);
    assert!(response.len() >= 25, "queue receive response too short");

    let message_count = u32::from_be_bytes([response[1], response[2], response[3], response[4]]);
    assert_eq!(
        message_count, 1,
        "expected exactly one received queue message"
    );

    let message_id = u64::from_be_bytes([
        response[5],
        response[6],
        response[7],
        response[8],
        response[9],
        response[10],
        response[11],
        response[12],
    ]);
    let token = u64::from_be_bytes([
        response[13],
        response[14],
        response[15],
        response[16],
        response[17],
        response[18],
        response[19],
        response[20],
    ]);

    (message_id, token)
}

fn build_queue_routes(queue_count: usize) -> Vec<String> {
    (0..queue_count)
        .map(|index| format!("queue://bench/subsystem/queue/{index}"))
        .collect()
}

fn prepare_dequeue_case() -> PreparedDequeueCase {
    let (router, family) = setup_queue_domain();
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    let (enqueue_source, enqueue_sink) =
        register_session_counting_sink(&router, family, SETUP_SESSION_ID);
    let enqueue_frame = build_queue_enqueue(ROUTE_STR, b"queue dequeue payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);

    for _ in 0..DEQUEUE_OPERATION_BATCH_SIZE {
        send_queue_request(
            router.as_ref(),
            &enqueue_source,
            ROUTE_STR,
            SETUP_SESSION_ID,
            enqueue_msg_type,
            enqueue_payload.clone(),
            family,
        );
    }
    assert_eq!(
        enqueue_sink.wait_for_count(DEQUEUE_OPERATION_BATCH_SIZE, Duration::from_secs(1)),
        DEQUEUE_OPERATION_BATCH_SIZE,
        "queue dequeue setup should ack every enqueue"
    );

    let dequeue_frame = build_queue_dequeue(ROUTE_STR);
    let (dequeue_msg_type, dequeue_payload) = extract_single_tlv_field(&dequeue_frame);

    PreparedDequeueCase {
        router,
        family,
        source,
        inbox,
        dequeue_msg_type,
        dequeue_payload,
    }
}

fn prepare_ack_case() -> PreparedAckCase {
    let (router, family) = setup_queue_domain();
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    let (enqueue_source, enqueue_sink) =
        register_session_counting_sink(&router, family, SETUP_SESSION_ID);
    let enqueue_frame = build_queue_enqueue(ROUTE_STR, b"queue ack payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let dequeue_frame = build_queue_dequeue(ROUTE_STR);
    let (dequeue_msg_type, dequeue_payload) = extract_single_tlv_field(&dequeue_frame);

    for _ in 0..ACK_OPERATION_BATCH_SIZE {
        send_queue_request(
            router.as_ref(),
            &enqueue_source,
            ROUTE_STR,
            SETUP_SESSION_ID,
            enqueue_msg_type,
            enqueue_payload.clone(),
            family,
        );
    }
    assert_eq!(
        enqueue_sink.wait_for_count(ACK_OPERATION_BATCH_SIZE, Duration::from_secs(1)),
        ACK_OPERATION_BATCH_SIZE,
        "queue ack setup should ack every enqueue"
    );

    let ack_requests = (0..ACK_OPERATION_BATCH_SIZE)
        .map(|_| {
            let dequeue_response = request_queue_response(
                &router,
                family,
                &source,
                &inbox,
                ROUTE_STR,
                dequeue_msg_type,
                dequeue_payload.clone(),
            );
            let (message_id, token) = parse_single_received_message(&dequeue_response);

            let ack_frame = build_queue_complete(ROUTE_STR, message_id, token);
            extract_single_tlv_field(&ack_frame)
        })
        .collect();

    PreparedAckCase {
        router,
        family,
        source,
        inbox,
        route: ROUTE_STR,
        ack_requests,
    }
}

fn prepare_watch_register_case() -> PreparedWatchRegisterCase {
    let (router, family) = setup_queue_domain();
    let registrations = (0..WATCH_REGISTER_BATCH_SIZE)
        .map(|index| {
            let session_id = 10_000 + u64::try_from(index).expect("watch index should fit u64");
            let (source, sink) = register_session_counting_sink(&router, family, session_id);
            let watch_route = format!("queue://bench/subsystem/watch/{index}/ready");
            let watch_frame = build_queue_watch(&watch_route);
            let (msg_type, payload) = extract_single_tlv_field(&watch_frame);
            (session_id, source, sink, msg_type, payload)
        })
        .collect();

    PreparedWatchRegisterCase {
        router,
        family,
        registrations,
    }
}

fn queue_enqueue(ctx: &mut StressContext, queue_count: usize, messages_per_queue: usize) {
    let mut total = Duration::ZERO;
    let expected_responses = queue_count * messages_per_queue;
    for _ in 0..QUEUE_MEASUREMENT_REPEAT_COUNT {
        let (router, family) = setup_queue_domain();
        let (source, sink) = register_session_counting_sink(&router, family, CLIENT_SESSION_ID);
        let queue_routes = build_queue_routes(queue_count);
        let enqueue_frames = queue_routes
            .iter()
            .map(|route| {
                let frame = build_queue_enqueue(route, b"queue enqueue payload");
                extract_single_tlv_field(&frame)
            })
            .collect::<Vec<_>>();

        let start = Instant::now();
        for _ in 0..messages_per_queue {
            for (route, (msg_type, payload)) in queue_routes.iter().zip(enqueue_frames.iter()) {
                send_queue_request(
                    router.as_ref(),
                    &source,
                    route,
                    CLIENT_SESSION_ID,
                    *msg_type,
                    black_box(payload.clone()),
                    family,
                );
            }
        }
        total += start.elapsed();

        assert_eq!(
            sink.wait_for_count(expected_responses, Duration::from_secs(1)),
            expected_responses,
            "queue enqueue should ack every request"
        );
        router.clear();
    }
    tier2_stress::record_duration(
        ctx,
        total / QUEUE_MEASUREMENT_REPEAT_COUNT,
        expected_responses as u64,
    );
}

#[stress(tier = 2, name = "enqueue_1024_messages_1_queue_primary")]
fn should_enqueue_1024_messages_1_queue_primary(ctx: &mut StressContext) {
    queue_enqueue(ctx, 1, SINGLE_QUEUE_ENQUEUE_BATCH_SIZE);
}

#[stress(tier = 2, name = "enqueue_1_messages_each_256_queues_primary")]
fn should_enqueue_1_messages_each_256_queues_primary(ctx: &mut StressContext) {
    queue_enqueue(ctx, 256, MULTI_QUEUE_MESSAGES_PER_QUEUE);
}

#[stress(tier = 2, name = "dequeue_256_messages_primary")]
fn should_dequeue_256_messages_primary(ctx: &mut StressContext) {
    let mut total = Duration::ZERO;
    for _ in 0..QUEUE_MEASUREMENT_REPEAT_COUNT {
        let case = prepare_dequeue_case();
        let start = Instant::now();
        for _ in 0..DEQUEUE_OPERATION_BATCH_SIZE {
            send_queue_request(
                case.router.as_ref(),
                &case.source,
                ROUTE_STR,
                CLIENT_SESSION_ID,
                case.dequeue_msg_type,
                black_box(case.dequeue_payload.clone()),
                case.family,
            );
        }
        total += start.elapsed();

        let frames = case
            .inbox
            .drain_after_count(DEQUEUE_OPERATION_BATCH_SIZE, Duration::from_secs(1));
        assert_eq!(
            frames.len(),
            DEQUEUE_OPERATION_BATCH_SIZE,
            "queue dequeue should respond to every request"
        );
        for frame in frames {
            assert_eq!(
                queue_response_message_count(&frame.payload),
                1,
                "expected a single received queue message"
            );
        }
        case.router.clear();
    }
    tier2_stress::record_duration(
        ctx,
        total / QUEUE_MEASUREMENT_REPEAT_COUNT,
        DEQUEUE_OPERATION_BATCH_SIZE as u64,
    );
}

#[stress(tier = 2, name = "ack_256_messages_primary")]
fn should_ack_256_messages_primary(ctx: &mut StressContext) {
    let mut total = Duration::ZERO;
    for _ in 0..QUEUE_MEASUREMENT_REPEAT_COUNT {
        let case = prepare_ack_case();
        let start = Instant::now();
        for (ack_msg_type, ack_payload) in &case.ack_requests {
            send_queue_request(
                case.router.as_ref(),
                &case.source,
                case.route,
                CLIENT_SESSION_ID,
                *ack_msg_type,
                black_box(ack_payload.clone()),
                case.family,
            );
        }
        total += start.elapsed();

        assert_queue_success_frames(
            case.inbox
                .drain_after_count(case.ack_requests.len(), Duration::from_secs(1)),
            case.ack_requests.len(),
        );
        case.router.clear();
    }
    tier2_stress::record_duration(
        ctx,
        total / QUEUE_MEASUREMENT_REPEAT_COUNT,
        ACK_OPERATION_BATCH_SIZE as u64,
    );
}

#[stress(tier = 2, name = "watch_register_64_sessions_primary")]
fn should_watch_register_64_sessions_primary(ctx: &mut StressContext) {
    let mut total = Duration::ZERO;
    for _ in 0..QUEUE_MEASUREMENT_REPEAT_COUNT {
        let case = prepare_watch_register_case();
        let start = Instant::now();
        for (session_id, source, _, msg_type, payload) in &case.registrations {
            send_queue_request(
                case.router.as_ref(),
                source,
                ROUTE_STR,
                *session_id,
                *msg_type,
                black_box(payload.clone()),
                case.family,
            );
        }
        total += start.elapsed();

        for (_, _, sink, _, _) in &case.registrations {
            assert_eq!(
                sink.wait_for_count(1, Duration::from_secs(1)),
                1,
                "queue watch registration should ack every request"
            );
        }
        case.router.clear();
    }
    tier2_stress::record_duration(
        ctx,
        total / QUEUE_MEASUREMENT_REPEAT_COUNT,
        WATCH_REGISTER_BATCH_SIZE as u64,
    );
}

stress_main!();
