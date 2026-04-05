use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::{
    build_queue_complete, build_queue_dequeue, build_queue_dequeue_batch, build_queue_enqueue,
    build_queue_watch, create_bench_queue_sink, extract_single_tlv_field,
    register_session_counting_sink, register_session_queue_sink, route_frame, CountingSink,
    FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

#[path = "criterion_config.rs"]
mod criterion_config;

const CLIENT_SESSION_ID: u64 = 1;
const RECEIVE_BATCH_SIZE: usize = 50;
const ROUTE_STR: &str = "queue://bench/subsystem/queue";

struct PreparedAckCase {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
    route: &'static str,
    ack_msg_type: u16,
    ack_payload: Bytes,
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

fn prepare_ack_case() -> PreparedAckCase {
    let (router, family, source, inbox) = setup_queue_request_sink();
    let enqueue_frame = build_queue_enqueue(ROUTE_STR, b"queue ack payload");
    let (enqueue_msg_type, enqueue_payload) = extract_single_tlv_field(&enqueue_frame);
    let enqueue_response = request_queue_response(
        &router,
        family,
        &source,
        &inbox,
        ROUTE_STR,
        enqueue_msg_type,
        enqueue_payload,
    );
    assert_queue_success(&enqueue_response);

    let dequeue_frame = build_queue_dequeue(ROUTE_STR);
    let (dequeue_msg_type, dequeue_payload) = extract_single_tlv_field(&dequeue_frame);
    let dequeue_response = request_queue_response(
        &router,
        family,
        &source,
        &inbox,
        ROUTE_STR,
        dequeue_msg_type,
        dequeue_payload,
    );
    let (message_id, token) = parse_single_received_message(&dequeue_response);

    let ack_frame = build_queue_complete(ROUTE_STR, message_id, token);
    let (ack_msg_type, ack_payload) = extract_single_tlv_field(&ack_frame);

    PreparedAckCase {
        router,
        family,
        source,
        inbox,
        route: ROUTE_STR,
        ack_msg_type,
        ack_payload,
    }
}

fn bench_queue_wait_register_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("watch_register_primary", |b| {
        b.iter_batched(
            || {
                let (router, family, source, inbox) = setup_queue_request_sink();
                (router, family, source, inbox)
            },
            |(router, family, source, inbox)| {
                register_queue_watch(&router, family, &source, ROUTE_STR, CLIENT_SESSION_ID);
                assert_eq!(
                    inbox.drain().len(),
                    1,
                    "watch registration should ack immediately"
                );
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_queue_enqueue_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(250));

    for queue_count in [1usize, 64usize, 256usize] {
        group.throughput(Throughput::Elements(queue_count as u64));
        group.bench_function(format!("enqueue_{}_queues_primary", queue_count), |b| {
            b.iter_batched(
                || {
                    let (router, family, source, inbox) = setup_queue_request_sink();
                    let routes = build_queue_routes(queue_count);
                    let enqueue_frames: Vec<(u16, Bytes)> = routes
                        .iter()
                        .map(|route| {
                            let frame = build_queue_enqueue(route, b"queue enqueue payload");
                            extract_single_tlv_field(&frame)
                        })
                        .collect();
                    (router, family, source, inbox, routes, enqueue_frames)
                },
                |(router, family, source, inbox, routes, enqueue_frames)| {
                    for (route, (msg_type, payload)) in routes.iter().zip(enqueue_frames.iter()) {
                        let response = request_queue_response(
                            &router,
                            family,
                            &source,
                            &inbox,
                            route,
                            *msg_type,
                            black_box(payload.clone()),
                        );
                        assert_queue_success(&response);
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_queue_dequeue_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);

    for batch_size in [1usize, RECEIVE_BATCH_SIZE] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_function(format!("dequeue_batch_{}_primary", batch_size), |b| {
            b.iter_batched(
                || {
                    let (router, family, source, inbox) = setup_queue_request_sink();
                    let enqueue_frame = build_queue_enqueue(ROUTE_STR, b"queue dequeue payload");
                    let (enqueue_msg_type, enqueue_payload) =
                        extract_single_tlv_field(&enqueue_frame);

                    for _ in 0..batch_size {
                        let enqueue_response = request_queue_response(
                            &router,
                            family,
                            &source,
                            &inbox,
                            ROUTE_STR,
                            enqueue_msg_type,
                            enqueue_payload.clone(),
                        );
                        assert_queue_success(&enqueue_response);
                    }

                    let dequeue_frame = if batch_size == 1 {
                        build_queue_dequeue(ROUTE_STR)
                    } else {
                        build_queue_dequeue_batch(ROUTE_STR, batch_size as u32)
                    };
                    let (dequeue_msg_type, dequeue_payload) =
                        extract_single_tlv_field(&dequeue_frame);
                    (
                        router,
                        family,
                        source,
                        inbox,
                        dequeue_msg_type,
                        dequeue_payload,
                        batch_size,
                    )
                },
                |(router, family, source, inbox, dequeue_msg_type, dequeue_payload, batch_size)| {
                    let response = request_queue_response(
                        &router,
                        family,
                        &source,
                        &inbox,
                        ROUTE_STR,
                        dequeue_msg_type,
                        black_box(dequeue_payload),
                    );
                    assert_eq!(
                        queue_response_message_count(&response),
                        batch_size,
                        "expected a full queue receive batch"
                    );
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_queue_ack_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("ack_single_primary", |b| {
        b.iter_batched(
            prepare_ack_case,
            |case| {
                let response = request_queue_response(
                    &case.router,
                    case.family,
                    &case.source,
                    &case.inbox,
                    case.route,
                    case.ack_msg_type,
                    black_box(case.ack_payload),
                );
                assert_queue_success(&response);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_queue_waiter_wake_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);

    for waiter_count in [1usize, 16usize, 64usize] {
        group.throughput(Throughput::Elements(waiter_count as u64));
        group.bench_function(format!("notify_{}_watchers_primary", waiter_count), |b| {
            b.iter_batched(
                || {
                    let family = RouteFamily::new(1);
                    let router = Arc::new(Router::new());
                    let sink = create_bench_queue_sink(router.clone());
                    router.register_domain_pattern("queue", sink as Arc<dyn MailboxSink>);
                    let (sender_source, sender_inbox) =
                        register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
                    let mut waiter_sinks: Vec<Arc<CountingSink>> = Vec::with_capacity(waiter_count);

                    for index in 0..waiter_count {
                        let session_id = 10_000 + index as u64;
                        let (wait_source, wait_sink) =
                            register_session_counting_sink(&router, family, session_id);
                        register_queue_watch(&router, family, &wait_source, ROUTE_STR, session_id);
                        wait_sink.reset();
                        waiter_sinks.push(wait_sink);
                    }

                    let enqueue_frame = build_queue_enqueue(ROUTE_STR, b"queue waiter payload");
                    let (enqueue_msg_type, enqueue_payload) =
                        extract_single_tlv_field(&enqueue_frame);
                    (
                        router,
                        family,
                        sender_source,
                        sender_inbox,
                        waiter_sinks,
                        enqueue_msg_type,
                        enqueue_payload,
                    )
                },
                |(
                    router,
                    family,
                    sender_source,
                    sender_inbox,
                    waiter_sinks,
                    enqueue_msg_type,
                    enqueue_payload,
                )| {
                    for _ in 0..waiter_count {
                        let response = request_queue_response(
                            &router,
                            family,
                            &sender_source,
                            &sender_inbox,
                            ROUTE_STR,
                            enqueue_msg_type,
                            black_box(enqueue_payload.clone()),
                        );
                        assert_queue_success(&response);
                    }

                    let deliveries: usize = waiter_sinks.iter().map(|sink| sink.count()).sum();
                    assert_eq!(
                        deliveries, waiter_count,
                        "expected queue send to wake all waiting receivers"
                    );
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets =
        bench_queue_wait_register_primary,
        bench_queue_enqueue_primary,
        bench_queue_dequeue_primary,
        bench_queue_ack_primary,
        bench_queue_waiter_wake_primary
}
criterion_main!(benches);
