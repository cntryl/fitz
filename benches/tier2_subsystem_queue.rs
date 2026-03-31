use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::{
    build_queue_complete, build_queue_dequeue, build_queue_dequeue_batch, build_queue_enqueue,
    build_queue_subscribe, create_bench_queue_sink, extract_single_tlv_field,
    register_session_counting_sink, register_session_queue_sink, route_frame, CountingSink,
    FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::DomainPublishEvent;
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

fn subscribe_queue(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    pattern: &str,
) {
    let subscribe_frame = build_queue_subscribe(pattern);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
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
    .expect("queue subscribe");
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

fn bench_queue_subscribe_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("availability_subscribe_primary", |b| {
        b.iter_batched(
            || {
                let (router, family, source, inbox) = setup_queue_request_sink();
                let subscribe_frame = build_queue_subscribe(ROUTE_STR);
                let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
                (router, family, source, inbox, msg_type, payload)
            },
            |(router, family, source, inbox, msg_type, payload)| {
                let response = request_queue_response(
                    &router,
                    family,
                    &source,
                    &inbox,
                    ROUTE_STR,
                    msg_type,
                    black_box(payload),
                );
                assert_queue_success(&response);
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

fn bench_queue_fanout_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_queue");
    group.sampling_mode(SamplingMode::Flat);

    for subscriber_count in [1usize, 16usize, 64usize] {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = create_bench_queue_sink(router.clone());
        router.register_domain_pattern("queue", sink as Arc<dyn MailboxSink>);
        let route = Route::new("queue://bench/subsystem/fanout");
        let mut subscriber_sinks: Vec<Arc<CountingSink>> = Vec::with_capacity(subscriber_count);

        for index in 0..subscriber_count {
            let session_id = 10_000 + index as u64;
            let (source, sink) = register_session_counting_sink(&router, family, session_id);
            subscribe_queue(
                &router,
                family,
                &source,
                route.as_str(),
                session_id,
                route.as_str(),
            );
            sink.reset();
            subscriber_sinks.push(sink);
        }

        let publish_event = DomainPublishEvent::new(
            family,
            route.clone(),
            Bytes::from_static(b"queue fanout payload"),
        );
        let publish_destination = RouteAddress::new(family, route.clone());

        group.throughput(Throughput::Elements(subscriber_count as u64));
        group.bench_function(
            format!("notify_{}_subscribers_primary", subscriber_count),
            |b| {
                b.iter(|| {
                    router
                        .route(Envelope::new(
                            publish_destination.clone(),
                            black_box(publish_event.clone()),
                        ))
                        .expect("queue fanout route should succeed");

                    let deliveries: usize = subscriber_sinks.iter().map(|sink| sink.count()).sum();
                    assert_eq!(
                        deliveries, subscriber_count,
                        "expected publish fanout to reach all subscribers"
                    );
                    for sink in &subscriber_sinks {
                        sink.reset();
                    }
                })
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets =
        bench_queue_subscribe_primary,
        bench_queue_enqueue_primary,
        bench_queue_dequeue_primary,
        bench_queue_ack_primary,
        bench_queue_fanout_primary
}
criterion_main!(benches);
