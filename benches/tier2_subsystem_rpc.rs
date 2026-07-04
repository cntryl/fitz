#![allow(deprecated)]
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::{
    build_rpc_request, build_rpc_response_frame, build_rpc_subscribe, create_bench_rpc_sink,
    create_bench_rpc_sink_with_timeout, drain_frame_queue_sinks_after_each_count,
    extract_single_tlv_field, register_session_counting_sink, register_session_queue_sink,
    route_frame, CountingSink, FrameQueueSink,
};
use fitz::domains::rpc::protocol::{RpcMessage, RpcResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::{encode_response_message, parse_request};
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "criterion_config.rs"]
mod criterion_config;

const ROUTE_STR: &str = "rpc://bench/subsystem/route";
const REQUESTER_SESSION_ID: u64 = 1;
const REQUEST_FRAME_RING_SIZE: usize = 4096;
const DISPATCH_BATCH_SIZE: usize = 1024;
const MULTI_ROUTE_COUNT: usize = 64;
const RPC_REQUEST_TIMEOUT: Duration = Duration::from_millis(10);
const RPC_TIMEOUT_SWEEP_WAIT: Duration = Duration::from_millis(25);
const WORKER_SUBSCRIBE_BATCH_SIZE: usize = 2048;
const RESPONSE_FORWARD_BATCH_SIZE: usize = 512;
const STREAM_RESPONSE_BATCH_SIZE: usize = 32;
const STREAM_RESPONSE_ROUTE_COUNT: usize = 8;

type WorkerHandle = (u64, RouteAddress, Arc<FrameQueueSink>);

struct PreparedWorkerResponse {
    worker_session_id: u64,
    worker_source: RouteAddress,
    destination: RouteAddress,
    payload: Bytes,
}

struct PreparedWorkerSubscribeCase {
    router: Arc<Router>,
    family: RouteFamily,
    destination: RouteAddress,
    subscriptions: Vec<(u64, RouteAddress, Arc<FrameQueueSink>)>,
    msg_type: u16,
    payload: Bytes,
}

struct RequestFrameRing {
    frames: Vec<(u16, Bytes)>,
    next: usize,
}

impl RequestFrameRing {
    fn new(route: &str, payload: &[u8], count: usize) -> Self {
        let frames = (0..count)
            .map(|_| {
                let frame = build_rpc_request(route, payload);
                extract_single_tlv_field(&frame)
            })
            .collect();

        Self { frames, next: 0 }
    }

    fn next_frame(&mut self) -> (u16, Bytes) {
        let (msg_type, payload) = &self.frames[self.next];
        self.next = (self.next + 1) % self.frames.len();
        (*msg_type, payload.clone())
    }
}

struct PreparedResponseCase {
    router: Arc<Router>,
    family: RouteFamily,
    requester_inbox: Arc<CountingSink>,
    response_msg_type: u16,
    responses: Vec<PreparedWorkerResponse>,
}

struct PreparedStreamingResponseCase {
    router: Arc<Router>,
    family: RouteFamily,
    requester_inbox: Arc<CountingSink>,
    response_msg_type: u16,
    responses: Vec<PreparedWorkerResponse>,
    expected_response_count: usize,
}

struct PreparedTimeoutSweepCase {
    router: Arc<Router>,
    family: RouteFamily,
    requester_source: RouteAddress,
    destination: RouteAddress,
    requester_inbox: Arc<FrameQueueSink>,
    worker_inbox: Arc<FrameQueueSink>,
    request_msg_type: u16,
    request_payload: Bytes,
}

struct PreparedTimeoutSweepBatchCase {
    cases: Vec<PreparedTimeoutSweepCase>,
}

fn route_address(family: RouteFamily, route: &str) -> RouteAddress {
    RouteAddress::new(family, Route::new(route))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn build_route_set(family: RouteFamily, route_count: usize) -> Vec<RouteAddress> {
    (0..route_count)
        .map(|index| route_address(family, &format!("rpc://bench/subsystem/route/{index}")))
        .collect()
}

fn timeout_sweep_case_batch_size(expired_pending: usize) -> usize {
    match expired_pending {
        64 => 32,
        _ => 16,
    }
}

fn assert_requester_received_worker_responses(inbox: &Arc<CountingSink>, expected_count: usize) {
    let response_count = inbox.wait_for_count(expected_count, Duration::from_secs(1));
    assert_eq!(
        response_count, expected_count,
        "expected requester inbox to contain {expected_count} worker responses"
    );
}

fn route_frame_to_address(
    router: &Router,
    source: &RouteAddress,
    destination: &RouteAddress,
    session_id: u64,
    msg_type: u16,
    payload: Bytes,
    family: RouteFamily,
) {
    route_frame(
        router,
        source,
        destination.route().as_str(),
        session_id,
        ChannelId::Rpc,
        msg_type,
        payload,
        family,
    )
    .expect("rpc route should succeed");
}

fn setup_rpc_sink(
    request_timeout: Option<Duration>,
) -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink: Arc<dyn MailboxSink> = match request_timeout {
        Some(timeout) => create_bench_rpc_sink_with_timeout(router.clone(), timeout),
        None => create_bench_rpc_sink(router.clone()),
    };
    router.register_domain_pattern("rpc", sink);
    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
    (router, family, requester_source, requester_inbox)
}

fn setup_rpc_sink_with_counting_requester(
    request_timeout: Option<Duration>,
) -> (Arc<Router>, RouteFamily, RouteAddress, Arc<CountingSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink: Arc<dyn MailboxSink> = match request_timeout {
        Some(timeout) => create_bench_rpc_sink_with_timeout(router.clone(), timeout),
        None => create_bench_rpc_sink(router.clone()),
    };
    router.register_domain_pattern("rpc", sink);
    let (requester_source, requester_inbox) =
        register_session_counting_sink(&router, family, REQUESTER_SESSION_ID);
    (router, family, requester_source, requester_inbox)
}

fn register_worker_for_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    session_id: u64,
    destination: &RouteAddress,
) -> WorkerHandle {
    let (worker_source, worker_inbox) = register_session_queue_sink(router, family, session_id);
    let subscribe_frame = build_rpc_subscribe(destination.route().as_str());
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame_to_address(
        router.as_ref(),
        &worker_source,
        destination,
        session_id,
        msg_type,
        payload,
        family,
    );
    worker_inbox.drain_after_count(1, Duration::from_secs(1));
    (session_id, worker_source, worker_inbox)
}

fn dispatch_request_to_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    requester_source: &RouteAddress,
    destination: &RouteAddress,
    request_msg_type: u16,
    request_payload: Bytes,
) {
    route_frame_to_address(
        router.as_ref(),
        requester_source,
        destination,
        REQUESTER_SESSION_ID,
        request_msg_type,
        request_payload,
        family,
    );
}

fn route_worker_frame_to_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    destination: &RouteAddress,
    msg_type: u16,
    payload: Bytes,
) {
    route_frame_to_address(
        router.as_ref(),
        worker_source,
        destination,
        worker_session_id,
        msg_type,
        payload,
        family,
    );
}

fn drain_request_correlation(
    worker_inbox: &Arc<FrameQueueSink>,
    family: RouteFamily,
) -> uuid::Uuid {
    worker_inbox
        .drain_after_count(1, Duration::from_secs(1))
        .into_iter()
        .find_map(|frame| match frame.msg_type.as_u16() {
            302 => match parse_request(&frame, &frame.payload, family) {
                Ok(RpcMessage::Request(request)) => Some(request.correlation_id),
                _ => None,
            },
            _ => None,
        })
        .expect("rpc dispatched request")
}

fn cleanup_worker_request_on_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    destination: &RouteAddress,
    worker: &WorkerHandle,
) {
    let (worker_session_id, worker_source, worker_inbox) = worker;
    let correlation_id = drain_request_correlation(worker_inbox, family);
    let (response_msg_type, response_payload) =
        extract_single_tlv_field(&build_rpc_response_frame(correlation_id, b"cleanup"));
    route_worker_frame_to_destination(
        router,
        family,
        *worker_session_id,
        worker_source,
        destination,
        response_msg_type,
        response_payload,
    );
}

fn prepare_worker_subscribe_case() -> PreparedWorkerSubscribeCase {
    let (router, family, _, _) = setup_rpc_sink(None);
    let destination = route_address(family, ROUTE_STR);
    let subscribe_frame = build_rpc_subscribe(ROUTE_STR);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    let subscriptions = (0..WORKER_SUBSCRIBE_BATCH_SIZE)
        .map(|index| {
            let session_id = 30_000_u64.saturating_add(usize_to_u64_saturating(index));
            let (worker_source, worker_inbox) =
                register_session_queue_sink(&router, family, session_id);
            (session_id, worker_source, worker_inbox)
        })
        .collect();

    PreparedWorkerSubscribeCase {
        router,
        family,
        destination,
        subscriptions,
        msg_type,
        payload,
    }
}

fn prepare_response_case() -> PreparedResponseCase {
    let (router, family, requester_source, requester_inbox) =
        setup_rpc_sink_with_counting_requester(None);
    let destination = route_address(family, ROUTE_STR);
    let workers: Vec<WorkerHandle> = (0..RESPONSE_FORWARD_BATCH_SIZE)
        .map(|index| {
            register_worker_for_destination(
                &router,
                family,
                10_000_u64.saturating_add(usize_to_u64_saturating(index)),
                &destination,
            )
        })
        .collect();
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"response payload", RESPONSE_FORWARD_BATCH_SIZE);

    for _ in 0..RESPONSE_FORWARD_BATCH_SIZE {
        let (request_msg_type, request_payload) = request_ring.next_frame();
        dispatch_request_to_destination(
            &router,
            family,
            &requester_source,
            &destination,
            request_msg_type,
            request_payload,
        );
    }

    let responses = workers
        .iter()
        .map(|(worker_session_id, worker_source, worker_inbox)| {
            let correlation_id = drain_request_correlation(worker_inbox, family);
            let response_frame = build_rpc_response_frame(correlation_id, b"response payload");
            let (_, response_payload) = extract_single_tlv_field(&response_frame);
            PreparedWorkerResponse {
                worker_session_id: *worker_session_id,
                worker_source: worker_source.clone(),
                destination: destination.clone(),
                payload: response_payload,
            }
        })
        .collect();
    requester_inbox.reset();

    PreparedResponseCase {
        router,
        family,
        requester_inbox,
        response_msg_type: 303,
        responses,
    }
}

fn prepare_streaming_response_case(chunk_count: usize) -> PreparedStreamingResponseCase {
    let (router, family, requester_source, requester_inbox) =
        setup_rpc_sink_with_counting_requester(None);
    let mut responses =
        Vec::with_capacity(STREAM_RESPONSE_ROUTE_COUNT * STREAM_RESPONSE_BATCH_SIZE * chunk_count);

    for route_index in 0..STREAM_RESPONSE_ROUTE_COUNT {
        let route = format!("{ROUTE_STR}/{route_index}");
        let destination = route_address(family, &route);
        let workers: Vec<WorkerHandle> = (0..STREAM_RESPONSE_BATCH_SIZE)
            .map(|worker_index| {
                let session_id = 11_000_u64
                    .saturating_add(usize_to_u64_saturating(route_index * 1_000))
                    .saturating_add(usize_to_u64_saturating(worker_index));
                register_worker_for_destination(&router, family, session_id, &destination)
            })
            .collect();
        let mut request_ring = RequestFrameRing::new(
            &route,
            b"stream response payload",
            STREAM_RESPONSE_BATCH_SIZE,
        );

        for _ in 0..STREAM_RESPONSE_BATCH_SIZE {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request_to_destination(
                &router,
                family,
                &requester_source,
                &destination,
                request_msg_type,
                request_payload,
            );
        }

        for (worker_session_id, worker_source, worker_inbox) in workers {
            let correlation_id = drain_request_correlation(&worker_inbox, family);
            responses.extend((0..chunk_count).map(|seq| {
                let response = RpcResponse::chunk(
                    correlation_id,
                    usize_to_u64_saturating(seq),
                    Bytes::from_static(b"stream response payload"),
                    seq + 1 == chunk_count,
                );
                PreparedWorkerResponse {
                    worker_session_id,
                    worker_source: worker_source.clone(),
                    destination: destination.clone(),
                    payload: Bytes::from(encode_response_message(&response)),
                }
            }));
        }
    }
    requester_inbox.reset();

    PreparedStreamingResponseCase {
        router,
        family,
        requester_inbox,
        response_msg_type: 303,
        responses,
        expected_response_count: STREAM_RESPONSE_ROUTE_COUNT
            * STREAM_RESPONSE_BATCH_SIZE
            * chunk_count,
    }
}

fn prepare_timeout_sweep_case(expired_pending: usize) -> PreparedTimeoutSweepCase {
    let (router, family, requester_source, requester_inbox) =
        setup_rpc_sink(Some(RPC_REQUEST_TIMEOUT));
    let destination = route_address(family, ROUTE_STR);
    let (_, _, worker_inbox) =
        register_worker_for_destination(&router, family, 20_000, &destination);
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"timeout sweep payload", expired_pending + 1);

    for _ in 0..expired_pending {
        let (request_msg_type, request_payload) = request_ring.next_frame();
        dispatch_request_to_destination(
            &router,
            family,
            &requester_source,
            &destination,
            request_msg_type,
            request_payload,
        );
    }

    worker_inbox.drain_after_count(1, Duration::from_secs(1));
    requester_inbox.clear();

    let (request_msg_type, request_payload) = request_ring.next_frame();
    PreparedTimeoutSweepCase {
        router,
        family,
        requester_source,
        destination,
        requester_inbox,
        worker_inbox,
        request_msg_type,
        request_payload,
    }
}

fn prepare_timeout_sweep_batch_case(expired_pending: usize) -> PreparedTimeoutSweepBatchCase {
    let cases = (0..timeout_sweep_case_batch_size(expired_pending))
        .map(|_| prepare_timeout_sweep_case(expired_pending))
        .collect();
    std::thread::sleep(RPC_TIMEOUT_SWEEP_WAIT);
    PreparedTimeoutSweepBatchCase { cases }
}

fn bench_rpc_worker_subscribe_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_rpc");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(usize_to_u64_saturating(
        WORKER_SUBSCRIBE_BATCH_SIZE,
    )));

    group.bench_function("worker_subscribe_2048_sessions_primary", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let case = prepare_worker_subscribe_case();
                let start = Instant::now();
                for (session_id, worker_source, _) in &case.subscriptions {
                    route_frame_to_address(
                        case.router.as_ref(),
                        worker_source,
                        &case.destination,
                        *session_id,
                        case.msg_type,
                        black_box(case.payload.clone()),
                        case.family,
                    );
                }
                total += start.elapsed();

                let inboxes: Vec<_> = case
                    .subscriptions
                    .iter()
                    .map(|(_, _, worker_inbox)| worker_inbox.clone())
                    .collect();
                let subscribe_response_count =
                    drain_frame_queue_sinks_after_each_count(&inboxes, 1, Duration::from_secs(1))
                        .len();
                assert_eq!(subscribe_response_count, WORKER_SUBSCRIBE_BATCH_SIZE);
                case.router.clear();
            }
            total
        });
    });

    group.finish();
}

#[allow(clippy::too_many_lines)]
fn bench_rpc_dispatch_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_rpc");
    group.sampling_mode(SamplingMode::Flat);

    for worker_count in [1usize, 64usize, 256usize] {
        let (router, family, requester_source, requester_inbox) = setup_rpc_sink(None);
        let destination = route_address(family, ROUTE_STR);
        let workers: Vec<WorkerHandle> = (0..worker_count)
            .map(|index| {
                register_worker_for_destination(
                    &router,
                    family,
                    40_000_u64.saturating_add(usize_to_u64_saturating(index)),
                    &destination,
                )
            })
            .collect();
        let mut request_ring =
            RequestFrameRing::new(ROUTE_STR, b"dispatch payload", REQUEST_FRAME_RING_SIZE);
        let mut next_worker_index = 0usize;

        group.throughput(Throughput::Elements(usize_to_u64_saturating(
            DISPATCH_BATCH_SIZE,
        )));
        group.bench_function(
            format!("dispatch_response_cleanup_1024_ops_{worker_count}_workers_primary"),
            |b| {
                b.iter(|| {
                    for _ in 0..DISPATCH_BATCH_SIZE {
                        let (request_msg_type, request_payload) = request_ring.next_frame();
                        dispatch_request_to_destination(
                            &router,
                            family,
                            &requester_source,
                            &destination,
                            request_msg_type,
                            black_box(request_payload),
                        );
                        cleanup_worker_request_on_destination(
                            &router,
                            family,
                            &destination,
                            &workers[next_worker_index],
                        );
                        next_worker_index = (next_worker_index + 1) % workers.len();
                        requester_inbox.clear();
                    }
                });
            },
        );
    }

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink(None);
    let destinations = build_route_set(family, MULTI_ROUTE_COUNT);
    let workers: Vec<WorkerHandle> = destinations
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            register_worker_for_destination(
                &router,
                family,
                50_000_u64.saturating_add(usize_to_u64_saturating(index)),
                destination,
            )
        })
        .collect();
    let mut request_rings: Vec<RequestFrameRing> = destinations
        .iter()
        .map(|destination| {
            RequestFrameRing::new(
                destination.route().as_str(),
                b"multi route dispatch payload",
                64,
            )
        })
        .collect();
    let mut next_route_index = 0usize;

    group.throughput(Throughput::Elements(usize_to_u64_saturating(
        DISPATCH_BATCH_SIZE,
    )));
    group.bench_function(
        "dispatch_response_cleanup_1024_ops_64_routes_primary",
        |b| {
            b.iter(|| {
                for _ in 0..DISPATCH_BATCH_SIZE {
                    let route_index = next_route_index;
                    next_route_index = (next_route_index + 1) % destinations.len();

                    let (request_msg_type, request_payload) =
                        request_rings[route_index].next_frame();
                    dispatch_request_to_destination(
                        &router,
                        family,
                        &requester_source,
                        &destinations[route_index],
                        request_msg_type,
                        black_box(request_payload),
                    );
                    cleanup_worker_request_on_destination(
                        &router,
                        family,
                        &destinations[route_index],
                        &workers[route_index],
                    );
                    requester_inbox.clear();
                }
            });
        },
    );

    group.finish();
}

fn bench_rpc_response_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_rpc");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(usize_to_u64_saturating(
        RESPONSE_FORWARD_BATCH_SIZE,
    )));

    group.bench_function(
        format!("response_forward_{RESPONSE_FORWARD_BATCH_SIZE}_pending_primary"),
        |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let case = prepare_response_case();
                    let start = Instant::now();
                    for response in &case.responses {
                        route_worker_frame_to_destination(
                            &case.router,
                            case.family,
                            response.worker_session_id,
                            &response.worker_source,
                            &response.destination,
                            case.response_msg_type,
                            black_box(response.payload.clone()),
                        );
                    }
                    total += start.elapsed();
                    assert_requester_received_worker_responses(
                        &case.requester_inbox,
                        case.responses.len(),
                    );
                    case.router.clear();
                }
                total
            });
        },
    );

    for chunk_count in [4usize, 16usize] {
        group.throughput(Throughput::Elements(usize_to_u64_saturating(
            chunk_count
                .saturating_mul(STREAM_RESPONSE_BATCH_SIZE)
                .saturating_mul(STREAM_RESPONSE_ROUTE_COUNT),
        )));
        group.bench_function(
            format!(
                "response_forward_stream_{STREAM_RESPONSE_BATCH_SIZE}_workers_{chunk_count}_chunks_x{STREAM_RESPONSE_ROUTE_COUNT}_routes_primary"
            ),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let case = prepare_streaming_response_case(chunk_count);
                        let start = Instant::now();
                        for response in &case.responses {
                            route_worker_frame_to_destination(
                                &case.router,
                                case.family,
                                response.worker_session_id,
                                &response.worker_source,
                                &response.destination,
                                case.response_msg_type,
                                black_box(response.payload.clone()),
                            );
                        }
                        total += start.elapsed();
                        assert_requester_received_worker_responses(
                            &case.requester_inbox,
                            case.expected_response_count,
                        );
                        case.router.clear();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

fn bench_rpc_timeout_sweep_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_rpc");
    group.sampling_mode(SamplingMode::Flat);

    for expired_pending in [64usize, 256usize] {
        let case_batch_size = timeout_sweep_case_batch_size(expired_pending);
        group.throughput(Throughput::Elements(usize_to_u64_saturating(
            expired_pending.saturating_mul(case_batch_size),
        )));
        group.bench_function(
            format!(
                "dispatch_timeout_sweep_{expired_pending}_expired_pending_x{case_batch_size}_cases_primary"
            ),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let batch = prepare_timeout_sweep_batch_case(expired_pending);
                        let start = Instant::now();
                        for case in &batch.cases {
                            dispatch_request_to_destination(
                                &case.router,
                                case.family,
                                &case.requester_source,
                                &case.destination,
                                case.request_msg_type,
                                black_box(case.request_payload.clone()),
                            );
                            black_box((case.requester_inbox.count(), case.worker_inbox.count()));
                        }
                        total += start.elapsed();
                        for case in batch.cases {
                            case.router.clear();
                        }
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets =
        bench_rpc_worker_subscribe_primary,
        bench_rpc_dispatch_primary,
        bench_rpc_response_primary,
        bench_rpc_timeout_sweep_primary
}
criterion_main!(benches);
