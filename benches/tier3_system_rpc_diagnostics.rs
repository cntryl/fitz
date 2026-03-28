//! RPC Domain Scaling Diagnostics Benchmark
//!
//! Instruments the RPC dispatch hot path to identify bottlenecks:
//! 1. Mutex state lock time (mutex contention)
//! 2. Admin snapshot rebuild time (observability overhead)
//! 3. Request dispatch latency (end-to-end dispatch time)
//!
//! Run with:
//! ```
//! cargo bench --bench tier3_system_rpc_diagnostics -- --quiet
//! ```
//!
//! To skip admin snapshots (helps identify if they're the bottleneck):
//! ```
//! cargo bench --bench tier3_system_rpc_diagnostics --features bench-no-snapshot -- --quiet
//! ```

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::create_bench_rpc_sink_with_metrics;
use fitz::benchkit::{
    build_rpc_request, build_rpc_subscribe, extract_single_tlv_field, register_session_queue_sink,
    route_frame, FrameQueueSink,
};
use fitz::observability::metrics::MetricsCollector;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::{encode_response_message, parse_request};
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::cell::Cell;
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

const ROUTE_STR: &str = "rpc://bench/system/route";
const REQUESTER_SESSION_ID: u64 = 1;

type WorkerHandle = (u64, RouteAddress, Arc<FrameQueueSink>);

struct DiagnosticRpcSetup {
    router: Arc<Router>,
    family: RouteFamily,
    requester_source: RouteAddress,
    requester_inbox: Arc<FrameQueueSink>,
    metrics: MetricsCollector,
}

fn setup_rpc_sink() -> DiagnosticRpcSetup {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let metrics = MetricsCollector::new();

    let sink = create_bench_rpc_sink_with_metrics(router.clone(), metrics.clone());
    router.register_domain_pattern("rpc", sink as Arc<dyn MailboxSink>);

    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);

    DiagnosticRpcSetup {
        router,
        family,
        requester_source,
        requester_inbox,
        metrics,
    }
}

fn register_worker(router: &Arc<Router>, family: RouteFamily, session_id: u64) -> WorkerHandle {
    let (worker_source, worker_inbox) = register_session_queue_sink(router, family, session_id);
    let subscribe_frame = build_rpc_subscribe(ROUTE_STR);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);

    route_frame(
        router.as_ref(),
        &worker_source,
        ROUTE_STR,
        session_id,
        ChannelId::Rpc,
        subscribe_msg_type,
        subscribe_payload,
        family,
    )
    .expect("rpc subscribe");

    let _ = worker_inbox.drain();
    (session_id, worker_source, worker_inbox)
}

fn dispatch_request(
    router: &Arc<Router>,
    family: RouteFamily,
    requester_source: &RouteAddress,
    request_msg_type: u16,
    request_payload: Bytes,
) {
    route_frame(
        router.as_ref(),
        requester_source,
        ROUTE_STR,
        REQUESTER_SESSION_ID,
        ChannelId::Rpc,
        request_msg_type,
        request_payload,
        family,
    )
    .expect("rpc request");
}

fn dispatch_worker_response(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    response_payload: Bytes,
) {
    route_frame(
        router.as_ref(),
        worker_source,
        ROUTE_STR,
        worker_session_id,
        ChannelId::Rpc,
        303,
        response_payload,
        family,
    )
    .expect("rpc response");
}

fn service_worker(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    worker_inbox: &Arc<FrameQueueSink>,
) -> usize {
    let mut responses = 0usize;

    loop {
        let frames = worker_inbox.drain();
        if frames.is_empty() {
            break;
        }

        let mut handled_request = false;
        for frame in frames {
            match frame.msg_type.as_u16() {
                302 => {
                    handled_request = true;
                    if let Ok(fitz::domains::rpc::protocol::RpcMessage::Request(request)) =
                        parse_request(&frame, &frame.payload, family)
                    {
                        let response = fitz::domains::rpc::protocol::RpcResponse::single(
                            request.correlation_id,
                            request.body.clone(),
                        );
                        dispatch_worker_response(
                            router,
                            family,
                            worker_session_id,
                            worker_source,
                            Bytes::from(encode_response_message(&response)),
                        );
                        responses += 1;
                    }
                }
                304 => {}
                _ => {}
            }
        }

        if !handled_request {
            break;
        }
    }

    responses
}

fn service_workers(router: &Arc<Router>, family: RouteFamily, workers: &[WorkerHandle]) -> usize {
    let mut total = 0usize;

    loop {
        let mut round = 0usize;
        for (session_id, source, inbox) in workers {
            round += service_worker(router, family, *session_id, source, inbox);
        }
        if round == 0 {
            break;
        }
        total += round;
    }

    total
}

fn report_metrics(worker_count: usize, metrics: &MetricsCollector) {
    let snapshot_mode = if cfg!(feature = "bench-no-snapshot") {
        "off"
    } else {
        "on"
    };
    let state_lock_histogram = metrics
        .histogram_get_buckets("rpc_dispatch_state_lock_us")
        .unwrap_or([0; 9]);
    let snapshot_histogram = metrics
        .histogram_get_buckets("rpc_admin_snapshot_us")
        .unwrap_or([0; 9]);
    let request_count = metrics.counter_get("rpc_requests_total");
    let pending_requests = metrics.gauge_get("rpc_pending_requests");

    eprintln!(
        "worker_count={} snapshot_mode={} requests={} pending={} state_lock_us={:?} snapshot_us={:?}",
        worker_count,
        snapshot_mode,
        request_count,
        pending_requests,
        state_lock_histogram,
        snapshot_histogram,
    );
}

fn bench_rpc_dispatch_diagnostics(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_dispatch_diagnostics");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    for worker_count in [64usize, 256usize] {
        let setup = setup_rpc_sink();
        let workers: Vec<WorkerHandle> = (0..worker_count)
            .map(|index| register_worker(&setup.router, setup.family, 1_000 + index as u64))
            .collect();
        let printed = Cell::new(false);

        let request_frame = build_rpc_request(ROUTE_STR, b"rpc diagnostic payload");
        let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

        group.bench_with_input(
            BenchmarkId::from_parameter(worker_count),
            &worker_count,
            move |b, &_count| {
                b.iter(|| {
                    dispatch_request(
                        &setup.router,
                        setup.family,
                        &setup.requester_source,
                        request_msg_type,
                        black_box(request_payload.clone()),
                    );
                    let response_count = service_workers(&setup.router, setup.family, &workers);
                    let _ = setup.requester_inbox.drain();
                    black_box(response_count);
                });

                if !printed.replace(true) {
                    report_metrics(worker_count, &setup.metrics);
                }
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_rpc_dispatch_diagnostics
}
criterion_main!(benches);
