#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::missing_panics_doc
)]

#[path = "proof_single_node/latency.rs"]
mod latency;
#[path = "proof_single_node/output.rs"]
mod output;
#[path = "proof_single_node/recovery.rs"]
mod recovery;
#[path = "proof_single_node/routes.rs"]
mod routes;
#[path = "proof_single_node/throughput.rs"]
mod throughput;

use bytes::Bytes;
use fitz::benchkit::{
    build_stream_begin, create_bench_queue_sink, create_bench_stream_sink,
    extract_single_tlv_field, parse_stream_session_id, register_session_queue_sink, route_frame,
    shared_bench_runtime, FrameQueueSink,
};
use fitz::domains::queue::{QueueActor, QueueKey};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::{TestServer, TestWebSocketClient};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub(super) const CLIENT_SESSION_ID: u64 = 1;
pub(super) const FAMILY_ID: u64 = 1;
pub(super) const STREAM_OWNER_SESSION_ID: u64 = 1;
pub(super) const STREAM_EVENT_BYTES: &[u8] = b"proof-event";
pub(super) const QUEUE_MESSAGE_BYTES: &[u8] = b"proof-message";
pub(super) const STREAM_READ_LIMIT: u64 = 1_000;
pub(super) const STREAM_SEED_BATCH: usize = 10_000;
pub(super) const QUEUE_SEED_BATCH: usize = 1_000;
pub(super) const MULTICLIENT_COUNT: usize = 64;
pub(super) const MAX_STAGED_EVENTS_PER_SESSION: usize = 8_000;
pub(super) const P99_LATENCY_GATE_US: u64 = 1_000;

#[derive(Clone, Copy)]
pub(super) struct ProofSettings {
    pub(super) samples: usize,
    pub(super) warmup: usize,
}

#[derive(Serialize)]
pub(super) struct SerializableSettings {
    samples: usize,
    warmup: usize,
    output_json: &'static str,
    output_markdown: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct LatencyStats {
    pub(super) ops_sec: f64,
    pub(super) p50_us: u64,
    pub(super) p95_us: u64,
    pub(super) p99_us: u64,
    pub(super) max_us: u64,
    samples: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct LatencyRow {
    domain: &'static str,
    operation: &'static str,
    pub(super) layer: &'static str,
    pub(super) client_count: usize,
    pub(super) stats: LatencyStats,
    gate_p99_under_1ms: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct RecoveryRow {
    pub(super) stream_events: usize,
    pub(super) queue_depth: usize,
    pub(super) recovered_events: usize,
    pub(super) recovery_us: u64,
    pub(super) events_sec: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct RouteSensitivityRow {
    pub(super) domain: &'static str,
    pub(super) route_count: usize,
    pub(super) stats: LatencyStats,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct ThroughputEvidence {
    pub(super) suite: String,
    pub(super) scenario: String,
    pub(super) layer: Option<String>,
    pub(super) measurement_scope: Option<String>,
    pub(super) ops_sec: f64,
    pub(super) source: String,
}

#[derive(Debug, Serialize)]
pub(super) struct Conclusions {
    event_sourcing_capacity: String,
    queue_enqueue_p99_under_1ms: bool,
    stream_recovery_is_queue_depth_isolated: bool,
    route_count_effect: String,
}

#[derive(Serialize)]
pub(super) struct ProofReport {
    pub(super) generated_at: String,
    settings: SerializableSettings,
    pub(super) conclusions: Conclusions,
    pub(super) throughput_evidence: Vec<ThroughputEvidence>,
    pub(super) queue_latency: Vec<LatencyRow>,
    pub(super) stream_append_latency: Vec<LatencyRow>,
    pub(super) recovery: Vec<RecoveryRow>,
    pub(super) route_sensitivity: Vec<RouteSensitivityRow>,
}

pub(super) struct RoutedContext {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
}

#[derive(Clone, Copy)]
pub(super) enum DomainKind {
    Queue,
    Stream,
}

pub(crate) fn run() {
    let settings = ProofSettings::from_env();
    let throughput_evidence = throughput::load_throughput_evidence();
    let queue_latency = latency::measure_queue_latency_layers(settings);
    let stream_append_latency = latency::measure_stream_append_latency_layers(settings);
    let recovery = recovery::measure_recovery_matrix();
    let route_sensitivity = routes::measure_route_sensitivity(settings);

    let conclusions = Conclusions {
        event_sourcing_capacity: throughput::event_sourcing_capacity_answer(&throughput_evidence),
        queue_enqueue_p99_under_1ms: queue_latency
            .iter()
            .all(|row| row.stats.p99_us < P99_LATENCY_GATE_US),
        stream_recovery_is_queue_depth_isolated: recovery::recovery_is_queue_depth_isolated(
            &recovery,
        ),
        route_count_effect: routes::route_count_effect_answer(&route_sensitivity),
    };

    let report = ProofReport {
        generated_at: chrono::Utc::now().to_rfc3339(),
        settings: SerializableSettings {
            samples: settings.samples,
            warmup: settings.warmup,
            output_json: "target/perf_proof/single_node.json",
            output_markdown: "target/perf_proof/single_node.md",
        },
        conclusions,
        throughput_evidence,
        queue_latency,
        stream_append_latency,
        recovery,
        route_sensitivity,
    };

    output::write_report(&report);
}

impl ProofSettings {
    fn from_env() -> Self {
        Self {
            samples: env_usize("FITZ_PROOF_SAMPLES", 20_000),
            warmup: env_usize("FITZ_PROOF_WARMUP", 1_000),
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

pub(super) fn stream_route_pool_size(settings: ProofSettings) -> usize {
    settings
        .samples
        .saturating_add(settings.warmup)
        .div_ceil(MAX_STAGED_EVENTS_PER_SESSION)
        .max(4)
}

pub(super) fn queue_actor_on_store(
    store: Arc<cntryl_midge::Engine>,
    realm: &str,
    area: &str,
    resource: &str,
) -> QueueActor {
    let family = RouteFamily::new(FAMILY_ID);
    QueueActor::new_with_write_options(
        family,
        QueueKey {
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        },
        store,
        None,
        fitz::utils::idempotency::default_dedup_store(),
        cntryl_midge::WriteOptions::best_effort(),
    )
}

pub(super) fn setup_routed_context(kind: DomainKind) -> RoutedContext {
    let family = RouteFamily::new(FAMILY_ID);
    let router = Arc::new(Router::new());
    match kind {
        DomainKind::Queue => {
            let sink = create_bench_queue_sink(router.clone());
            router.register_domain_pattern("queue", sink as Arc<dyn MailboxSink>);
        }
        DomainKind::Stream => {
            let sink = create_bench_stream_sink(router.clone());
            router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
        }
    }
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    RoutedContext {
        router,
        family,
        source,
        inbox,
    }
}

pub(super) fn routed_request(
    context: &RoutedContext,
    destination: &str,
    channel_id: ChannelId,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        CLIENT_SESSION_ID,
        channel_id,
        msg_type,
        payload,
        context.family,
    )
    .expect("route proof frame");
    context
        .inbox
        .drain_after_count(1, Duration::from_secs(1))
        .last()
        .map(|frame| frame.payload.clone())
        .expect("proof response")
}

pub(super) fn assert_queue_payload_ok(payload: &[u8]) {
    assert_eq!(payload.first().copied(), Some(0), "queue operation failed");
}

pub(super) fn assert_stream_payload_ok(payload: &[u8]) {
    assert_eq!(payload.first().copied(), Some(0), "stream operation failed");
}

pub(super) fn begin_routed_stream_sessions(context: &RoutedContext, routes: &[String]) -> Vec<u64> {
    routes
        .iter()
        .map(|route| {
            let frame = build_stream_begin(route);
            let (msg_type, payload) = extract_single_tlv_field(&frame);
            let response = routed_request(context, route, ChannelId::Pub, msg_type, payload);
            parse_stream_session_id(response.as_ref()).expect("routed stream session id")
        })
        .collect()
}

pub(super) fn proof_stream_routes(prefix: &str, route_count: usize) -> Vec<String> {
    (0..route_count)
        .map(|index| format!("{prefix}/stream-{index}/append"))
        .collect()
}

pub(super) fn websocket_clients(
    runtime: &tokio::runtime::Runtime,
    server: &TestServer,
    client_count: usize,
) -> Arc<Vec<Arc<Mutex<TestWebSocketClient>>>> {
    Arc::new(
        (0..client_count)
            .map(|_| {
                let client = runtime
                    .block_on(TestWebSocketClient::connect(&format!(
                        "ws://{}",
                        server.ws_addr
                    )))
                    .expect("connect proof websocket client");
                Arc::new(Mutex::new(client))
            })
            .collect(),
    )
}

pub(super) fn measure_sequential<F>(settings: ProofSettings, mut operation: F) -> LatencyStats
where
    F: FnMut(usize) -> Duration,
{
    let total = settings.samples.saturating_add(settings.warmup);
    let mut latencies = Vec::with_capacity(settings.samples);
    let mut wall = Duration::ZERO;

    for index in 0..total {
        let elapsed = operation(index);
        if index >= settings.warmup {
            wall += elapsed;
            latencies.push(duration_to_us(elapsed));
        }
    }

    stats_from_latencies(latencies, wall, settings.samples)
}

pub(super) fn measure_multiclient<F, Fut>(
    settings: ProofSettings,
    client_count: usize,
    operation: F,
) -> LatencyStats
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = Duration>,
{
    let runtime = shared_bench_runtime();
    let mut latencies = Vec::with_capacity(settings.samples);
    let mut measured_ops = 0usize;
    let mut submitted_ops = 0usize;
    let total_ops = settings.samples.saturating_add(settings.warmup);
    let mut wall = Duration::ZERO;

    while submitted_ops < total_ops {
        let batch_size = client_count.min(total_ops - submitted_ops);
        let client_indexes = (0..batch_size).collect::<Vec<_>>();
        let record_batch = submitted_ops.saturating_add(batch_size) > settings.warmup;
        let start = Instant::now();
        let batch_latencies = runtime.block_on(futures::future::join_all(
            client_indexes.into_iter().map(&operation),
        ));
        let elapsed = start.elapsed();
        if record_batch {
            wall += elapsed;
        }
        for latency in batch_latencies {
            if submitted_ops >= settings.warmup && measured_ops < settings.samples {
                latencies.push(duration_to_us(latency));
                measured_ops += 1;
            }
            submitted_ops += 1;
        }
    }

    stats_from_latencies(latencies, wall, measured_ops)
}

fn stats_from_latencies(
    mut latencies: Vec<u64>,
    wall: Duration,
    measured_ops: usize,
) -> LatencyStats {
    latencies.sort_unstable();
    LatencyStats {
        ops_sec: if wall.is_zero() {
            0.0
        } else {
            measured_ops as f64 / wall.as_secs_f64()
        },
        p50_us: percentile(&latencies, 50.0),
        p95_us: percentile(&latencies, 95.0),
        p99_us: percentile(&latencies, 99.0),
        max_us: latencies.last().copied().unwrap_or(0),
        samples: measured_ops,
    }
}

fn percentile(sorted_latencies: &[u64], percentile: f64) -> u64 {
    if sorted_latencies.is_empty() {
        return 0;
    }
    let rank = ((percentile / 100.0) * sorted_latencies.len() as f64).ceil() as usize;
    sorted_latencies[rank.saturating_sub(1).min(sorted_latencies.len() - 1)]
}

pub(super) fn duration_to_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
