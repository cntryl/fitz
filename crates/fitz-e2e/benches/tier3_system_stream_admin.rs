//! In-process Stream admin projection scaling characterization.
//!
//! Each dirty invocation alternates a real routed begin or rollback in stress's
//! per-invocation setup; the timed closure performs the subsequent actor refresh
//! and measurement bookkeeping. The live state changes on every dirty
//! refresh while durable cardinality stays fixed.

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, LogicalUnit, OperationOutcome, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_rollback,
    create_bench_stream_admin_sink, extract_single_tlv_field, parse_stream_session_id,
    register_session_queue_sink, route_frame, FrameQueueSink, StreamAdminBench,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::{Duration, Instant};

cntryl_stress::stress_allocator!();

const SESSION_ID: u64 = 1;
const SYNC_COMMIT_MODE: u8 = 1;
const MAX_LATENCY_SAMPLES: usize = 65_536;

#[derive(Clone, Copy)]
enum Scenario {
    Clean,
    OneDirty,
    AllDirty,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::OneDirty => "one_dirty",
            Self::AllDirty => "all_dirty",
        }
    }

    fn writes_per_refresh(self, family_count: usize) -> usize {
        match self {
            Self::Clean => 0,
            Self::OneDirty => 1,
            Self::AllDirty => family_count,
        }
    }
}

struct FamilyClient {
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
    routes: Vec<String>,
    next_offsets: Vec<u64>,
    next_dirty_route: usize,
    active_session_id: Option<u64>,
}

impl FamilyClient {
    fn commit_one(&mut self, router: &Router, route_index: usize) {
        let route = &self.routes[route_index];
        let begin = request(self, router, route, &build_stream_begin(route));
        let session_id = parse_stream_session_id(begin.as_ref()).expect("Stream append session");
        let append = request(
            self,
            router,
            route,
            &build_stream_append(session_id, self.next_offsets[route_index], b"projection"),
        );
        assert_eq!(
            append.first().copied(),
            Some(0),
            "Stream append failed: {append:?}"
        );
        let commit = request(
            self,
            router,
            route,
            &build_stream_commit(session_id, SYNC_COMMIT_MODE),
        );
        assert_eq!(
            commit.first().copied(),
            Some(0),
            "Stream commit failed: {commit:?}"
        );
        self.next_offsets[route_index] += 1;
    }

    fn touch_next_dirty(&mut self, router: &Router) {
        let route_index = self.next_dirty_route;
        let route = &self.routes[route_index];
        if let Some(session_id) = self.active_session_id.take() {
            let rollback = request(self, router, route, &build_stream_rollback(session_id));
            assert_eq!(
                rollback.first().copied(),
                Some(0),
                "Stream rollback failed: {rollback:?}"
            );
            self.next_dirty_route = (route_index + 1) % self.routes.len();
        } else {
            let begin = request(self, router, route, &build_stream_begin(route));
            self.active_session_id =
                Some(parse_stream_session_id(begin.as_ref()).expect("Stream append session"));
        }
    }
}

fn request(client: &FamilyClient, router: &Router, route: &str, frame: &[u8]) -> Bytes {
    let (msg_type, payload) = extract_single_tlv_field(frame);
    route_frame(
        router,
        &client.source,
        route,
        SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        client.family,
    )
    .expect("route Stream request");
    client
        .inbox
        .drain_after_count(1, Duration::from_secs(1))
        .last()
        .expect("Stream response")
        .payload
        .clone()
}

fn setup(
    family_count: usize,
    resources_per_family: usize,
) -> (Arc<Router>, Arc<StreamAdminBench>, Vec<FamilyClient>) {
    assert!(family_count > 0 && resources_per_family > 0);
    let router = Arc::new(Router::new());
    let families = (1..=family_count)
        .map(|id| RouteFamily::new(u32::try_from(id).expect("family ID fits u32")))
        .collect::<Vec<_>>();
    let sink = create_bench_stream_admin_sink(Arc::clone(&router), &families);
    router.register_domain_pattern("stream", sink.clone() as Arc<dyn MailboxSink>);
    let mut clients = families
        .into_iter()
        .map(|family| {
            let (source, inbox) = register_session_queue_sink(&router, family, SESSION_ID);
            let resource_routes = (0..resources_per_family)
                .map(|resource| {
                    format!(
                        "stream://bench{}/area{}/resource{resource}",
                        resource % 2,
                        (resource / 2) % 2
                    )
                })
                .collect::<Vec<_>>();
            FamilyClient {
                family,
                source,
                inbox,
                next_offsets: vec![0; resources_per_family],
                routes: resource_routes,
                next_dirty_route: 0,
                active_session_id: None,
            }
        })
        .collect::<Vec<_>>();
    for client in &mut clients {
        for route_index in 0..resources_per_family {
            client.commit_one(&router, route_index);
        }
    }
    sink.refresh();
    let expected_rows = family_count * resources_per_family;
    assert_eq!(sink.snapshot_counts(), (expected_rows, expected_rows));
    assert_eq!(sink.sessions_active_total(), 0);
    (router, sink, clients)
}

fn measure_refresh(
    ctx: &mut StressContext,
    scenario: Scenario,
    family_count: usize,
    resources_per_family: usize,
) {
    ctx.parameter("scenario", scenario.name());
    ctx.parameter("family_count", family_count);
    ctx.parameter("resources_per_family", resources_per_family);
    ctx.parameter("measurement_scope", "in_process_stream_domain");
    ctx.metadata("timed_work", "admin_refresh_plus_measurement_bookkeeping");
    ctx.metadata(
        "dirty_setup",
        "routed_begin_or_rollback_excluded_per_invocation",
    );
    ctx.metadata("storage", "memory");
    ctx.metadata("latency_quantiles", "p50,p95,p99");
    ctx.metadata("latency_sampling", "rolling_last_65536_invocations");

    let (router, sink, mut clients) = setup(family_count, resources_per_family);
    let expected_rows = family_count * resources_per_family;
    let mut latencies = vec![Duration::ZERO; MAX_LATENCY_SAMPLES];
    let mut latency_count = 0usize;
    let writes_per_refresh = scenario.writes_per_refresh(family_count);
    let outcome = ctx.measure_outcome_with_setup(
        "refresh_stream_admin_snapshot",
        LogicalUnit::new("admin_refresh"),
        || {
            if writes_per_refresh != 0 {
                assert_eq!(sink.snapshot_counts(), (expected_rows, expected_rows));
                assert_eq!(
                    sink.sessions_active_total(),
                    clients
                        .iter()
                        .filter(|client| client.active_session_id.is_some())
                        .count()
                );
                for client in clients.iter_mut().take(writes_per_refresh) {
                    client.touch_next_dirty(&router);
                }
            }
        },
        |()| {
            let started = Instant::now();
            sink.refresh();
            latencies[latency_count % MAX_LATENCY_SAMPLES] = started.elapsed();
            latency_count = latency_count.saturating_add(1);
            OperationOutcome::success(1)
        },
    );
    assert!(
        outcome.completed > 0,
        "benchmark must refresh at least once"
    );
    assert_eq!(sink.snapshot_counts(), (expected_rows, expected_rows));
    assert_eq!(
        sink.sessions_active_total(),
        clients
            .iter()
            .filter(|client| client.active_session_id.is_some())
            .count()
    );
    for latency in latencies
        .into_iter()
        .take(latency_count.min(MAX_LATENCY_SAMPLES))
    {
        ctx.record_latency(latency);
    }
}

#[stress(tier = 3)]
fn should_measure_clean_stream_admin_refresh_four_families(ctx: &mut StressContext) {
    measure_refresh(ctx, Scenario::Clean, 4, 8);
}

#[stress(tier = 3)]
fn should_measure_one_dirty_stream_admin_refresh_four_families(ctx: &mut StressContext) {
    measure_refresh(ctx, Scenario::OneDirty, 4, 8);
}

#[stress(tier = 3)]
fn should_measure_all_dirty_stream_admin_refresh_four_families(ctx: &mut StressContext) {
    measure_refresh(ctx, Scenario::AllDirty, 4, 8);
}

#[stress(tier = 3)]
fn should_measure_clean_stream_admin_refresh_sixteen_families(ctx: &mut StressContext) {
    measure_refresh(ctx, Scenario::Clean, 16, 16);
}

#[stress(tier = 3)]
fn should_measure_one_dirty_stream_admin_refresh_sixteen_families(ctx: &mut StressContext) {
    measure_refresh(ctx, Scenario::OneDirty, 16, 16);
}

#[stress(tier = 3)]
fn should_measure_all_dirty_stream_admin_refresh_sixteen_families(ctx: &mut StressContext) {
    measure_refresh(ctx, Scenario::AllDirty, 16, 16);
}

stress_main!();
