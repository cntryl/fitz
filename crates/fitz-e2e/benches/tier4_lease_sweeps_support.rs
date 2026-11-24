use bytes::Bytes;
use cntryl_stress::{ObservationDirection, ObservationUnit, StressContext};
use fitz::benchkit::{
    build_lease_acquire_immediate, build_lease_query, create_bench_lease_sweep_fixture,
    register_session_queue_sink, route_frame_to_address, shared_bench_runtime,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz_core::testkit::operations::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions,
};
use fitz_e2e::testkit::{FrameParser, TestClient, TestServer};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SEED_SESSION_ID: u64 = 41;
const LEASE_TTL_SECONDS: i32 = 86_400;

struct SweepFixture {
    router: Arc<Router>,
    sink: Arc<fitz::benchkit::LeaseSweepFixture>,
    _source: RouteAddress,
    _inbox: Arc<fitz::benchkit::FrameQueueSink>,
    expected_live: usize,
}

impl SweepFixture {
    fn new(additional_leases: usize) -> Self {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = Arc::new(create_bench_lease_sweep_fixture(router.clone()));
        let mailbox: Arc<dyn fitz::runtime::router::MailboxSink> = sink.clone();
        router.register_domain_pattern("lease", mailbox);
        let (source, inbox) = register_session_queue_sink(&router, family, SEED_SESSION_ID);
        let expected_live = additional_leases + 1;

        for index in 0..expected_live {
            let route = if index == 0 {
                "lease://bench/locks/seed".to_string()
            } else {
                format!("lease://bench/locks/extra-{index}")
            };
            let frame =
                build_lease_acquire_immediate(&route, &format!("owner-{index}"), LEASE_TTL_SECONDS);
            let destination = RouteAddress::new(family, Route::new(route));
            route_frame_to_address(
                &router,
                &source,
                &destination,
                SEED_SESSION_ID,
                ChannelId::Lease,
                400,
                Bytes::copy_from_slice(&frame[fitz::protocol::wire2::header::HEADER_LEN..]),
            )
            .expect("route live Lease acquire");
            let responses = inbox.drain_after_count(1, Duration::from_secs(5));
            assert_eq!(responses.len(), 1, "one acquire response per seeded lease");
            let response = &responses[0];
            assert_eq!(response.msg_type.as_u16(), 400);
            assert_eq!(
                response.payload.first(),
                Some(&0),
                "Lease acquire succeeded"
            );
            assert_eq!(response.payload.get(1), Some(&0), "Lease acquired new key");
        }
        assert_eq!(sink.live_count(), expected_live);
        Self {
            router,
            sink,
            _source: source,
            _inbox: inbox,
            expected_live,
        }
    }

    fn sweep_and_check(&self) {
        let count = self.sink.sweep_and_count();
        assert_eq!(count, self.expected_live, "all long-TTL leases remain live");
    }
}

fn tag(
    ctx: &mut StressContext,
    additional_leases: usize,
    layer: LayerKind,
    scenario: &'static str,
    logical_unit: &'static str,
) {
    tag_dimensions(
        ctx,
        &Tier4Dimensions {
            domain: "lease",
            scenario,
            storage_profile: StorageProfile::Memory,
            layer,
            write_mode: "not_applicable",
            payload_size: 0,
            history_depth: 0,
            read_limit: 0,
            read_scope: "none",
            route_count: additional_leases + 1,
            filter_selectivity: "not_applicable",
            client_count: 1,
            workload_mix: "expiry_sweep",
            completed_unit: logical_unit,
            gate_class: "characterization",
        },
    );
    ctx.parameter("additional_live_leases", additional_leases);
    ctx.parameter("live_leases_including_seed", additional_leases + 1);
}

pub(crate) fn measure_sweeps(ctx: &mut StressContext, additional_leases: usize) {
    tag(
        ctx,
        additional_leases,
        LayerKind::Direct,
        "live_lease_sweep",
        "lease_sweep_with_same_lane_barrier",
    );
    ctx.parameter("process_cpu_clock", "getrusage_user_plus_system");
    ctx.parameter("cpu_scope", "sweep_and_same_lane_count_barrier");
    let fixture = SweepFixture::new(additional_leases);
    ctx.parameter("cpu_unit", "nanoseconds_per_completed_sweep");
    let cpu_before = process_cpu_ns();
    let completed = ctx.measure_batch("lease_sweep_with_barrier", 1, || {
        fixture.sweep_and_check();
    });
    assert!(completed > 0, "Lease sweep benchmark completed a sample");
    let cpu_ns = process_cpu_ns().saturating_sub(cpu_before);
    let cpu_ns_per_sweep = u32::try_from(cpu_ns / u128::from(completed))
        .expect("process CPU time per sweep fits in nanoseconds range");
    ctx.record_observation(
        "process_cpu_ns_per_completed_sweep",
        f64::from(cpu_ns_per_sweep),
        ObservationUnit::Nanoseconds,
        ObservationDirection::LowerIsBetter,
    );
    assert_eq!(fixture.sink.live_count(), fixture.expected_live);
    fixture.sink.stop();
    fixture.router.clear();
}

pub(crate) fn measure_tcp_query(ctx: &mut StressContext, additional_leases: usize) {
    tag(
        ctx,
        additional_leases,
        LayerKind::Tcp,
        "held_key_query_under_live_leases",
        "tcp_lease_query",
    );
    ctx.parameter("query_transport", "tcp");
    ctx.parameter("maintenance_interval_ms", 50);
    ctx.parameter("seed_connection_retained", true);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start TCP Lease server");
    let mut seed = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect Lease seed client");

    for index in 0..=additional_leases {
        let route = if index == 0 {
            "lease://bench/locks/query-held".to_string()
        } else {
            format!("lease://bench/locks/query-extra-{index}")
        };
        let owner = format!("query-owner-{index}");
        let response = runtime
            .block_on(seed.request(
                &build_lease_acquire_immediate(&route, &owner, LEASE_TTL_SECONDS),
                5_000,
            ))
            .expect("acquire long-TTL Lease for TCP query fixture");
        assert_lease_acquired(&response);
    }
    let query = build_lease_query("lease://bench/locks/query-held");
    let initial = runtime
        .block_on(seed.request(&query, 5_000))
        .expect("initial TCP Lease QUERY");
    assert_held_query(&initial);
    runtime
        .block_on(server.wait_for_lease_count(additional_leases + 1))
        .expect("all seeded TCP leases remain live before measurement");
    measure_operations(ctx, "lease_tcp_query_held_key", 1, |observations| {
        let started = Instant::now();
        let response = runtime
            .block_on(seed.request(&query, 5_000))
            .expect("TCP Lease QUERY");
        assert_held_query(&response);
        observations.push(started.elapsed());
    });
    runtime
        .block_on(server.wait_for_lease_count(additional_leases + 1))
        .expect("all seeded TCP leases remain live after measurement");
    let after = runtime
        .block_on(seed.request(&query, 5_000))
        .expect("final TCP Lease QUERY");
    assert_held_query(&after);
    runtime
        .block_on(seed.close())
        .expect("close Lease seed client");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown TCP Lease server");
}

fn assert_lease_acquired(frame: &[u8]) {
    let mut parser = FrameParser::new(frame);
    let (message_type, body) = parser.next_field_ref().expect("Lease response frame");
    assert_eq!(message_type, 400);
    assert!(parser.next_field_ref().is_none(), "one Lease response");
    assert_eq!(body.first(), Some(&0), "Lease response is successful");
    assert_eq!(body.get(1), Some(&0), "Lease ACQUIRE got a fresh lease");
}

fn assert_held_query(frame: &[u8]) {
    let mut parser = FrameParser::new(frame);
    let (message_type, body) = parser.next_field_ref().expect("Lease QUERY response");
    assert_eq!(message_type, 403);
    assert!(
        parser.next_field_ref().is_none(),
        "one Lease QUERY response"
    );
    assert_eq!(body.first(), Some(&0), "Lease QUERY succeeded");
    assert_eq!(body.get(1), Some(&1), "fixed key remains held");
}

fn process_cpu_ns() -> u128 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the output structure when it succeeds.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(result, 0, "read process CPU usage");
    // SAFETY: the successful getrusage call initialized the structure.
    let usage = unsafe { usage.assume_init() };
    let timeval_ns = |time: libc::timeval| {
        u128::try_from(time.tv_sec).unwrap_or_default() * 1_000_000_000
            + u128::try_from(time.tv_usec).unwrap_or_default() * 1_000
    };
    timeval_ns(usage.ru_utime) + timeval_ns(usage.ru_stime)
}
