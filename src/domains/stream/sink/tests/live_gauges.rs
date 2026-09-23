use super::*;
use crate::observability::metrics::MetricsCollector;

fn setup_metrics_context() -> (TestContext, MetricsCollector) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let metrics = MetricsCollector::new();
    let sink = Arc::new(
        StreamDomain::new_with_storage_layout_and_families(
            crate::storage::FitzStorageEngine::new(crate::testkit::create_test_engine_with_cfs(
                vec![1, 2],
            )),
            router.clone(),
            read_model.clone(),
            StreamStorageLayout::default(),
            Some(&[family, RouteFamily::new(2)]),
            StreamStorageWriteOptions::local(),
        )
        .expect("create Stream test sink")
        .with_metrics(metrics.clone()),
    );
    router.register_domain_pattern("stream", sink.clone() as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, TEST_CLIENT_SESSION_ID);
    (
        TestContext {
            router,
            family,
            source,
            inbox,
            sink,
            admin_read_model: read_model,
        },
        metrics,
    )
}

fn context_for_family(context: &TestContext, family: RouteFamily) -> TestContext {
    let (source, inbox) =
        register_session_queue_sink(&context.router, family, TEST_CLIENT_SESSION_ID);
    TestContext {
        router: Arc::clone(&context.router),
        family,
        source,
        inbox,
        sink: Arc::clone(&context.sink),
        admin_read_model: Arc::clone(&context.admin_read_model),
    }
}

fn subscribe(context: &TestContext) {
    let pattern = "stream://bench/events/*";
    let frame = build_stream_subscribe(pattern);
    let (message_type, payload) = extract_single_tlv_field(&frame);
    let _ = request(context, pattern, message_type, payload);
}

fn live_gauges(metrics: &MetricsCollector) -> (u64, u64, u64) {
    (
        metrics.gauge_get(crate::domains::stream::metrics::METRIC_ACTIVE_GAUGE),
        metrics.gauge_get(crate::domains::stream::metrics::METRIC_APPEND_SESSIONS_GAUGE),
        metrics.gauge_get(crate::domains::stream::metrics::METRIC_SUBSCRIPTIONS_GAUGE),
    )
}

#[test]
fn should_aggregate_stream_live_gauges_across_route_families() {
    // Arrange
    let (first, metrics) = setup_metrics_context();
    let second = context_for_family(&first, RouteFamily::new(2));
    begin_stream(&first, "stream://bench/events/first");
    subscribe(&first);
    begin_stream(&second, "stream://bench/events/second");
    subscribe(&second);

    // Act
    let counts = live_gauges(&metrics);

    // Assert
    assert_eq!(counts, (2, 2, 2));
}

#[test]
fn should_clear_append_session_gauge_after_session_cleanup() {
    // Arrange
    let (context, metrics) = setup_metrics_context();
    begin_stream(&context, "stream://bench/events/first");

    // Act
    context
        .sink
        .deliver(Envelope::new(
            RouteAddress::new(context.family, Route::new("stream://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .expect("deliver Stream cleanup");

    // Assert
    assert_eq!(
        metrics.gauge_get(crate::domains::stream::metrics::METRIC_APPEND_SESSIONS_GAUGE),
        0
    );
}

#[test]
fn should_preserve_sibling_stream_live_gauges_after_cleanup() {
    // Arrange
    let (first, metrics) = setup_metrics_context();
    let second = context_for_family(&first, RouteFamily::new(2));
    begin_stream(&first, "stream://bench/events/first");
    subscribe(&first);
    begin_stream(&second, "stream://bench/events/second");
    subscribe(&second);

    // Act
    second
        .sink
        .deliver(Envelope::new(
            RouteAddress::new(second.family, Route::new("stream://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .expect("deliver Stream cleanup");

    // Assert
    assert_eq!(live_gauges(&metrics), (2, 1, 1));
}

#[test]
fn should_exclude_failed_family_live_gauges_after_admin_failure_observation() {
    // Arrange
    let (first, metrics) = setup_metrics_context();
    let second = context_for_family(&first, RouteFamily::new(2));
    begin_stream(&first, "stream://bench/events/first");
    subscribe(&first);
    begin_stream(&second, "stream://bench/events/second");
    subscribe(&second);
    begin_stream(&first, "stream://bench/events/another");
    first.sink.panic_family_actor_for_failpoint(first.family);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !first
        .sink
        .family_health_snapshot()
        .failed_families
        .contains(&first.family)
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }

    // Act
    first.sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert_eq!(live_gauges(&metrics), (1, 1, 1));
}
