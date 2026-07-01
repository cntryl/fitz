use super::*;

#[test]
fn should_create_schedule_domain_sink() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);

    // Assert
    assert!(sink.state.active.load(Ordering::Relaxed));
    assert!(sink.is_actor_running());
}

#[test]
fn should_store_cloud_strict_write_options_given_strict_cloud_policy() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = ScheduleDomainSink::new(store, router, admin_read_model)
        .with_write_options(cntryl_midge::WriteOptions::cloud_strict());

    // Assert
    assert!(sink.state.core.write_options.is_cloud_strict());
}

#[test]
fn should_read_admin_pending_claims_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model);
    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        store,
        cntryl_midge::WriteOptions::buffered(),
    );
    actor
        .create_schedule(
            "schedule://acme/jobs/nightly/run".to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    assert_eq!(actor.bench_claim_due_fires().len(), 1);
    sink.state.core.actors.lock().insert(family, actor);

    // Act
    sink.stop();
    let claims = sink.admin_pending_claims(family);

    // Assert
    assert!(claims.is_empty());
}

#[test]
fn should_route_schedule_force_due_scan_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model);
    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        store,
        cntryl_midge::WriteOptions::buffered(),
    );
    actor
        .create_schedule(
            "schedule://acme/jobs/nightly/run".to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    sink.state.core.actors.lock().insert(family, actor);

    // Act
    sink.stop_actor_for_tests();
    sink.force_due_scan_for_tests(1);
    let pending_fire_count = sink
        .state
        .core
        .actors
        .lock()
        .get(&family)
        .expect("schedule actor")
        .pending_fire_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(pending_fire_count, 0);
}

#[test]
fn should_route_schedule_preload_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        store.clone(),
        cntryl_midge::WriteOptions::buffered(),
    );
    actor
        .create_schedule(
            "schedule://acme/jobs/preload/run".to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"preload"),
        )
        .expect("create persisted schedule");
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);

    // Act
    sink.stop_actor_for_tests();
    let preload_result = sink.preload_persisted_families();
    let actor_count = sink.state.core.actors.lock().len();

    // Assert
    assert!(!sink.is_actor_running());
    assert!(preload_result.is_err());
    assert_eq!(actor_count, 0);
}

#[test]
fn should_route_schedule_admin_refresh_through_actor_command() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);
    sink.state
        .core
        .snapshot_dirty
        .store(true, Ordering::Relaxed);
    assert!(sink.state.core.snapshot_dirty.load(Ordering::Relaxed));

    // Act
    sink.stop_actor_for_tests();
    sink.refresh_admin_snapshot_if_dirty();
    let snapshot_dirty = sink.state.core.snapshot_dirty.load(Ordering::Relaxed);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(snapshot_dirty);
}

#[test]
fn should_route_schedule_live_stats_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/nightly/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model);
    let clock = Arc::new(MockClock::new(now_epoch_ms().saturating_sub(45_000)));
    let mut actor = crate::domains::schedule::ScheduleActor::new_with_clock(
        family,
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock.clone(),
    );
    actor
        .create_schedule(
            schedule_route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    assert_eq!(actor.bench_claim_due_fires().len(), 1);
    sink.state.core.actors.lock().insert(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    subscriptions.insert(ScheduleSubscription {
        route: schedule_route.to_string(),
        session_id,
        subscription_id: 1,
        subscriber: RouteAddress::new(family, Route::new("inbox://session/7")),
    });
    sink.state
        .core
        .sub_families
        .lock()
        .insert(family.as_u64(), subscriptions);
    sink.state
        .core
        .recent_acknowledgement_ms
        .lock()
        .push_back(now_epoch_ms());
    sink.state
        .core
        .live_publish_failures
        .store(2, Ordering::Relaxed);
    sink.state.core.ack_failures.store(3, Ordering::Relaxed);
    sink.state
        .core
        .pending_ack_retries
        .lock()
        .entry(family.as_u64())
        .or_default()
        .insert((1, schedule_route.to_string()));
    assert_eq!(sink.subscription_count(), 1);
    assert_eq!(sink.schedule_count(), 1);
    assert_eq!(sink.pending_fire_count(), 1);
    assert!((sink.executions_per_minute() - 1.0).abs() < f64::EPSILON);
    assert_eq!(sink.notify_failure_count(), 2);
    assert_eq!(sink.ack_failure_count(), 3);
    assert_eq!(sink.pending_ack_retry_count(), 1);
    assert_eq!(sink.oldest_pending_claim_age_seconds(), 45);

    // Act
    sink.stop_actor_for_tests();
    let executions_per_minute = sink.executions_per_minute();
    let live_stats = (
        sink.subscription_count(),
        sink.schedule_count(),
        sink.pending_fire_count(),
        sink.notify_failure_count(),
        sink.ack_failure_count(),
        sink.pending_ack_retry_count(),
        sink.oldest_pending_claim_age_seconds(),
        sink.overdue_normalization_count(),
    );

    // Assert
    assert!(!sink.is_actor_running());
    assert!(executions_per_minute.abs() < f64::EPSILON);
    assert_eq!(live_stats, (0, 0, 0, 0, 0, 0, 0, 0));
}
