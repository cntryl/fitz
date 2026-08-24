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
    assert!(sink.is_active());
    assert!(sink.is_actor_running());
}

#[test]
fn should_prune_unmatched_schedule_cursor_when_registration_is_removed() {
    // Arrange
    let family = RouteFamily::new(1);
    let removed_route = "schedule://acme/churn/nightly/run";
    let mut subscriptions = ScheduleSubscriptionSet::new();
    for (subscription_id, route) in [(1, "schedule://acme/keeper/**"), (2, removed_route)] {
        subscriptions.insert(
            family,
            ScheduleSubscription {
                pattern: crate::runtime::matcher::Pattern::new(route),
                session_id: subscription_id,
                subscription_id,
                subscriber: RouteAddress::new(
                    family,
                    Route::new(format!("inbox://session/{subscription_id}")),
                ),
            },
        );
    }
    subscriptions
        .round_robin_cursors
        .insert(removed_route.to_string(), 1);

    // Act
    let removed = subscriptions.remove_session_route(family, 2, removed_route);

    // Assert
    assert_eq!(removed, 1);
    assert_eq!(subscriptions.subscription_count(), 1);
    assert!(subscriptions.round_robin_cursors.is_empty());
}

#[test]
fn should_retain_schedule_cursor_while_registration_still_matches() {
    // Arrange
    let family = RouteFamily::new(1);
    let concrete_route = "schedule://acme/jobs/nightly/run";
    let mut subscriptions = ScheduleSubscriptionSet::new();
    for (subscription_id, route) in [(1, "schedule://acme/jobs/**"), (2, concrete_route)] {
        subscriptions.insert(
            family,
            ScheduleSubscription {
                pattern: crate::runtime::matcher::Pattern::new(route),
                session_id: subscription_id,
                subscription_id,
                subscriber: RouteAddress::new(
                    family,
                    Route::new(format!("inbox://session/{subscription_id}")),
                ),
            },
        );
    }
    subscriptions
        .round_robin_cursors
        .insert(concrete_route.to_string(), 1);

    // Act
    let removed = subscriptions.remove_session_route(family, 2, concrete_route);

    // Assert
    assert_eq!(removed, 1);
    assert_eq!(
        subscriptions.round_robin_cursors.get(concrete_route),
        Some(&1)
    );
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
    assert!(sink.write_options_are_cloud_strict_for_tests());
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
    sink.insert_actor_for_tests(family, actor);

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
    sink.insert_actor_for_tests(family, actor);

    // Act
    sink.stop_actor_for_tests();
    sink.force_due_scan_for_tests(1);
    let pending_fire_count = sink.actor_pending_fire_count_for_tests(family);

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
    let actor_count = sink.actor_count_for_tests();

    // Assert
    assert!(!sink.is_actor_running());
    assert!(preload_result.is_err());
    assert_eq!(actor_count, 0);
}

#[test]
fn should_wait_for_schedule_preload_reply_beyond_one_second() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx.recv().expect("Schedule actor should block");

    // Act
    let preload_result = std::thread::scope(|scope| {
        let preload = scope.spawn(|| sink.preload_persisted_families());
        std::thread::sleep(Duration::from_millis(1_100));
        release_tx.send(()).expect("release Schedule actor");
        preload.join().expect("join schedule preload")
    });

    // Assert
    assert!(preload_result.is_ok());
}

#[test]
fn should_timeout_schedule_preload_when_actor_does_not_reply_before_deadline() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx.recv().expect("Schedule actor should block");

    // Act
    let preload_result = std::thread::scope(|scope| {
        let release = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
            release_tx.send(()).expect("release Schedule actor");
        });
        let result = sink.preload_persisted_families_with_timeout(Duration::from_millis(20));
        release.join().expect("join Schedule actor release");
        result
    });

    // Assert
    assert!(preload_result
        .expect_err("Schedule preload should time out")
        .contains("timed out"));
}

#[test]
fn should_route_schedule_admin_refresh_through_actor_command() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);
    sink.set_snapshot_dirty_for_tests(true);
    assert!(sink.snapshot_dirty_for_tests());

    // Act
    sink.stop_actor_for_tests();
    sink.refresh_admin_snapshot_if_dirty();
    let snapshot_dirty = sink.snapshot_dirty_for_tests();

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
    sink.insert_actor_for_tests(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    subscriptions.insert(
        family,
        ScheduleSubscription {
            pattern: crate::runtime::matcher::Pattern::new(schedule_route),
            session_id,
            subscription_id: 1,
            subscriber: RouteAddress::new(family, Route::new("inbox://session/7")),
        },
    );
    sink.insert_subscriptions_for_tests(family.as_u64(), subscriptions);
    sink.push_recent_acknowledgement_for_tests(now_epoch_ms());
    sink.set_live_publish_failures_for_tests(2);
    sink.set_ack_failures_for_tests(3);
    sink.insert_pending_ack_retry_for_tests(family.as_u64(), 1, schedule_route);
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
