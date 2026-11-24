use super::super::model::{now_epoch_ms, ScheduleSubscription, ScheduleSubscriptionSet};
use super::*;

#[test]
fn should_confirm_schedule_session_cleanup_before_reporting_delivery() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(crate::testkit::create_test_engine_with_cfs(vec![1])),
        Arc::new(Router::new()),
        crate::control::admin::AdminReadModel::new(),
    );
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("Schedule actor should block");
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);

    // Act
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let result = sink.deliver_high_priority(Envelope::new(
                RouteAddress::new(family, Route::new("schedule://cleanup")),
                fitz_core::runtime::SessionCleanup { session_id: 7 },
            ));
            let _ = result_tx.send(result);
        });
        let early_result = result_rx.recv_timeout(Duration::from_millis(50));
        let returned_early = early_result.is_ok();
        release_tx.send(()).expect("release Schedule actor");
        let final_result = early_result.unwrap_or_else(|_| {
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Schedule cleanup result")
        });

        // Assert
        assert!(!returned_early, "cleanup returned before it executed");
        assert_eq!(final_result, Ok(()));
    });
}

#[test]
fn should_create_schedule_domain_sink() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::AdminReadModel::new();

    // Act
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store),
        router,
        admin_read_model,
    );

    // Assert
    assert!(sink.is_active());
    assert!(sink.is_actor_running());
}

#[test]
fn should_retain_explicit_schedule_family_inventory() {
    // Arrange
    let families = [RouteFamily::new(1), RouteFamily::new(2)];

    // Act
    let sink = ScheduleDomain::new_with_store_and_families(
        crate::domain::ScheduleStore::new_with_storage(
            fitz_storage::storage::FitzStorageEngine::new(
                crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
            ),
        ),
        Arc::new(Router::new()),
        crate::control::admin::AdminReadModel::new(),
        &families,
    );

    // Assert
    assert_eq!(sink.route_families_for_tests(), families);
}

#[test]
fn should_keep_sibling_schedule_family_running_after_family_panic() {
    // Arrange
    let failed_family = RouteFamily::new(1);
    let sibling_family = RouteFamily::new(2);
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(crate::testkit::create_test_engine_with_cfs(vec![1, 2])),
        Arc::new(Router::new()),
        crate::control::admin::AdminReadModel::new(),
    );

    // Act
    sink.panic_family_for_tests(failed_family);
    let deadline = Instant::now() + Duration::from_secs(1);
    while sink.is_family_running(failed_family) && Instant::now() < deadline {
        std::thread::yield_now();
    }

    // Assert
    assert!(!sink.is_family_running(failed_family));
    assert!(sink.is_family_running(sibling_family));
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
                pattern: fitz_core::runtime::matcher::Pattern::new(route),
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
                pattern: fitz_core::runtime::matcher::Pattern::new(route),
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
fn should_store_cloud_strict_write_policy_given_strict_cloud_policy() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::AdminReadModel::new();

    // Act
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store),
        router,
        admin_read_model,
    )
    .with_write_policy(fitz_storage::domains::WritePolicy::CloudStrict);

    // Assert
    assert!(sink.write_policy_are_cloud_strict_for_tests());
}

#[test]
fn should_read_live_pending_claim_from_schedule_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "schedule://acme/jobs/nightly/run";
    let start_ms = 1_700_000_040_000;
    let clock = Arc::new(MockClock::new(start_ms));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::AdminReadModel::new();
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store.clone()),
        router,
        admin_read_model,
    );
    let mut actor = crate::domain::ScheduleActor::new_with_clock(
        family,
        crate::domain::ScheduleStore::new(store),
        fitz_storage::domains::WritePolicy::Buffered,
        clock.clone(),
    );
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    let fire_ms = start_ms + 60_000;
    clock.advance(Duration::from_mins(1));
    assert_eq!(actor.collect_due_occurrences_for_publish().len(), 1);
    sink.insert_actor_for_tests(family, actor);

    // Act
    let claims = sink.admin_pending_claims(family);

    // Assert
    let [claim] = claims.as_slice() else {
        panic!("expected one pending claim, got {claims:?}");
    };
    assert_eq!(claim.route_family, family.as_u64());
    assert_eq!(claim.route, route);
    assert_eq!(
        claim.delivery_mode,
        crate::domain::ScheduleDeliveryMode::Broadcast
    );
    assert_eq!(claim.fire_ms, fire_ms);
    assert_eq!(claim.claimed_at_ms, fire_ms);
}

#[test]
fn should_skip_run_now_delivery_when_command_expires_while_queued() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "schedule://acme/jobs/nightly/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store.clone()),
        Arc::new(Router::new()),
        crate::control::admin::AdminReadModel::new(),
    );
    let mut actor = crate::domain::ScheduleActor::new(
        family,
        crate::domain::ScheduleStore::new(store),
        fitz_storage::domains::WritePolicy::Buffered,
    );
    actor
        .create_schedule_with_mode(
            route.to_string(),
            "* * * * *".to_string(),
            crate::domain::ScheduleDeliveryMode::Single,
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    sink.insert_actor_for_tests(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    for subscription_id in [1, 2] {
        subscriptions.insert(
            family,
            ScheduleSubscription {
                pattern: fitz_core::runtime::matcher::Pattern::new(route),
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
        .insert(route.to_string(), 0);
    sink.insert_subscriptions_for_tests(family, subscriptions);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx.recv().expect("Schedule actor should block");

    // Act
    let result = sink.run_now(family, route.to_string(), Duration::from_millis(10));
    release_tx.send(()).expect("release Schedule actor");
    let cursor = sink.round_robin_cursor_for_tests(family, route);

    // Assert
    assert!(matches!(
        result,
        Err(ScheduleRunNowError::ReplyTimeout { .. })
    ));
    assert_eq!(cursor, Some(0));
}

#[test]
fn should_deliver_scheduled_payload_when_the_actor_scans_due_work() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "schedule://acme/jobs/nightly/run";
    let subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(subscriber.clone(), mailbox.clone());
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store.clone()),
        router,
        crate::control::admin::AdminReadModel::new(),
    );
    let clock = Arc::new(MockClock::new(1_700_000_040_000));
    let mut actor = crate::domain::ScheduleActor::new_with_clock(
        family,
        crate::domain::ScheduleStore::new(store),
        fitz_storage::domains::WritePolicy::Buffered,
        clock.clone(),
    );
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    clock.advance(Duration::from_mins(1));
    sink.insert_actor_for_tests(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    subscriptions.insert(
        family,
        ScheduleSubscription {
            pattern: fitz_core::runtime::matcher::Pattern::new(route),
            session_id: 7,
            subscription_id: 1,
            subscriber,
        },
    );
    sink.insert_subscriptions_for_tests(family, subscriptions);

    // Act
    sink.scan_due_schedules();
    let response = receive_envelope(&mailbox, "due schedule notification")
        .into_payload::<FrameContext>()
        .expect("schedule notification frame");

    // Assert
    assert_eq!(response.msg_type.as_u16(), 705);
    let mut payload = PayloadDecoder::new(&response.payload);
    assert_eq!(payload.get_u64().expect("subscription ID"), 1);
    assert_eq!(payload.get_string().expect("schedule route"), route);
    assert_eq!(
        payload.get_bytes().expect("schedule payload").as_ref(),
        b"nightly"
    );
    assert!(payload.is_complete());
}

#[test]
fn should_route_schedule_preload_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::AdminReadModel::new();
    let mut actor = crate::domain::ScheduleActor::new(
        family,
        crate::domain::ScheduleStore::new(store.clone()),
        fitz_storage::domains::WritePolicy::Buffered,
    );
    actor
        .create_schedule(
            "schedule://acme/jobs/preload/run".to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"preload"),
        )
        .expect("create persisted schedule");
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store),
        router,
        admin_read_model,
    );

    // Act
    sink.stop_actor_for_tests();
    let preload_result = sink.preload_persisted_families();

    // Assert
    assert!(!sink.is_actor_running());
    assert!(preload_result.is_err());
}

#[test]
fn should_wait_for_schedule_preload_reply_beyond_one_second() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::AdminReadModel::new();
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store),
        router,
        admin_read_model,
    );
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
    let admin_read_model = crate::control::admin::AdminReadModel::new();
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store),
        router,
        admin_read_model,
    );
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
fn should_publish_schedule_definition_when_admin_refresh_is_requested() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let admin_read_model = crate::control::admin::AdminReadModel::new();
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store.clone()),
        Arc::new(Router::new()),
        admin_read_model.clone(),
    );
    let mut actor = crate::domain::ScheduleActor::new_with_clock(
        family,
        crate::domain::ScheduleStore::new(store),
        fitz_storage::domains::WritePolicy::Buffered,
        Arc::new(MockClock::new(1_700_000_000_000)),
    );
    actor
        .create_schedule(
            "schedule://acme/jobs/nightly/run".to_string(),
            "0 3 * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    sink.insert_actor_for_tests(family, actor);
    sink.set_snapshot_dirty_for_tests(true);
    assert!(sink.snapshot_dirty_for_tests());
    assert!(admin_read_model
        .schedules_for_route_family(family.as_u64())
        .is_empty());

    // Act
    sink.refresh_admin_snapshot_if_dirty();
    let schedules = admin_read_model.schedules_for_route_family(family.as_u64());

    // Assert
    let [schedule] = schedules.as_slice() else {
        panic!("expected one published schedule, got {schedules:?}");
    };
    assert_eq!(schedule.realm, "acme");
    assert_eq!(schedule.area, "jobs");
    assert_eq!(schedule.resource, "nightly");
    assert_eq!(schedule.operation, "run");
    assert_eq!(schedule.cron, "0 3 * * *");
}

#[test]
fn should_route_schedule_live_stats_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/nightly/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::AdminReadModel::new();
    let sink = ScheduleDomain::new(
        crate::domain::ScheduleStore::new(store.clone()),
        router,
        admin_read_model,
    );
    let clock = Arc::new(MockClock::new(now_epoch_ms().saturating_sub(45_000)));
    let mut actor = crate::domain::ScheduleActor::new_with_clock(
        family,
        crate::domain::ScheduleStore::new(store),
        fitz_storage::domains::WritePolicy::Buffered,
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
            pattern: fitz_core::runtime::matcher::Pattern::new(schedule_route),
            session_id,
            subscription_id: 1,
            subscriber: RouteAddress::new(family, Route::new("inbox://session/7")),
        },
    );
    sink.insert_subscriptions_for_tests(family, subscriptions);
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
