use super::*;
use crate::domains::schedule::sink::model::{ScheduleSubscription, ScheduleSubscriptionSet};
use crate::domains::schedule::ScheduleDeliveryMode;

fn register_full_mailbox(router: &Arc<Router>, family: RouteFamily, session_id: u64) {
    let address = RouteAddress::new(family, Route::new(format!("inbox://session/{session_id}")));
    let mailbox = Arc::new(Mailbox::new(1));
    router.register(address.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(address, 1_u8))
        .expect("fill subscriber mailbox to force MailboxFull");
}

fn add_single_mode_candidates(
    subscriptions: &mut ScheduleSubscriptionSet,
    family: RouteFamily,
    route: &str,
    candidate_session_ids: &[u64],
) {
    for &subscription_id in candidate_session_ids {
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
        .insert(route.to_string(), 0);
}

/// Sequence: scan claims a due `single`-mode fire whose only two candidates
/// both reject (`MailboxFull`), and separately `runScheduleNow` hits the
/// same all-rejecting shape for a sibling route. The spec's live-delivery
/// table requires "advance the cursor safely" in both cases; the two
/// duplicated delivery paths (periodic scan vs admin run-now) must land on
/// the identical cursor value for an identical candidate set, and both must
/// count the all-rejecting handoff as one live-publish failure.
#[test]
fn should_advance_cursor_identically_via_scan_and_run_now_when_all_single_mode_candidates_reject() {
    // Arrange
    let family = RouteFamily::new(1);
    let scan_route = "schedule://acme/jobs/scan-reject/run";
    let run_now_route = "schedule://acme/jobs/run-now-reject/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    register_full_mailbox(&router, family, 10);
    register_full_mailbox(&router, family, 11);
    register_full_mailbox(&router, family, 20);
    register_full_mailbox(&router, family, 21);
    let sink = ScheduleDomain::new(
        crate::domains::schedule::ScheduleStore::new(store.clone()),
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        crate::domains::schedule::ScheduleStore::new(store),
        crate::domains::WritePolicy::Buffered,
    );
    actor
        .create_schedule_with_mode(
            scan_route.to_string(),
            "* * * * *".to_string(),
            ScheduleDeliveryMode::Single,
            Bytes::from_static(b"scan"),
        )
        .expect("create scan-delivered schedule");
    actor
        .create_schedule_with_mode(
            run_now_route.to_string(),
            "* * * * *".to_string(),
            ScheduleDeliveryMode::Single,
            Bytes::from_static(b"run-now"),
        )
        .expect("create run-now schedule");
    // Only the first list entry (scan_route) becomes due; run_now_route's
    // fire timing is irrelevant to run_now, which ignores due state.
    actor.bench_prepare_scan(1);
    sink.insert_actor_for_tests(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    add_single_mode_candidates(&mut subscriptions, family, scan_route, &[10, 11]);
    add_single_mode_candidates(&mut subscriptions, family, run_now_route, &[20, 21]);
    sink.insert_subscriptions_for_tests(family, subscriptions);

    // Act
    sink.scan_due_schedules();
    let run_now_result = sink
        .run_now(family, run_now_route.to_string(), Duration::from_secs(1))
        .expect("run-now command completes")
        .expect("run-now definition exists");

    let scan_cursor = sink.round_robin_cursor_for_tests(family, scan_route);
    let run_now_cursor = sink.round_robin_cursor_for_tests(family, run_now_route);

    // Assert
    assert_eq!(
        scan_cursor,
        Some(1),
        "scan path must advance past an all-rejecting candidate"
    );
    assert_eq!(
        run_now_cursor,
        Some(1),
        "run-now path must advance identically to the scan path for the same shape"
    );
    assert_eq!(scan_cursor, run_now_cursor);
    assert_eq!(run_now_result.accepted_handoffs, 0);
    assert_eq!(run_now_result.attempted_handoffs, 2);
    assert_eq!(
        sink.notify_failure_count(),
        2,
        "both the scan's all-rejecting handoff and run-now's all-rejecting \
         handoff must each count exactly one live-publish failure"
    );
}

/// A `runScheduleNow` handoff whose only subscriber's mailbox is full must
/// report zero accepted handoffs, count one live-publish failure (the same
/// counter the periodic scan path uses), and must not tear down the
/// subscription: `runScheduleNow` "changes no cron timing or durable
/// counters" per spec, so a rejected live handoff is not a
/// subscription-lifecycle event.
#[test]
fn should_report_zero_accepted_handoffs_via_run_now_when_subscriber_mailbox_is_full() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "schedule://acme/jobs/run-now-full/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    register_full_mailbox(&router, family, 30);
    let sink = ScheduleDomain::new(
        crate::domains::schedule::ScheduleStore::new(store.clone()),
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        crate::domains::schedule::ScheduleStore::new(store),
        crate::domains::WritePolicy::Buffered,
    );
    actor
        .create_schedule_with_mode(
            route.to_string(),
            "* * * * *".to_string(),
            ScheduleDeliveryMode::Broadcast,
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    sink.insert_actor_for_tests(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    add_single_mode_candidates(&mut subscriptions, family, route, &[30]);
    sink.insert_subscriptions_for_tests(family, subscriptions);
    wait_for_subscription_count(&sink, 1);

    // Act
    let result = sink
        .run_now(family, route.to_string(), Duration::from_secs(1))
        .expect("run-now command completes")
        .expect("run-now definition exists");

    // Assert
    assert_eq!(result.accepted_handoffs, 0);
    assert_eq!(result.attempted_handoffs, 1);
    assert_eq!(sink.subscription_count(), 1);
    assert_eq!(sink.notify_failure_count(), 1);
}

/// A definition `Cancel` that lands between a scan's claim and its next scan
/// pass must remove the pending claim along with the definition (already
/// enforced by `remove_pending_claims_for_route`); the next scan pass must
/// neither deliver nor acknowledge a fire for a route that no longer exists,
/// and must not leave any dangling pending-fire bookkeeping behind.
#[test]
fn should_not_deliver_or_ack_pending_claim_after_definition_cancelled_before_next_scan() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "schedule://acme/jobs/cancel-race/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    register_full_mailbox(&router, family, 40);
    let sink = ScheduleDomain::new(
        crate::domains::schedule::ScheduleStore::new(store.clone()),
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        crate::domains::schedule::ScheduleStore::new(store),
        crate::domains::WritePolicy::Buffered,
    );
    actor
        .create_schedule_with_mode(
            route.to_string(),
            "* * * * *".to_string(),
            ScheduleDeliveryMode::Broadcast,
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    let claimed = actor.bench_claim_due_fires();
    assert_eq!(
        claimed.len(),
        1,
        "fire must be claimed before cancel races it"
    );
    actor
        .delete_schedule(route)
        .expect("cancel racing the in-flight claim");
    sink.insert_actor_for_tests(family, actor);
    let mut subscriptions = ScheduleSubscriptionSet::new();
    add_single_mode_candidates(&mut subscriptions, family, route, &[40]);
    sink.insert_subscriptions_for_tests(family, subscriptions);

    // Act
    sink.scan_due_schedules();

    // Assert
    assert_eq!(
        sink.pending_fire_count(),
        0,
        "a claim for a cancelled definition must not linger as a pending fire"
    );
    assert_eq!(
        sink.ack_failure_count(),
        0,
        "a cancelled definition's claim must not be attempted for ack at all"
    );
    assert_eq!(
        sink.notify_failure_count(),
        0,
        "a cancelled definition's claim must not be handed off for live delivery"
    );
}

/// Two live broadcast subscribers, one of which disconnects between the
/// claim and the scan's delivery pass. Cleanup runs on the high-priority
/// lane and can therefore run before the queued scan's delivery step. The
/// surviving subscriber must still be notified exactly once, and delivery
/// bookkeeping must reflect only the subscriber that was actually live at
/// delivery time - not the two that existed when the fire was claimed.
#[test]
fn should_notify_only_the_surviving_subscriber_when_cleanup_races_broadcast_delivery() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "schedule://acme/jobs/cleanup-race/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let surviving_mailbox = Arc::new(Mailbox::new(8));
    let surviving_address = RouteAddress::new(family, Route::new("inbox://session/51"));
    router.register(surviving_address.clone(), surviving_mailbox.clone());
    register_full_mailbox(&router, family, 50);
    let sink = Arc::new(ScheduleDomain::new(
        crate::domains::schedule::ScheduleStore::new(store.clone()),
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));
    router.register_domain_pattern("schedule", sink.clone());

    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        crate::domains::schedule::ScheduleStore::new(store),
        crate::domains::WritePolicy::Buffered,
    );
    actor
        .create_schedule_with_mode(
            route.to_string(),
            "* * * * *".to_string(),
            ScheduleDeliveryMode::Broadcast,
            Bytes::from_static(b"broadcast"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    sink.insert_actor_for_tests(family, actor);

    let mut subscriptions = ScheduleSubscriptionSet::new();
    subscriptions.insert(
        family,
        ScheduleSubscription {
            pattern: crate::runtime::matcher::Pattern::new(route),
            session_id: 50,
            subscription_id: 50,
            subscriber: RouteAddress::new(family, Route::new("inbox://session/50")),
        },
    );
    subscriptions.insert(
        family,
        ScheduleSubscription {
            pattern: crate::runtime::matcher::Pattern::new(route),
            session_id: 51,
            subscription_id: 51,
            subscriber: surviving_address,
        },
    );
    sink.insert_subscriptions_for_tests(family, subscriptions);
    wait_for_subscription_count(&sink, 2);

    // Act
    // Disconnect session 50 on the high-priority lane before the
    // queued scan's delivery step runs.
    sink.cleanup_session(50)
        .expect("cleanup disconnected session 50");
    wait_for_subscription_count(&sink, 1);
    sink.scan_due_schedules();

    // Assert
    let notify_envelope = receive_envelope(&surviving_mailbox, "surviving subscriber notify");
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);
    assert_no_envelope(&surviving_mailbox);
    assert_eq!(sink.notify_failure_count(), 0);
    assert_eq!(sink.pending_fire_count(), 0);
}
