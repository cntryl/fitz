use super::actor_delivery::DlqSeedClock;
use super::*;

fn delayed_queue_with_clock(
    router: Arc<Router>,
) -> (
    QueueDomain,
    Arc<crate::control::admin::read_model::AdminReadModel>,
    DlqSeedClock,
) {
    let family = RouteFamily::new(1);
    let key =
        QueueKey::from_route(family, &Route::new("queue://acme/jobs/delayed")).expect("queue key");
    let clock = DlqSeedClock::new();
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut actor = crate::domains::queue::QueueActor::with_clock(
        family,
        key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(matches!(
        actor.handle_send(bytes::Bytes::from_static(b"delayed"), Some(1)),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        read_model.clone(),
        crate::domains::WritePolicy::Buffered,
    );
    sink.install_actor_for_tests(key, actor);
    (sink, read_model, clock)
}

#[test]
fn should_not_promote_due_delayed_message_during_queue_admin_refresh() {
    // Arrange
    let (sink, read_model, clock) = delayed_queue_with_clock(Arc::new(Router::new()));
    let family = RouteFamily::new(1);
    sink.inspect_family_for_tests(family, QueueFamilyState::mark_admin_snapshot_dirty);
    clock.advance(Duration::from_secs(2));

    // Act
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let queues = read_model.queues(None);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].messages_delayed, 1);
    assert_eq!(queues[0].messages_ready, 0);
    let snapshot = sink.queue_snapshot_for_tests(family, "queue://acme/jobs/delayed");
    assert_eq!(snapshot.messages_delayed, 1);
    assert_eq!(snapshot.messages_ready, 0);
}

#[test]
fn should_promote_due_delayed_message_during_runtime_sweep_without_admin_refresh() {
    // Arrange
    let (sink, _read_model, clock) = delayed_queue_with_clock(Arc::new(Router::new()));
    let family = RouteFamily::new(1);
    clock.advance(Duration::from_secs(2));

    // Act
    sink.sweep_runtime_state_at(Instant::now());

    // Assert
    let snapshot = sink.queue_snapshot_for_tests(family, "queue://acme/jobs/delayed");
    assert_eq!(snapshot.messages_delayed, 0);
    assert_eq!(snapshot.messages_ready, 1);
}

#[test]
fn should_not_evict_idle_queue_actor_during_queue_admin_refresh() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/idle";
    let key = QueueKey::from_route(family, &Route::new(queue_route)).expect("queue key");
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let actor = crate::domains::queue::QueueActor::new(
        family,
        key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let sink = new_queue_domain_sink(
        store,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::Buffered,
    );
    sink.install_actor_for_tests(key, actor);
    sink.force_actor_idle_for_tests(family, queue_route);
    sink.inspect_family_for_tests(family, QueueFamilyState::mark_admin_snapshot_dirty);

    // Act
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert_eq!(sink.actor_count_for_tests(), 1);
}

#[test]
fn should_not_expire_inflight_message_during_queue_admin_refresh() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/inflight";
    let key = QueueKey::from_route(family, &Route::new(queue_route)).expect("queue key");
    let clock = DlqSeedClock::new();
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut actor = crate::domains::queue::QueueActor::with_clock(
        family,
        key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(matches!(
        actor.handle_send(bytes::Bytes::from_static(b"inflight"), None),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    assert!(matches!(
        actor.handle_receive_for_session(7, 1, Some(1)),
        crate::domains::queue::QueueResponse::Received { ref messages } if messages.len() == 1
    ));
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        Arc::new(Router::new()),
        read_model.clone(),
        crate::domains::WritePolicy::Buffered,
    );
    sink.install_actor_for_tests(key, actor);
    sink.inspect_family_for_tests(family, QueueFamilyState::mark_admin_snapshot_dirty);
    clock.advance(Duration::from_secs(2));

    // Act
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let queues = read_model.queues(None);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].messages_inflight, 1);
    assert_eq!(queues[0].messages_ready, 0);
    assert_eq!(read_model.queue_inflight(None).len(), 1);
    let snapshot = sink.queue_snapshot_for_tests(family, queue_route);
    assert_eq!(snapshot.messages_inflight, 1);
}

#[test]
fn should_not_wake_parked_reserve_during_queue_admin_refresh() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/delayed";
    let consumer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let consumer_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(consumer_address.clone(), consumer_mailbox.clone());
    let (sink, _read_model, clock) = delayed_queue_with_clock(router);
    sink.deliver(Envelope::from_route(
        consumer_address,
        RouteAddress::new(family, Route::new("queue://inbound")),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve_wait(queue_route, 30, 1, 5),
            family,
        ),
    ))
    .expect("park queue reserve");
    assert!(consumer_mailbox.receiver().try_recv().is_err());
    clock.advance(Duration::from_secs(2));

    // Act
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert!(consumer_mailbox.receiver().try_recv().is_err());
    let pending = sink.inspect_family_for_tests(family, |state| state.pending_reserves.len());
    assert_eq!(pending, 1);
}
