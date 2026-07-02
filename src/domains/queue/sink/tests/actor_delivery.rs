use super::routing_watch_and_admin::{encode_queue_send, new_queue_domain_sink};
use super::*;

#[derive(Clone)]
struct DlqSeedClock {
    state: Arc<std::sync::Mutex<DlqSeedClockState>>,
}

#[derive(Clone, Copy)]
struct DlqSeedClockState {
    instant: Instant,
    epoch_ms: u64,
}

impl DlqSeedClock {
    fn new() -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(DlqSeedClockState {
                instant: Instant::now(),
                epoch_ms: 1_700_000_000_000,
            })),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().expect("clock state");
        state.instant += duration;
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    }
}

impl crate::runtime::clock::Clock for DlqSeedClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().expect("clock state").instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().expect("clock state").epoch_ms
    }
}

fn queue_send_envelope(family: RouteFamily, queue_route: &str) -> Envelope {
    let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new(queue_route));
    Envelope::from_route(
        client_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    )
}

fn queue_snapshot(
    sink: &QueueDomainSink,
    family: RouteFamily,
    queue_route: &str,
) -> QueueAdminSnapshot {
    sink.queue_snapshot_for_tests(family, queue_route)
}

fn seed_dead_letter(
    store: Arc<cntryl_midge::Engine>,
    key: &crate::domains::queue::QueueKey,
) -> crate::domains::queue::MessageId {
    let clock = DlqSeedClock::new();
    let mut actor = crate::domains::queue::QueueActor::with_clock(
        key.family,
        key.clone(),
        store,
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = match actor.handle_send(bytes::Bytes::from_static(b"email"), None) {
        crate::domains::queue::QueueResponse::Sent { id } => id,
        other => panic!("expected queue send to succeed, found {other:?}"),
    };
    match actor.handle_receive_for_session(7, 1, Some(1)) {
        crate::domains::queue::QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
        }
        other => panic!("expected queue receive to succeed, found {other:?}"),
    }
    clock.advance(Duration::from_secs(2));
    actor.process_expired_timers();
    assert_eq!(actor.admin_dead_letters().len(), 1);
    msg_id
}

fn persisted_dead_letter_count(
    store: Arc<cntryl_midge::Engine>,
    key: &crate::domains::queue::QueueKey,
) -> usize {
    crate::domains::queue::QueueActor::new(
        key.family,
        key.clone(),
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    )
    .admin_dead_letters()
    .len()
}

#[test]
fn should_route_queue_delivery_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );
    let envelope = queue_send_envelope(family, queue_route);

    // Act
    sink.stop_actor_for_tests();
    let result = sink.deliver(envelope);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    assert!(sink.actors_are_empty_for_tests());
}

#[test]
fn should_route_queue_admin_refresh_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    sink.deliver(queue_send_envelope(family, queue_route))
        .expect("enqueue queue message");

    // Act
    sink.stop_actor_for_tests();
    sink.refresh_admin_snapshot_if_dirty();
    let queues = admin_read_model.queues(None);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(queues.is_empty());
}

#[test]
fn should_route_queue_live_counts_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );
    sink.deliver(queue_send_envelope(family, queue_route))
        .expect("enqueue queue message");
    assert_eq!(sink.ready_message_count(), 1);

    // Act
    sink.stop_actor_for_tests();
    let ready_messages = sink.ready_message_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(ready_messages, 0);
}

#[test]
fn should_route_queue_cleanup_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let worker_session_id = 8;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::buffered(),
    );
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address.clone(),
        FrameContext::new(
            sender_session_id,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    ))
    .expect("enqueue queue message");
    let _send_ack = receive_queue_frame(&sender_mailbox, "enqueue response");
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("reserve queue message");
    let _reserve_ack = receive_queue_frame(&worker_mailbox, "reserve response");
    assert_eq!(
        queue_snapshot(&sink, family, queue_route).messages_inflight,
        1
    );

    // Act
    sink.stop_actor_for_tests();
    sink.cleanup_session(worker_session_id);
    let snapshot = queue_snapshot(&sink, family, queue_route);

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(snapshot.messages_inflight, 1);
    assert_eq!(snapshot.messages_ready, 0);
}

#[test]
fn should_route_queue_runtime_sweep_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    )
    .with_fast_flush_interval(Some(Duration::from_millis(100)));
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send("queue://acme/email/jobs", b"email"),
            family,
        ),
    ))
    .expect("send should enqueue");
    let _send_ack = receive_queue_frame(&sender_mailbox, "send response");
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));

    // Act
    sink.stop_actor_for_tests();
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(!sink.is_actor_running());
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));
}

#[test]
fn should_route_queue_dead_letter_replay_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "emails".to_string(),
    };
    let msg_id = seed_dead_letter(store.clone(), &key);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store.clone(),
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::buffered(),
    );

    // Act
    sink.stop_actor_for_tests();
    let replayed = sink.replay_dead_letter(&key, msg_id);
    let dead_letters = persisted_dead_letter_count(store, &key);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(replayed.is_err());
    assert_eq!(dead_letters, 1);
    assert!(sink.actors_are_empty_for_tests());
}

#[test]
fn should_route_queue_dead_letter_purge_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "emails".to_string(),
    };
    let msg_id = seed_dead_letter(store.clone(), &key);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store.clone(),
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::buffered(),
    );

    // Act
    sink.stop_actor_for_tests();
    let purged = sink.purge_dead_letter(&key, msg_id);
    let dead_letters = persisted_dead_letter_count(store, &key);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(purged.is_err());
    assert_eq!(dead_letters, 1);
    assert!(sink.actors_are_empty_for_tests());
}
