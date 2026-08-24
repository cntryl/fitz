use super::routing_watch_and_admin::{encode_queue_send, new_queue_domain_sink};
use super::*;
use crate::domains::queue::QueueAdminSnapshot;

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

fn decode_routed_reserve_response(frame: &FrameContext) -> Vec<(String, Vec<u8>)> {
    let mut decoder =
        crate::dispatch::protocol::payload_codec::PayloadDecoder::new(frame.payload.as_ref());
    assert_eq!(decoder.get_u8().expect("reserve status"), 0);
    let count = decoder.get_u32().expect("reserve count");
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let route = decoder.get_string().expect("reserved route");
        let _id = decoder.get_u64().expect("reserved message id");
        let _token = decoder.get_u64().expect("reserved lease token");
        let body = decoder.get_bytes().expect("reserved body").to_vec();
        items.push((route, body));
    }
    assert!(
        decoder.is_complete(),
        "reserve response should be fully consumed"
    );
    items
}

fn decode_concrete_reserve_response(frame: &FrameContext) -> Vec<Vec<u8>> {
    let mut decoder =
        crate::dispatch::protocol::payload_codec::PayloadDecoder::new(frame.payload.as_ref());
    assert_eq!(decoder.get_u8().expect("reserve status"), 0);
    let count = decoder.get_u32().expect("reserve count");
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let _id = decoder.get_u64().expect("message ID");
        let _token = decoder.get_u64().expect("lease token");
        items.push(decoder.get_bytes().expect("message body").to_vec());
    }
    assert!(decoder.is_complete());
    items
}

#[test]
fn should_wake_fifo_long_poll_reserve_when_matching_message_is_enqueued() {
    // Arrange
    // Act
    // Assert
    let family = RouteFamily::new(1);
    let route = "queue://acme/jobs/email";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let producer_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let consumer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let producer_mailbox = Arc::new(Mailbox::new(8));
    let consumer_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(producer_address.clone(), producer_mailbox.clone());
    router.register(consumer_address.clone(), consumer_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );

    sink.deliver(Envelope::from_route(
        consumer_address,
        queue_address.clone(),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve_wait(route, 30, 1, 5),
            family,
        ),
    ))
    .expect("queue long-poll reserve");
    assert!(consumer_mailbox.receiver().try_recv().is_err());

    sink.deliver(Envelope::from_route(
        producer_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(route, b"late"),
            family,
        ),
    ))
    .expect("enqueue matching queue message");
    let _enqueue = receive_queue_frame(&producer_mailbox, "enqueue response");
    let reserve = receive_queue_frame(&consumer_mailbox, "deferred reserve response");
    assert_eq!(
        decode_concrete_reserve_response(&reserve),
        vec![b"late".to_vec()]
    );
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
fn should_report_due_work_mutation_when_total_count_is_unchanged() {
    // Arrange
    let family = RouteFamily::new(1);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "dead".to_string(),
    };
    let clock = DlqSeedClock::new();
    let mut actor = crate::domains::queue::QueueActor::with_clock(
        family,
        key,
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(matches!(
        actor.handle_send(bytes::Bytes::from_static(b"body"), None),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    assert!(matches!(
        actor.handle_receive_for_session(7, 1, Some(1)),
        crate::domains::queue::QueueResponse::Received { .. }
    ));
    let before = actor.live_counts();
    clock.advance(Duration::from_secs(2));

    // Act
    let mutated = actor.process_due_work();
    let after = actor.live_counts();

    // Assert
    assert!(mutated);
    assert_eq!(before.total(), after.total());
    assert_eq!(after.dead_letters, 1);
}

#[test]
fn should_preserve_legacy_wire_shape_given_concrete_queue_reserve() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send("acme/cats/cat", b"body"),
            family,
        ),
    ))
    .expect("enqueue queue message");
    let _response = receive_queue_frame(&sender_mailbox, "enqueue response");

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve("acme/cats/cat", 30, 1),
            family,
        ),
    ))
    .expect("reserve concrete queue message");
    let response = receive_queue_frame(&worker_mailbox, "reserve response");

    // Assert
    assert_eq!(
        decode_concrete_reserve_response(&response),
        vec![b"body".to_vec()]
    );
}

#[test]
fn should_retain_queue_identity_when_dead_letter_actor_is_evicted() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/dead";
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "dead".to_string(),
    };
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    seed_dead_letter(store.clone(), &key);
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
        cntryl_midge::WriteOptions::buffered(),
    );
    sink.install_actor_for_tests(key.clone(), actor);

    // Act
    sink.force_actor_idle_for_tests(family, queue_route);
    sink.sweep_runtime_state_at(Instant::now());

    // Assert
    assert!(sink.actors_are_empty_for_tests());
    assert!(sink.known_queue_contains_for_tests(&key));
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
fn should_reserve_concrete_items_given_wildcards_in_unknown_queue_segments() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        store,
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );
    for (route, body) in [
        ("queue://acme/cats/cat", b"acme".as_slice()),
        ("queue://globex/cats/cat", b"globex".as_slice()),
    ] {
        sink.deliver(Envelope::from_route(
            sender_address.clone(),
            queue_address.clone(),
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(route, body),
                family,
            ),
        ))
        .expect("enqueue concrete queue message");
        let _response = receive_queue_frame(&sender_mailbox, "enqueue response");
    }

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve("queue://*/cats/*", 30, 2),
            family,
        ),
    ))
    .expect("reserve wildcard queue messages");
    let response = receive_queue_frame(&worker_mailbox, "wildcard reserve response");
    let mut items = decode_routed_reserve_response(&response);
    items.sort();

    // Assert
    assert_eq!(
        items,
        vec![
            ("queue://acme/cats/cat".to_string(), b"acme".to_vec()),
            ("queue://globex/cats/cat".to_string(), b"globex".to_vec()),
        ]
    );
}

#[test]
fn should_surface_startup_inventory_failure_to_wildcard_reserve() {
    // Arrange
    let family = RouteFamily::new(1);
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );
    sink.set_inventory_error_for_tests("inventory scan failed");

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve("queue://*/cats/*", 30, 2),
            family,
        ),
    ))
    .expect("dispatch wildcard queue reserve");
    let response = receive_queue_frame(&worker_mailbox, "wildcard inventory error response");

    // Assert
    let (code, message) =
        crate::dispatch::protocol::error_codes::decode_error_body(response.payload.as_ref())
            .expect("decode queue inventory error");
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::queue::ERR_BACKEND_ERROR
    );
    assert!(message.contains("Queue inventory unavailable: inventory scan failed"));
}

#[test]
fn should_discover_durable_queue_routes_given_wildcard_reserve_after_sink_restart() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let first_router = Arc::new(Router::new());
    first_router.register(sender_address.clone(), sender_mailbox.clone());
    let first_sink = new_queue_domain_sink(
        store.clone(),
        first_router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::sync(),
    );
    first_sink
        .deliver(Envelope::from_route(
            sender_address,
            queue_address.clone(),
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send("queue://acme/cats/cat", b"durable"),
                family,
            ),
        ))
        .expect("enqueue durable queue message");
    let _response = receive_queue_frame(&sender_mailbox, "enqueue response");
    first_sink.stop_actor_for_tests();
    drop(first_sink);

    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let second_router = Arc::new(Router::new());
    second_router.register(worker_address.clone(), worker_mailbox.clone());
    let second_sink = new_queue_domain_sink(
        store,
        second_router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::sync(),
    );

    // Act
    second_sink
        .deliver(Envelope::from_route(
            worker_address,
            queue_address,
            FrameContext::new(
                8,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve("queue://*/cats/*", 30, 1),
                family,
            ),
        ))
        .expect("reserve durable wildcard queue message");
    let response = receive_queue_frame(&worker_mailbox, "wildcard reserve response");

    // Assert
    assert_eq!(
        decode_routed_reserve_response(&response),
        vec![("queue://acme/cats/cat".to_string(), b"durable".to_vec())]
    );
}

#[test]
fn should_reject_wildcard_reserve_above_maximum_batch_size() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/cats/cat";
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"one"),
            family,
        ),
    ))
    .expect("enqueue queue message");
    let _response = receive_queue_frame(&sender_mailbox, "enqueue response");

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve("queue://acme/cats/*", 30, u32::MAX),
            family,
        ),
    ))
    .expect("reserve with maximum batch size");
    let response = receive_queue_frame(&worker_mailbox, "wildcard reserve error response");

    // Assert
    let (code, message) =
        crate::dispatch::protocol::error_codes::decode_error_body(response.payload.as_ref())
            .expect("decode reserve batch size error");
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::queue::ERR_BAD_REQUEST
    );
    assert_eq!(message, "Queue reserve batch_size must be <= 1024");
}

#[test]
fn should_inventory_only_authoritative_non_empty_queue_rows() {
    // Arrange
    let family = RouteFamily::new(1);
    let populated_key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "populated".to_string(),
    };
    let empty_key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "empty".to_string(),
    };
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut populated = crate::domains::queue::QueueActor::new(
        family,
        populated_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(matches!(
        populated.handle_send(Bytes::from_static(b"queued"), None),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    let _empty = crate::domains::queue::QueueActor::new(
        family,
        empty_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let mut txn = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin foreign-domain write");
    txn.put(
        crate::utils::storage_key::prefixed_key(
            "acme",
            crate::utils::storage_key::DomainKeyspace::Kv,
            b"foreign",
        ),
        vec![0; 1024],
        None,
    )
    .expect("write foreign-domain row");
    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit foreign-domain row");

    // Act
    let sink = new_queue_domain_sink(
        store,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );

    // Assert
    assert_eq!(sink.known_queue_count_for_tests(), 1);
    assert!(sink.known_queue_contains_for_tests(&populated_key));
    assert!(!sink.known_queue_contains_for_tests(&empty_key));
}

#[test]
fn should_mark_fast_flush_plus_admin_dirty_when_wildcard_poll_only_expires_work() {
    // Arrange
    let family = RouteFamily::new(1);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "expiring".to_string(),
    };
    let clock = DlqSeedClock::new();
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut actor = crate::domains::queue::QueueActor::with_clock_and_write_options(
        family,
        key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"expire"), None),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    assert!(matches!(
        actor.handle_receive_for_session(7, 1, Some(1)),
        crate::domains::queue::QueueResponse::Received { .. }
    ));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    )
    .with_fast_flush_interval(Some(Duration::from_millis(100)));
    sink.install_actor_for_tests(key, actor);
    sink.clear_dirty_fast_flush_for_tests();
    clock.advance(Duration::from_secs(2));

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        RouteAddress::new(family, Route::new("queue://inbound")),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve("queue://acme/jobs/*", 30, 1),
            family,
        ),
    ))
    .expect("poll expiring queue");
    let response = receive_queue_frame(&worker_mailbox, "wildcard reserve response");
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert!(decode_routed_reserve_response(&response).is_empty());
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));
    assert_eq!(admin_read_model.queues(None)[0].messages_dead_lettered, 1);
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
    assert_eq!(sink.counts().ready, 1);

    // Act
    sink.stop_actor_for_tests();
    let ready_messages = sink.counts().ready;

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(ready_messages, 0);
}

#[test]
fn should_bound_concrete_reserve_response_before_messages_become_inflight() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/wire-capacity";
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(2));
    let worker_mailbox = Arc::new(Mailbox::new(2));
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::buffered(),
    );
    let body = vec![0x5a; 1024];
    for _ in 0..100 {
        sink.deliver(Envelope::from_route(
            sender_address.clone(),
            queue_address.clone(),
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, &body),
                family,
            ),
        ))
        .expect("enqueue queue message");
        let _response = receive_queue_frame(&sender_mailbox, "enqueue response");
    }

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 100),
            family,
        ),
    ))
    .expect("reserve queue batch");
    let response = receive_queue_frame(&worker_mailbox, "reserve response");

    // Assert
    assert!(u16::try_from(response.payload.len()).is_ok());
    assert_eq!(decode_concrete_reserve_response(&response).len(), 62);
    let snapshot = queue_snapshot(&sink, family, queue_route);
    assert_eq!(snapshot.messages_ready, 38);
    assert_eq!(snapshot.messages_inflight, 62);
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
    let reserve_ack = receive_queue_frame(&worker_mailbox, "reserve response");
    assert_eq!(
        decode_concrete_reserve_response(&reserve_ack),
        vec![b"email".to_vec()]
    );
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
fn should_coalesce_queue_runtime_sweeps_while_actor_is_busy() {
    // Arrange
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let pending_reserves = sink.core.pending_reserves.lock();

    // Act
    let first_enqueued = sink.request_runtime_sweep_at(Instant::now());
    let second_enqueued = sink.request_runtime_sweep_at(Instant::now());

    // Assert
    assert!(first_enqueued);
    assert!(!second_enqueued);
    drop(pending_reserves);
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
