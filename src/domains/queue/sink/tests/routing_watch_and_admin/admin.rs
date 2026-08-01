use super::*;

#[test]
pub(super) fn should_emit_ready_notification_with_counts_given_first_enqueue_on_watched_queue() {
    // Arrange
    let harness = QueueWatchHarness::new();
    let queue_route = "queue://acme/jobs/emails";
    let subscription_id = harness.watch("queue://acme/jobs/emails");

    // Act
    harness.send(queue_route, b"email");
    let (delivered_subscription_id, delivered_route, ready, delayed, inflight) =
        harness.next_watch_notification();

    // Assert
    assert_eq!(delivered_subscription_id, subscription_id);
    assert_eq!(delivered_route, "queue://acme/jobs/emails");
    assert_eq!(ready, 1);
    assert_eq!(delayed, 0);
    assert_eq!(inflight, 0);
    harness.assert_no_watch_notification();
}

#[test]
pub(super) fn should_not_emit_duplicate_ready_notification_given_enqueue_while_already_ready() {
    // Arrange
    let harness = QueueWatchHarness::new();
    let queue_route = "queue://acme/jobs/emails";
    harness.watch("queue://acme/jobs/emails");
    harness.send(queue_route, b"first");
    let _initial_ready = harness.next_watch_notification();

    // Act
    harness.send(queue_route, b"second");

    // Assert
    harness.assert_no_watch_notification();
}

#[test]
pub(super) fn should_emit_ready_notification_after_queue_returns_to_empty() {
    // Arrange
    let harness = QueueWatchHarness::new();
    let queue_route = "queue://acme/jobs/emails";
    let subscription_id = harness.watch("queue://acme/jobs/emails");
    harness.send(queue_route, b"first");
    let _initial_ready = harness.next_watch_notification();
    let (id, token) = harness.reserve_one(queue_route);
    harness.complete(queue_route, id, token);

    // Act
    harness.send(queue_route, b"second");
    let (delivered_subscription_id, delivered_route, ready, delayed, inflight) =
        harness.next_watch_notification();

    // Assert
    assert_eq!(delivered_subscription_id, subscription_id);
    assert_eq!(delivered_route, "queue://acme/jobs/emails");
    assert_eq!(ready, 1);
    assert_eq!(delayed, 0);
    assert_eq!(inflight, 0);
    harness.assert_no_watch_notification();
}

#[test]
pub(super) fn should_delay_ready_notification_until_delayed_message_is_promoted() {
    // Arrange
    let harness = QueueWatchHarness::new();
    let queue_route = "queue://acme/jobs/emails";
    let subscription_id = harness.watch("queue://acme/jobs/emails");

    // Act
    harness.send_delayed(queue_route, b"later", 1);

    // Assert
    harness.assert_no_watch_notification();

    // Act
    std::thread::sleep(Duration::from_millis(1_100));
    harness.sink.sweep_runtime_state();
    let (delivered_subscription_id, delivered_route, ready, delayed, inflight) =
        harness.next_watch_notification();

    // Assert
    assert_eq!(delivered_subscription_id, subscription_id);
    assert_eq!(delivered_route, "queue://acme/jobs/emails");
    assert_eq!(ready, 1);
    assert_eq!(delayed, 0);
    assert_eq!(inflight, 0);
    harness.assert_no_watch_notification();
}

#[test]
pub(super) fn should_register_queue_watch_given_watch_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_session_id = 7;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address,
        queue_address,
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Pub,
            MessageType::new(207),
            encode_route_pattern(queue_route),
            family,
        ),
    ))
    .expect("register queue watch path");

    // Assert
    let subscribe_envelope = subscriber_mailbox
        .receiver()
        .try_recv()
        .expect("queue watch ack envelope");
    let subscribe_frame = subscribe_envelope
        .into_payload::<FrameContext>()
        .expect("queue watch ack frame");
    assert_eq!(subscribe_frame.msg_type.as_u16(), 207);
    assert!(watch_response_subscription_id(&subscribe_frame) > 0);
}

#[test]
pub(super) fn should_remove_queue_watch_given_unwatch_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_session_id = 7;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let subscriber_mailbox = Arc::new(Mailbox::new(16));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register(sender_address.clone(), sender_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Pub,
            MessageType::new(207),
            encode_queue_watch(queue_route),
            family,
        ),
    ))
    .expect("register queue watch path");
    let _ = subscriber_mailbox
        .receiver()
        .try_recv()
        .expect("watch ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address,
        queue_address.clone(),
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Pub,
            MessageType::new(208),
            encode_queue_unwatch(queue_route),
            family,
        ),
    ))
    .expect("remove queue watch path");

    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address,
        FrameContext::new(
            9,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send("queue://acme/jobs/emails", b"email"),
            family,
        ),
    ))
    .expect("enqueue watched queue message");

    // Assert
    let unsubscribe_ack_envelope = subscriber_mailbox
        .receiver()
        .try_recv()
        .expect("unsubscribe ack envelope");
    let unsubscribe_ack_frame = unsubscribe_ack_envelope
        .into_payload::<FrameContext>()
        .expect("unsubscribe ack frame");
    assert_eq!(unsubscribe_ack_frame.msg_type.as_u16(), 208);
    assert_eq!(
        unsubscribe_ack_frame.payload,
        bytes::Bytes::from_static(&[0])
    );
    let _ = sender_mailbox
        .receiver()
        .try_recv()
        .expect("send ack envelope");
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
    assert!(sink.watch_families_are_empty_for_tests());
}

#[test]
pub(super) fn should_cleanup_expired_queue_dedup_entries_during_runtime_sweep() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let dedup_store = Arc::new(crate::utils::idempotency::DedupStore::new(
        Duration::from_millis(1),
    ));
    let sink = QueueDomainSink::new(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
        dedup_store.clone(),
    );
    let dedup_key = crate::utils::idempotency::DedupKey {
        realm: "acme".to_string(),
        domain: crate::utils::idempotency::Domain::Queue,
        identifier: crate::utils::idempotency::DedupIdentifier::QueueComplete {
            family: family.as_u64(),
            area: "jobs".to_string(),
            resource: "emails".to_string(),
            owner_session_id: 1,
            message_id: 1,
            token: 99,
        },
    };
    dedup_store.record(dedup_key, vec![1, 2, 3]);
    std::thread::sleep(Duration::from_millis(5));

    let now = Instant::now();
    sink.set_next_dedup_sweep_at_for_tests(now);

    // Act
    sink.sweep_runtime_state_at(now);

    // Assert
    assert!(dedup_store.is_empty());
}

#[test]
pub(super) fn should_refresh_queue_admin_snapshot_with_live_queue_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let worker_session_id = 8;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let watcher_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::buffered(),
    );

    // Act
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
        watcher_address,
        queue_address.clone(),
        FrameContext::new(
            9,
            ChannelId::Pub,
            MessageType::new(207),
            encode_queue_watch("queue://acme/jobs/emails"),
            family,
        ),
    ))
    .expect("watch queue readiness");
    let _watch_ack = receive_queue_frame(&watcher_mailbox, "watch response");
    let _initial_ready_notify = receive_queue_frame(&watcher_mailbox, "initial ready notify");

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
    let reserve_envelope = worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response");
    let reserve_frame = reserve_envelope
        .into_payload::<FrameContext>()
        .expect("reserve response frame");
    assert_eq!(reserve_frame.msg_type.as_u16(), 202);
    assert_eq!(receive_response_message_count(&reserve_frame), 1);

    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let queues = admin_read_model.queues(None);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].realm, "acme");
    assert_eq!(queues[0].area, "jobs");
    assert_eq!(queues[0].resource, "emails");
    assert_eq!(queues[0].messages_ready, 0);
    assert_eq!(queues[0].messages_delayed, 0);
    assert_eq!(queues[0].messages_inflight, 1);
    assert_eq!(queues[0].messages_dead_lettered, 0);
    assert_eq!(queues[0].messages_total, 1);
    assert_eq!(queues[0].subscriptions_active, 1);

    let inflight = admin_read_model.queue_inflight(None);
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].realm, "acme");
    assert_eq!(inflight[0].area, "jobs");
    assert_eq!(inflight[0].resource, "emails");
    assert_eq!(inflight[0].message_id, 1);
    assert_eq!(inflight[0].session_id, worker_session_id.to_string());
    assert_eq!(inflight[0].attempts, 1);
    assert!(!inflight[0].expires_at.is_empty());
}
