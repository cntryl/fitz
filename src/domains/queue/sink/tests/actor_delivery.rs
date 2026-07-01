use super::routing_watch_and_admin::{encode_queue_send, new_queue_domain_sink};
use super::*;

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
    let key = crate::domains::queue::QueueKey::from_route(family, &Route::new(queue_route))
        .expect("queue key");
    let actors = sink.actors.lock();
    let snapshot = actors
        .get(&key)
        .expect("warm queue actor")
        .actor
        .lock()
        .admin_snapshot();
    snapshot
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
    assert!(sink.actors.lock().is_empty());
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
    assert!(sink.dirty_fast_flush_families.lock().contains(&1));

    // Act
    sink.stop_actor_for_tests();
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(!sink.is_actor_running());
    assert!(sink.dirty_fast_flush_families.lock().contains(&1));
}
