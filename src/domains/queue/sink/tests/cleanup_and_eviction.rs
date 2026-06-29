use super::*;

#[test]
fn should_cleanup_queue_inflight_for_disconnected_session() {
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
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address.clone(),
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("reserve queue message");
    let _reserve_ack = worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response");

    sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(admin_read_model.queue_inflight(None).len(), 1);

    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("queue://cleanup")),
        crate::runtime::SessionCleanup {
            session_id: worker_session_id,
        },
    ))
    .expect("cleanup queue session");

    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let queues = admin_read_model.queues(None);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].messages_ready, 1);
    assert_eq!(queues[0].messages_delayed, 0);
    assert_eq!(queues[0].messages_inflight, 0);
    assert_eq!(queues[0].messages_dead_lettered, 0);
    assert_eq!(queues[0].messages_total, 1);
    assert!(admin_read_model.queue_inflight(None).is_empty());
}

#[test]
fn should_reject_queue_inflight_followups_from_non_owner_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let worker_session_id = 8;
    let other_session_id = 9;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let other_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let other_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    router.register(other_address.clone(), other_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
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
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

    sink.deliver(Envelope::from_route(
        worker_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("reserve queue message");
    let reserve_frame = receive_queue_frame(&worker_mailbox, "reserve response");
    let (id, token) = receive_response_first_message(&reserve_frame);

    sink.deliver(Envelope::from_route(
        other_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            other_session_id,
            ChannelId::Pub,
            MessageType::new(203),
            encode_queue_extend(queue_route, id, token, 60),
            family,
        ),
    ))
    .expect("non-owner extend");
    let other_extend = receive_queue_frame(&other_mailbox, "non-owner extend response");

    sink.deliver(Envelope::from_route(
        worker_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(203),
            encode_queue_extend(queue_route, id, token, 60),
            family,
        ),
    ))
    .expect("owner extend");
    let owner_extend = receive_queue_frame(&worker_mailbox, "owner extend response");

    sink.deliver(Envelope::from_route(
        other_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            other_session_id,
            ChannelId::Pub,
            MessageType::new(204),
            encode_queue_ack(queue_route, id, token),
            family,
        ),
    ))
    .expect("non-owner complete");
    let other_complete = receive_queue_frame(&other_mailbox, "non-owner complete response");

    sink.deliver(Envelope::from_route(
        worker_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(204),
            encode_queue_ack(queue_route, id, token),
            family,
        ),
    ))
    .expect("owner complete");
    let owner_complete = receive_queue_frame(&worker_mailbox, "owner complete response");

    sink.deliver(Envelope::from_route(
        other_address,
        queue_address.clone(),
        FrameContext::new(
            other_session_id,
            ChannelId::Pub,
            MessageType::new(204),
            encode_queue_ack(queue_route, id, token),
            family,
        ),
    ))
    .expect("non-owner complete after owner cached success");
    let other_complete_after_owner =
        receive_queue_frame(&other_mailbox, "non-owner cached complete response");

    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(204),
            encode_queue_ack(queue_route, id, token),
            family,
        ),
    ))
    .expect("owner complete retry");
    let owner_complete_retry = receive_queue_frame(&worker_mailbox, "owner complete retry");

    // Assert
    assert_eq!(
        queue_simple_error_code(&other_extend),
        crate::protocol::error_codes::queue::ERR_MESSAGE_NOT_FOUND
    );
    assert_eq!(owner_extend.payload[0], 0);
    assert_eq!(
        queue_simple_error_code(&other_complete),
        crate::protocol::error_codes::queue::ERR_MESSAGE_NOT_FOUND
    );
    assert_eq!(owner_complete.payload[0], 0);
    assert_eq!(
        queue_simple_error_code(&other_complete_after_owner),
        crate::protocol::error_codes::queue::ERR_MESSAGE_NOT_FOUND
    );
    assert_eq!(owner_complete_retry.payload[0], 0);
}

#[test]
fn should_include_delayed_messages_in_queue_admin_snapshot() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
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
        queue_address,
        FrameContext::new(
            sender_session_id,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send_with_delay(queue_route, b"email", 60),
            family,
        ),
    ))
    .expect("enqueue delayed queue message");
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue delayed response");
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let queues = admin_read_model.queues(None);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].messages_ready, 0);
    assert_eq!(queues[0].messages_delayed, 1);
    assert_eq!(queues[0].messages_inflight, 0);
    assert_eq!(queues[0].messages_dead_lettered, 0);
    assert_eq!(queues[0].messages_total, 1);
}

#[test]
fn should_evict_idle_queue_actor_without_losing_committed_state() {
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
        admin_read_model.clone(),
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
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");
    assert_eq!(sink.actors.lock().len(), 1);

    // Act
    force_actor_idle(&sink, queue_route, family);
    sink.refresh_admin_snapshot_if_dirty();
    assert!(
        sink.actors.lock().is_empty(),
        "idle actor should be evicted"
    );
    assert!(
        admin_read_model.queues(None).is_empty(),
        "cold queue should disappear from warm admin snapshot"
    );

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
    .expect("reserve queue message after eviction");
    let reserve_envelope = worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response after eviction");
    let reserve_frame = reserve_envelope
        .into_payload::<FrameContext>()
        .expect("reserve response frame after eviction");
    assert_eq!(receive_response_message_count(&reserve_frame), 1);

    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert_eq!(sink.actors.lock().len(), 1);
    assert_eq!(admin_read_model.queues(None)[0].messages_inflight, 1);
}

#[test]
fn should_not_evict_idle_queue_actor_with_live_inflight() {
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
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

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
    let _reserve_ack = worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response");

    // Act
    force_actor_idle(&sink, queue_route, family);
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert_eq!(
        sink.actors.lock().len(),
        1,
        "actors with live inflight entries must stay warm until the inflight entry is gone"
    );
}
