use super::*;

#[test]
pub(super) fn should_create_queue_domain_sink() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );

    // Assert
    assert!(sink.is_active());
}

#[test]
pub(super) fn should_create_fast_queue_sink_with_explicit_background_cloud_recovery() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path(),
                "fitz-queue-sink",
                "background",
            )
            .build()
            .expect("build cloud-simulated options"),
        )
        .expect("open cloud-simulated engine"),
    );
    store
        .create_column_family("tenant_default")
        .expect("create route-family column family");

    // Act
    let result = QueueDomainSink::try_new(
        Arc::clone(&store),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
        cntryl_midge::WriteOptions::cloud_async(),
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert!(result.is_ok());
    drop(result);
    crate::testkit::midge::shutdown_test_engine(store);
}

#[test]
pub(super) fn should_mark_fast_queue_family_dirty_given_successful_send() {
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

    // Act
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

    // Assert
    let response_frame = receive_queue_frame(&sender_mailbox, "send response");
    assert_eq!(response_frame.payload[0], 0);
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));
}

#[test]
pub(super) fn should_clear_dirty_fast_queue_family_after_flush_window() {
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
    let _ = receive_queue_frame(&sender_mailbox, "send response");
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));

    // Act
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(sink.dirty_fast_flush_is_empty_for_tests());
}

#[test]
pub(super) fn should_keep_dirty_fast_queue_family_when_flush_cannot_find_cf() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    )
    .with_fast_flush_interval(Some(Duration::from_millis(100)));
    sink.insert_dirty_fast_flush_family_for_tests(99);

    // Act
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(sink.dirty_fast_flush_contains_family_for_tests(99));
}

#[test]
pub(super) fn should_reject_send_given_malformed_queue_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let invalid_route = "queue://acme/jobs";
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let queue_sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    );

    // Act
    queue_sink
        .deliver(Envelope::from_route(
            sender_address,
            queue_address,
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(invalid_route, b"email"),
                family,
            ),
        ))
        .expect("reject malformed send");
    queue_sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let response_envelope = sender_mailbox
        .receiver()
        .try_recv()
        .expect("send response envelope");
    let response_frame = response_envelope
        .into_payload::<FrameContext>()
        .expect("send response frame");
    assert_eq!(response_frame.msg_type.as_u16(), 200);
    assert_eq!(
        bad_request_reason(&response_frame),
        "invalid queue route: queue://acme/jobs"
    );
    assert!(queue_sink.actors_are_empty_for_tests());
    assert!(admin_read_model.queues(None).is_empty());
}

#[test]
pub(super) fn should_reject_receive_given_malformed_queue_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let invalid_route = "queue://acme/jobs";
    let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let client_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(client_address.clone(), client_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    );

    // Act
    sink.deliver(Envelope::from_route(
        client_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(invalid_route, 30, 1),
            family,
        ),
    ))
    .expect("reject malformed receive");
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let response_envelope = client_mailbox
        .receiver()
        .try_recv()
        .expect("receive response envelope");
    let response_frame = response_envelope
        .into_payload::<FrameContext>()
        .expect("receive response frame");
    assert_eq!(response_frame.msg_type.as_u16(), 202);
    assert_eq!(
        bad_request_reason(&response_frame),
        "invalid queue route: queue://acme/jobs"
    );
    assert!(sink.actors_are_empty_for_tests());
    assert!(admin_read_model.queues(None).is_empty());
}

#[test]
pub(super) fn should_reject_extend_given_malformed_queue_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let invalid_route = "queue://acme/jobs";
    let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let client_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(client_address.clone(), client_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    );

    // Act
    sink.deliver(Envelope::from_route(
        client_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(203),
            encode_queue_extend(invalid_route, 1, 99, 30),
            family,
        ),
    ))
    .expect("reject malformed extend");
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let response_envelope = client_mailbox
        .receiver()
        .try_recv()
        .expect("extend response envelope");
    let response_frame = response_envelope
        .into_payload::<FrameContext>()
        .expect("extend response frame");
    assert_eq!(response_frame.msg_type.as_u16(), 203);
    assert_eq!(
        bad_request_reason(&response_frame),
        "invalid queue route: queue://acme/jobs"
    );
    assert!(sink.actors_are_empty_for_tests());
    assert!(admin_read_model.queues(None).is_empty());
}

#[test]
pub(super) fn should_reject_ack_given_malformed_queue_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let invalid_route = "queue://acme/jobs";
    let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let client_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(client_address.clone(), client_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    );

    // Act
    sink.deliver(Envelope::from_route(
        client_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(204),
            encode_queue_ack(invalid_route, 1, 99),
            family,
        ),
    ))
    .expect("reject malformed ack");
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let response_envelope = client_mailbox
        .receiver()
        .try_recv()
        .expect("ack response envelope");
    let response_frame = response_envelope
        .into_payload::<FrameContext>()
        .expect("ack response frame");
    assert_eq!(response_frame.msg_type.as_u16(), 204);
    assert_eq!(
        bad_request_reason(&response_frame),
        "invalid queue route: queue://acme/jobs"
    );
    assert!(sink.actors_are_empty_for_tests());
    assert!(admin_read_model.queues(None).is_empty());
}

#[test]
pub(super) fn should_notify_queue_watch_given_queue_send_when_queue_transitions_to_ready() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let queue_sink = Arc::new(new_queue_domain_sink(
        store.clone(),
        router.clone(),
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    ));
    let family = RouteFamily::new(1);
    let receiver_addr = RouteAddress::new(family, Route::new("inbox://session/1"));
    let sender_addr = RouteAddress::new(family, Route::new("inbox://session/2"));
    let queue_inbound_addr = RouteAddress::new(family, Route::new("queue://inbound"));
    let receiver_mailbox = Arc::new(Mailbox::new(8));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    router.register(receiver_addr.clone(), receiver_mailbox.clone());
    router.register(sender_addr.clone(), sender_mailbox.clone());
    router.register_domain_pattern("queue", queue_sink as Arc<dyn MailboxSink>);
    let route = "queue://realm/area/resource";
    let watch_ctx = FrameContext::new(
        1,
        ChannelId::Pub,
        MessageType::new(207),
        encode_queue_watch("queue://realm/area/resource"),
        family,
    );
    let watch_env =
        Envelope::from_route(receiver_addr.clone(), queue_inbound_addr.clone(), watch_ctx);
    let body: &[u8] = b"x";
    let mut send_payload = Vec::new();
    send_payload.extend_from_slice(&len_to_u32(route.len()).to_be_bytes());
    send_payload.extend_from_slice(route.as_bytes());
    send_payload.extend_from_slice(&len_to_u32(body.len()).to_be_bytes());
    send_payload.extend_from_slice(body);
    let send_ctx = FrameContext::new(
        2,
        ChannelId::Pub,
        MessageType::new(200),
        Bytes::from(send_payload),
        family,
    );
    let send_env = Envelope::from_route(sender_addr, queue_inbound_addr, send_ctx);

    // Act
    router.route(watch_env).expect("route queue watch");
    router.route(send_env).expect("route send");

    // Assert
    let watch_ack = receiver_mailbox
        .receiver()
        .try_recv()
        .expect("watch ack envelope")
        .into_payload::<FrameContext>()
        .expect("watch ack frame");
    assert_eq!(watch_ack.msg_type.as_u16(), 207);
    let subscription_id = watch_response_subscription_id(&watch_ack);

    let send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("send ack envelope")
        .into_payload::<FrameContext>()
        .expect("send ack frame");
    assert_eq!(send_ack.msg_type.as_u16(), 200);

    let notify_frame = receiver_mailbox
        .receiver()
        .try_recv()
        .expect("queue watch notify envelope")
        .into_payload::<FrameContext>()
        .expect("queue watch notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 209);
    let (delivered_subscription_id, delivered_route, ready_messages) =
        decode_queue_watch_delivery(&notify_frame);
    assert_eq!(delivered_subscription_id, subscription_id);
    assert_eq!(delivered_route, "queue://realm/area/resource");
    assert_eq!(ready_messages, 1);
    assert!(receiver_mailbox.receiver().try_recv().is_err());
}
