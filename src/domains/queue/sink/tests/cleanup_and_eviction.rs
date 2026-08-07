use super::*;

struct QueueRequestContext<'a> {
    sink: &'a QueueDomainSink,
    queue_address: RouteAddress,
    queue_route: &'a str,
    family: RouteFamily,
}

impl QueueRequestContext<'_> {
    fn deliver_send(&self, sender_address: RouteAddress, session_id: u64) {
        self.sink
            .deliver(Envelope::from_route(
                sender_address,
                self.queue_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(200),
                    encode_queue_send(self.queue_route, b"email"),
                    self.family,
                ),
            ))
            .expect("enqueue queue message");
    }

    fn deliver_reserve(&self, worker_address: RouteAddress, session_id: u64) {
        self.sink
            .deliver(Envelope::from_route(
                worker_address,
                self.queue_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(202),
                    encode_queue_reserve(self.queue_route, 30, 1),
                    self.family,
                ),
            ))
            .expect("reserve queue message");
    }

    fn deliver_extend(
        &self,
        address: RouteAddress,
        session_id: u64,
        id: u64,
        token: u64,
        label: &str,
    ) {
        self.sink
            .deliver(Envelope::from_route(
                address,
                self.queue_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(203),
                    encode_queue_extend(self.queue_route, id, token, 60),
                    self.family,
                ),
            ))
            .expect(label);
    }

    fn deliver_ack(
        &self,
        address: RouteAddress,
        session_id: u64,
        id: u64,
        token: u64,
        label: &str,
    ) {
        self.sink
            .deliver(Envelope::from_route(
                address,
                self.queue_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(204),
                    encode_queue_ack(self.queue_route, id, token),
                    self.family,
                ),
            ))
            .expect(label);
    }
}

fn assert_not_found(frame: &FrameContext) {
    let mut decoder =
        crate::dispatch::protocol::payload_codec::PayloadDecoder::new(frame.payload.as_ref());
    assert_eq!(decoder.get_u8().expect("error status"), 1);
    assert_eq!(decoder.get_string().expect("plain queue error"), "NotFound");
}

fn assert_success(frame: &FrameContext) {
    assert_eq!(frame.payload[0], 0);
}

fn deliver_send(
    sink: &QueueDomainSink,
    sender_address: RouteAddress,
    queue_address: RouteAddress,
    session_id: u64,
    queue_route: &str,
    family: RouteFamily,
) {
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address,
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    ))
    .expect("enqueue queue message");
}

fn deliver_reserve(
    sink: &QueueDomainSink,
    worker_address: RouteAddress,
    queue_address: RouteAddress,
    session_id: u64,
    queue_route: &str,
    family: RouteFamily,
) {
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("reserve queue message");
}

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
    deliver_send(
        &sink,
        sender_address,
        queue_address.clone(),
        sender_session_id,
        queue_route,
        family,
    );
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

    deliver_reserve(
        &sink,
        worker_address,
        queue_address.clone(),
        worker_session_id,
        queue_route,
        family,
    );
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
    let request_ctx = QueueRequestContext {
        sink: &sink,
        queue_address: queue_address.clone(),
        queue_route,
        family,
    };

    // Act
    request_ctx.deliver_send(sender_address, sender_session_id);
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

    request_ctx.deliver_reserve(worker_address.clone(), worker_session_id);
    let reserve_frame = receive_queue_frame(&worker_mailbox, "reserve response");
    let (id, token) = receive_response_first_message(&reserve_frame);

    request_ctx.deliver_extend(
        other_address.clone(),
        other_session_id,
        id,
        token,
        "non-owner extend",
    );
    let other_extend = receive_queue_frame(&other_mailbox, "non-owner extend response");

    request_ctx.deliver_extend(
        worker_address.clone(),
        worker_session_id,
        id,
        token,
        "owner extend",
    );
    let owner_extend = receive_queue_frame(&worker_mailbox, "owner extend response");

    request_ctx.deliver_ack(
        other_address.clone(),
        other_session_id,
        id,
        token,
        "non-owner complete",
    );
    let other_complete = receive_queue_frame(&other_mailbox, "non-owner complete response");

    request_ctx.deliver_ack(
        worker_address.clone(),
        worker_session_id,
        id,
        token,
        "owner complete",
    );
    let owner_complete = receive_queue_frame(&worker_mailbox, "owner complete response");

    request_ctx.deliver_ack(
        other_address,
        other_session_id,
        id,
        token,
        "non-owner complete after owner cached success",
    );
    let other_complete_after_owner =
        receive_queue_frame(&other_mailbox, "non-owner cached complete response");

    request_ctx.deliver_ack(
        worker_address,
        worker_session_id,
        id,
        token,
        "owner complete retry",
    );
    let owner_complete_retry = receive_queue_frame(&worker_mailbox, "owner complete retry");

    // Assert
    assert_not_found(&other_extend);
    assert_success(&owner_extend);
    assert_not_found(&other_complete);
    assert_success(&owner_complete);
    assert_not_found(&other_complete_after_owner);
    assert_success(&owner_complete_retry);
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
    assert_eq!(sink.actor_count_for_tests(), 1);

    // Act
    force_actor_idle(&sink, queue_route, family);
    sink.refresh_admin_snapshot_if_dirty();
    assert!(
        sink.actors_are_empty_for_tests(),
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
    assert_eq!(sink.actor_count_for_tests(), 1);
    assert_eq!(admin_read_model.queues(None)[0].messages_inflight, 1);
}

#[test]
fn should_prune_empty_queue_identity_when_actor_is_evicted() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/finished";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
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
    let request_context = QueueRequestContext {
        sink: &sink,
        queue_address,
        queue_route,
        family,
    };
    request_context.deliver_send(sender_address, 7);
    let _send = receive_queue_frame(&sender_mailbox, "send response");
    request_context.deliver_reserve(worker_address.clone(), 8);
    let reserve = receive_queue_frame(&worker_mailbox, "reserve response");
    let (id, token) = receive_response_first_message(&reserve);
    request_context.deliver_ack(worker_address, 8, id, token, "complete queue message");
    let complete = receive_queue_frame(&worker_mailbox, "complete response");
    assert_success(&complete);
    assert_eq!(sink.known_queue_count_for_tests(), 1);

    // Act
    force_actor_idle(&sink, queue_route, family);
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert!(sink.actors_are_empty_for_tests());
    assert_eq!(sink.known_queue_count_for_tests(), 0);
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
        sink.actor_count_for_tests(),
        1,
        "actors with live inflight entries must stay warm until the inflight entry is gone"
    );
}
