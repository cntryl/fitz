use super::*;
pub(super) use crate::protocol::frame::ChannelId;
pub(super) use crate::protocol::tlv::MessageType;
pub(super) use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
pub(super) use crate::runtime::Mailbox;
pub(super) use bytes::{BufMut, Bytes};

fn len_to_u32(len: usize) -> u32 {
    u32::try_from(len).expect("payload length should fit in u32")
}

pub(super) fn encode_route_pattern(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

pub(super) fn encode_queue_send(route: &str, body: &[u8]) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(len_to_u32(body.len()));
    payload.put_slice(body);
    Bytes::from(payload)
}

pub(super) fn encode_queue_send_with_delay(route: &str, body: &[u8], delay_seconds: u64) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(len_to_u32(body.len()));
    payload.put_slice(body);
    payload.put_u8(1);
    payload.put_u64(delay_seconds);
    Bytes::from(payload)
}

pub(super) fn encode_queue_reserve(route: &str, inflight_seconds: u64, batch_size: u32) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(inflight_seconds);
    payload.put_u8(1);
    payload.put_u32(batch_size);
    Bytes::from(payload)
}

pub(super) fn encode_queue_watch(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

pub(super) fn encode_queue_unwatch(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

pub(super) fn encode_queue_extend(
    route: &str,
    id: u64,
    token: u64,
    inflight_seconds: u64,
) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(id);
    payload.put_u64(token);
    payload.put_u64(inflight_seconds);
    Bytes::from(payload)
}

pub(super) fn encode_queue_ack(route: &str, id: u64, token: u64) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(len_to_u32(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u64(id);
    payload.put_u64(token);
    Bytes::from(payload)
}

pub(super) fn bad_request_reason(frame: &FrameContext) -> String {
    let (code, message) = crate::protocol::error_codes::decode_error_body(frame.payload.as_ref())
        .expect("bad request error envelope");
    assert_eq!(code, crate::protocol::error_codes::queue::ERR_BAD_REQUEST);
    message
}

pub(super) fn new_queue_domain_sink(
    store: Arc<cntryl_midge::Engine>,
    router: Arc<Router>,
    admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    queue_write_options: cntryl_midge::WriteOptions,
) -> QueueDomainSink {
    QueueDomainSink::new(
        store,
        router,
        admin_read_model,
        queue_write_options,
        crate::utils::idempotency::default_dedup_store(),
    )
}

pub(super) fn receive_response_message_count(frame: &FrameContext) -> u32 {
    assert_eq!(frame.payload[0], 0, "expected success status");
    u32::from_be_bytes(
        frame.payload[1..5]
            .try_into()
            .expect("receive payload should include count"),
    )
}

pub(super) fn receive_response_first_message(frame: &FrameContext) -> (u64, u64) {
    assert_eq!(receive_response_message_count(frame), 1);
    let id = u64::from_be_bytes(
        frame.payload[5..13]
            .try_into()
            .expect("receive payload should include message id"),
    );
    let token = u64::from_be_bytes(
        frame.payload[13..21]
            .try_into()
            .expect("receive payload should include token"),
    );
    (id, token)
}

pub(super) fn queue_simple_error_code(frame: &FrameContext) -> u16 {
    let (code, _) = crate::protocol::error_codes::decode_error_body(frame.payload.as_ref())
        .expect("queue error envelope");
    code
}

pub(super) fn receive_queue_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
    mailbox
        .receiver()
        .try_recv()
        .expect(label)
        .into_payload::<FrameContext>()
        .expect("queue response frame")
}

pub(super) fn watch_response_subscription_id(frame: &FrameContext) -> u64 {
    assert_eq!(frame.payload[0], 0, "expected success status");
    u64::from_be_bytes(
        frame.payload[1..9]
            .try_into()
            .expect("watch payload should include subscription id"),
    )
}

pub(super) fn decode_queue_watch_delivery(frame: &FrameContext) -> (u64, String, u64) {
    let subscription_id = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
    let route_len = usize::try_from(u32::from_be_bytes(frame.payload[8..12].try_into().unwrap()))
        .expect("route length should fit in usize");
    let route = String::from_utf8(frame.payload[12..12 + route_len].to_vec())
        .expect("queue watch route should be utf-8");
    let offset = 12 + route_len;
    let ready_messages = u64::from_be_bytes(frame.payload[offset..offset + 8].try_into().unwrap());
    (subscription_id, route, ready_messages)
}

pub(super) fn force_actor_idle(sink: &QueueDomainSink, queue_route: &str, family: RouteFamily) {
    let key = crate::domains::queue::QueueKey::from_route(family, &Route::new(queue_route))
        .expect("queue key");
    let mut actors = sink.actors.lock();
    let warm_actor = actors.get_mut(&key).expect("warm queue actor");
    warm_actor.last_used = Instant::now()
        .checked_sub(QUEUE_ACTOR_IDLE_TTL + Duration::from_secs(1))
        .expect("idle deadline should remain representable");
}

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
    assert!(sink.active.load(Ordering::Relaxed));
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
    assert!(sink.dirty_fast_flush_families.lock().contains(&1));
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
    assert!(sink.dirty_fast_flush_families.lock().contains(&1));

    // Act
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(sink.dirty_fast_flush_families.lock().is_empty());
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
    sink.dirty_fast_flush_families.lock().insert(99);

    // Act
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(sink.dirty_fast_flush_families.lock().contains(&99));
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
    assert!(queue_sink.actors.lock().is_empty());
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
    assert!(sink.actors.lock().is_empty());
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
    assert!(sink.actors.lock().is_empty());
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
    assert!(sink.actors.lock().is_empty());
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
        encode_queue_watch("queue://realm/area/resource/ready"),
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
    assert_eq!(delivered_route, "queue://realm/area/resource/ready");
    assert_eq!(ready_messages, 1);
    assert!(receiver_mailbox.receiver().try_recv().is_err());
}

#[test]
pub(super) fn should_register_queue_watch_given_watch_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_session_id = 7;
    let queue_route = "queue://acme/jobs/emails/ready";
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
    let queue_route = "queue://acme/jobs/emails/ready";
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
    assert!(sink.families.lock().is_empty());
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
    *sink.next_dedup_sweep_at.lock() = now;

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
            encode_queue_watch("queue://acme/jobs/emails/ready"),
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
