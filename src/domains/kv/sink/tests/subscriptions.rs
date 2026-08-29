use super::*;

#[test]
fn should_not_retain_kv_subscription_when_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let route = Route::new("kv://acme/app/undeliverable");
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, route.clone());
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .deliver(Envelope::new(destination.clone(), Bytes::new()))
        .expect("fill response mailbox");
    let sink = KvDomainSink::new(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let request = Envelope::from_route(source.clone(), destination, Bytes::new());
    let meta = crate::runtime::ClientFrameMeta::new(
        session_id,
        crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::kv::msg_type::SUBSCRIBE,
        family,
    );

    // Act
    let result = sink.state.runtime().handle_subscription_frame(
        &request,
        meta,
        Instant::now(),
        crate::domains::kv::KvSubscriptionMessage::Subscribe {
            family_id: family,
            pattern: route,
            session_id,
            subscriber: source,
        },
    );

    // Assert
    assert!(matches!(
        result,
        Err(crate::runtime::DeliveryError::MailboxFull { .. })
    ));
    assert!(sink.watch_registries_are_empty_for_tests());
}

#[test]
fn should_notify_kv_subscriber_given_committed_put() {
    // Arrange
    let family = RouteFamily::new(1);
    let watch_session_id = 7;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_mailbox = Arc::new(Mailbox::new(16));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    // Act
    sink.deliver(Envelope::from_route(
        watcher_address,
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe to KV route");
    let subscribe_frame = receive_frame(&watcher_mailbox, "subscribe ack envelope");
    let subscription_id = decode_kv_subscription_id(&subscribe_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&writer_mailbox, "put ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    let notify_frame = receive_frame(&watcher_mailbox, "KV notify envelope");
    assert_eq!(
        notify_frame.msg_type.as_u16(),
        crate::dispatch::protocol::kv::msg_type::NOTIFY
    );
    let (delivered_subscription_id, delivered_route, mutation_count) =
        decode_kv_watch_delivery(&notify_frame);
    assert_eq!(delivered_subscription_id, subscription_id);
    assert_eq!(delivered_route, kv_route);
    assert_eq!(mutation_count, 1);
}

#[test]
fn should_not_notify_kv_subscriber_given_empty_commit() {
    // Arrange
    let family = RouteFamily::new(1);
    let watch_session_id = 7;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_mailbox = Arc::new(Mailbox::new(16));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        watcher_address,
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe to KV route");
    let _ = receive_envelope(&watcher_mailbox, "subscribe ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit empty KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    assert_no_envelope(&watcher_mailbox);
}

#[test]
fn should_remove_kv_subscription_given_unsubscribe() {
    // Arrange
    let family = RouteFamily::new(1);
    let watch_session_id = 7;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_mailbox = Arc::new(Mailbox::new(16));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        watcher_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe to KV route");
    let _ = receive_envelope(&watcher_mailbox, "subscribe ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        watcher_address,
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::UNSUBSCRIBE),
            encode_kv_unsubscribe(kv_route),
            family,
        ),
    ))
    .expect("unsubscribe from KV route");
    let _ = receive_envelope(&watcher_mailbox, "unsubscribe ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&writer_mailbox, "put ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    assert_no_envelope(&watcher_mailbox);
    assert!(sink.watch_registries_are_empty_for_tests());
}
