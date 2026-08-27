use super::*;

fn new_correctness_sink(router: Arc<Router>) -> KvDomainSink {
    KvDomainSink::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

#[test]
fn should_treat_a_response_to_a_closed_session_as_an_expected_drop() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("kv://inbound"));
    let sink = new_correctness_sink(Arc::new(Router::new()));
    let request = Envelope::from_route(
        source,
        destination,
        FrameContext::new(
            7,
            ChannelId::Sub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            Bytes::new(),
            family,
        ),
    );
    let meta = crate::runtime::ClientFrameMeta::new(
        7,
        crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::kv::msg_type::BEGIN,
        family,
    );

    // Act
    let result = sink.state.runtime().route_kv_response(
        &request,
        meta,
        &crate::domains::kv::KvResponse::Error {
            error: crate::domains::kv::KvError::BackendError("late response".to_string()),
        },
        Instant::now(),
    );

    // Assert
    assert_eq!(result, Ok(()));
}

#[test]
fn should_preserve_response_mailbox_backpressure_for_an_active_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("kv://inbound"));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .deliver(Envelope::new(destination.clone(), Bytes::new()))
        .expect("fill response mailbox");
    let sink = new_correctness_sink(router);
    let request = Envelope::from_route(source, destination, Bytes::new());
    let meta = crate::runtime::ClientFrameMeta::new(
        7,
        crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::kv::msg_type::BEGIN,
        family,
    );

    // Act
    let result = sink.state.runtime().route_kv_response(
        &request,
        meta,
        &crate::domains::kv::KvResponse::Error {
            error: crate::domains::kv::KvError::BackendError("busy".to_string()),
        },
        Instant::now(),
    );

    // Assert
    assert_eq!(
        result,
        Err(crate::runtime::DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        })
    );
}

#[test]
fn should_roll_back_read_write_begin_when_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let route = "kv://acme/app/users";
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new(route));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .deliver(Envelope::new(destination.clone(), Bytes::new()))
        .expect("fill response mailbox");
    let sink = new_correctness_sink(router);

    // Act
    let request = Envelope::from_route(source, destination, Bytes::new());
    let meta = crate::runtime::ClientFrameMeta::new(
        session_id,
        crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::kv::msg_type::BEGIN,
        family,
    );
    let result = sink.state.runtime().handle_actor_operation_frame(
        &request,
        meta,
        Instant::now(),
        Instant::now(),
        crate::domains::kv::KvMessage::Begin {
            scope: KvResourceScope::new(family, "acme", "app", "users"),
            mode: crate::domains::kv::TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        },
    );

    // Assert
    assert!(matches!(
        result,
        Err(crate::runtime::DeliveryError::MailboxFull { .. })
    ));
    assert!(sink.resource_locks_are_empty_for_tests());
    assert_eq!(sink.active_transaction_count(), 0);
}

#[test]
fn should_update_kv_admin_transaction_incrementally_given_lifecycle() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(source_address.clone(), mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        source_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    // Act
    let after_begin = admin_read_model.kv_transactions(None);
    sink.deliver(Envelope::from_route(
        source_address,
        kv_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&mailbox, "commit ack envelope");
    let after_commit = admin_read_model.kv_transactions(None);

    // Assert
    assert_eq!(after_begin.len(), 1);
    assert_eq!(after_begin[0].tx_id, tx_id);
    assert_eq!(after_begin[0].resource, "users");
    assert!(after_commit.is_empty());
}

#[test]
fn should_read_admin_transaction_count_while_session_actor_is_busy() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let route = "kv://acme/app/users";
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new(route));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_sink(router);
    sink.deliver(Envelope::from_route(
        source,
        destination,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(route, 1, 0),
            family,
        ),
    ))
    .expect("begin transaction");
    let _ = receive_frame(&mailbox, "begin response");
    let actor = sink
        .state
        .core
        .actors
        .lock()
        .get(&session_id)
        .expect("session actor")
        .clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = actor.lock();
        locked_tx.send(()).expect("report held actor lock");
        release_rx.recv().expect("wait to release actor lock");
    });
    locked_rx.recv().expect("wait for held actor lock");

    // Act
    let (count_tx, count_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        count_tx
            .send(sink.active_transaction_count())
            .expect("report admin transaction count");
    });
    let count = count_rx.recv_timeout(Duration::from_millis(200));
    release_tx.send(()).expect("release actor lock");
    holder.join().expect("actor lock holder");
    reader.join().expect("admin transaction reader");

    // Assert
    assert_eq!(count.expect("admin read must not wait for actor lock"), 1);
}

#[test]
fn should_expire_idle_read_write_transaction_before_competing_begin() {
    // Arrange
    let family = RouteFamily::new(1);
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let first_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let second_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let first_mailbox = Arc::new(Mailbox::new(8));
    let second_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(first_address.clone(), first_mailbox.clone());
    router.register(second_address.clone(), second_mailbox.clone());
    let sink = KvDomainSink::new(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
    .with_idle_transaction_ttl(Duration::from_millis(1));
    sink.deliver(Envelope::from_route(
        first_address,
        kv_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin first KV transaction");
    let _ = receive_frame(&first_mailbox, "first begin response");
    std::thread::sleep(Duration::from_millis(5));

    // Act
    sink.deliver(Envelope::from_route(
        second_address,
        kv_address,
        FrameContext::new(
            8,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin competing KV transaction");

    // Assert
    let response = receive_frame(&second_mailbox, "competing begin response");
    assert_eq!(response.payload[0], 0);
    assert_eq!(sink.active_transaction_count(), 1);
}

#[test]
fn should_begin_write_without_scanning_unrelated_session_actors() {
    // Arrange
    let family = RouteFamily::new(1);
    let first_route = "kv://acme/app/users";
    let second_route = "kv://acme/app/orders";
    let first_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let second_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let first_mailbox = Arc::new(Mailbox::new(8));
    let second_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(first_address.clone(), first_mailbox.clone());
    router.register(second_address.clone(), second_mailbox.clone());
    let sink = new_correctness_sink(router);
    sink.deliver(Envelope::from_route(
        first_address,
        RouteAddress::new(family, Route::new(first_route)),
        FrameContext::new(
            7,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(first_route, 1, 0),
            family,
        ),
    ))
    .expect("begin unrelated transaction");
    let _ = receive_frame(&first_mailbox, "unrelated begin response");
    let actor = sink
        .state
        .core
        .actors
        .lock()
        .get(&7)
        .expect("unrelated actor")
        .clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = actor.lock();
        locked_tx.send(()).expect("report actor lock");
        release_rx.recv().expect("wait to release actor lock");
    });
    locked_rx.recv().expect("wait for actor lock");

    // Act
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        sink.deliver(Envelope::from_route(
            second_address,
            RouteAddress::new(family, Route::new(second_route)),
            FrameContext::new(
                8,
                ChannelId::Sub,
                MessageType::new(100),
                encode_kv_begin(second_route, 1, 0),
                family,
            ),
        ))
        .expect("deliver independent begin");
        let status = receive_frame(&second_mailbox, "independent begin response").payload[0];
        result_tx.send(status).expect("report competing begin");
    });
    let result = result_rx.recv_timeout(Duration::from_millis(200));
    release_tx.send(()).expect("release unrelated actor");
    holder.join().expect("actor lock holder");
    worker.join().expect("begin worker");

    // Assert
    assert_eq!(result.expect("begin must not scan unrelated actors"), 0);
}

#[test]
fn should_reject_invalid_admin_inventory_route_family_without_panicking() {
    // Arrange
    let sink = new_correctness_sink(Arc::new(Router::new()));

    // Act
    let result = sink.admin_inventory_for_family_for_tests(u64::MAX);

    // Assert
    assert_eq!(
        result.expect_err("invalid family must fail"),
        "invalid route family ID: 18446744073709551615"
    );
}

#[test]
fn should_reject_kv_request_when_source_and_destination_families_differ() {
    // Arrange
    let source_family = RouteFamily::new(2);
    let destination_family = RouteFamily::new(1);
    let source = RouteAddress::new(source_family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(destination_family, Route::new("kv://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_sink(router);
    let request = crate::domains::kv::KvClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            crate::runtime::ClientChannel::Pub,
            crate::dispatch::protocol::kv::msg_type::BEGIN,
            destination_family,
        ),
        Ok(crate::domains::kv::KvClientFrame::Op(
            crate::domains::kv::KvMessage::Begin {
                scope: KvResourceScope::new(
                    destination_family,
                    "acme".to_string(),
                    "app".to_string(),
                    "users".to_string(),
                ),
                mode: crate::domains::kv::TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::best_effort(),
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver mismatched KV request");
    let response = receive_frame(&mailbox, "KV family mismatch response");

    // Assert
    assert_eq!(response.route_family, source_family);
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_INVALID_ROUTE
    );
    assert!(sink.actors_are_empty_for_tests());
}

#[test]
fn should_reject_kv_operation_when_decoded_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let other_family = RouteFamily::new(2);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("kv://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_sink(router);
    let request = crate::domains::kv::KvClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            crate::runtime::ClientChannel::Pub,
            crate::dispatch::protocol::kv::msg_type::BEGIN,
            family,
        ),
        Ok(crate::domains::kv::KvClientFrame::Op(
            crate::domains::kv::KvMessage::Begin {
                scope: KvResourceScope::new(
                    other_family,
                    "acme".to_string(),
                    "app".to_string(),
                    "users".to_string(),
                ),
                mode: crate::domains::kv::TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::best_effort(),
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver decoded-family mismatch");
    let response = receive_frame(&mailbox, "KV decoded family mismatch response");

    // Assert
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_INVALID_ROUTE
    );
    assert!(sink.actors_are_empty_for_tests());
}

#[test]
fn should_reject_kv_subscription_before_allocating_family_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("kv://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_sink(router);
    let request = crate::domains::kv::KvClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            crate::runtime::ClientChannel::Sub,
            crate::dispatch::protocol::kv::msg_type::SUBSCRIBE,
            family,
        ),
        Ok(crate::domains::kv::KvClientFrame::Sub(
            crate::domains::kv::KvSubscriptionMessage::Subscribe {
                family_id: family,
                pattern: Route::new(""),
                session_id: 7,
                subscriber: source.clone(),
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver empty KV subscription");
    let response = receive_frame(&mailbox, "KV empty subscription response");

    // Assert
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_INVALID_SUBSCRIPTION_PATTERN
    );
    assert!(sink.watch_registries_are_empty_for_tests());
}
