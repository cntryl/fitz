use super::*;

fn new_correctness_sink(router: Arc<Router>) -> KvDomainSink {
    KvDomainSink::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
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
    assert!(sink.watch_actors_are_empty_for_tests());
}
