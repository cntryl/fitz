use super::*;

fn new_correctness_sink(router: Arc<Router>) -> KvDomainSink {
    KvDomainSink::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
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
                route_family: destination_family,
                realm: "acme".to_string(),
                area: "app".to_string(),
                resource: "users".to_string(),
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
                route_family: other_family,
                realm: "acme".to_string(),
                area: "app".to_string(),
                resource: "users".to_string(),
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
        error_codes::kv::ERR_INVALID_ROUTE
    );
    assert!(sink.watch_actors_are_empty_for_tests());
}
