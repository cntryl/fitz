use super::*;

fn lease_error_code(mailbox: &Mailbox, label: &str) -> u16 {
    let frame = receive_envelope(mailbox, label)
        .into_payload::<FrameContext>()
        .expect("lease response frame");
    crate::dispatch::protocol::error_codes::decode_error_body(&frame.payload)
        .expect("lease error body")
        .0
}

fn new_correctness_lease_sink(router: Arc<Router>) -> LeaseDomainSink {
    LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

#[test]
fn should_reject_lease_request_when_source_and_destination_families_differ() {
    // Arrange
    let source_family = RouteFamily::new(2);
    let destination_family = RouteFamily::new(1);
    let source = RouteAddress::new(source_family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(destination_family, Route::new("lease://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_lease_sink(router);
    let request = crate::domains::lease::LeaseClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            ClientChannel::Lease,
            crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
            destination_family,
        ),
        Ok(crate::domains::lease::LeaseClientFrame::Op(
            crate::domains::lease::LeaseMessage::Acquire {
                family_id: destination_family,
                route: Route::new("lease://acme/locks/resource"),
                owner_id: String::new(),
                ttl_secs: 30,
                wait_seconds: 0,
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver mismatched lease request");
    let code = lease_error_code(&mailbox, "lease family mismatch response");

    // Assert
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::lease::ERR_BAD_REQUEST
    );
    assert_eq!(sink.lease_count(), 0);
}

#[test]
fn should_reject_lease_subscription_before_allocating_family_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("lease://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_lease_sink(router);
    let request = crate::domains::lease::LeaseClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            ClientChannel::Sub,
            crate::dispatch::protocol::lease_codec::msg_type::SUBSCRIBE,
            family,
        ),
        Ok(crate::domains::lease::LeaseClientFrame::Sub(
            crate::domains::lease::LeaseSubscriptionMessage::Subscribe {
                family_id: family,
                route: Route::new(""),
                session_id: 7,
                subscriber: source.clone(),
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver empty lease subscription");
    let code = lease_error_code(&mailbox, "lease empty subscription response");

    // Assert
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::lease::ERR_INVALID_SUBSCRIPTION_ROUTE
    );
    assert!(sink.watch_families_are_empty_for_tests());
}

#[test]
fn should_reject_lease_operation_when_decoded_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let other_family = RouteFamily::new(2);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("lease://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_lease_sink(router);
    let request = crate::domains::lease::LeaseClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            ClientChannel::Lease,
            crate::dispatch::protocol::lease_codec::msg_type::QUERY,
            family,
        ),
        Ok(crate::domains::lease::LeaseClientFrame::Op(
            crate::domains::lease::LeaseMessage::Query {
                family_id: other_family,
                route: Route::new("lease://acme/locks/resource"),
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver decoded-family mismatch");
    let code = lease_error_code(&mailbox, "lease decoded family mismatch response");

    // Assert
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::lease::ERR_BAD_REQUEST
    );
    assert_eq!(sink.lease_count(), 0);
}
