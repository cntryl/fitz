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

fn lease_acquire_request(ttl_secs: u64, wait_seconds: u32) -> LeaseAcquireRequest {
    let family = RouteFamily::new(1);
    LeaseAcquireRequest {
        key: lease_key(family, "lease://acme/locks/resource"),
        owner_session_id: 7,
        owner_id: "owner".to_string(),
        ttl_secs,
        wait_seconds,
        reply_source: RouteAddress::new(family, Route::new("internal://domain/lease")),
        reply_destination: None,
        channel: ClientChannel::Lease,
        route_family: family,
    }
}

#[test]
fn should_reject_oversized_ttl_without_stopping_actor() {
    // Arrange
    let sink = new_correctness_lease_sink(Arc::new(Router::new()));

    // Act
    let acquire_response = sink.acquire_for_tests(lease_acquire_request(u64::MAX, 0));
    let acquired = sink.acquire_for_tests(lease_acquire_request(30, 0));
    let LeaseResponse::Acquired { fencing_token } = acquired else {
        panic!("expected acquired response");
    };
    let key = lease_key(RouteFamily::new(1), "lease://acme/locks/resource");
    let extend_response = sink.extend_for_tests(&key, "owner", fencing_token, u64::MAX);

    // Assert
    assert!(matches!(acquire_response, LeaseResponse::Error(_)));
    assert!(matches!(extend_response, LeaseResponse::Error(_)));
    assert!(sink.is_actor_running());
    assert_eq!(sink.lease_count(), 1);
}

#[test]
fn should_reject_zero_ttl_for_acquire_plus_extend() {
    // Arrange
    let sink = new_correctness_lease_sink(Arc::new(Router::new()));
    let acquired = sink.acquire_for_tests(lease_acquire_request(30, 0));
    let LeaseResponse::Acquired { fencing_token } = acquired else {
        panic!("expected acquired response");
    };
    let key = lease_key(RouteFamily::new(1), "lease://acme/locks/resource");

    // Act
    let acquire_response = sink.acquire_for_tests(lease_acquire_request(0, 0));
    let extend_response = sink.extend_for_tests(&key, "owner", fencing_token, 0);

    // Assert
    assert!(matches!(acquire_response, LeaseResponse::Error(_)));
    assert!(matches!(extend_response, LeaseResponse::Error(_)));
    assert!(sink.is_actor_running());
}

#[test]
fn should_reject_wait_above_server_cap_as_invalid() {
    // Arrange
    let sink = new_correctness_lease_sink(Arc::new(Router::new()));

    // Act
    let response = sink.acquire_for_tests(lease_acquire_request(30, LEASE_MAX_WAIT_SECONDS + 1));

    // Assert
    assert!(matches!(response, LeaseResponse::Error(_)));
    assert_ne!(response, LeaseResponse::Timeout);
}

#[test]
fn should_count_sweep_enqueue_failure() {
    // Arrange
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let sink = new_correctness_lease_sink(Arc::new(Router::new())).with_metrics(metrics.clone());
    sink.stop_actor_for_tests();

    // Act
    sink.sweep_expired_state();

    // Assert
    assert_eq!(
        metrics.counter_get(crate::domains::lease::metrics::METRIC_SWEEP_ENQUEUE_FAILURES_TOTAL),
        1
    );
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
fn should_not_retain_lease_subscription_when_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("lease://inbound"));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .deliver(Envelope::new(destination.clone(), Bytes::new()))
        .expect("fill response mailbox");
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
                route: Route::new("lease://acme/locks/resource"),
                session_id: 7,
                subscriber: source.clone(),
            },
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver lease subscription");
    std::thread::sleep(Duration::from_millis(50));
    let _filler = receive_envelope(&mailbox, "filled response mailbox");
    let query_source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let query_destination = RouteAddress::new(family, Route::new("lease://inbound"));
    let query = crate::domains::lease::LeaseClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            ClientChannel::Lease,
            crate::dispatch::protocol::lease_codec::msg_type::QUERY,
            family,
        ),
        Ok(crate::domains::lease::LeaseClientFrame::Op(
            crate::domains::lease::LeaseMessage::Query {
                family_id: family,
                route: Route::new("lease://acme/locks/resource"),
            },
        )),
    );
    sink.deliver(Envelope::from_route(query_source, query_destination, query))
        .expect("deliver ordering query");
    let _query_response = receive_envelope(&mailbox, "ordering query response");

    // Assert
    assert_eq!(sink.subscription_count(), 0);
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
