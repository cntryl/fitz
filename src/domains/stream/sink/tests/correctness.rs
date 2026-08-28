use super::*;
use crate::benchkit::{
    build_stream_append, build_stream_begin, build_stream_subscribe, extract_single_tlv_field,
    route_frame_to_address, FrameQueueSink,
};
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::Mailbox;
use bytes::Bytes;

fn request_from_session_to_address(
    context: &TestContext,
    source: &RouteAddress,
    inbox: &FrameQueueSink,
    session_id: u64,
    destination: &RouteAddress,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame_to_address(
        context.router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
    )
    .expect("stream route");

    let responses = inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

#[test]
fn should_fail_closed_after_stream_actor_panic() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = StreamDomainSink::new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    );
    sink.panic_actor_for_failpoint();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

    // Act
    while sink.actor_health_snapshot().running && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let destination = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://bench/events/orders"),
    );
    let result = sink.deliver(Envelope::new(destination, 42_u64));
    let health = sink.actor_health_snapshot();

    // Assert
    assert!(!health.running);
    assert_eq!(health.panic_count, 1);
    assert!(health.restart_exhausted);
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
}

#[test]
fn should_not_retain_subscription_when_subscribe_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "stream://bench/events/undeliverable";
    let source = RouteAddress::new(family, Route::new("inbox://session/1"));
    let destination = RouteAddress::new(family, Route::new(route));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(source.clone(), 1_u8))
        .expect("fill subscriber mailbox");
    let sink = StreamDomainSink::new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    );
    let frame = build_stream_subscribe(route);
    let (message_type, payload) = extract_single_tlv_field(&frame);

    // Act
    sink.deliver(Envelope::from_route(
        source,
        destination,
        FrameContext::new(
            1,
            ChannelId::Pub,
            crate::dispatch::protocol::tlv::MessageType::new(message_type),
            payload,
            family,
        ),
    ))
    .expect("deliver stream subscription");

    // Assert
    assert_eq!(sink.subscription_count(), 0);
}

#[test]
fn should_not_retain_append_session_when_begin_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "stream://bench/events/undeliverable-begin";
    let source = RouteAddress::new(family, Route::new("inbox://session/1"));
    let destination = RouteAddress::new(family, Route::new(route));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(source.clone(), 1_u8))
        .expect("fill response mailbox");
    let sink = StreamDomainSink::new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    );
    let frame = build_stream_begin(route);
    let (message_type, payload) = extract_single_tlv_field(&frame);

    // Act
    sink.deliver(Envelope::from_route(
        source,
        destination,
        FrameContext::new(
            1,
            ChannelId::Pub,
            crate::dispatch::protocol::tlv::MessageType::new(message_type),
            payload,
            family,
        ),
    ))
    .expect("deliver stream begin");

    // Assert
    assert_eq!(sink.append_session_count(), 0);
}

#[test]
fn should_fail_closed_given_malformed_stream_commit_notification() {
    // Arrange
    let context = setup_test_context();
    let pattern = "stream://bench/events/*";
    let (subscriber, inbox) = register_session_queue_sink(&context.router, context.family, 2);
    let subscribe = build_stream_subscribe(pattern);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe);
    let _response =
        request_from_session(&context, &subscriber, &inbox, 2, pattern, msg_type, payload);
    inbox.clear();
    let malformed = crate::runtime::DomainPublishEvent::new(
        context.family,
        Route::new("stream://bench/events/orders"),
        Bytes::from_static(b"not-json"),
    );

    // Act
    context.sink.core.handle_domain_publish(&malformed);

    // Assert
    assert_eq!(inbox.count(), 0);
    assert!(context.sink.core.subscriptions.pending.lock().is_empty());
}

#[test]
fn should_reject_stream_request_when_source_and_destination_families_differ() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/family-boundary";
    let stream_session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(stream_session_id, 0, b"blocked");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);

    // Act
    let mismatched_destination = RouteAddress::new(RouteFamily::new(2), Route::new(route));
    let mismatched_response = request_from_session_to_address(
        &context,
        &context.source,
        &context.inbox,
        TEST_CLIENT_SESSION_ID,
        &mismatched_destination,
        append_msg_type,
        append_payload,
    );
    let owner_append_frame = build_stream_append(stream_session_id, 0, b"allowed");
    let (owner_append_type, owner_append_payload) = extract_single_tlv_field(&owner_append_frame);
    let owner_append_response = request(&context, route, owner_append_type, owner_append_payload);

    // Assert
    assert_eq!(
        decode_stream_error_message(mismatched_response.as_ref()).expect("mismatched family error"),
        "route family mismatch"
    );
    assert!(decode_stream_error_message(owner_append_response.as_ref()).is_err());
}

#[test]
fn should_reject_stream_subscription_given_empty_pattern() {
    // Arrange
    let context = setup_test_context();
    let subscribe_frame = build_stream_subscribe("");
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);

    // Act
    let response = request(
        &context,
        "stream://bench/events/orders",
        subscribe_msg_type,
        subscribe_payload,
    );

    // Assert
    assert_eq!(
        decode_stream_error_message(response.as_ref()).expect("empty pattern error"),
        "subscription pattern must use stream://"
    );
    assert_eq!(context.sink.subscription_count(), 0);
}

#[test]
fn should_reject_stream_begin_given_wildcard_resource_route() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/*";
    let begin_frame = build_stream_begin(route);
    let (begin_msg_type, begin_payload) = extract_single_tlv_field(&begin_frame);

    // Act
    let response = request(&context, route, begin_msg_type, begin_payload);

    // Assert
    assert_eq!(
        decode_stream_error_message(response.as_ref()).expect("wildcard begin error"),
        "stream append routes require concrete realm/area/resource"
    );
    assert_eq!(context.sink.append_session_count(), 0);
}

#[test]
fn should_reject_noncanonical_stream_read_selectors() {
    // Arrange
    let context = setup_test_context();
    let selectors = [
        "stream://bench/**/*",
        "stream://bench/events/**",
        "stream://*/**",
        "stream://**/orders",
    ];

    // Act
    let errors = selectors.map(|selector| {
        let frame = build_stream_read(selector, 0);
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let response = request(&context, selector, msg_type, payload);
        decode_stream_error_message(response.as_ref()).expect("invalid read selector error")
    });

    // Assert
    assert!(errors
        .into_iter()
        .all(|error| error.contains("stream route selector must be one of the 10 shapes")));
}

#[test]
fn should_accept_realm_plus_global_filtered_stream_read_selectors() {
    // Arrange
    let context = setup_test_context();
    let selectors = [
        "stream://bench/*/orders",
        "stream://*/events/*",
        "stream://*/events/orders",
        "stream://*/*/orders",
    ];

    // Act
    for selector in selectors {
        // Assert
        let frame = build_stream_read(selector, 0);
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let response = request(&context, selector, msg_type, payload);
        assert!(
            decode_stream_error_message(response.as_ref()).is_err(),
            "expected {selector} to be accepted"
        );
    }
}

#[test]
fn should_accept_global_catch_all_stream_read_selector() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://**";
    let frame = build_stream_read(route, 0);
    let (msg_type, payload) = extract_single_tlv_field(&frame);

    // Act
    let response = request(&context, route, msg_type, payload);

    // Assert
    assert!(decode_stream_error_message(response.as_ref()).is_err());
}

#[test]
fn should_accept_global_catch_all_stream_subscribe_pattern() {
    // Arrange
    let context = setup_test_context();
    let pattern = "stream://**";
    let subscribe_frame = build_stream_subscribe(pattern);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);

    // Act
    let response = request(
        &context,
        "stream://bench/events/orders",
        subscribe_msg_type,
        subscribe_payload,
    );

    // Assert
    assert!(decode_stream_error_message(response.as_ref()).is_err());
    assert_eq!(context.sink.subscription_count(), 1);
}

#[test]
fn should_reject_stream_subscribe_patterns_wider_than_read_grammar() {
    // Arrange
    let context = setup_test_context();
    let rejected_patterns = ["stream://bench/events/**", "stream://bench/**/orders"];

    // Act
    let errors = rejected_patterns.map(|pattern| {
        let subscribe_frame = build_stream_subscribe(pattern);
        let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
        let response = request(
            &context,
            "stream://bench/events/orders",
            subscribe_msg_type,
            subscribe_payload,
        );
        decode_stream_error_message(response.as_ref())
    });

    // Assert
    assert!(errors.into_iter().all(|error| error.is_ok()));
}

#[test]
fn should_reject_global_realm_wildcard_for_concrete_stream_lookups() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://*/events/orders";

    // Act
    let errors = [
        crate::benchkit::build_stream_last(route),
        crate::benchkit::build_stream_get_metadata(route),
    ]
    .map(|frame| {
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let response = request(&context, route, msg_type, payload);
        decode_stream_error_message(response.as_ref())
            .expect("global-realm wildcard lookup should return an error")
    });

    // Assert
    assert!(errors.iter().all(|error| error.contains("concrete")));
}
