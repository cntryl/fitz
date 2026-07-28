use super::*;
use crate::benchkit::{
    build_stream_append, build_stream_begin, build_stream_subscribe, extract_single_tlv_field,
    route_frame_to_address, FrameQueueSink,
};
use crate::dispatch::protocol::frame::ChannelId;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
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
    sink.panic_actor_for_tests();
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
        "empty pattern"
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
