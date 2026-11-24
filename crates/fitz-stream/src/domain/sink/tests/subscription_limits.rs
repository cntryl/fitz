use super::*;
use fitz_core::protocol::error_codes::{decode_error_body, stream::ERR_SUBSCRIPTION_LIMIT};

const ROUTE: &str = "stream://bench/events/orders";

#[test]
fn should_enforce_capacity_for_mixed_registrations() {
    // Arrange
    let context = setup_test_context();
    for index in 0..10_000 {
        let resource = if index % 2 == 0 { "*" } else { "orders" };
        let route = format!("stream://bench/area-{index}/{resource}");
        assert_eq!(subscribe(&context, &route)[0], 0);
    }

    // Act
    let response = subscribe(&context, ROUTE);

    // Assert
    assert_limit(&response);
    assert_eq!(context.sink.subscription_count(), 10_000);
}

#[test]
fn should_reuse_original_registration_at_capacity() {
    // Arrange
    let context = one_registration_context();
    let first = subscribe(&context, ROUTE);

    // Act
    let second = subscribe(&context, ROUTE);

    // Assert
    assert_eq!(first, second);
    assert_eq!(context.sink.subscription_count(), 1);
    assert_eq!(
        context.sink.config.subscription_bytes.used(),
        512 + 2 * ROUTE.len() - 9
    );
}

#[test]
fn should_deliver_to_both_independent_alias_registrations() {
    // Arrange
    let context = setup_test_context();
    let (source, inbox) = register_session_queue_sink(&context.router, context.family, 2);
    let mut ids = Vec::new();
    for selector in ["stream://bench/**", "stream://bench/*/*"] {
        let (kind, payload) = extract_single_tlv_field(&build_stream_subscribe(selector));
        let response = request_from_session(&context, &source, &inbox, 2, selector, kind, payload);
        let mut decoder = fitz_core::protocol::payload_codec::PayloadDecoder::new(&response);
        assert_eq!(decoder.get_u8().unwrap(), 0);
        ids.push(decoder.get_optional_u64().unwrap().unwrap());
    }

    // Act
    seed_committed_stream_route(&context, ROUTE, 1, b"event");
    let frames = inbox.drain_after_count(2, Duration::from_secs(2));

    // Assert
    assert!(ids[0] < ids[1]);
    assert_eq!(
        frames
            .iter()
            .map(|frame| {
                fitz_core::protocol::payload_codec::PayloadDecoder::new(&frame.payload)
                    .get_u64()
                    .unwrap()
            })
            .collect::<Vec<_>>(),
        ids
    );
}

#[test]
fn should_share_subscription_bytes_across_families() {
    // Arrange
    let context = one_registration_context();
    assert_eq!(subscribe(&context, ROUTE)[0], 0);

    // Act
    let response = subscribe_in_sibling(&context);

    // Assert
    assert_limit(&response);
}

#[test]
fn should_return_registration_credit_after_failed_acknowledgment() {
    // Arrange
    let context = one_registration_context();
    let family = context.family;
    let missing_subscriber = RouteAddress::new(family, Route::new("session://missing/99"));
    let (kind, payload) = extract_single_tlv_field(&build_stream_subscribe(ROUTE));

    // Act
    let _ = route_frame(
        &context.router,
        &missing_subscriber,
        ROUTE,
        99,
        ChannelId::Pub,
        kind,
        payload,
        family,
    );

    // Assert
    assert_eq!(context.sink.subscription_count(), 0);
    assert_eq!(context.sink.config.subscription_bytes.used(), 0);
    assert_eq!(subscribe_in_sibling(&context)[0], 0);
}

#[test]
fn should_allow_sibling_admission_after_disconnect() {
    // Arrange
    let context = one_registration_context();
    assert_eq!(subscribe(&context, ROUTE)[0], 0);

    // Act
    context
        .sink
        .deliver_high_priority(Envelope::new(
            RouteAddress::new(context.family, Route::new(ROUTE)),
            fitz_core::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .unwrap();

    // Assert
    assert_eq!(context.sink.config.subscription_bytes.used(), 0);
    assert_eq!(subscribe_in_sibling(&context)[0], 0);
}

#[test]
fn should_allow_sibling_admission_after_family_panics() {
    // Arrange
    let context = one_registration_context();
    assert_eq!(subscribe(&context, ROUTE)[0], 0);

    // Act
    context
        .sink
        .panic_family_actor_for_failpoint(context.family);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while context
        .sink
        .family_runtime
        .is_family_running(context.family)
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }

    // Assert
    assert!(!context
        .sink
        .family_runtime
        .is_family_running(context.family));
    assert_eq!(context.sink.config.subscription_bytes.used(), 0);
    assert_eq!(subscribe_in_sibling(&context)[0], 0);
}

fn one_registration_context() -> TestContext {
    setup_test_context_with_limits(
        None,
        StreamSubscriptionLimits {
            per_session: std::num::NonZeroUsize::MIN,
            bytes: std::num::NonZeroUsize::new(600).unwrap(),
        },
    )
}

fn subscribe(context: &TestContext, route: &str) -> Bytes {
    let (kind, payload) = extract_single_tlv_field(&build_stream_subscribe(route));
    request(context, route, kind, payload)
}

fn subscribe_in_sibling(context: &TestContext) -> Bytes {
    let family = RouteFamily::new(2);
    let (source, inbox) = register_session_queue_sink(&context.router, family, 3);
    let (kind, payload) = extract_single_tlv_field(&build_stream_subscribe(ROUTE));
    route_frame(
        &context.router,
        &source,
        ROUTE,
        3,
        ChannelId::Pub,
        kind,
        payload,
        family,
    )
    .unwrap();
    inbox.drain().pop().expect("subscription response").payload
}

fn assert_limit(response: &Bytes) {
    assert_eq!(response[0], 1);
    assert_eq!(
        decode_error_body(response).unwrap().0,
        ERR_SUBSCRIPTION_LIMIT
    );
}
