//! Stream subscription inventory projection and cleanup behavior.

use super::*;

#[test]
fn should_project_exact_and_wildcard_stream_subscriptions_until_session_cleanup() {
    // Arrange
    let context = setup_test_context();
    let known_route = "stream://bench/events/orders";
    let exact_only_route = "stream://bench/events/empty";
    seed_committed_stream_route(&context, known_route, 1, b"persisted");
    for route in [known_route, "stream://bench/events/*", exact_only_route] {
        let frame = build_stream_subscribe(route);
        let (message_type, payload) = extract_single_tlv_field(&frame);
        let _response = request(&context, route, message_type, payload);
    }

    // Act
    context.sink.refresh_admin_snapshot_if_dirty();
    let before_cleanup = context.admin_read_model.streams(None);
    let unsubscribe_frame = crate::benchkit::build_stream_unsubscribe(exact_only_route);
    let (message_type, payload) = extract_single_tlv_field(&unsubscribe_frame);
    let _response = request(&context, exact_only_route, message_type, payload);
    context.sink.refresh_admin_snapshot_if_dirty();
    let after_unsubscribe = context.admin_read_model.streams(None);
    context
        .sink
        .deliver(Envelope::new(
            RouteAddress::new(context.family, Route::new(known_route)),
            crate::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .expect("deliver session cleanup");
    context.sink.refresh_admin_snapshot_if_dirty();
    let after_cleanup = context.admin_read_model.streams(None);

    // Assert
    let orders = before_cleanup
        .iter()
        .find(|stream| stream.resource == "orders")
        .expect("committed resource row");
    let empty = before_cleanup
        .iter()
        .find(|stream| stream.resource == "empty")
        .expect("exact subscription-only row");
    assert_eq!(orders.subscriptions_active, 2);
    assert_eq!(empty.subscriptions_active, 2);
    assert_eq!(empty.committed_event_count, 0);
    assert_eq!(before_cleanup.len(), 2);
    assert!(!before_cleanup
        .iter()
        .any(|stream| stream.resource == "other"));
    assert_eq!(after_unsubscribe.len(), 1);
    assert_eq!(after_unsubscribe[0].subscriptions_active, 2);
    assert_eq!(after_cleanup.len(), 1);
    assert_eq!(after_cleanup[0].subscriptions_active, 0);
}

#[test]
fn should_enforce_exact_and_wildcard_stream_registration_limits_per_session() {
    // Arrange
    let exact_context = setup_test_context();
    let subscribe = |context: &TestContext, route: &str| {
        let frame = build_stream_subscribe(route);
        let (message_type, payload) = extract_single_tlv_field(&frame);
        request(context, route, message_type, payload)
    };
    for index in 0..crate::domains::subscription_state::MAX_TOTAL_REGISTRATIONS_PER_SESSION {
        let route = format!("stream://bench/events/resource-{index}");
        assert_eq!(subscribe(&exact_context, &route)[0], 0);
    }
    let wildcard_context = setup_test_context();
    for index in 0..crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION {
        let route = format!("stream://bench/area{index}/*");
        assert_eq!(subscribe(&wildcard_context, &route)[0], 0);
    }

    // Act
    let exact_overflow = subscribe(&exact_context, "stream://bench/events/overflow");
    let wildcard_overflow = subscribe(&wildcard_context, "stream://bench/overflow/*");

    // Assert
    assert_ne!(exact_overflow[0], 0);
    assert_ne!(wildcard_overflow[0], 0);
    assert_eq!(
        exact_context.sink.subscription_count(),
        crate::domains::subscription_state::MAX_TOTAL_REGISTRATIONS_PER_SESSION
    );
    assert_eq!(
        wildcard_context.sink.subscription_count(),
        crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION
    );
}
