use super::*;

#[test]
fn should_preserve_redelivery_after_stale_ack_domain_invariant() {
    // Arrange
    let clock = MockClock::new();
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut key = unique_queue_key("stale-ack");
    key.family = RouteFamily::new(1);
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(1),
        key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    actor.handle_send(Bytes::from_static(b"retained"), None);
    let QueueResponse::Received { messages: first } =
        actor.handle_receive_for_session(TEST_SESSION_ID, 1, Some(1))
    else {
        panic!("first reservation")
    };
    clock.advance(Duration::from_secs(2));
    actor.process_expired_timers();
    let QueueResponse::Received { messages: second } =
        actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1))
    else {
        panic!("redelivery")
    };

    // Act
    let stale = actor.handle_ack_for_session(TEST_SESSION_ID, first[0].id, first[0].token);
    let still_reserved = actor.handle_receive_for_session(TEST_SESSION_ID + 1, 30, Some(1));
    let current = actor.handle_ack_for_session(TEST_SESSION_ID, second[0].id, second[0].token);

    // Assert
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, first[0].id);
    assert_ne!(second[0].token, first[0].token);
    assert_eq!(second[0].body.as_ref(), b"retained");
    assert_eq!(stale, QueueResponse::InvalidToken);
    assert!(matches!(still_reserved, QueueResponse::Received { messages } if messages.is_empty()));
    assert_eq!(current, QueueResponse::Acked);
    assert_eq!(actor.ready_len(), 0);
    assert!(actor.inflight.is_empty());
}
