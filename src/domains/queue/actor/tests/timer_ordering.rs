use super::*;

fn actor_with_clock() -> (QueueActor, MockClock) {
    let clock = MockClock::new();
    let mut key = unique_queue_key("timer-ordering");
    key.family = RouteFamily::new(1);
    let actor = QueueActor::with_clock(
        key.family,
        key,
        create_test_engine_with_cfs(vec![1]),
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    (actor, clock)
}

#[test]
fn should_promote_due_work_before_a_later_visibility_timer() {
    // Arrange
    let (mut actor, clock) = actor_with_clock();
    let QueueResponse::Sent { id } = actor.handle_send(Bytes::new(), Some(1)) else {
        panic!("enqueued")
    };
    actor.handle_send(Bytes::new(), Some(100));
    clock.advance(Duration::from_secs(2));

    // Act
    actor.process_due_work();

    // Assert
    assert!(actor.ready_contains(id));
    assert_eq!(actor.admin_snapshot().messages_delayed, 1);
}

#[test]
fn should_expire_the_due_reservation_before_a_later_lease_timer() {
    // Arrange
    let (mut actor, clock) = actor_with_clock();
    actor.handle_send_batch(&[(Bytes::new(), None), (Bytes::new(), None)]);
    let QueueResponse::Received { messages } =
        actor.handle_receive_for_session(TEST_SESSION_ID, 1, Some(1))
    else {
        panic!("reserved")
    };
    let first = messages[0].id;
    actor.handle_receive_for_session(TEST_SESSION_ID, 100, Some(1));
    clock.advance(Duration::from_secs(2));

    // Act
    actor.process_due_work();

    // Assert
    assert!(actor.ready_contains(first));
    assert_eq!(actor.inflight.len(), 1);
}

#[test]
fn should_promote_equal_visibility_messages_in_enqueue_order() {
    // Arrange
    let (mut actor, clock) = actor_with_clock();
    let QueueResponse::SentBatch { ids } =
        actor.handle_send_batch(&[(Bytes::new(), Some(1)), (Bytes::new(), Some(1))])
    else {
        panic!("enqueued")
    };
    clock.advance(Duration::from_secs(2));

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(
        actor.ready.iter().map(|entry| entry.id).collect::<Vec<_>>(),
        ids
    );
}
