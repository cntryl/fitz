use super::*;
use crate::domains::queue::retention::QueueRetention;

fn expire_delayed_generation(actor: &mut QueueActor, clock: &MockClock) -> usize {
    actor.handle_send_batch(&vec![(Bytes::new(), Some(10_000)); 128]);
    clock.advance(Duration::from_secs(1));
    for _ in 0..4 {
        actor.process_due_work();
    }
    actor.delayed.len()
}

fn expire_inflight_generation(actor: &mut QueueActor, clock: &MockClock) -> usize {
    actor.handle_send_batch(&vec![(Bytes::new(), None); 128]);
    actor.handle_receive_for_session(TEST_SESSION_ID, 10_000, Some(128));
    clock.advance(Duration::from_secs(1));
    for _ in 0..4 {
        actor.process_due_work();
    }
    actor.timers.len()
}

fn retained_actor(clock: &MockClock) -> QueueActor {
    let mut key = unique_queue_key("expiry-capacity");
    key.family = RouteFamily::new(1);
    QueueActor::with_clock(
        key.family,
        key,
        create_test_engine_with_cfs(vec![1]),
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    )
    .with_retention(QueueRetention {
        message_seconds: Some(1),
        dlq_seconds: None,
    })
}

#[test]
fn should_bound_delayed_timer_storage_across_repeated_message_expiry() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    actor.handle_send_with_ttl(Bytes::new(), Some(10_000), std::num::NonZeroU32::new(1));

    // Act
    let largest_remaining_generation = (0..4)
        .map(|_| expire_delayed_generation(&mut actor, &clock))
        .max()
        .unwrap();

    // Assert
    assert_eq!(actor.admin_snapshot().messages_total, 1);
    assert!(
        largest_remaining_generation <= 129,
        "expired generations accumulated {largest_remaining_generation} timers"
    );
}

#[test]
fn should_bound_inflight_timer_storage_across_repeated_message_expiry() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(1));
    actor.handle_receive_for_session(TEST_SESSION_ID, 10_000, Some(1));

    // Act
    let largest_remaining_generation = (0..4)
        .map(|_| expire_inflight_generation(&mut actor, &clock))
        .max()
        .unwrap();

    // Assert
    assert_eq!(actor.admin_snapshot().messages_total, 1);
    assert!(
        largest_remaining_generation <= 129,
        "expired generations accumulated {largest_remaining_generation} timers"
    );
}

#[test]
fn should_promote_a_live_delayed_survivor_after_timer_compaction() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    let QueueResponse::Sent { id } =
        actor.handle_send_with_ttl(Bytes::new(), Some(10), std::num::NonZeroU32::new(1))
    else {
        panic!("enqueued")
    };
    for _ in 0..4 {
        expire_delayed_generation(&mut actor, &clock);
    }
    clock.advance(Duration::from_secs(6));

    // Act
    actor.process_due_work();

    // Assert
    assert!(actor.ready_contains(id));
}

#[test]
fn should_expire_a_live_reservation_after_timer_compaction() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    let QueueResponse::Sent { id } =
        actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(1))
    else {
        panic!("enqueued")
    };
    actor.handle_receive_for_session(TEST_SESSION_ID, 10, Some(1));
    for _ in 0..4 {
        expire_inflight_generation(&mut actor, &clock);
    }
    clock.advance(Duration::from_secs(6));

    // Act
    actor.process_due_work();

    // Assert
    assert!(actor.ready_contains(id));
}

fn stored_next_visibility(actor: &QueueActor) -> Option<u64> {
    let recovery = &actor.persistence.recovery;
    recovery
        .read_index(&recovery.snapshot().unwrap())
        .unwrap_or_else(|_| panic!("persisted index must decode"))
        .next_delayed_visibility_ms
}

#[test]
fn should_preserve_a_shared_earliest_visibility_when_one_message_expires() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    let expected = clock.now_epoch_ms() + 10_000;
    actor.handle_send(Bytes::new(), Some(10));
    actor.handle_send_with_ttl(Bytes::new(), Some(10), std::num::NonZeroU32::new(1));
    actor.handle_send_with_ttl(Bytes::new(), Some(20), std::num::NonZeroU32::new(1));
    clock.advance(Duration::from_secs(1));

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(stored_next_visibility(&actor), Some(expected));
}

#[test]
fn should_advance_the_earliest_visibility_when_its_only_message_expires() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    let expected = clock.now_epoch_ms() + 20_000;
    actor.handle_send(Bytes::new(), Some(10));
    actor.handle_send_with_ttl(Bytes::new(), Some(20), std::num::NonZeroU32::new(1));
    clock.advance(Duration::from_secs(1));

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(stored_next_visibility(&actor), Some(expected));
}

#[test]
fn should_coalesce_duplicate_retry_timers_during_compaction() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock);
    let QueueResponse::Sent { id } =
        actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(1))
    else {
        panic!("enqueued")
    };
    actor.handle_receive_for_session(TEST_SESSION_ID, 1, Some(1));
    let inflight = actor.inflight.get(&id).unwrap().clone();
    clock.advance(Duration::from_secs(2));
    for _ in 0..128 {
        actor.schedule_inflight_retry(id, &inflight);
    }
    QueueActor::fail_next_redelivery_commit_for_tests();

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(actor.inflight.len(), 1);
    assert_eq!(actor.timers.len(), 1);
}
