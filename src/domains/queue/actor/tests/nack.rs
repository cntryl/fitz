use super::*;

#[test]
fn should_reject_an_old_unapplied_nack_after_the_same_session_reserves_again() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, old_token) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();
    actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    let current_token = actor.inflight[&id].token;

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, old_token, 0);

    // Assert
    assert_eq!(response, QueueResponse::InvalidToken);
    assert_eq!(actor.inflight[&id].token, current_token);
}

#[test]
fn should_recover_applied_nack_visibility_after_an_unknown_commit_outcome() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    let key = actor.queue_key.clone();
    let store = actor.persistence.store.clone();
    super::super::recovery_store::QueueStore::fail_next_commit_after_apply_for_tests(
        cntryl_midge::MidgeError::Timeout("outcome unknown".into()),
    );
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);
    assert!(matches!(response, QueueResponse::Timeout { .. }));
    assert!(actor.storage_outcome_is_unknown());
    drop(actor);

    // Act
    let actor = QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.admin_snapshot().messages_ready, 0);
    assert_eq!(actor.admin_snapshot().messages_delayed, 1);
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}

fn nack_actor(clock: &MockClock, max_attempts: Option<u32>) -> QueueActor {
    let store = create_test_engine_with_cfs(vec![1]);
    let mut key = unique_queue_key("nack");
    key.family = RouteFamily::new(1);
    QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock.clone()),
        max_attempts,
        crate::utils::idempotency::default_dedup_store(),
    )
}

#[test]
fn should_relinquish_a_reservation_without_completing_the_message() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 0);

    // Assert
    assert_eq!(response, QueueResponse::Nacked);
    assert!(actor.ready_contains(id));
    assert!(actor.inflight.is_empty());
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}

#[test]
fn should_delay_the_next_worker_after_nack() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);
    clock.advance(Duration::from_secs(59));
    actor.process_due_work();

    // Act
    let response = actor.handle_receive_for_session(99, 30, Some(1));

    // Assert
    assert!(matches!(response, QueueResponse::Received { messages } if messages.is_empty()));
    assert_eq!(actor.admin_snapshot().messages_delayed, 1);
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}

#[test]
fn should_increment_attempts_only_when_the_next_worker_reserves() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);
    clock.advance(Duration::from_secs(60));
    actor.process_due_work();

    // Act
    let response = actor.handle_receive_for_session(99, 30, Some(1));

    // Assert
    let QueueResponse::Received { messages } = response else {
        panic!("reserved");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].attempts, 2);
    assert_ne!(messages[0].token, token);
}

#[test]
fn should_reject_nack_from_a_different_session() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID + 1, id, token, 0);

    // Assert
    assert_eq!(response, QueueResponse::NotFound);
    assert!(actor.inflight.contains_key(&id));
}

#[test]
fn should_reject_nack_with_an_invalid_token() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token ^ 1, 0);

    // Assert
    assert_eq!(response, QueueResponse::InvalidToken);
    assert!(actor.inflight.contains_key(&id));
}

#[test]
fn should_keep_a_new_reservation_when_an_old_nack_is_retried() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 0);
    actor.handle_receive_for_session(99, 30, Some(1));
    let new_token = actor.inflight[&id].token;

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);

    // Assert
    assert_eq!(response, QueueResponse::Nacked);
    assert_eq!(actor.inflight[&id].token, new_token);
    assert_eq!(actor.inflight[&id].owner_session_id, Some(99));
    assert_eq!(actor.admin_snapshot().messages_delayed, 0);
}

#[test]
fn should_preserve_the_original_nack_visibility_on_duplicate_requests() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);
    let original_visibility = actor.persisted_delayed[&id];

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 120);

    // Assert
    assert_eq!(response, QueueResponse::Nacked);
    assert_eq!(actor.persisted_delayed[&id], original_visibility);
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}

#[test]
fn should_dead_letter_a_nacked_final_attempt_without_incrementing_it() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, Some(1));
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);

    // Assert
    assert_eq!(response, QueueResponse::Nacked);
    assert_eq!(actor.admin_snapshot().messages_dead_lettered, 1);
    assert_eq!(actor.admin_dead_letters()[0].attempts, 1);
    assert_eq!(actor.admin_snapshot().messages_delayed, 0);
}

#[test]
fn should_leave_the_reservation_active_when_nack_commit_fails() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    QueueActor::fail_next_redelivery_commit_for_tests();

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.inflight[&id].token, token);
    assert_eq!(actor.admin_snapshot().messages_delayed, 0);
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}

#[test]
fn should_reject_overflowing_nack_visibility_without_mutation() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, u64::MAX);

    // Assert
    assert!(matches!(response, QueueResponse::BadRequest { .. }));
    assert_eq!(actor.inflight[&id].token, token);
}

#[test]
fn should_reject_nack_after_the_reservation_expires() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(30));

    // Act
    let response = actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);

    // Assert
    assert_eq!(response, QueueResponse::InflightExpired);
    assert!(actor.ready_contains(id));
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}

#[test]
fn should_preserve_publish_expiry_when_nack_delays_visibility_beyond_it() {
    // Arrange
    let clock = MockClock::new();
    let mut actor =
        nack_actor(&clock, None).with_retention(crate::domains::queue::retention::QueueRetention {
            message_seconds: Some(10),
            dlq_seconds: None,
        });
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);
    clock.advance(Duration::from_secs(10));

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(actor.admin_snapshot().messages_total, 0);
}

#[test]
fn should_keep_delayed_nack_visibility_after_actor_reconstruction() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = nack_actor(&clock, None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    actor.handle_nack_for_session(TEST_SESSION_ID, id, token, 60);
    let key = actor.queue_key.clone();
    let store = actor.persistence.store.clone();
    drop(actor);
    let mut actor = QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let response = actor.handle_receive_for_session(99, 30, Some(1));

    // Assert
    assert!(matches!(response, QueueResponse::Received { messages } if messages.is_empty()));
    assert_eq!(actor.admin_snapshot().messages_delayed, 1);
    assert_eq!(
        actor.load_record_metadata_from_store(id).unwrap().attempts,
        1
    );
}
