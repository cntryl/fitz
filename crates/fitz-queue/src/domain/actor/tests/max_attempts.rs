use super::*;

#[test]
fn should_restore_the_ready_batch_when_reservation_counter_commit_fails() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = limited_actor(&clock, 3);
    actor.handle_send_batch(&[
        (Bytes::from_static(b"first"), None),
        (Bytes::from_static(b"second"), None),
    ]);
    super::super::recovery_store::QueueStore::fail_next_commit_for_tests(
        cntryl_midge::MidgeError::WriteConflict("counter commit rejected".into()),
    );

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(2));

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.admin_snapshot().messages_ready, 2);
    assert_eq!(actor.admin_snapshot().messages_inflight, 0);
    assert_eq!(
        actor
            .load_record_metadata_from_store(MessageId::new(1))
            .unwrap()
            .attempts,
        0
    );
    assert_eq!(
        actor
            .load_record_metadata_from_store(MessageId::new(2))
            .unwrap()
            .attempts,
        0
    );
}

fn limited_actor(clock: &MockClock, limit: u32) -> QueueActor {
    let store = create_test_engine_with_cfs(vec![1]);
    let mut key = unique_queue_key("limited");
    key.family = RouteFamily::new(1);
    QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock.clone()),
        Some(limit),
        crate::utils::idempotency::default_dedup_store(),
    )
}

#[test]
fn should_retry_failed_dead_letter_commit_without_counting_expiry_twice() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = limited_actor(&clock, 1);
    send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));
    QueueActor::fail_next_redelivery_commit_for_tests();
    actor.process_expired_timers();
    assert_eq!(actor.admin_snapshot().messages_inflight, 1);
    assert_eq!(actor.admin_snapshot().messages_dead_lettered, 0);
    clock.advance(Duration::from_secs(1));

    // Act
    actor.process_expired_timers();

    // Assert
    let dead_letters = actor.admin_dead_letters();
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].attempts, 1);
}

#[test]
fn should_increment_the_next_reserve_attempt_after_session_disconnect() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = limited_actor(&clock, 3);
    send_and_reserve_single_message(&mut actor, "job");
    actor.cleanup_session_inflight(TEST_SESSION_ID);

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    let QueueResponse::Received { messages } = response else {
        panic!("reserved message");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].attempts, 2);
    assert_eq!(actor.admin_snapshot().messages_dead_lettered, 0);
}

#[test]
fn should_ignore_the_original_expiry_after_a_limited_delivery_is_extended() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = limited_actor(&clock, 1);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    assert_eq!(
        actor.handle_extend_for_session(TEST_SESSION_ID, id, token, 60),
        QueueResponse::Extended
    );
    clock.advance(Duration::from_secs(31));

    // Act
    actor.process_expired_timers();

    // Assert
    let snapshot = actor.admin_snapshot();
    assert_eq!(snapshot.messages_inflight, 1);
    assert_eq!(snapshot.messages_dead_lettered, 0);
}

#[test]
fn should_divert_ready_backlog_at_a_lower_reconstructed_reservation_limit() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = limited_actor(&clock, 4);
    let QueueResponse::Sent { id } = actor.handle_send(Bytes::from_static(b"job"), None) else {
        panic!("enqueued message");
    };
    for _ in 0..2 {
        actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();
    }
    let key = actor.queue_key.clone();
    let store = actor.persistence.store.clone();
    drop(actor);
    let mut actor = QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    );
    assert_eq!(actor.admin_dead_letters().len(), 0);
    assert_eq!(actor.ready_len(), 1);
    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    let dead_letters = actor.admin_dead_letters();
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].message_id, id.as_u64());
    assert_eq!(dead_letters[0].attempts, 2);
    assert!(matches!(response, QueueResponse::Received { messages } if messages.is_empty()));
}

fn flushed_expired_message(path: &std::path::Path, expirations: u32) -> (QueueKey, MessageId) {
    let store = super::fast_flush::local_engine(path);
    let family = store.create_column_family("queue").unwrap();
    let mut key = unique_queue_key("flushed-dlq");
    key.family = RouteFamily::new(family.id());
    let clock = MockClock::new();
    let mut actor = QueueActor::with_clock(
        key.family,
        key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let QueueResponse::Sent { id } = actor.handle_send(Bytes::from_static(b"job"), None) else {
        panic!("enqueued message");
    };
    for _ in 0..expirations {
        let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
        assert!(matches!(response, QueueResponse::Received { messages } if messages.len() == 1));
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();
    }
    store.flush_cf(&family).unwrap();
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(store);
    (key, id)
}

#[test]
fn should_recover_a_flushed_dead_letter_after_persistent_store_reopen() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let (key, id) = flushed_expired_message(directory.path(), 2);

    // Act
    let store = super::fast_flush::local_engine(directory.path());
    let actor = QueueActor::with_clock(
        key.family,
        key,
        store.clone(),
        Box::new(MockClock::new()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let dead_letters = actor.admin_dead_letters();

    // Assert
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].message_id, id.as_u64());
    assert_eq!(dead_letters[0].attempts, 2);
    assert_eq!(dead_letters[0].reason, "max_attempts_exceeded");
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(store);
}

#[test]
fn should_reset_expired_attempts_when_replaying_a_recovered_dead_letter() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let (key, id) = flushed_expired_message(directory.path(), 2);
    let store = super::fast_flush::local_engine(directory.path());
    let mut actor = QueueActor::with_clock(
        key.family,
        key,
        store.clone(),
        Box::new(MockClock::new()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(actor.replay_dead_letter(id).unwrap());

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    let QueueResponse::Received { messages } = response else {
        panic!("replayed message");
    };
    assert_eq!(messages[0].id, id);
    assert_eq!(messages[0].attempts, 1);
    assert_eq!(messages[0].body, Bytes::from_static(b"job"));
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(store);
}

#[test]
fn should_recover_flushed_expired_attempts_before_reaching_the_limit() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let (key, id) = flushed_expired_message(directory.path(), 1);
    let store = super::fast_flush::local_engine(directory.path());
    let clock = MockClock::new();
    let mut actor = QueueActor::with_clock(
        key.family,
        key,
        store.clone(),
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let QueueResponse::Received { messages } =
        actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1))
    else {
        panic!("recovered ready message");
    };
    assert_eq!(messages[0].attempts, 2);
    clock.advance(Duration::from_secs(31));

    // Act
    actor.process_expired_timers();

    // Assert
    let dead_letters = actor.admin_dead_letters();
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].message_id, id.as_u64());
    assert_eq!(dead_letters[0].attempts, 2);
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(store);
}
