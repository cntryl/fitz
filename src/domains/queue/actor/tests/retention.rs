use super::*;

fn prepare_next_reservation(
    actor: &mut QueueActor,
    clock: &MockClock,
) -> super::super::reservation_batch::PreparedReservation {
    let ready = actor.pop_ready_entry().unwrap();
    let body = actor.load_body_from_store(ready.id).unwrap();
    actor.prepare_reservation(
        ready,
        body,
        Inflight {
            token: ready.id.as_u64(),
            expires_at: clock.now_instant() + Duration::from_secs(30),
            expires_at_epoch_ms: clock.now_epoch_ms() + 30_000,
            owner_session_id: Some(TEST_SESSION_ID),
            attempts: 1,
            inflight_epoch: 1,
        },
        30,
        clock.now_epoch_ms(),
    )
}

#[test]
fn should_restore_ungranted_reservations_after_expired_cleanup_commit_is_rejected() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send(Bytes::from_static(b"short"), None);
    actor.handle_send_with_ttl(
        Bytes::from_static(b"long"),
        None,
        std::num::NonZeroU32::new(1),
    );
    let prepared = vec![
        prepare_next_reservation(&mut actor, &clock),
        prepare_next_reservation(&mut actor, &clock),
    ];
    clock.advance(Duration::from_secs(1));
    QueueActor::fail_next_redelivery_commit_for_tests();

    // Act
    let response = actor.commit_reservation_batch(prepared);

    // Assert
    assert!(response.is_err());
    assert_eq!(actor.ready_len(), 2);
    assert_eq!(actor.inflight.len(), 0);
    assert_eq!(
        actor
            .load_record_metadata_from_store(MessageId::new(2))
            .unwrap()
            .attempts,
        1
    );
}

#[test]
fn should_use_committed_attempts_after_recovering_a_rejected_expiry_cleanup() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.max_attempts = Some(3);
    actor.handle_send(Bytes::from_static(b"short"), None);
    actor.handle_send_with_ttl(
        Bytes::from_static(b"long"),
        None,
        std::num::NonZeroU32::new(1),
    );
    let prepared = vec![
        prepare_next_reservation(&mut actor, &clock),
        prepare_next_reservation(&mut actor, &clock),
    ];
    clock.advance(Duration::from_secs(1));
    QueueActor::fail_next_redelivery_commit_for_tests();
    assert!(actor.commit_reservation_batch(prepared).is_err());
    actor.process_due_work();

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(
        matches!(response, QueueResponse::Received { messages } if messages.len() == 1 && messages[0].attempts == 2)
    );
}

#[test]
fn should_reserve_live_work_after_already_expired_non_head_entries() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(1));
    actor.handle_send_batch(&vec![(Bytes::new(), None); 64]);
    actor.handle_send_with_ttl(
        Bytes::from_static(b"tail"),
        None,
        std::num::NonZeroU32::new(1),
    );
    clock.advance(Duration::from_secs(1));
    for _ in 0..2 {
        actor.process_due_work();
    }
    actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(
        matches!(response, QueueResponse::Received { messages } if messages.len() == 1 && messages[0].body.as_ref() == b"tail")
    );
}

#[test]
fn should_keep_ready_counts_after_a_rejected_reserve_with_non_head_expirations() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send_with_ttl(
        Bytes::from_static(b"long"),
        None,
        std::num::NonZeroU32::new(1),
    );
    actor.handle_send_batch(&vec![(Bytes::new(), None); 100]);
    clock.advance(Duration::from_secs(1));
    actor.process_due_work();
    super::super::recovery_store::QueueStore::fail_next_commit_for_tests(
        cntryl_midge::MidgeError::WriteConflict("reserve rejected".into()),
    );

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.ready_len(), 69);
    assert!(actor.ready_contains(MessageId::new(1)));
    assert!(!actor.ready_contains(MessageId::new(2)));
}

#[test]
fn should_preserve_other_ready_messages_when_a_prepared_reservation_expires() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send(Bytes::from_static(b"short"), None);
    actor.handle_send_with_ttl(
        Bytes::from_static(b"long"),
        None,
        std::num::NonZeroU32::new(1),
    );
    let ready = actor.pop_ready_entry().unwrap();
    let prepared = actor.prepare_reservation(
        ready,
        Bytes::from_static(b"short"),
        Inflight {
            token: 9,
            expires_at: clock.now_instant() + Duration::from_secs(30),
            expires_at_epoch_ms: clock.now_epoch_ms() + 30_000,
            owner_session_id: Some(TEST_SESSION_ID),
            attempts: 1,
            inflight_epoch: 1,
        },
        30,
        clock.now_epoch_ms(),
    );
    clock.advance(Duration::from_secs(1));

    // Act
    let response = actor.commit_reservation_batch(vec![prepared]).unwrap();

    // Assert
    assert_eq!(response.len(), 0);
    assert_eq!(actor.ready_len(), 1);
    assert!(actor.ready_contains(MessageId::new(2)));
}

#[test]
fn should_bound_stale_ready_entries_after_non_head_expiry_cleanup() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(1));
    actor.handle_send_batch(&vec![(Bytes::new(), None); 256]);
    clock.advance(Duration::from_secs(1));

    // Act
    for _ in 0..8 {
        actor.process_due_work();
    }

    // Assert
    assert_eq!(actor.ready_len(), 1);
    assert!(actor.ready.len() <= actor.ready_len() * 2 + super::super::MAX_DUE_ITEMS_PER_PASS * 2);
}

#[test]
fn should_report_a_cold_expired_dead_letter_as_absent_before_reading_its_body() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(100), Some(10));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();
    let key = actor.queue_key.clone();
    let store = actor.persistence.store.clone();
    let mut txn = store
        .begin(
            1,
            super::super::recovery_store::QueueTransactionMode::ReadWrite,
        )
        .unwrap();
    txn.delete(actor.cached_body_key(id)).unwrap();
    txn.commit().unwrap();
    drop(actor);
    clock.advance(Duration::from_secs(11));
    let mut actor = QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let result = actor.replay_dead_letter(id);

    // Assert
    assert!(
        matches!(result, Ok(false)),
        "expired replay result: {result:?}"
    );
}
use crate::domains::queue::retention::QueueRetention;

fn retained_actor(
    clock: &MockClock,
    message_seconds: Option<u64>,
    dlq_seconds: Option<u64>,
) -> QueueActor {
    let store = create_test_engine_with_cfs(vec![1]);
    let mut key = unique_queue_key("retention");
    key.family = RouteFamily::new(1);
    QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    )
    .with_retention(QueueRetention {
        message_seconds,
        dlq_seconds,
    })
}

#[test]
fn should_cap_a_long_message_lifetime_at_the_dead_letter_deadline() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(100), Some(10));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    let original = actor
        .load_record_metadata_from_store(id)
        .unwrap()
        .expires_at_ms;
    clock.advance(Duration::from_secs(31));

    // Act
    actor.process_expired_timers();

    // Assert
    let record = actor.load_record_metadata_from_store(id).unwrap();
    assert_eq!(record.state, QueueState::Dlq);
    assert_eq!(record.expires_at_ms, original);
    assert_eq!(
        record.retention_expires_at_ms,
        Some(clock.now_epoch_ms() + 10_000)
    );
}

#[test]
fn should_keep_a_short_message_deadline_when_entering_the_dead_letter_queue() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(40), Some(100));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    let original = actor
        .load_record_metadata_from_store(id)
        .unwrap()
        .expires_at_ms;
    clock.advance(Duration::from_secs(31));

    // Act
    actor.process_expired_timers();

    // Assert
    let record = actor.load_record_metadata_from_store(id).unwrap();
    assert_eq!(record.retention_expires_at_ms, original);
}

#[test]
fn should_expire_a_cached_ready_message_without_creating_a_dead_letter() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send(Bytes::from_static(b"job"), None);
    clock.advance(Duration::from_secs(1));

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(matches!(response, QueueResponse::Received { messages } if messages.is_empty()));
    assert_eq!(actor.admin_snapshot().messages_total, 0);
}

#[test]
fn should_expire_a_delayed_message_before_its_visibility_time() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send(Bytes::from_static(b"job"), Some(100));
    clock.advance(Duration::from_secs(1));

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(actor.admin_snapshot().messages_total, 0);
}

#[test]
fn should_reject_extend_after_the_original_message_deadline() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(1));

    // Act
    let response = actor.handle_extend_for_session(TEST_SESSION_ID, id, token, 60);

    // Assert
    assert_eq!(response, QueueResponse::NotFound);
    assert_eq!(actor.admin_snapshot().messages_total, 0);
}

#[test]
fn should_reject_complete_after_the_original_message_deadline() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    let (id, token) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(1));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert_eq!(response, QueueResponse::NotFound);
}

#[test]
fn should_prevent_replay_past_the_dead_letter_retention_deadline() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(100), Some(1));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();
    clock.advance(Duration::from_secs(1));

    // Act
    let replayed = actor.replay_dead_letter(id).unwrap();

    // Assert
    assert!(!replayed);
    assert_eq!(actor.admin_dead_letters().len(), 0);
}

#[test]
fn should_preserve_the_original_publish_deadline_on_replay() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(100), Some(10));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    let original = actor
        .load_record_metadata_from_store(id)
        .unwrap()
        .expires_at_ms;
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();

    // Act
    let replayed = actor.replay_dead_letter(id).unwrap();

    // Assert
    let record = actor.load_record_metadata_from_store(id).unwrap();
    assert!(replayed);
    assert_eq!(record.expires_at_ms, original);
    assert_eq!(record.retention_expires_at_ms, original);
}

#[test]
fn should_use_publish_ttl_hours_instead_of_the_configured_default() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(86_400), None);

    // Act
    let QueueResponse::Sent { id } =
        actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(2))
    else {
        panic!("enqueued message");
    };

    // Assert
    let record = actor.load_record_metadata_from_store(id).unwrap();
    assert_eq!(record.expires_at_ms, Some(clock.now_epoch_ms() + 7_200_000));
}

#[test]
fn should_apply_a_dead_letter_ttl_without_an_original_message_ttl() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, None, Some(10));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));

    // Act
    actor.process_expired_timers();

    // Assert
    let record = actor.load_record_metadata_from_store(id).unwrap();
    assert_eq!(record.expires_at_ms, None);
    assert_eq!(
        record.retention_expires_at_ms,
        Some(clock.now_epoch_ms() + 10_000)
    );
}

#[test]
fn should_preserve_neighboring_messages_when_expiring_a_compressed_ready_range() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send_batch(&vec![(Bytes::new(), None); 17]);
    clock.advance(Duration::from_secs(1));
    actor.handle_send_with_ttl(
        Bytes::from_static(b"keep"),
        None,
        std::num::NonZeroU32::new(1),
    );

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    let QueueResponse::Received { messages } = response else {
        panic!("reserve response");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, Bytes::from_static(b"keep"));
    assert_eq!(actor.admin_dead_letters().len(), 0);
}

#[test]
fn should_expire_dead_letter_data_through_midge_without_broker_cleanup() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut key = unique_queue_key("native-ttl");
    key.family = RouteFamily::new(1);
    let mut actor = QueueActor::new(
        key.family,
        key,
        store,
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    )
    .with_retention(QueueRetention {
        message_seconds: Some(100),
        dlq_seconds: Some(1),
    });
    let QueueResponse::Sent { id } = actor.handle_send(Bytes::from_static(b"job"), None) else {
        panic!("enqueued message");
    };
    actor.handle_receive_for_session(TEST_SESSION_ID, 0, Some(1));
    actor.process_expired_timers();
    let entered_at = actor
        .load_record_metadata_from_store(id)
        .unwrap()
        .dead_lettered_at_ms
        .unwrap();
    std::thread::sleep(Duration::from_millis(1_250));

    // Act
    let txn = actor
        .persistence
        .store
        .begin(
            1,
            super::super::recovery_store::QueueTransactionMode::ReadOnly,
        )
        .unwrap();
    let header = txn.get(&actor.cached_header_key(id)).unwrap();
    let body = txn.get(&actor.cached_body_key(id)).unwrap();
    let index = txn.get(&actor.dlq_index_key(entered_at, id)).unwrap();

    // Assert
    assert!(header.is_none());
    assert!(body.is_none());
    assert!(index.is_none());
}

#[test]
fn should_ignore_the_previous_dead_letter_deadline_after_replay() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(100), Some(10));
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();
    actor.replay_dead_letter(id).unwrap();
    clock.advance(Duration::from_secs(11));

    // Act
    actor.process_due_work();

    // Assert
    assert!(actor.ready_contains(id));
    assert_eq!(actor.admin_snapshot().messages_total, 1);
}

#[test]
fn should_retry_expired_index_cleanup_after_a_storage_failure() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send(Bytes::new(), None);
    clock.advance(Duration::from_secs(1));
    QueueActor::fail_next_redelivery_commit_for_tests();
    actor.process_due_work();
    clock.advance(Duration::from_secs(1));

    // Act
    actor.process_due_work();

    // Assert
    assert_eq!(actor.admin_snapshot().messages_total, 0);
}

#[test]
fn should_expire_a_reconstructed_ready_message_at_its_original_deadline() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(1), None);
    actor.handle_send(Bytes::new(), None);
    let key = actor.queue_key.clone();
    let store = actor.persistence.store.clone();
    drop(actor);
    clock.advance(Duration::from_secs(1));
    let mut actor = QueueActor::with_clock(
        key.family,
        key,
        store,
        Box::new(clock),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(matches!(response, QueueResponse::Received { messages } if messages.is_empty()));
    assert_eq!(actor.admin_snapshot().messages_total, 0);
}

#[test]
fn should_recover_a_flushed_dead_letter_retention_deadline_without_resetting_it() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let store = super::fast_flush::local_engine(directory.path());
    let family = store.create_column_family("queue").unwrap();
    let mut key = unique_queue_key("ttl-reopen");
    key.family = RouteFamily::new(family.id());
    let clock = MockClock::new();
    let mut actor = QueueActor::with_clock(
        key.family,
        key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    )
    .with_retention(QueueRetention {
        message_seconds: Some(100),
        dlq_seconds: Some(10),
    });
    let (id, _) = send_and_reserve_single_message(&mut actor, "job");
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();
    let expected = actor.load_record_metadata_from_store(id).unwrap();
    store.flush_cf(&family).unwrap();
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(store);

    // Act
    let store = super::fast_flush::local_engine(directory.path());
    let actor = QueueActor::with_clock(
        key.family,
        key,
        store.clone(),
        Box::new(clock),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let actual = actor.load_record_metadata_from_store(id).unwrap();

    // Assert
    assert_eq!(actual.expires_at_ms, expected.expires_at_ms);
    assert_eq!(
        actual.retention_expires_at_ms,
        expected.retention_expires_at_ms
    );
    drop(actor);
    crate::testkit::midge::shutdown_test_engine(store);
}

#[test]
fn should_retain_a_hydration_failure_with_its_original_deadline() {
    // Arrange
    let clock = MockClock::new();
    let mut actor = retained_actor(&clock, Some(100), Some(10));
    let QueueResponse::Sent { id } = actor.handle_send(Bytes::new(), None) else {
        panic!("enqueued");
    };
    let expected = actor
        .load_record_metadata_from_store(id)
        .unwrap()
        .expires_at_ms;
    actor.evict_cached_record(id);
    actor.evict_cached_body(id);
    let mut txn = actor
        .persistence
        .store
        .begin(
            1,
            super::super::recovery_store::QueueTransactionMode::ReadWrite,
        )
        .unwrap();
    txn.delete(actor.cached_body_key(id)).unwrap();
    txn.commit().unwrap();

    // Act
    actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    let record = actor.load_record_metadata_from_store(id).unwrap();
    assert_eq!(record.dlq_reason, Some(DlqReason::HydrationFailed));
    assert_eq!(record.expires_at_ms, expected);
    assert_eq!(
        record.retention_expires_at_ms,
        Some(clock.now_epoch_ms() + 10_000)
    );
}
