use super::*;
use crate::domains::queue::actor::recovery_store::QueueStore;

fn actor() -> QueueActor {
    let mut key = unique_queue_key("ack-admission");
    key.family = RouteFamily::new(1);
    QueueActor::new(
        RouteFamily::new(1),
        key,
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        None,
        crate::utils::idempotency::default_dedup_store(),
    )
}

#[test]
fn should_enqueue_when_l0_admission_closes_after_pressure_wait() {
    // Arrange
    let mut actor = actor();
    let next_id = actor.next_id;
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "column family 1 has no free L0 slot (15/14)".into(),
    ));

    // Act
    let response = actor.handle_send(Bytes::from_static(b"work"), None);

    // Assert
    assert_eq!(
        response,
        QueueResponse::Sent {
            id: MessageId::new(next_id)
        }
    );
    assert_eq!(actor.next_id, next_id + 1);
    assert_eq!(actor.ready_count, 1);
    assert!(actor
        .load_record_metadata_from_store(MessageId::new(next_id))
        .is_ok());
    assert_eq!(
        actor.load_body_from_store(MessageId::new(next_id)).unwrap(),
        Bytes::from_static(b"work")
    );
}

#[test]
fn should_enqueue_batch_once_when_l0_admission_closes_after_pressure_wait() {
    // Arrange
    let mut actor = actor();
    let next_id = actor.next_id;
    let reserved_limit = actor
        .reserved_id_limit_for(2)
        .unwrap_or(actor.next_id_limit);
    let items = [
        (Bytes::from_static(b"ready"), None),
        (Bytes::from_static(b"delayed"), Some(60)),
    ];
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "column family 1 has no free L0 slot (15/14)".into(),
    ));

    // Act
    let response = actor.handle_send_batch(&items);

    // Assert
    let ids = vec![MessageId::new(next_id), MessageId::new(next_id + 1)];
    assert_eq!(response, QueueResponse::SentBatch { ids: ids.clone() });
    assert_eq!(actor.next_id, next_id + 2);
    assert_eq!(actor.ready_count, 1);
    assert_eq!(actor.persisted_delayed.len(), 1);
    for (id, (body, _)) in ids.iter().zip(&items) {
        assert_eq!(actor.load_body_from_store(*id).unwrap(), *body);
    }
    let ready = actor.load_record_metadata_from_store(ids[0]).unwrap();
    let delayed = actor.load_record_metadata_from_store(ids[1]).unwrap();
    let visible_at = ready.first_enqueued_at_ms + 60_000;
    assert_eq!(delayed.first_enqueued_at_ms, ready.first_enqueued_at_ms);
    assert_eq!(delayed.visible_at_ms, visible_at);
    let recovery = &actor.persistence.recovery;
    let snapshot = recovery.snapshot().unwrap();
    let index = recovery
        .read_index(&snapshot)
        .unwrap_or_else(|_| panic!("persisted index must decode"));
    assert_eq!(index.ready_count, 1);
    assert_eq!(index.delayed_count, 1);
    assert_eq!(index.next_delayed_visibility_ms, Some(visible_at));
    assert_eq!(recovery.next_id(&snapshot).unwrap(), Some(reserved_limit));
    let ranges: Vec<_> = recovery
        .ready_ranges(&snapshot)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        ranges,
        vec![ReadyRange {
            next: next_id,
            end: next_id
        }]
    );
    let delayed_index: Vec<_> = recovery
        .delayed_entries(&snapshot)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(delayed_index, vec![(visible_at, ids[1])]);
}

#[test]
fn should_leave_unknown_enqueue_commit_outcome_terminal() {
    // Arrange
    let mut actor = actor();
    let next_id = actor.next_id;
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::Timeout(
        "commit outcome unknown".into(),
    ));

    // Act
    let response = actor.handle_send(Bytes::from_static(b"work"), None);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.next_id, next_id);
    assert_eq!(actor.ready_count, 0);
    assert!(actor
        .load_record_metadata_from_store(MessageId::new(next_id))
        .is_err());
}

#[test]
fn should_leave_unknown_batch_enqueue_commit_outcome_terminal() {
    // Arrange
    let mut actor = actor();
    let next_id = actor.next_id;
    let items = [
        (Bytes::from_static(b"first"), None),
        (Bytes::from_static(b"second"), None),
    ];
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::Timeout(
        "commit outcome unknown".into(),
    ));

    // Act
    let response = actor.handle_send_batch(&items);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.next_id, next_id);
    assert_eq!(actor.ready_count, 0);
    for id in [MessageId::new(next_id), MessageId::new(next_id + 1)] {
        assert!(actor.load_record_metadata_from_store(id).is_err());
    }
}

#[test]
fn should_ack_when_l0_admission_closes_after_pressure_wait() {
    // Arrange
    let mut actor = actor();
    let (id, token) = send_and_reserve_single_message(&mut actor, "work");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "column family 1 has no free L0 slot (15/14)".into(),
    ));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert_eq!(response, QueueResponse::Acked);
    assert!(!actor.inflight.contains_key(&id));
    assert!(actor.load_record_metadata_from_store(id).is_err());
}

#[test]
fn should_leave_unknown_ack_commit_outcome_terminal() {
    // Arrange
    let mut actor = actor();
    let (id, token) = send_and_reserve_single_message(&mut actor, "work");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::Timeout(
        "commit outcome unknown".into(),
    ));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert!(actor.inflight.contains_key(&id));
    assert!(actor.load_record_metadata_from_store(id).is_ok());
}

#[test]
fn should_ack_batch_when_l0_admission_closes_after_pressure_wait() {
    // Arrange
    let mut actor = actor();
    let first = send_and_reserve_single_message(&mut actor, "first");
    let second = send_and_reserve_single_message(&mut actor, "second");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "column family 1 has no free L0 slot (15/14)".into(),
    ));

    // Act
    let responses = actor.handle_ack_batch_for_session(TEST_SESSION_ID, &[first, second]);

    // Assert
    assert_eq!(responses, vec![QueueResponse::Acked; 2]);
    for (id, _) in [first, second] {
        assert!(!actor.inflight.contains_key(&id));
        assert!(actor.load_record_metadata_from_store(id).is_err());
    }
}

#[test]
fn should_leave_unclassified_write_stall_terminal() {
    // Arrange
    let mut actor = actor();
    let (id, token) = send_and_reserve_single_message(&mut actor, "work");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "post-write durability is stalled".into(),
    ));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert!(actor.inflight.contains_key(&id));
    assert!(actor.load_record_metadata_from_store(id).is_ok());
}

#[test]
fn should_leave_ack_write_conflict_terminal() {
    // Arrange
    let mut actor = actor();
    let (id, token) = send_and_reserve_single_message(&mut actor, "work");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteConflict(
        "record changed since transaction began".into(),
    ));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert!(actor.inflight.contains_key(&id));
    assert!(actor.load_record_metadata_from_store(id).is_ok());
}

#[test]
fn should_leave_other_family_admission_rejection_terminal() {
    // Arrange
    let mut actor = actor();
    let (id, token) = send_and_reserve_single_message(&mut actor, "work");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "column family 2 has no free L0 slot (15/14)".into(),
    ));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert!(actor.inflight.contains_key(&id));
    assert!(actor.load_record_metadata_from_store(id).is_ok());
}

#[test]
fn should_leave_unknown_batch_ack_commit_outcome_terminal() {
    // Arrange
    let mut actor = actor();
    let first = send_and_reserve_single_message(&mut actor, "first");
    let second = send_and_reserve_single_message(&mut actor, "second");
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::Timeout(
        "commit outcome unknown".into(),
    ));

    // Act
    let responses = actor.handle_ack_batch_for_session(TEST_SESSION_ID, &[first, second]);

    // Assert
    assert!(responses
        .iter()
        .all(|response| matches!(response, QueueResponse::Error { .. })));
    for (id, _) in [first, second] {
        assert!(actor.inflight.contains_key(&id));
        assert!(actor.load_record_metadata_from_store(id).is_ok());
    }
}

#[test]
fn should_bound_repeated_ack_admission_rejections_to_one_budget() {
    // Arrange
    let mut actor = actor();
    let (id, _) = send_and_reserve_single_message(&mut actor, "work");
    let started = Instant::now();
    let mut attempts = 0;

    // Act
    let result = actor.commit_ack_with_admission_budget(Duration::from_millis(30), |txn| {
        attempts += 1;
        QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
            "column family 1 has no free L0 slot (15/14)".into(),
        ));
        txn.delete(actor.cached_header_key(id))
            .map_err(|error| error.to_string())
    });

    // Assert
    assert!(result.is_err());
    assert!(attempts >= 1);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(actor.load_record_metadata_from_store(id).is_ok());
    // If admission expired before the injected rejection, discard the test hook.
    QueueStore::clear_commit_error_for_tests();
}

#[test]
fn should_count_l0_admission_retry_given_rejection_before_commit() {
    // Arrange
    let mut actor = actor();
    let metric = crate::domains::queue::metrics::METRIC_L0_ADMISSION_RETRIES_TOTAL;
    let before = crate::observability::metrics().counter_get(metric);
    QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
        "column family 1 has no free L0 slot (15/14)".into(),
    ));

    // Act
    let response = actor.handle_send(Bytes::from_static(b"work"), None);

    // Assert
    assert!(matches!(response, QueueResponse::Sent { .. }));
    assert!(crate::observability::metrics().counter_get(metric) > before);
}

#[test]
fn should_count_admission_budget_exhaustion_given_l0_rejections_past_deadline() {
    // Arrange
    let mut actor = actor();
    let (id, _) = send_and_reserve_single_message(&mut actor, "work");
    let metric = crate::domains::queue::metrics::METRIC_ADMISSION_BUDGET_EXHAUSTED_TOTAL;
    let before = crate::observability::metrics().counter_get(metric);

    // Act
    let result = actor.commit_ack_with_admission_budget(Duration::from_millis(30), |txn| {
        QueueStore::fail_next_commit_for_tests(cntryl_midge::MidgeError::WriteStall(
            "column family 1 has no free L0 slot (15/14)".into(),
        ));
        txn.delete(actor.cached_header_key(id))
            .map_err(|error| error.to_string())
    });
    QueueStore::clear_commit_error_for_tests();

    // Assert
    assert!(result.is_err());
    assert!(crate::observability::metrics().counter_get(metric) > before);
}
