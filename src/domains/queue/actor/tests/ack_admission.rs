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
