use super::super::write_admission::{inject_commit_outcomes, override_admission_budget};
use super::*;

fn rejection() -> cntryl_midge::MidgeError {
    cntryl_midge::MidgeError::WriteStall("column family 1 has no free L0 slot (15/14)".to_string())
}

fn store() -> StreamStore {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store.ensure_layout_activation_for_family(1).unwrap();
    store
}

#[test]
fn should_retry_global_reservation_rejected_before_wal() {
    // Arrange
    let store = store();
    let _outcomes = inject_commit_outcomes([Some(rejection())]);

    // Act
    let reservation = store.reserve_global_range(1, 3);

    // Assert
    assert_eq!(reservation.unwrap().first_offset, 0);
    assert_eq!(store.reserve_global_range(1, 1).unwrap().first_offset, 3);
}

#[test]
fn should_not_retry_global_reservation_with_unknown_commit_outcome() {
    // Arrange
    let store = store();
    let _outcomes = inject_commit_outcomes([Some(cntryl_midge::MidgeError::Timeout(
        "indeterminate".to_string(),
    ))]);

    // Act
    let reservation = store.reserve_global_range(1, 3);

    // Assert
    assert!(reservation.err().unwrap().contains("indeterminate"));
    assert_eq!(store.reserve_global_range(1, 1).unwrap().first_offset, 0);
}

#[test]
fn should_resolve_applied_unknown_global_reservation_before_later_commit() {
    // Arrange
    let store = store();
    assert_eq!(store.get_global_watermark(1).expect("initialize family"), 0);
    let failed_events = single_event(b"unknown reservation");
    let later_events = single_event(b"later committed event");
    let _outcome = super::super::write_admission::inject_commit_outcomes_after_apply([Some(
        cntryl_midge::MidgeError::OutcomeUnknown("reservation applied; response lost".to_string()),
    )]);

    // Act
    let failed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        expected_resource_next_offset: 0,
        events: &failed_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let later = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        expected_resource_next_offset: 0,
        events: &later_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let watermark = store
        .get_global_watermark(1)
        .expect("read global watermark");
    let (records, _) = store
        .read_global(1, 0, 10, None, None)
        .expect("read global stream");

    // Assert
    assert!(failed
        .expect_err("reservation result must remain unknown")
        .contains("OutcomeUnknown"));
    assert!(
        later.is_ok(),
        "later append should commit after the lost reservation"
    );
    assert_eq!(
        watermark, 2,
        "the applied reservation must be resolved as a skip"
    );
    assert_eq!(event_records(records).len(), 1);
}

#[test]
fn should_recover_applied_unknown_promotion_before_later_commit() {
    // Arrange
    let store = store();
    assert_eq!(store.get_global_watermark(1).expect("initialize family"), 0);
    let uncertain_events = single_event(b"uncertain committed event");
    let later_events = single_event(b"later committed event");
    let _outcomes = super::super::write_admission::inject_commit_outcomes_after_apply([
        None,
        Some(cntryl_midge::MidgeError::OutcomeUnknown(
            "promotion applied; response lost".to_string(),
        )),
    ]);

    // Act
    let uncertain = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "uncertain",
        expected_resource_next_offset: 0,
        events: &uncertain_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let later = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "later",
        expected_resource_next_offset: 0,
        events: &later_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let watermark = store
        .get_global_watermark(1)
        .expect("read global watermark");
    let (records, _) = store
        .read_global(1, 0, 10, None, None)
        .expect("read global stream");

    // Assert
    assert!(uncertain
        .expect_err("promotion result must remain unknown")
        .contains("OutcomeUnknown"));
    assert!(later.is_ok(), "later append should commit after recovery");
    assert_eq!(watermark, 2);
    assert_eq!(event_records(records).len(), 2);
}

#[test]
fn should_retry_unapplied_unknown_promotion_after_resolving_its_reservation() {
    // Arrange
    let store = store();
    assert_eq!(store.get_global_watermark(1).expect("initialize family"), 0);
    let events = single_event(b"retryable uncertain event");
    let _outcomes = inject_commit_outcomes([
        None,
        Some(cntryl_midge::MidgeError::OutcomeUnknown(
            "promotion did not apply; response lost".to_string(),
        )),
    ]);

    // Act
    let uncertain = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "retryable",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let retried = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "retryable",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let watermark = store
        .get_global_watermark(1)
        .expect("read global watermark");
    let (records, _) = store
        .read_global(1, 0, 10, None, None)
        .expect("read global stream");

    // Assert
    assert!(uncertain
        .expect_err("promotion result must remain unknown")
        .contains("OutcomeUnknown"));
    assert!(retried.is_ok(), "unapplied event should be retryable");
    assert_eq!(watermark, 2);
    assert_eq!(event_records(records).len(), 1);
}

#[test]
fn should_retry_promotion_rejected_before_wal_without_allocating_another_range() {
    // Arrange
    let store = store();
    let events = single_event(b"one");
    let _outcomes = inject_commit_outcomes([None, Some(rejection())]);

    // Act
    let committed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });

    // Assert
    assert_eq!(committed.unwrap().first_global_offset, 0);
    assert_eq!(store.reserve_global_range(1, 1).unwrap().first_offset, 1);
    let records = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        })
        .unwrap();
    assert_eq!(records.0.len(), 1);
}

#[test]
fn should_retry_global_watermark_rejected_before_wal() {
    // Arrange
    let store = store();
    let _outcomes = inject_commit_outcomes([Some(rejection())]);

    // Act
    let persisted = store.set_global_watermark(1, 12);

    // Assert
    assert!(persisted.is_ok());
    let transaction = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let bytes = transaction
        .get(&encode_global_watermark_key())
        .unwrap()
        .unwrap();
    assert_eq!(WatermarkValue::decode(&bytes).unwrap().watermark, 12);
}

#[test]
fn should_not_retry_promotion_with_unknown_commit_outcome() {
    // Arrange
    let store = store();
    let events = single_event(b"one");
    let _outcomes = inject_commit_outcomes([
        None,
        Some(cntryl_midge::MidgeError::Timeout(
            "indeterminate".to_string(),
        )),
    ]);

    // Act
    let committed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });

    // Assert
    assert!(committed.err().unwrap().contains("indeterminate"));
}

#[test]
fn should_not_retry_global_watermark_with_unknown_commit_outcome() {
    // Arrange
    let store = store();
    let _outcomes = inject_commit_outcomes([Some(cntryl_midge::MidgeError::Timeout(
        "indeterminate".to_string(),
    ))]);

    // Act
    let persisted = store.set_global_watermark(1, 12);

    // Assert
    assert!(persisted.err().unwrap().contains("indeterminate"));
}

#[test]
fn should_stop_global_reservation_retry_at_the_original_admission_deadline() {
    // Arrange
    let store = store();
    let _budget = override_admission_budget(std::time::Duration::ZERO);
    let _outcomes = inject_commit_outcomes([
        Some(rejection()),
        Some(cntryl_midge::MidgeError::Timeout(
            "attempted retry after expiration".to_string(),
        )),
    ]);

    // Act
    let reservation = store.reserve_global_range(1, 1);

    // Assert
    assert!(reservation
        .err()
        .unwrap()
        .contains("admission remained stalled"));
}

#[test]
fn should_stop_promotion_retry_at_the_original_admission_deadline() {
    // Arrange
    let store = store();
    let events = single_event(b"one");
    let _budget = override_admission_budget(std::time::Duration::ZERO);
    let _outcomes = inject_commit_outcomes([
        None,
        Some(rejection()),
        Some(cntryl_midge::MidgeError::Timeout(
            "attempted retry after expiration".to_string(),
        )),
    ]);

    // Act
    let committed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });

    // Assert
    assert!(committed
        .err()
        .unwrap()
        .contains("admission remained stalled"));
}

#[test]
fn should_stop_global_watermark_retry_at_the_original_admission_deadline() {
    // Arrange
    let store = store();
    let _budget = override_admission_budget(std::time::Duration::ZERO);
    let _outcomes = inject_commit_outcomes([
        Some(rejection()),
        Some(cntryl_midge::MidgeError::Timeout(
            "attempted retry after expiration".to_string(),
        )),
    ]);

    // Act
    let persisted = store.set_global_watermark(1, 12);

    // Assert
    assert!(persisted
        .err()
        .unwrap()
        .contains("admission remained stalled"));
}
