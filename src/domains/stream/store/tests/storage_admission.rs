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
