use super::*;
use crate::runtime::routing::RouteFamily;

fn seeded_queue() -> (QueueActor, Arc<QueueRecoveryStore>) {
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let key = QueueKey {
        family: RouteFamily::new(1),
        realm: "recovery".to_string(),
        area: "jobs".to_string(),
        resource: "snapshot".to_string(),
    };
    let mut actor = QueueActor::new(
        key.family,
        key,
        engine,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"first"), None),
        super::super::QueueResponse::Sent { .. }
    ));
    let store = actor.persistence.recovery.clone();
    (actor, store)
}

#[test]
fn should_read_index_rows_from_the_same_snapshot_as_metadata() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let snapshot = store.snapshot().expect("read recovery snapshot");
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"second"), None),
        super::super::QueueResponse::Sent { .. }
    ));

    // Act
    let ranges = store
        .ready_ranges(&snapshot)
        .expect("read old snapshot rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("decode ranges");

    // Assert
    assert_eq!(
        store
            .read_index(&snapshot)
            .unwrap_or_else(|_| panic!("index metadata"))
            .ready_count,
        1
    );
    assert_eq!(ranges, vec![ReadyRange { next: 1, end: 1 }]);
}

#[test]
fn should_preserve_previous_index_when_replacement_commit_fails() {
    // Arrange
    let (_actor, store) = seeded_queue();
    let delayed = FastMap::default();
    let dlq = FastMap::default();
    let replacement = QueueIndexRebuild {
        meta: IndexMetaSnapshot {
            next_id: 2,
            ready_count: 0,
            delayed_count: 0,
            next_delayed_visibility_ms: None,
        },
        ready: &[],
        delayed: &delayed,
        dlq: &dlq,
    };

    // Act
    let result = store.replace_index(&replacement, WritePolicy::CloudStrict);

    // Assert
    assert!(result.is_err(), "cloud policy must fail on local storage");
    let snapshot = store.snapshot().expect("read original index");
    assert_eq!(
        store
            .read_index(&snapshot)
            .unwrap_or_else(|_| panic!("index metadata"))
            .ready_count,
        1
    );
    let ranges = store
        .ready_ranges(&snapshot)
        .expect("read original rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("decode original ranges");
    assert_eq!(ranges, vec![ReadyRange { next: 1, end: 1 }]);
}

#[test]
fn should_read_reserved_id_from_recovery_snapshot_after_concurrent_commit() {
    // Arrange
    let (_actor, store) = seeded_queue();
    let snapshot = store.snapshot().expect("read snapshot");
    let expected = store
        .read_index(&snapshot)
        .unwrap_or_else(|_| panic!("index metadata"))
        .next_id;
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin concurrent writer");
    write
        .put(
            QueueActor::meta_key(&store.key),
            999_999_u64.to_le_bytes().to_vec(),
            None,
        )
        .expect("advance ID reservation");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit reservation");

    // Act
    let reserved = store
        .next_id(&snapshot)
        .expect("read reservation row")
        .expect("reservation row exists");

    // Assert
    assert_eq!(reserved, expected);
}

#[test]
fn should_use_authoritative_reservation_when_index_hit_has_stale_next_id() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let super::super::QueueResponse::Received { messages } =
        actor.handle_receive_for_session(1, 30, Some(1))
    else {
        panic!("reserve seeded queue message");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(
        actor.handle_ack_for_session(1, messages[0].id, messages[0].token),
        super::super::QueueResponse::Acked
    );
    let reserved = store
        .next_id(&store.snapshot().expect("read queue snapshot"))
        .expect("read reservation row")
        .expect("reservation row exists");
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin stale index write");
    write
        .put(
            store.index_meta_key.clone(),
            QueueActor::encode_index_meta(1, 0, 0, None),
            None,
        )
        .expect("write stale index next ID");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit stale index");

    // Act
    actor.recover_from_store().expect("recover empty queue");

    // Assert
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.next_id, reserved);
    assert_eq!(actor.recovery_path, super::super::RecoveryPath::Empty);
}

#[test]
fn should_use_authoritative_reservation_when_index_counters_are_invalid() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let original = store.snapshot().expect("read original snapshot");
    let reserved = store
        .next_id(&original)
        .expect("read reservation row")
        .expect("reservation row exists");
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin corrupt index write");
    write
        .put(
            store.index_meta_key.clone(),
            QueueActor::encode_index_meta(999_999, 2, 0, None),
            None,
        )
        .expect("write invalid counters and ID");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit corrupt index");

    // Act
    let recovery = actor.recover_from_store();

    // Assert
    recovery.expect("recover authoritative headers");
    assert_eq!(actor.next_id, reserved);
}

#[test]
fn should_use_authoritative_reservation_when_index_rows_are_invalid_and_queue_is_empty() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let super::super::QueueResponse::Received { messages } =
        actor.handle_receive_for_session(1, 30, Some(1))
    else {
        panic!("reserve seeded queue message");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(
        actor.handle_ack_for_session(1, messages[0].id, messages[0].token),
        super::super::QueueResponse::Acked
    );
    let reserved = store
        .next_id(&store.snapshot().expect("read queue snapshot"))
        .expect("read reservation row")
        .expect("reservation row exists");
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin corrupt index write");
    write
        .put(
            store.index_meta_key.clone(),
            QueueActor::encode_index_meta(1, 0, 0, None),
            None,
        )
        .expect("write stale index next ID");
    write
        .put(
            QueueActor::ready_range_key_with_prefix(&store.ready_index_prefix, 1, 1),
            vec![0],
            None,
        )
        .expect("write malformed ready index row");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit corrupt index");

    // Act
    actor.recover_from_store().expect("recover empty queue");

    // Assert
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.next_id, reserved);
}

#[test]
fn should_fail_reservation_recovery_when_persisted_row_cannot_be_read() {
    // Arrange
    let error = QueueStoreError {
        message: "injected reservation read failure".to_string(),
        midge_error: None,
        admission_stalled: false,
    };

    // Act
    let result = QueueRecoveryStore::decode_reservation_id(Err(error));

    // Assert
    assert!(result
        .expect_err("an unreadable reservation cannot prove a safe ID")
        .contains("Failed to read queue ID reservation"));
}

#[test]
fn should_reject_malformed_reservation_encoding_during_recovery() {
    // Arrange
    let malformed = Some(Bytes::from_static(b"bad"));

    // Act
    let result = QueueRecoveryStore::decode_reservation_id(Ok(malformed));

    // Assert
    assert!(result
        .expect_err("a malformed reservation cannot prove a safe ID")
        .contains("invalid encoding"));
}

#[test]
fn should_initialize_new_queue_when_reservation_row_is_absent() {
    // Arrange
    let reservation = None;

    // Act
    let result = QueueRecoveryStore::decode_reservation_id(Ok(reservation));

    // Assert
    assert_eq!(result, Ok(None));
}

#[test]
fn should_fail_closed_when_existing_index_has_no_reservation_row() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let super::super::QueueResponse::Received { messages } =
        actor.handle_receive_for_session(1, 30, Some(1))
    else {
        panic!("reserve seeded queue message");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(
        actor.handle_ack_for_session(1, messages[0].id, messages[0].token),
        super::super::QueueResponse::Acked
    );
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin reservation removal");
    write
        .delete(QueueActor::meta_key(&store.key))
        .expect("remove reservation row");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit reservation removal");

    // Act
    let result = actor.recover_from_store();

    // Assert
    assert!(result
        .expect_err("existing index without its reservation must fail closed")
        .contains("reservation is missing"));
}

#[test]
fn should_fail_closed_when_header_scan_finds_messages_without_reservation_metadata() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin reservation and index removal");
    write
        .delete(QueueActor::meta_key(&store.key))
        .expect("remove reservation row");
    write
        .delete(store.index_meta_key.clone())
        .expect("remove index metadata");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit metadata removal");

    // Act
    let result = actor.recover_from_store();

    // Assert
    assert!(result
        .expect_err("live headers cannot recover acknowledged ID history")
        .contains("reservation is missing"));
}

#[test]
fn should_fail_closed_when_header_ids_exceed_reservation_metadata() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin stale reservation write");
    write
        .put(store.meta_key.clone(), 1_u64.to_le_bytes().to_vec(), None)
        .expect("write stale reservation row");
    write
        .delete(store.index_meta_key.clone())
        .expect("remove index metadata to force header scan");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit stale reservation");

    // Act
    let result = actor.recover_from_store();

    // Assert
    assert!(result
        .expect_err("header IDs at or above the reservation cannot prove a safe next ID")
        .contains("below the recovered queue IDs"));
}

#[test]
fn should_decode_header_rows_only_as_consumed() {
    // Arrange
    let (_actor, store) = seeded_queue();
    let mut write = store
        .store
        .begin(store.key.family.id(), QueueTransactionMode::ReadWrite)
        .expect("begin corrupt header write");
    write
        .put(
            QueueActor::cached_id_key(&store.header_key_prefix, MessageId::new(2)),
            vec![0],
            None,
        )
        .expect("write malformed later header");
    write
        .commit(WritePolicy::Buffered)
        .expect("commit malformed header");
    let snapshot = store.snapshot().expect("read snapshot");

    // Act
    let first = store
        .headers(&snapshot)
        .expect("open header scan")
        .take(1)
        .collect::<Result<Vec<_>, _>>();

    // Assert
    let rows = first.expect("an unread malformed row must not fail an earlier valid row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, MessageId::new(1));
}

/// Fills family 1's L0 slots with compaction disabled until Midge itself
/// rejects a commit, then returns that rejection unmodified.
fn real_midge_l0_rejection() -> QueueStoreError {
    let directory = tempfile::tempdir().expect("create Midge directory");
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .background_compaction(false)
                .build()
                .expect("build Midge options"),
        )
        .expect("open Midge engine"),
    );
    let family = engine
        .create_column_family("cf_1")
        .expect("create column family 1");
    let store = QueueStore::new(engine.clone());
    let mut rejection = None;
    for round in 0_u32..64 {
        let mut txn = store
            .begin(family.id(), QueueTransactionMode::ReadWrite)
            .expect("begin queue transaction");
        txn.put(round.to_be_bytes().to_vec(), b"value".to_vec(), None)
            .expect("stage queue row");
        if let Err(error) = txn
            .inner
            .commit(cntryl_midge::WriteOptions::sync())
            .map_err(QueueStoreError::from_midge)
        {
            rejection = Some(error);
            break;
        }
        engine.flush_cf(&family).expect("publish one L0 file");
    }
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
    rejection.expect("Midge rejects a write once every L0 slot is used")
}

#[test]
fn should_classify_real_midge_l0_rejection_as_retryable_admission() {
    // Arrange
    let rejection = real_midge_l0_rejection();

    // Act
    let retryable = rejection.is_l0_admission_rejection(1);

    // Assert
    assert!(
        retryable,
        "Midge L0 rejection no longer matches Queue retry detection: {rejection}"
    );
    assert!(!rejection.is_l0_admission_rejection(2));
}
