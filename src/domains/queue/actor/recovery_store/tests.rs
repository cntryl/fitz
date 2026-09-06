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
    let store = actor.recovery_store.clone();
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
    let result = store.replace_index(&replacement, WriteOptions::cloud_strict());

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
        .engine
        .begin_tx(store.key.family.id(), TransactionMode::ReadWrite)
        .expect("begin concurrent writer");
    write
        .put(
            QueueActor::meta_key(&store.key),
            999_999_u64.to_le_bytes().to_vec(),
            None,
        )
        .expect("advance ID reservation");
    write
        .commit(WriteOptions::buffered())
        .expect("commit reservation");

    // Act
    let reserved = store.next_id(&snapshot);

    // Assert
    assert_eq!(reserved, expected);
}

#[test]
fn should_use_authoritative_reservation_when_index_counters_are_invalid() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let original = store.snapshot().expect("read original snapshot");
    let reserved = store.next_id(&original);
    let mut write = store
        .engine
        .begin_tx(store.key.family.id(), TransactionMode::ReadWrite)
        .expect("begin corrupt index write");
    write
        .put(
            store.index_meta_key.clone(),
            QueueActor::encode_index_meta(999_999, 2, 0, None),
            None,
        )
        .expect("write invalid counters and ID");
    write
        .commit(WriteOptions::buffered())
        .expect("commit corrupt index");

    // Act
    let recovery = actor.recover_from_store();

    // Assert
    recovery.expect("recover authoritative headers");
    assert_eq!(actor.next_id, reserved);
}

#[test]
fn should_decode_header_rows_only_as_consumed() {
    // Arrange
    let (_actor, store) = seeded_queue();
    let mut write = store
        .engine
        .begin_tx(store.key.family.id(), TransactionMode::ReadWrite)
        .expect("begin corrupt header write");
    write
        .put(
            QueueActor::cached_id_key(&store.header_key_prefix, MessageId::new(2)),
            vec![0],
            None,
        )
        .expect("write malformed later header");
    write
        .commit(WriteOptions::buffered())
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
