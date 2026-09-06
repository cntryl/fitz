use super::*;
use crate::runtime::routing::RouteFamily;

fn seeded_queue() -> (QueueActor, QueueRecoveryStore) {
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let key = QueueKey {
        family: RouteFamily::new(1),
        realm: "recovery".to_string(),
        area: "jobs".to_string(),
        resource: "snapshot".to_string(),
    };
    let store = QueueRecoveryStore::new(engine.clone(), key.clone());
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
    (actor, store)
}

#[test]
fn should_read_index_rows_from_the_same_snapshot_as_metadata() {
    // Arrange
    let (mut actor, store) = seeded_queue();
    let snapshot = store
        .read_index()
        .unwrap_or_else(|_| panic!("read recovery snapshot"));
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"second"), None),
        super::super::QueueResponse::Sent { .. }
    ));

    // Act
    let ranges = snapshot
        .rows()
        .expect("read old snapshot rows")
        .ready
        .collect::<Result<Vec<_>, _>>()
        .expect("decode ranges");

    // Assert
    assert_eq!(snapshot.meta.ready_count, 1);
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
    let snapshot = store
        .read_index()
        .unwrap_or_else(|_| panic!("read original index"));
    assert_eq!(snapshot.meta.ready_count, 1);
    let ranges = snapshot
        .rows()
        .expect("read original rows")
        .ready
        .collect::<Result<Vec<_>, _>>()
        .expect("decode original ranges");
    assert_eq!(ranges, vec![ReadyRange { next: 1, end: 1 }]);
}
