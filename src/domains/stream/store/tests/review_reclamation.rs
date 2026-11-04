use super::*;
use crate::domains::stream::StreamDropScope;
use crate::runtime::routing::RouteFamily;

fn scope(selector: &str) -> StreamDropScope {
    StreamDropScope::parse(RouteFamily::new(1), selector).unwrap()
}

fn append(store: &StreamStore, resource: &str, expected: u64, count: usize) {
    let events = vec![
        EventPayload {
            body: Bytes::from(vec![0xAB; INLINE_PAYLOAD_LIMIT + 1]),
            metadata: None,
            discriminator: None,
        };
        count
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource,
            expected_resource_next_offset: expected,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
}

fn compactable_history_during_drop() -> StreamStore {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for offset in 0..9 {
        append(&store, "retained", offset, 1);
    }
    append(&store, "deleted", 0, 130);
    store
        .drop_scope(
            &scope("stream://north/orders/deleted"),
            StreamWriteMode::Sync,
        )
        .unwrap();
    store
}

fn queued_compaction_during_drop() -> StreamStore {
    let store = compactable_history_during_drop();
    let key = encode_compact_resource_page_key("north", "orders", "retained", 0);
    let mut queues = store.maintenance_queues.lock();
    let queue = queues.entry(1).or_default();
    queue.initialized = true;
    queue.buckets.clear();
    queue.buckets.insert(key[..key.len() - 16].to_vec());
    drop(queues);
    store
}

fn retained_fragment_count(store: &StreamStore) -> usize {
    store
        .db
        .read_tx(1)
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compact_resource_page_prefix("north", "orders", "retained"),
        )))
        .unwrap()
        .try_collect()
        .unwrap()
        .len()
}

#[test]
fn should_compact_retained_history_before_a_drop_sweep_finishes() {
    // Arrange
    let store = queued_compaction_during_drop();
    assert_eq!(retained_fragment_count(&store), 9);

    // Act
    store.run_maintenance(1).unwrap();
    store.run_maintenance(1).unwrap();

    // Assert
    assert_eq!(retained_fragment_count(&store), 1);
    assert!(store.has_pending_drop_reclamation(1));
}

#[test]
fn should_allow_normal_compaction_after_a_drop_sweep_decode_error() {
    // Arrange
    let store = queued_compaction_during_drop();
    let mut transaction = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    transaction
        .put(
            encode_compact_global_page_key(0),
            b"bad global page".to_vec(),
            None,
        )
        .unwrap();
    transaction
        .commit(cntryl_midge::WriteOptions::sync())
        .unwrap();

    // Act
    let first = store.run_maintenance(1);
    let second = store.run_maintenance(1);

    // Assert
    assert!(first.is_err());
    assert!(second.is_ok(), "{second:?}");
    assert_eq!(retained_fragment_count(&store), 1);
    assert!(store.has_pending_drop_reclamation(1));
}

#[test]
fn should_compact_unrelated_fragments_despite_a_corrupt_queued_global_group() {
    // Arrange
    let store = compactable_history_during_drop();
    let mut transaction = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    transaction
        .put(
            encode_compact_global_page_key(0),
            b"bad global page".to_vec(),
            None,
        )
        .unwrap();
    transaction
        .commit(cntryl_midge::WriteOptions::sync())
        .unwrap();
    let mut errors = 0;

    // Act
    for _ in 0..64 {
        errors += usize::from(store.run_maintenance(1).is_err());
        if retained_fragment_count(&store) == 1 {
            break;
        }
    }

    // Assert
    assert!(errors > 0);
    assert_eq!(retained_fragment_count(&store), 1);
    assert!(store.has_pending_drop_reclamation(1));
}

#[test]
fn should_not_queue_payload_reclamation_for_absent_scopes() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "retained", 0, 1);
    let selectors = [
        "stream://missing/*",
        "stream://north/missing/*",
        "stream://north/orders/missing",
    ];

    // Act
    for selector in selectors {
        store
            .drop_scope(&scope(selector), StreamWriteMode::Sync)
            .unwrap();

        // Assert
        assert!(
            !store.has_pending_drop_reclamation(1),
            "{selector} queued a family-wide sweep"
        );
    }
}

#[test]
fn should_not_restart_completed_cleanup_after_an_unrelated_append() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "deleted", 0, 1);
    let deleted = scope("stream://north/orders/deleted");
    store.drop_scope(&deleted, StreamWriteMode::Sync).unwrap();
    drain_maintenance(&store, 1);
    append(&store, "retained", 0, 1);

    // Act
    store.drop_scope(&deleted, StreamWriteMode::Sync).unwrap();

    // Assert
    assert!(!store.has_pending_drop_reclamation(1));
    assert!(store
        .db
        .read_tx(1)
        .get(&encode_payload_blob_key(1))
        .unwrap()
        .is_some());
}

#[test]
fn should_count_only_newly_scrubbed_payloads_when_revisiting_a_bucket() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "first", 0, 1);
    append(&store, "second", 0, 1);
    store
        .drop_scope(&scope("stream://north/orders/first"), StreamWriteMode::Sync)
        .unwrap();
    let first = store.run_drop_reclamation(1).unwrap().unwrap();
    store
        .drop_scope(
            &scope("stream://north/orders/second"),
            StreamWriteMode::Sync,
        )
        .unwrap();

    // Act
    let second = store.run_drop_reclamation(1).unwrap().unwrap();

    // Assert
    assert_eq!(first.records_compacted, 1);
    assert_eq!(second.records_compacted, 1);
    for offset in 0..2 {
        assert!(store
            .db
            .read_tx(1)
            .get(&encode_payload_blob_key(offset))
            .unwrap()
            .is_none());
    }
}

#[test]
fn should_queue_reclamation_for_an_append_committed_after_an_empty_scope_snapshot() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(engine.clone());
    store.ensure_layout_activation_for_family(1).unwrap();
    let writer = StreamStore::new(engine);
    writer.ensure_layout_activation_for_family(1).unwrap();
    let reservation = writer.reserve_global_range(1, 1).unwrap();
    let _hook =
        super::super::write_admission::inject_commit_hook(Box::new(move |transaction, options| {
            let events = vec![EventPayload {
                body: Bytes::from(vec![0xAA; INLINE_PAYLOAD_LIMIT + 1]),
                metadata: None,
                discriminator: None,
            }];
            writer
                .commit_promotion_frontier_batch(CommitPromotionFrontierBatchParams {
                    family: 1,
                    realm: "north",
                    area: "orders",
                    resource: "racing",
                    first_resource_offset: 0,
                    first_area_offset: 0,
                    first_realm_offset: 0,
                    first_global_offset: reservation.first_offset,
                    writer_epoch: reservation.writer_epoch,
                    expected_drop_cutoff: 0,
                    events: &events,
                    committed_size_before: 0,
                    ingest_metadata: None,
                    mode: StreamWriteMode::Sync,
                })
                .map_err(super::super::PromotionCommitFailure::into_message)
                .unwrap();
            transaction.commit(options)
        }));

    // Act
    store
        .drop_scope(&scope("stream://north/orders/*"), StreamWriteMode::Sync)
        .unwrap();
    let pending = store.has_pending_drop_reclamation(1);
    drain_maintenance(&store, 1);

    // Assert
    assert!(pending);
    assert!(store
        .db
        .read_tx(1)
        .get(&encode_payload_blob_key(reservation.first_offset))
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "racing")
            .unwrap(),
        0
    );
}

#[test]
fn should_reclaim_recoverable_history_when_resource_metadata_is_missing() {
    // Arrange
    for selector in [
        "stream://north/orders/recovered",
        "stream://north/orders/*",
        "stream://north/*",
    ] {
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        append(&store, "recovered", 0, 1);
        let mut transaction = store
            .db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        transaction
            .delete(encode_resource_meta_key("north", "orders", "recovered"))
            .unwrap();
        transaction
            .commit(cntryl_midge::WriteOptions::sync())
            .unwrap();
        assert_eq!(
            store
                .get_next_resource_offset(1, "north", "orders", "recovered")
                .unwrap(),
            1
        );

        // Act
        store
            .drop_scope(&scope(selector), StreamWriteMode::Sync)
            .unwrap();
        let pending = store.has_pending_drop_reclamation(1);
        drain_maintenance(&store, 1);

        // Assert
        assert!(pending, "{selector} skipped recoverable resource history");
        assert!(store
            .db
            .read_tx(1)
            .get(&encode_payload_blob_key(0))
            .unwrap()
            .is_none());
    }
}
