use super::*;
use crate::domains::stream::StreamDropScope;
use crate::runtime::routing::RouteFamily;

fn append(store: &StreamStore, realm: &str, area: &str, resource: &str, expected: u64) {
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm,
            area,
            resource,
            expected_resource_next_offset: expected,
            events: &single_event(b"history"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
}

fn selector(route: &str) -> StreamDropScope {
    StreamDropScope::parse(RouteFamily::new(1), route).unwrap()
}

#[test]
fn should_reset_only_the_deleted_resource_offset() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    append(&store, "north", "orders", "created2", 0);

    // Act
    store
        .drop_scope(
            &selector("stream://north/orders/created"),
            StreamWriteMode::Sync,
        )
        .unwrap();

    // Assert
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created2")
            .unwrap(),
        1
    );
    assert_eq!(store.realm_visible_end(1, "north").unwrap(), 2);
    assert_eq!(store.get_global_watermark(1).unwrap(), 2);
}

#[test]
fn should_recreate_an_area_from_zero_without_resetting_realm_or_global_order() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    append(&store, "north", "payments", "created", 0);
    store
        .drop_scope(&selector("stream://north/orders/*"), StreamWriteMode::Sync)
        .unwrap();

    // Act
    let result = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &single_event(b"new"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();

    // Assert
    assert_eq!(result.first_resource_offset, 0);
    assert_eq!(result.first_area_offset, 0);
    assert_eq!(result.first_realm_offset, 2);
    assert_eq!(result.first_global_offset, 2);
}

#[test]
fn should_recreate_a_realm_from_zero_without_reusing_global_offsets() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    append(&store, "south", "orders", "created", 0);
    store
        .drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync)
        .unwrap();

    // Act
    let result = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &single_event(b"new"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();

    // Assert
    assert_eq!(result.first_resource_offset, 0);
    assert_eq!(result.first_area_offset, 0);
    assert_eq!(result.first_realm_offset, 0);
    assert_eq!(result.first_global_offset, 2);
}

#[test]
fn should_reclaim_deleted_large_payloads_from_a_single_fragment() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = [EventPayload {
        body: Bytes::from(vec![7; 32 * 1024]),
        metadata: Some(Bytes::from_static(b"private")),
        discriminator: Some(StreamDiscriminator::from("old")),
    }];
    for resource in ["created", "neighbor"] {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource,
                expected_resource_next_offset: 0,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .unwrap();
    }
    store
        .drop_scope(
            &selector("stream://north/orders/created"),
            StreamWriteMode::Sync,
        )
        .unwrap();

    // Act
    drain_maintenance(&store, 1);

    // Assert
    let txn = store.db.read_tx(1);
    assert!(txn.get(&encode_payload_blob_key(0)).unwrap().is_none());
    assert!(txn.get(&encode_payload_blob_key(1)).unwrap().is_some());
    let old = CompactGlobalPageValue::try_decode(
        &txn.get(&encode_compact_global_page_key(0))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(old.records[0].body.is_empty());
    assert!(old.records[0].metadata.is_none());
    assert_eq!(old.records[0].expires_at, Some(0));
    assert!(txn
        .get(&encode_global_discriminator_key(0))
        .unwrap()
        .is_none());
}

#[test]
fn should_keep_recreated_resource_discriminator_during_old_payload_reclamation() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    store
        .drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync)
        .unwrap();
    let events = single_event_with_discriminator(b"new", "new-type");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();

    // Act
    drain_maintenance(&store, 1);

    // Assert
    let txn = store.db.read_tx(1);
    for key in [
        crate::domains::stream::storage::encode_resource_discriminator_key(
            "north", "orders", "created", 0,
        ),
        crate::domains::stream::storage::encode_area_discriminator_key("north", "orders", 0),
        crate::domains::stream::storage::encode_realm_discriminator_key("north", 0),
    ] {
        assert_eq!(txn.get(&key).unwrap().unwrap().as_ref(), b"new-type");
    }
}

#[test]
fn should_resume_drop_reclamation_from_durable_state_in_a_new_store() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(engine.clone());
    append(&store, "north", "orders", "created", 0);
    store
        .drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync)
        .unwrap();
    drop(store);
    let reopened = StreamStore::new(engine);

    // Act
    drain_maintenance(&reopened, 1);

    // Assert
    assert_eq!(
        reopened
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        0
    );
    assert_eq!(
        reopened
            .resource_drop_cutoff(1, "north", "orders", "created")
            .unwrap(),
        1
    );
    assert!(reopened
        .db
        .read_tx(1)
        .get(&super::super::drop_reclamation::progress_key())
        .unwrap()
        .is_none());
    assert_eq!(reopened.get_global_watermark(1).unwrap(), 1);
}

#[test]
fn should_reject_a_stale_append_session_after_delayed_scope_reset() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    let frozen = std::rc::Rc::new(std::cell::RefCell::new(None));
    let captured = frozen.clone();
    let hook =
        super::super::write_admission::inject_commit_hook(Box::new(move |transaction, options| {
            *captured.borrow_mut() = Some((transaction, options));
            Err(cntryl_midge::MidgeError::OutcomeUnknown(
                "delayed DROP".to_string(),
            ))
        }));
    let outcome = store.drop_scope(
        &selector("stream://north/orders/created"),
        StreamWriteMode::Sync,
    );
    drop(hook);
    let stale_cutoff = store
        .resource_drop_cutoff(1, "north", "orders", "created")
        .unwrap();
    let (transaction, options) = frozen.borrow_mut().take().unwrap();
    transaction.commit(options).unwrap();

    // Act
    let append = store.commit_records_fenced(
        CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &single_event(b"new"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        },
        stale_cutoff,
    );

    // Assert
    assert!(outcome.is_err());
    assert_eq!(append.unwrap_err(), "ERR_STREAM_DROPPED");
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        0
    );
}

#[test]
fn should_prevent_a_delayed_drop_from_deleting_a_later_committed_append() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    let frozen = std::rc::Rc::new(std::cell::RefCell::new(None));
    let captured = frozen.clone();
    let hook =
        super::super::write_admission::inject_commit_hook(Box::new(move |transaction, options| {
            *captured.borrow_mut() = Some((transaction, options));
            Err(cntryl_midge::MidgeError::OutcomeUnknown(
                "delayed DROP".to_string(),
            ))
        }));
    store
        .drop_scope(
            &selector("stream://north/orders/created"),
            StreamWriteMode::Sync,
        )
        .unwrap_err();
    drop(hook);
    append(&store, "north", "orders", "created", 1);
    let (transaction, options) = frozen.borrow_mut().take().unwrap();

    // Act
    let outcome = transaction.commit(options);

    // Assert
    assert!(matches!(
        outcome,
        Err(cntryl_midge::MidgeError::WriteConflict(_))
    ));
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        2
    );
}

#[test]
fn should_fail_an_expired_drop_before_mutating_an_unlocked_store() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);

    // Act
    let result = store.drop_scope_with_deadline(
        &selector("stream://north/*"),
        StreamWriteMode::Sync,
        Some(std::time::Instant::now()),
    );

    // Assert
    assert!(result.is_err());
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        1
    );
}

#[test]
fn should_reject_stale_resource_compaction_after_a_namespace_reset() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
    for offset in 0..9 {
        append(&store, "north", "orders", "created", offset);
    }
    let resource_prefix =
        StreamStore::build_compact_resource_page_prefix("north", "orders", "created");
    {
        let mut queues = store.maintenance_queues.lock();
        let queue = queues.get_mut(&1).unwrap();
        queue.initialized = true;
        queue
            .buckets
            .retain(|key| key.starts_with(&resource_prefix));
    }
    let during_scan = store.clone();
    super::super::maintenance::after_next_scan_for_tests(Box::new(move || {
        during_scan
            .drop_scope(
                &selector("stream://north/orders/created"),
                StreamWriteMode::Sync,
            )
            .unwrap();
    }));

    // Act
    let result = store.run_maintenance(1);

    // Assert
    assert!(result.is_err());
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        0
    );
    let (items, _) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 64,
            max_bytes: None,
        })
        .unwrap();
    assert!(items.is_empty());
}

#[test]
fn should_resume_deleted_payload_reclamation_after_an_engine_restart() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let options = cntryl_midge::OpenOptions::local(directory.path())
        .build()
        .unwrap();
    let engine = Arc::new(cntryl_midge::Engine::open(options).unwrap());
    engine.create_column_family("family-one").unwrap();
    let store = StreamStore::new(engine.clone());
    append(&store, "north", "orders", "created", 0);
    store
        .drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync)
        .unwrap();
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);

    // Act
    let reopened_engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let reopened = StreamStore::new(reopened_engine.clone());
    reopened
        .validate_persisted_state_for_existing_families()
        .unwrap();
    drain_maintenance(&reopened, 1);

    // Assert
    assert_eq!(
        reopened
            .resource_drop_cutoff(1, "north", "orders", "created")
            .unwrap(),
        1
    );
    assert_eq!(
        reopened
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        0
    );
    assert_eq!(
        reopened
            .db
            .read_tx(1)
            .get(&encode_stream_layout_marker_key())
            .unwrap()
            .unwrap()
            .as_ref(),
        [0, 0xD5, 2]
    );
    let old = CompactGlobalPageValue::try_decode(
        &reopened
            .db
            .read_tx(1)
            .get(&encode_compact_global_page_key(0))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(old.records[0].body.is_empty());
    drop(reopened);
    crate::testkit::midge::shutdown_test_engine(reopened_engine);
}

#[test]
fn should_keep_identical_namespace_data_in_another_route_family() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1, 2]));
    append(&store, "north", "orders", "created", 0);
    store
        .commit_records(CommitRecordsParams {
            family: 2,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &single_event(b"other-family"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();

    // Act
    store
        .drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync)
        .unwrap();

    // Assert
    assert_eq!(
        store
            .get_next_resource_offset(2, "north", "orders", "created")
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .resource_drop_cutoff(2, "north", "orders", "created")
            .unwrap(),
        0
    );
    let (other, _) = store.read_global(2, 0, 64, None, None).unwrap();
    assert_eq!(event_records(other)[0].body.as_ref(), b"other-family");
}

#[test]
fn should_reclaim_retained_discriminator_ranges_without_deleting_neighboring_events() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event_with_discriminator(b"old", "old-type");
    for expected in 0..2 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: expected,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .unwrap();
    }
    let neighbor = single_event_with_discriminator(b"neighbor", "neighbor-type");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "neighbor",
            expected_resource_next_offset: 0,
            events: &neighbor,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
    store
        .drop_scope(
            &selector("stream://north/orders/created"),
            StreamWriteMode::Sync,
        )
        .unwrap();

    // Act
    drain_maintenance(&store, 1);

    // Assert
    let txn = store.db.read_tx(1);
    for offset in 0..2 {
        assert!(txn
            .get(
                &crate::domains::stream::storage::encode_area_discriminator_key(
                    "north", "orders", offset
                )
            )
            .unwrap()
            .is_none());
        assert!(txn
            .get(&crate::domains::stream::storage::encode_realm_discriminator_key("north", offset))
            .unwrap()
            .is_none());
    }
    assert_eq!(
        txn.get(
            &crate::domains::stream::storage::encode_area_discriminator_key("north", "orders", 2)
        )
        .unwrap()
        .unwrap()
        .as_ref(),
        b"neighbor-type"
    );
    assert_eq!(
        txn.get(&crate::domains::stream::storage::encode_realm_discriminator_key("north", 2))
            .unwrap()
            .unwrap()
            .as_ref(),
        b"neighbor-type"
    );
}

#[test]
fn should_retry_drop_rejected_before_storage_admission() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    let _outcomes = super::super::write_admission::inject_commit_outcomes([Some(
        cntryl_midge::MidgeError::WriteStall(
            "column family 1 has no free L0 slot (15/14)".to_string(),
        ),
    )]);

    // Act
    let result = store.drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync);

    // Assert
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(
        store
            .get_next_resource_offset(1, "north", "orders", "created")
            .unwrap(),
        0
    );
}

#[test]
fn should_preserve_the_configured_sync_policy_for_drop() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append(&store, "north", "orders", "created", 0);
    let store = store.with_write_options(
        cntryl_midge::WriteOptions::cloud_strict(),
        cntryl_midge::WriteOptions::cloud_async(),
    );
    let observed = std::rc::Rc::new(std::cell::RefCell::new(None));
    let capture = observed.clone();
    let _hook =
        super::super::write_admission::inject_commit_hook(Box::new(move |transaction, options| {
            *capture.borrow_mut() = Some(options);
            transaction.commit(cntryl_midge::WriteOptions::sync())
        }));

    // Act
    store
        .drop_scope(&selector("stream://north/*"), StreamWriteMode::Sync)
        .unwrap();

    // Assert
    assert_eq!(
        *observed.borrow(),
        Some(cntryl_midge::WriteOptions::cloud_strict())
    );
}

#[test]
fn should_report_concurrency_for_a_frozen_commit_after_an_ordinary_append() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store.ensure_layout_activation_for_family(1).unwrap();
    let reservation = store.reserve_global_range(1, 1).unwrap();
    let events = single_event(b"frozen");
    let frozen = CommitPromotionFrontierBatchParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
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
    };
    append(&store, "north", "orders", "created", 0);

    // Act
    let failure = store.commit_promotion_frontier_batch(frozen).err().unwrap();

    // Assert
    let retryable = failure.is_retryable();
    assert_eq!(failure.into_message(), "ERR_CONCURRENCY_CONFLICT");
    assert!(retryable);
    assert_eq!(
        store
            .resource_drop_cutoff(1, "north", "orders", "created")
            .unwrap(),
        0
    );
}
