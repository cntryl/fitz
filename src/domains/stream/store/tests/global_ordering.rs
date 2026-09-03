use super::*;

#[test]
fn should_append_resource_history_with_immutable_fragments() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit first fragment");
    let first_key = encode_compact_resource_page_key("north", "orders", "created", 0);
    let first_value = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin first fragment read")
        .get(&first_key)
        .expect("read first fragment")
        .expect("first fragment exists");

    // Act
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 1,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit second fragment");
    let txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin immutable-fragment scan");
    let rows: Vec<_> = txn
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compact_resource_page_prefix("north", "orders", "created"),
        )))
        .expect("scan resource fragments")
        .try_collect()
        .expect("collect resource fragments");

    // Assert
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].1, first_value);
    assert_eq!(decode_resource_offset_from_key(&rows[0].0), Ok(0));
    assert_eq!(decode_resource_offset_from_key(&rows[1].0), Ok(1));
}

#[test]
fn should_compact_over_fragmented_resource_bucket_in_one_atomic_replacement() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    for offset in 0..9 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: offset,
                events: &single_event(b"event"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit resource fragment");
    }

    // Act
    let result = drain_maintenance(&store, 1);
    let replay = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 64,
            max_bytes: None,
        })
        .expect("replay compacted resource");
    let area_replay = store
        .read_area(1, "north", "orders", 0, 64, None)
        .expect("replay compacted area");
    let realm_replay = store
        .read_realm(1, "north", 0, 64, None)
        .expect("replay compacted realm");
    let global_replay = store
        .read_global(1, 0, 64, None, None)
        .expect("replay compacted global stream");
    let posting_replay = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 0,
                limit: 64,
                max_bytes: None,
                area: Some("orders"),
                resource: Some("created"),
            },
            None,
        )
        .expect("replay compacted posting");
    let txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin compacted fragment scan");
    let rows: Vec<_> = txn
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compact_resource_page_prefix("north", "orders", "created"),
        )))
        .expect("scan compacted resource")
        .try_collect()
        .expect("collect compacted resource");

    // Assert
    assert!(result.buckets_compacted > 0);
    assert!(result.records_compacted >= 9);
    assert_eq!(rows.len(), 1);
    assert_eq!(event_records(replay.0).len(), 9);
    assert_eq!(event_records(area_replay.0).len(), 9);
    assert_eq!(event_records(realm_replay.0).len(), 9);
    assert_eq!(event_records(global_replay.0).len(), 9);
    assert_eq!(event_records(posting_replay.0).len(), 9);
}

fn stored_payload_occurrences(db: &cntryl_midge::Engine, family: u32, payload: &[u8]) -> usize {
    let txn = db
        .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin payload occurrence scan");
    txn.scan(&cntryl_midge::Query::new())
        .expect("scan payload occurrences")
        .map(|row| row.expect("read payload occurrence row").1)
        .map(|value| {
            value
                .windows(payload.len())
                .filter(|window| *window == payload)
                .count()
        })
        .sum()
}

#[test]
fn should_store_inline_payload_twice_plus_large_payload_once() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1, 2]);
    let store = StreamStore::new(db.clone());
    let small = Bytes::from_static(b"unique-small-D4");
    let large = Bytes::from(
        (0..17 * 1024)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>(),
    );
    for (family, body) in [(1, small.clone()), (2, large.clone())] {
        store
            .commit_records(CommitRecordsParams {
                family,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: 0,
                events: &[EventPayload {
                    body,
                    metadata: None,
                    discriminator: None,
                }],
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit payload boundary record");
    }

    // Act
    let large_resource = store
        .read_resource(&ReadResourceParams {
            family: 2,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 1,
            max_bytes: None,
        })
        .expect("hydrate large resource payload");
    let large_area = store
        .read_area(2, "north", "orders", 0, 1, None)
        .expect("hydrate large area payload");
    let large_realm = store
        .read_realm(2, "north", 0, 1, None)
        .expect("hydrate large realm payload");

    // Assert
    assert_eq!(stored_payload_occurrences(db.as_ref(), 1, &small), 2);
    assert_eq!(stored_payload_occurrences(db.as_ref(), 2, &large), 1);
    assert_eq!(event_records(large_resource.0)[0].body, large);
    assert_eq!(event_records(large_area.0)[0].body, large);
    assert_eq!(event_records(large_realm.0)[0].body, large);
}

#[test]
fn should_fail_closed_when_large_payload_blob_is_missing() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &[EventPayload {
                body: Bytes::from(vec![0xA5; 17 * 1024]),
                metadata: None,
                discriminator: None,
            }],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit blob-backed record");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin blob deletion");
    txn.delete(encode_payload_blob_key(0))
        .expect("delete payload blob");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit blob deletion");

    // Act
    let result = store.read_resource(&ReadResourceParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        from_offset: 0,
        limit: 1,
        max_bytes: None,
    });

    // Assert
    assert!(result
        .expect_err("missing blob must fail closed")
        .contains("missing payload blob"));
}

#[test]
fn should_yield_stream_maintenance_after_one_bucket() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for resource_index in 0..9 {
        for offset in 0..9 {
            store
                .commit_records(CommitRecordsParams {
                    family: 1,
                    realm: "north",
                    area: "orders",
                    resource: &format!("resource-{resource_index}"),
                    expected_resource_next_offset: offset,
                    events: &single_event(b"event"),
                    ingest_metadata: None,
                    mode: StreamWriteMode::Sync,
                })
                .expect("commit maintenance-bound fragment");
        }
    }

    // Act
    let first = store
        .run_maintenance(1)
        .expect("run first maintenance slice");
    let pending_after_first = store.has_pending_maintenance(1);
    let second = store
        .run_maintenance(1)
        .expect("run second maintenance slice");

    // Assert
    assert_eq!(first.buckets_compacted, 1);
    assert!(first.records_compacted <= 64);
    assert!(first.bytes_examined <= 4 * 1024 * 1024);
    assert!(pending_after_first);
    assert_eq!(second.buckets_compacted, 1);
    assert_eq!(store.maintenance_full_scan_count_for_tests(), 1);
}

#[test]
fn should_queue_touched_bucket_after_maintenance_discovery() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let empty = store
        .run_maintenance(1)
        .expect("initialize maintenance discovery");
    for offset in 0..9 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: offset,
                events: &single_event(b"event"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit queued fragment");
    }

    // Act
    let compacted = store
        .run_maintenance(1)
        .expect("compact commit-queued bucket");

    // Assert
    assert_eq!(empty.buckets_compacted, 0);
    assert!(compacted.buckets_compacted > 0);
    assert_eq!(store.maintenance_full_scan_count_for_tests(), 1);
}

#[test]
fn should_leave_complete_source_set_when_maintenance_commit_fails() {
    for stage in [
        MaintenanceFailureStage::BeforeReplacement,
        MaintenanceFailureStage::DuringReplacement,
        MaintenanceFailureStage::BeforeDeletion,
    ] {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let metrics = crate::observability::metrics::MetricsCollector::new();
        let store =
            StreamStore::new(db.clone()).with_maintenance_metrics_for_tests(metrics.clone());
        for offset in 0..9 {
            store
                .commit_records(CommitRecordsParams {
                    family: 1,
                    realm: "north",
                    area: "orders",
                    resource: "created",
                    expected_resource_next_offset: offset,
                    events: &single_event(b"event"),
                    ingest_metadata: None,
                    mode: StreamWriteMode::Sync,
                })
                .expect("commit source fragment");
        }
        store.fail_maintenance_at_for_tests(stage);

        // Act
        let failed = store.run_maintenance(1);
        let reopened = StreamStore::new(db);
        let before_retry = reopened
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                from_offset: 0,
                limit: 64,
                max_bytes: None,
            })
            .expect("read intact sources after failed maintenance");
        let retried = store.run_maintenance(1);

        // Assert
        assert!(failed
            .expect_err("maintenance failure should surface")
            .contains("injected"));
        assert_eq!(event_records(before_retry.0).len(), 9);
        assert!(retried.expect("retry maintenance").buckets_compacted > 0);
        assert_eq!(
            metrics.counter_get(crate::domains::stream::metrics::METRIC_MAINTENANCE_FAILURES_TOTAL),
            1
        );
        assert_eq!(
            metrics.counter_get(crate::domains::stream::metrics::METRIC_MAINTENANCE_RETRIES_TOTAL),
            1
        );
        assert!(
            metrics.counter_get(
                crate::domains::stream::metrics::METRIC_MAINTENANCE_BUCKETS_COMPACTED_TOTAL
            ) > 0
        );
    }
}

#[test]
fn should_keep_read_snapshot_stable_across_atomic_compaction() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    for offset in 0..9 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: offset,
                events: &single_event(b"event"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit snapshot source fragment");
    }
    let prefix = Bytes::from(StreamStore::build_compact_resource_page_prefix(
        "north", "orders", "created",
    ));
    let snapshot = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin pre-compaction snapshot");
    let before: Vec<_> = snapshot
        .scan(&cntryl_midge::Query::new().prefix(prefix.clone()))
        .expect("scan pre-compaction snapshot")
        .try_collect()
        .expect("collect pre-compaction snapshot");

    // Act
    drain_maintenance(&store, 1);
    let held: Vec<_> = snapshot
        .scan(&cntryl_midge::Query::new().prefix(prefix.clone()))
        .expect("rescan held snapshot")
        .try_collect()
        .expect("collect held snapshot");
    let fresh: Vec<_> = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin post-compaction snapshot")
        .scan(&cntryl_midge::Query::new().prefix(prefix))
        .expect("scan post-compaction snapshot")
        .try_collect()
        .expect("collect post-compaction snapshot");

    // Assert
    assert_eq!(before.len(), 9);
    assert_eq!(held, before);
    assert_eq!(fresh.len(), 1);
    assert_eq!(
        event_records(
            store
                .read_resource(&ReadResourceParams {
                    family: 1,
                    realm: "north",
                    area: "orders",
                    resource: "created",
                    from_offset: 0,
                    limit: 64,
                    max_bytes: None,
                })
                .expect("read post-compaction resource")
                .0
        )
        .len(),
        9
    );
}

#[test]
fn should_assign_global_offsets_across_realms_plus_isolate_route_families() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1, 2]));
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");

    // Act
    let first = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit first family-one record");
    let second = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "south",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit second family-one record");
    let other_family = store
        .commit_records(CommitRecordsParams {
            family: 2,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit family-two record");

    // Assert
    assert_eq!(
        (first.first_global_offset, first.last_global_offset),
        (0, 0)
    );
    assert_eq!(
        (second.first_global_offset, second.last_global_offset),
        (1, 1)
    );
    assert_eq!(
        (
            other_family.first_global_offset,
            other_family.last_global_offset
        ),
        (0, 0)
    );
}

#[test]
fn should_read_global_records_in_assigned_order_across_realms() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for (realm, body) in [
        ("zeta", b"first".as_slice()),
        ("alpha", b"second".as_slice()),
    ] {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm,
                area: "orders",
                resource: "created",
                expected_resource_next_offset: 0,
                events: &[EventPayload {
                    body: Bytes::copy_from_slice(body),
                    metadata: None,
                    discriminator: None,
                }],
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit global record");
    }
    store
        .set_global_watermark(1, 2)
        .expect("publish global watermark");

    // Act
    let (items, cursor) = store
        .read_global(1, 0, 10, None, None)
        .expect("read global stream");
    let records = event_records(items);

    // Assert
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].route.as_str(), "stream://zeta/orders/created");
    assert_eq!(records[0].global_offset, Some(0));
    assert_eq!(records[1].route.as_str(), "stream://alpha/orders/created");
    assert_eq!(records[1].global_offset, Some(1));
    assert_eq!(cursor.last_global_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_read_each_global_route_filter_through_its_sparse_posting() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for (realm, area, resource) in [
        ("north", "orders", "created"),
        ("south", "billing", "created"),
        ("west", "orders", "cancelled"),
    ] {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm,
                area,
                resource,
                expected_resource_next_offset: 0,
                events: &single_event(resource.as_bytes()),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit posting source record");
    }

    // Act
    let area = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 0,
                limit: 10,
                max_bytes: None,
                area: Some("orders"),
                resource: None,
            },
            None,
        )
        .expect("read area posting");
    let resource = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 0,
                limit: 10,
                max_bytes: None,
                area: None,
                resource: Some("created"),
            },
            None,
        )
        .expect("read resource posting");
    let pair = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 0,
                limit: 10,
                max_bytes: None,
                area: Some("orders"),
                resource: Some("created"),
            },
            None,
        )
        .expect("read area-resource posting");

    // Assert
    assert_eq!(event_records(area.0).len(), 2);
    assert_eq!(event_records(resource.0).len(), 2);
    let pair_records = event_records(pair.0);
    assert_eq!(pair_records.len(), 1);
    assert_eq!(
        pair_records[0].route.as_str(),
        "stream://north/orders/created"
    );
}

#[test]
fn should_read_resource_name_across_realm_through_sparse_posting() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for (area, resource) in [
        ("orders", "created"),
        ("billing", "created"),
        ("orders", "cancelled"),
    ] {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area,
                resource,
                expected_resource_next_offset: 0,
                events: &single_event(resource.as_bytes()),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit realm posting source record");
    }

    // Act
    let (items, cursor) = store
        .read_realm_resource_posting(
            &ReadRealmPostingParams {
                family: 1,
                realm: "north",
                resource: "created",
                from_offset: 0,
                limit: 10,
                max_bytes: None,
            },
            None,
        )
        .expect("read realm-resource posting");
    let records = event_records(items);

    // Assert
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|record| record.route.as_str().ends_with("/created")));
    assert_eq!(cursor.last_realm_offset, Some(2));
    assert!(!cursor.has_more);
}

#[test]
fn should_resume_sparse_postings_across_storage_page_boundary() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events: Vec<EventPayload> = (0..130)
        .map(|index| EventPayload {
            body: Bytes::from(index.to_string()),
            metadata: None,
            discriminator: None,
        })
        .collect();
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
        .expect("commit records across posting pages");

    // Act
    let global = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 64,
                limit: 3,
                max_bytes: None,
                area: Some("orders"),
                resource: None,
            },
            None,
        )
        .expect("resume global posting");
    let realm = store
        .read_realm_resource_posting(
            &ReadRealmPostingParams {
                family: 1,
                realm: "north",
                resource: "created",
                from_offset: 64,
                limit: 3,
                max_bytes: None,
            },
            None,
        )
        .expect("resume realm posting");

    // Assert
    assert_eq!(
        event_records(global.0)
            .iter()
            .map(|record| record.global_offset)
            .collect::<Vec<_>>(),
        vec![Some(64), Some(65), Some(66)]
    );
    assert_eq!(
        event_records(realm.0)
            .iter()
            .map(|record| record.realm_offset)
            .collect::<Vec<_>>(),
        vec![Some(64), Some(65), Some(66)]
    );
}

#[test]
fn should_not_regress_global_posting_cursor_when_read_starts_past_watermark() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &single_event(b"present"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit posting source record");

    // Act
    let (items, cursor) = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 30,
                limit: 10,
                max_bytes: None,
                area: Some("orders"),
                resource: None,
            },
            None,
        )
        .expect("read past global watermark");

    // Assert
    assert!(items.is_empty());
    assert_eq!(cursor.last_global_offset, Some(30));
    assert!(!cursor.has_more);
}

#[test]
fn should_not_regress_realm_posting_cursor_when_read_starts_past_watermark() {
    // Arrange
    // This path is sparse - it deliberately steps over the realm offsets
    // owned by other resources - so an empty page cannot mean "stay put" and
    // the caller always resumes at `last_realm_offset + 1`. "Covered nothing"
    // therefore has to encode as one BEHIND the requested offset; naming the
    // requested offset itself would resume at 31 and skip 30 once it commits.
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &single_event(b"present"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit posting source record");

    // Act
    let (items, cursor) = store
        .read_realm_resource_posting(
            &ReadRealmPostingParams {
                family: 1,
                realm: "north",
                resource: "created",
                from_offset: 30,
                limit: 10,
                max_bytes: None,
            },
            None,
        )
        .expect("read past realm watermark");

    // Assert
    assert!(items.is_empty());
    assert_eq!(
        cursor.last_realm_offset,
        Some(29),
        "an uncovered read must resume exactly where it asked, not past it"
    );
    assert!(!cursor.has_more);
}

#[test]
fn should_not_skip_posting_record_when_byte_budget_stops_a_page() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"a"),
            metadata: None,
            discriminator: None,
        },
        EventPayload {
            body: Bytes::from_static(b"oversized"),
            metadata: None,
            discriminator: None,
        },
    ];
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
        .expect("commit posting records");

    // Act
    let (global_first, global_cursor) = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: 0,
                limit: 10,
                max_bytes: Some(1),
                area: Some("orders"),
                resource: None,
            },
            None,
        )
        .expect("read first global posting page");
    let (global_second, _) = store
        .read_global_posting(
            &ReadGlobalPostingParams {
                family: 1,
                from_offset: global_cursor
                    .last_global_offset
                    .expect("global cursor offset")
                    .saturating_add(1),
                limit: 10,
                max_bytes: Some(1),
                area: Some("orders"),
                resource: None,
            },
            None,
        )
        .expect("resume global posting page");
    let (realm_first, realm_cursor) = store
        .read_realm_resource_posting(
            &ReadRealmPostingParams {
                family: 1,
                realm: "north",
                resource: "created",
                from_offset: 0,
                limit: 10,
                max_bytes: Some(1),
            },
            None,
        )
        .expect("read first realm posting page");
    let (realm_second, _) = store
        .read_realm_resource_posting(
            &ReadRealmPostingParams {
                family: 1,
                realm: "north",
                resource: "created",
                from_offset: realm_cursor
                    .last_realm_offset
                    .expect("realm cursor offset")
                    .saturating_add(1),
                limit: 10,
                max_bytes: Some(1),
            },
            None,
        )
        .expect("resume realm posting page");

    // Assert
    assert_eq!(event_records(global_first).len(), 1);
    assert_eq!(global_cursor.last_global_offset, Some(0));
    assert_eq!(event_records(global_second)[0].body.as_ref(), b"oversized");
    assert_eq!(event_records(realm_first).len(), 1);
    assert_eq!(realm_cursor.last_realm_offset, Some(0));
    assert_eq!(event_records(realm_second)[0].body.as_ref(), b"oversized");
}
