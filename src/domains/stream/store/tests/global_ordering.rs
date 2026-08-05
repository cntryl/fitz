use super::*;

#[test]
fn should_assign_global_offsets_across_realms_and_isolate_route_families() {
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
    assert_eq!(cursor.last_realm_offset, Some(30));
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

#[test]
fn should_not_truncate_fragment_reads_when_starting_mid_bucket() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for index in 0..8 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: &format!("resource-{index}"),
                expected_resource_next_offset: 0,
                events: &single_event(b"event"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit one-record broad-scope fragment");
    }
    store
        .set_watermark(1, "north", "orders", 7)
        .expect("make area fragments visible");
    store
        .set_realm_watermark(1, "north", 7)
        .expect("make realm fragments visible");

    // Act
    let area = store
        .read_area(1, "north", "orders", 7, 1, None)
        .expect("read area from middle of fragment bucket");
    let realm = store
        .read_realm(1, "north", 7, 1, None)
        .expect("read realm from middle of fragment bucket");

    // Assert
    assert_eq!(event_records(area.0).len(), 1);
    assert_eq!(area.1.last_area_offset, Some(7));
    assert_eq!(event_records(realm.0).len(), 1);
    assert_eq!(realm.1.last_realm_offset, Some(7));
}

#[test]
fn should_hold_global_watermark_at_gap_then_advance_across_resolved_suffix() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

    // Act
    store
        .resolve_global_range(1, 2, 4)
        .expect("resolve later range");
    let before = store.get_global_watermark(1).expect("read held watermark");
    store
        .resolve_global_range(1, 0, 2)
        .expect("resolve gap range");
    let after = store
        .get_global_watermark(1)
        .expect("read advanced watermark");

    // Assert
    assert_eq!(before, 0);
    assert_eq!(after, 4);
}

#[test]
fn should_reuse_unresolved_global_range_and_clear_completion_suffix_after_retry() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let failed_events = single_event(b"failed");
    let committed_events = single_event(b"committed");
    store.fail_next_promotion_frontier_commit_for_tests();

    // Act
    let failed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "failed",
        expected_resource_next_offset: 0,
        events: &failed_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let committed = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "south",
            area: "orders",
            resource: "committed",
            expected_resource_next_offset: 0,
            events: &committed_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit after skipped range");
    let retried = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "failed",
            expected_resource_next_offset: 0,
            events: &failed_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("retry original reserved range");

    // Assert
    assert!(failed.is_err());
    assert_eq!(committed.first_global_offset, 1);
    assert_eq!(retried.first_global_offset, 0);
    assert_eq!(store.get_global_watermark(1).expect("read watermark"), 2);
    assert!(store.global_completion_state(1).lock().resolved.is_empty());
}

#[test]
fn should_reject_data_transaction_after_writer_epoch_fence() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event(b"stale-writer");
    store.fence_next_global_reservation_for_tests();

    // Act
    let result = store.commit_records(CommitRecordsParams {
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
    assert_eq!(
        result.expect_err("old epoch must be fenced"),
        "ERR_STREAM_WRITER_FENCED"
    );
    assert_eq!(store.get_global_watermark(1).expect("read watermark"), 1);
}

#[test]
fn should_reject_data_transaction_after_direct_writer_epoch_advance() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event(b"direct-fenced-writer");
    store
        .advance_family_writer_epoch(1)
        .expect("advance writer epoch");

    // Act
    let result = store.commit_records(CommitRecordsParams {
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
    assert_eq!(
        result.expect_err("fenced writer must fail"),
        "ERR_STREAM_WRITER_FENCED"
    );
}

#[test]
fn should_fence_and_skip_abandoned_reservation_during_restart_recovery() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let first_store = StreamStore::new(engine.clone());
    first_store
        .ensure_layout_activation_for_family(1)
        .expect("activate first store");
    let abandoned = first_store
        .reserve_global_range(1, 2)
        .expect("reserve abandoned range");
    drop(first_store);
    let restarted = StreamStore::new(engine);
    let events = single_event(b"after-restart");

    // Act
    let recovered_watermark = restarted
        .get_global_watermark(1)
        .expect("recover global ordering");
    let commit = restarted
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
        .expect("commit after recovery");

    // Assert
    assert_eq!(abandoned.first_offset, 0);
    assert_eq!(recovered_watermark, 2);
    assert_eq!(commit.first_global_offset, 2);
}

#[test]
fn should_apply_record_filter_in_global_offset_space() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"hidden"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("hide")),
        },
        EventPayload {
            body: Bytes::from_static(b"visible"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("keep")),
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
        .expect("commit discriminated records");
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("keep".to_string())],
    };

    // Act
    let (items, cursor) = store
        .read_global(1, 0, 10, None, Some(&filter))
        .expect("read filtered global stream");

    // Assert
    assert!(matches!(
        items[0],
        StreamReadItem::Filtered { offset: 0, .. }
    ));
    assert!(
        matches!(&items[1], StreamReadItem::Event(record) if record.body.as_ref() == b"visible")
    );
    assert_eq!(cursor.last_global_offset, Some(1));
}

#[test]
fn should_write_distinct_broad_scope_fragments_for_concurrent_resources() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let store = Arc::new(StreamStore::new(engine.clone()));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for resource in ["created", "updated"] {
        let store = store.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let events = single_event(b"event");
            barrier.wait();
            store.commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource,
                expected_resource_next_offset: 0,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
        }));
    }

    // Act
    barrier.wait();
    let mut ranges = workers
        .into_iter()
        .map(|worker| worker.join().expect("join writer").expect("commit writer"))
        .map(|response| response.first_global_offset)
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    store
        .set_watermark(1, "north", "orders", 1)
        .expect("make both area fragments visible");
    store
        .set_realm_watermark(1, "north", 1)
        .expect("make both realm fragments visible");
    let area_records = event_records(
        store
            .read_area(1, "north", "orders", 0, 10, None)
            .expect("read area fragments")
            .0,
    );
    let realm_records = event_records(
        store
            .read_realm(1, "north", 0, 10, None)
            .expect("read realm fragments")
            .0,
    );
    let txn = engine
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin fragment scan");
    let area_rows = txn
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compact_area_page_prefix("north", "orders"),
        )))
        .expect("scan area fragments")
        .try_collect()
        .expect("collect area fragments");
    let realm_rows = txn
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compressed_compact_realm_page_prefix("north"),
        )))
        .expect("scan realm fragments")
        .try_collect()
        .expect("collect realm fragments");

    // Assert
    assert_eq!(ranges, vec![0, 1]);
    assert_eq!(area_rows.len(), 2);
    assert_eq!(realm_rows.len(), 2);
    assert_eq!(area_records.len(), 2);
    assert_eq!(realm_records.len(), 2);
}

#[test]
fn should_resolve_stale_global_reservation_before_changed_batch_retry() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_attempt = single_event(b"first");
    let retry = vec![
        EventPayload {
            body: Bytes::from_static(b"first"),
            metadata: None,
            discriminator: None,
        },
        EventPayload {
            body: Bytes::from_static(b"second"),
            metadata: None,
            discriminator: None,
        },
    ];
    store.fail_next_promotion_frontier_commit_for_tests();

    // Act
    let failed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "north",
        area: "orders",
        resource: "created",
        expected_resource_next_offset: 0,
        events: &first_attempt,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let committed = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &retry,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit changed retry batch");
    let watermark = store.get_global_watermark(1).expect("global watermark");
    let records = event_records(
        store
            .read_global(1, 0, 10, None, None)
            .expect("read globally visible retry")
            .0,
    );

    // Assert
    assert!(failed.is_err());
    assert_eq!(
        (committed.first_global_offset, committed.last_global_offset),
        (1, 2)
    );
    assert_eq!(watermark, 3);
    assert_eq!(records.len(), 2);
    assert!(store.pending_global_reservations.lock().is_empty());
    assert!(store.global_completion_state(1).lock().resolved.is_empty());
}

#[test]
fn should_return_first_oversized_global_record_to_advance_cursor() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event(b"oversized");
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
        .expect("commit oversized global record");

    // Act
    let (items, cursor) = store
        .read_global(1, 0, 10, Some(1), None)
        .expect("read oversized global record");

    // Assert
    assert_eq!(event_records(items).len(), 1);
    assert_eq!(cursor.last_global_offset, Some(0));
    assert!(!cursor.has_more);
}

#[test]
fn should_acknowledge_durable_commit_when_global_watermark_persistence_fails() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    StreamStore::fail_next_global_watermark_persist_for_tests();
    let events = single_event(b"durable");

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
    assert!(
        committed.is_ok(),
        "durable event commit must remain successful"
    );
    assert_eq!(store.get_global_watermark(1).expect("retried watermark"), 1);
    assert_eq!(
        event_records(
            store
                .read_global(1, 0, 10, None, None)
                .expect("read durable event")
                .0,
        )
        .len(),
        1
    );
}

#[test]
fn should_resolve_stale_session_reservation_before_changed_batch_retry() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let session_id = store
        .begin_session(1, "north", "orders", "created", None)
        .expect("begin store session");
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"first"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append first event");
    store.fail_next_promotion_frontier_commit_for_tests();
    let failed = store.commit_session(1, session_id, 0, 0, 0, StreamWriteMode::Sync);
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"second"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append changed retry event");

    // Act
    let committed = store
        .commit_session(1, session_id, 0, 0, 0, StreamWriteMode::Sync)
        .expect("commit changed session retry batch");

    // Assert
    assert!(failed.is_err());
    assert_eq!(
        (committed.first_global_offset, committed.last_global_offset),
        (1, 2)
    );
    assert_eq!(store.get_global_watermark(1).expect("global watermark"), 3);
    assert!(store.pending_global_reservations.lock().is_empty());
    assert!(store.global_completion_state(1).lock().resolved.is_empty());
}
