use super::*;

#[test]
fn should_preserve_realm_watermark_given_lower_value_than_derived_committed_watermark() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
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
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("seed committed realm watermark");

    // Act
    store
        .set_realm_watermark(1, "test", 0)
        .expect("lower realm watermark should no-op");

    // Assert
    assert_eq!(
        store
            .get_realm_watermark(1, "test")
            .expect("read realm watermark"),
        1
    );
}

#[test]
fn should_allocate_sequential_offsets_given_same_process_commits() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");

    // Act
    let first = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first commit");
    let second = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Assert
    assert_eq!(first.first_area_offset, 0);
    assert_eq!(first.first_realm_offset, 0);
    assert_eq!(second.first_area_offset, 1);
    assert_eq!(second.first_realm_offset, 1);
}

#[test]
fn should_continue_sequential_offsets_given_recreated_store() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let first_store = StreamStore::new(db.clone());
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    first_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("seed commit");
    let second_store = StreamStore::new(db);

    // Act
    let second = second_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Assert
    assert_eq!(second.first_area_offset, 1);
    assert_eq!(second.first_realm_offset, 1);
}

#[test]
fn should_refresh_scope_offsets_given_stale_store_cache() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let first_store = StreamStore::new(db.clone());
    let second_store = StreamStore::new(db);
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    let third_events = single_event(b"third");
    first_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first commit");
    second_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit from separate store");

    // Act
    let third = first_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &third_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("third commit from stale store");
    let (area_items, area_cursor) = first_store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read area records");
    let area_records = event_records(area_items);

    // Assert
    assert_eq!(third.first_area_offset, 2);
    assert_eq!(third.first_realm_offset, 2);
    assert_eq!(area_records.len(), 3);
    assert_eq!(area_records[0].body, Bytes::from_static(b"first"));
    assert_eq!(area_records[1].body, Bytes::from_static(b"second"));
    assert_eq!(area_records[2].body, Bytes::from_static(b"third"));
    assert_eq!(area_cursor.last_area_offset, Some(2));
    assert!(!area_cursor.has_more);
}

#[test]
fn should_allocate_next_resource_offset_given_same_process_commits() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");

    // Act
    let first = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first commit");
    let second = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Assert
    assert_eq!(first.first_resource_offset, 0);
    assert_eq!(second.first_resource_offset, 1);
    assert_eq!(
        store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("next resource offset"),
        2
    );
}

#[test]
fn should_continue_next_resource_offset_given_recreated_store() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let first_store = StreamStore::new(db.clone());
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    first_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("seed commit");
    let second_store = StreamStore::new(db);

    // Act
    let second = second_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Assert
    assert_eq!(second.first_resource_offset, 1);
    assert_eq!(
        second_store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("next resource offset"),
        2
    );
}

#[test]
fn should_reject_stale_expected_resource_offset_given_other_store_advanced_resource() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let first_store = StreamStore::new(db.clone());
    let second_store = StreamStore::new(db);
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    let third_events = single_event(b"third");
    first_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first commit");
    second_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit from separate store");

    // Act
    let stale_result = first_store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "events",
        resource: "orders",
        expected_resource_next_offset: 1,
        events: &third_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });
    let fresh_result = first_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 2,
            events: &third_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("fresh retry should commit");

    // Assert
    assert_eq!(
        stale_result.expect_err("stale expected resource offset should fail"),
        "ERR_CONCURRENCY_CONFLICT"
    );
    assert_eq!(fresh_result.first_resource_offset, 2);
    assert_eq!(
        first_store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("next resource offset"),
        3
    );
}

#[test]
fn should_not_advance_sequence_cache_given_injected_commit_failure() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let failed_events = single_event(b"failed");
    let retry_events = single_event(b"retry");
    store.fail_next_promotion_frontier_commit_for_tests();

    // Act
    let failed = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "events",
        resource: "orders",
        expected_resource_next_offset: 0,
        events: &failed_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });
    let retry = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &retry_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("retry commit should reuse original offsets");
    let (records, cursor) = store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read committed area records");
    let records = event_records(records);

    // Assert
    assert_eq!(
        failed.expect_err("injected commit failure should fail"),
        "Injected stream commit failure"
    );
    assert_eq!(retry.first_resource_offset, 0);
    assert_eq!(retry.first_area_offset, 0);
    assert_eq!(retry.first_realm_offset, 0);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, Bytes::from_static(b"retry"));
    assert_eq!(cursor.last_area_offset, Some(0));
    assert!(!cursor.has_more);
}

#[test]
fn should_reject_future_expected_resource_offset_given_store_commit() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"first");
    let future_events = single_event(b"future");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("seed commit");

    // Act
    let result = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "events",
        resource: "orders",
        expected_resource_next_offset: 2,
        events: &future_events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });

    // Assert
    let error = result.expect_err("future expected offset should fail store commit");
    assert_eq!(error, "ERR_CONCURRENCY_CONFLICT");
    assert_eq!(
        store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("next resource offset"),
        1
    );
}

#[test]
fn should_report_has_more_given_single_record_resource_fast_path() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first commit");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            from_offset: 0,
            limit: 1,
            max_bytes: None,
        })
        .expect("read first resource record");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].resource_offset, 0);
    assert!(cursor.has_more);
    assert_eq!(cursor.last_resource_offset, 0);
}

#[test]
fn should_not_report_has_more_given_single_record_resource_fast_path_at_end() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"first");
    let second_events = single_event(b"second");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first commit");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            from_offset: 1,
            limit: 1,
            max_bytes: None,
        })
        .expect("read last resource record");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].resource_offset, 1);
    assert!(!cursor.has_more);
    assert_eq!(cursor.last_resource_offset, 1);
}

#[test]
fn should_return_matching_record_given_filtered_resource_read() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event_with_discriminator(b"first", "keep");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit filtered resource record");

    // Act
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("keep".to_string())],
    };
    let (records, cursor) = store
        .read_resource_with_filter(
            &ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: 0,
                limit: 1,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("read filtered resource record");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].resource_offset, 0);
    assert_eq!(records[0].body, Bytes::from_static(b"first"));
    assert!(!cursor.has_more);
    assert_eq!(cursor.last_resource_offset, 0);
}

#[test]
fn should_skip_non_matching_records_given_filtered_realm_read() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let keep_events = single_event_with_discriminator(b"keep", "match");
    let skip_events = single_event_with_discriminator(b"skip", "ignore");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &keep_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit matching realm record");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "audits",
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &skip_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit non-matching realm record");

    // Act
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("match".to_string())],
    };
    let (records, cursor) = store
        .read_realm_with_filter(1, "test", 0, 10, None, Some(&filter))
        .expect("read filtered realm stream");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].realm_offset, Some(0));
    assert_eq!(records[0].body, Bytes::from_static(b"keep"));
    assert!(!cursor.has_more);
    assert_eq!(cursor.last_realm_offset, Some(1));
}

#[test]
fn should_page_filtered_resource_read_through_filtered_items() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"skip-0"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        },
        EventPayload {
            body: Bytes::from_static(b"skip-1"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        },
        EventPayload {
            body: Bytes::from_static(b"keep"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("match")),
        },
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit filtered resource records");

    // Act
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("match".to_string())],
    };
    let (first_page, first_cursor) = store
        .read_resource_with_filter(
            &ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: 0,
                limit: 2,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("read first filtered page");
    let (second_page, second_cursor) = store
        .read_resource_with_filter(
            &ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: first_cursor.last_resource_offset + 1,
                limit: 2,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("read second filtered page");
    let second_records = event_records(second_page);

    // Assert
    assert_eq!(first_page.len(), 2);
    assert!(matches!(
        first_page[0],
        StreamReadItem::Filtered { offset: 0, .. }
    ));
    assert!(matches!(
        first_page[1],
        StreamReadItem::Filtered { offset: 1, .. }
    ));
    assert!(first_cursor.has_more);
    assert_eq!(first_cursor.last_resource_offset, 1);
    assert_eq!(second_records.len(), 1);
    assert_eq!(second_records[0].body, Bytes::from_static(b"keep"));
    assert_eq!(second_records[0].resource_offset, 2);
    assert!(!second_cursor.has_more);
}

#[test]
fn should_page_filtered_area_read_through_filtered_items() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"skip-0"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        },
        EventPayload {
            body: Bytes::from_static(b"skip-1"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        },
        EventPayload {
            body: Bytes::from_static(b"keep"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("match")),
        },
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit filtered area records");

    // Act
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("match".to_string())],
    };
    let (first_page, first_cursor) = store
        .read_area_with_filter(
            &ReadAreaParams {
                family: 1,
                realm: "test",
                area: "events",
                from_offset: 0,
                limit: 2,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("read first filtered area page");
    let (second_page, second_cursor) = store
        .read_area_with_filter(
            &ReadAreaParams {
                family: 1,
                realm: "test",
                area: "events",
                from_offset: first_cursor.last_area_offset.expect("area cursor") + 1,
                limit: 2,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("read second filtered area page");
    let second_records = event_records(second_page);

    // Assert
    assert_eq!(first_page.len(), 2);
    assert!(matches!(
        first_page[0],
        StreamReadItem::Filtered { offset: 0, .. }
    ));
    assert!(matches!(
        first_page[1],
        StreamReadItem::Filtered { offset: 1, .. }
    ));
    assert!(first_cursor.has_more);
    assert_eq!(first_cursor.last_area_offset, Some(1));
    assert_eq!(second_records.len(), 1);
    assert_eq!(second_records[0].body, Bytes::from_static(b"keep"));
    assert_eq!(second_records[0].area_offset, Some(2));
    assert!(!second_cursor.has_more);
}

#[test]
fn should_page_filtered_realm_read_through_filtered_items() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"skip-0"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        },
        EventPayload {
            body: Bytes::from_static(b"skip-1"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        },
        EventPayload {
            body: Bytes::from_static(b"keep"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("match")),
        },
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit filtered realm records");

    // Act
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("match".to_string())],
    };
    let (first_page, first_cursor) = store
        .read_realm_with_filter(1, "test", 0, 2, None, Some(&filter))
        .expect("read first filtered realm page");
    let (second_page, second_cursor) = store
        .read_realm_with_filter(
            1,
            "test",
            first_cursor.last_realm_offset.expect("realm cursor") + 1,
            2,
            None,
            Some(&filter),
        )
        .expect("read second filtered realm page");
    let second_records = event_records(second_page);

    // Assert
    assert_eq!(first_page.len(), 2);
    assert!(matches!(
        first_page[0],
        StreamReadItem::Filtered { offset: 0, .. }
    ));
    assert!(matches!(
        first_page[1],
        StreamReadItem::Filtered { offset: 1, .. }
    ));
    assert!(first_cursor.has_more);
    assert_eq!(first_cursor.last_realm_offset, Some(1));
    assert_eq!(second_records.len(), 1);
    assert_eq!(second_records[0].body, Bytes::from_static(b"keep"));
    assert_eq!(second_records[0].realm_offset, Some(2));
    assert!(!second_cursor.has_more);
}
