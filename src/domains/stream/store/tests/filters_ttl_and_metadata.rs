use super::*;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

struct TestStreamClock {
    epoch_ms: AtomicU64,
}

impl TestStreamClock {
    fn new(epoch_ms: u64) -> Self {
        Self {
            epoch_ms: AtomicU64::new(epoch_ms),
        }
    }

    fn set(&self, epoch_ms: u64) {
        self.epoch_ms.store(epoch_ms, Ordering::Release);
    }
}

impl crate::runtime::clock::Clock for TestStreamClock {
    fn now_instant(&self) -> std::time::Instant {
        std::time::Instant::now()
    }

    fn now_epoch_ms(&self) -> u64 {
        self.epoch_ms.load(Ordering::Acquire)
    }
}

#[test]
fn should_compact_zero_ttl_fragments_without_positional_gaps() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::with_config(
        db.clone(),
        BatchLimits::default(),
        StreamTTL::with_seconds(0),
    );
    for round in 0..2 {
        for offset in round * 9..(round + 1) * 9 {
            store
                .commit_records(CommitRecordsParams {
                    family: 1,
                    realm: "north",
                    area: "orders",
                    resource: "created",
                    expected_resource_next_offset: offset,
                    events: &single_event(b"expired"),
                    ingest_metadata: None,
                    mode: StreamWriteMode::Sync,
                })
                .expect("commit zero-TTL fragment");
        }
        store
            .run_maintenance(1)
            .expect("compact zero-TTL fragment round");
    }

    // Act
    let result = store.run_maintenance(1);
    let records = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 64,
            max_bytes: None,
        })
        .expect("read zero-TTL resource")
        .0;
    let compacted_rows: Vec<_> = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin zero-TTL generation scan")
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compact_resource_page_prefix("north", "orders", "created"),
        )))
        .expect("scan zero-TTL generations")
        .try_collect()
        .expect("collect zero-TTL generations");
    let generation = u64::from_be_bytes(
        compacted_rows[0].0[compacted_rows[0].0.len() - 8..]
            .try_into()
            .expect("decode compacted generation"),
    );

    // Assert
    assert_eq!(
        result.expect("drain zero-TTL fragments").buckets_compacted,
        0
    );
    assert!(records.is_empty());
    assert_eq!(compacted_rows.len(), 1);
    assert_eq!(generation, 2);
}

fn compacted_resource_records(db: &cntryl_midge::Engine) -> Vec<CompactResourcePageRecord> {
    let rows: Vec<_> = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin compacted TTL row scan")
        .scan(&cntryl_midge::Query::new().prefix(Bytes::from(
            StreamStore::build_compact_resource_page_prefix("north", "orders", "created"),
        )))
        .expect("scan compacted TTL resource rows")
        .try_collect()
        .expect("collect compacted TTL resource rows");
    CompactResourcePageValue::try_decode(&rows[0].1)
        .expect("decode compacted TTL resource row")
        .records
}

#[test]
fn should_preserve_absolute_expiration_before_and_after_compaction() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let clock = Arc::new(TestStreamClock::new(1_000));
    let store = StreamStore::with_config(
        db.clone(),
        BatchLimits::default(),
        StreamTTL::with_seconds(10),
    )
    .with_clock_for_tests(clock.clone());
    for offset in 0..5 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: offset,
                events: &single_event(b"early"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit early TTL fragment");
    }
    clock.set(5_000);
    for offset in 5..9 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: offset,
                events: &single_event(b"late"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit late TTL fragment");
    }
    clock.set(12_000);

    // Act
    let before = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 64,
            max_bytes: None,
        })
        .expect("read before TTL compaction")
        .0;
    let maintenance = store.run_maintenance(1).expect("compact TTL fragments");
    let after = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 0,
            limit: 64,
            max_bytes: None,
        })
        .expect("read after TTL compaction")
        .0;
    let area_after = store
        .read_area(1, "north", "orders", 0, 64, None)
        .expect("read area after TTL compaction")
        .0;
    let realm_after = store
        .read_realm(1, "north", 0, 64, None)
        .expect("read realm after TTL compaction")
        .0;
    let compacted_records = compacted_resource_records(&db);
    let reopened =
        StreamStore::with_config(db, BatchLimits::default(), StreamTTL::with_seconds(10))
            .with_clock_for_tests(clock);
    let global_after_reopen = reopened
        .read_global(1, 0, 64, None, None)
        .expect("read global after reopen")
        .0;

    // Assert
    let before = event_records(before);
    let after = event_records(after);
    assert_eq!(before.len(), 4);
    assert!(before.iter().all(|record| record.body == b"late"[..]));
    assert_eq!(after.len(), 4);
    assert!(after.iter().all(|record| record.body == b"late"[..]));
    assert_eq!(event_records(area_after).len(), 4);
    assert_eq!(event_records(realm_after).len(), 4);
    assert_eq!(event_records(global_after_reopen).len(), 4);
    assert!(compacted_records[..5]
        .iter()
        .all(|record| record.body.is_empty() && record.metadata.is_none()));
    assert!(compacted_records[5..]
        .iter()
        .all(|record| record.body == b"late"[..]));
    assert!(maintenance.buckets_compacted > 0);
}

#[test]
fn should_resume_filtered_resource_read_across_compact_page_boundary() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let mut events = Vec::with_capacity(REALM_PAGE_RECORD_LIMIT + 1);
    for _ in 0..REALM_PAGE_RECORD_LIMIT {
        events.push(EventPayload {
            body: Bytes::from_static(b"skip"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("ignore")),
        });
    }
    events.push(EventPayload {
        body: Bytes::from_static(b"keep"),
        metadata: None,
        discriminator: Some(StreamDiscriminator::from("match")),
    });
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
        .expect("commit page-boundary filtered records");

    // Act
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("match".to_string())],
    };
    let boundary_offset = REALM_PAGE_RECORD_LIMIT as u64 - 1;
    let (first_page, first_cursor) = store
        .read_resource_with_filter(
            &ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: boundary_offset,
                limit: 1,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("read filtered page boundary");
    let (second_page, second_cursor) = store
        .read_resource_with_filter(
            &ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: first_cursor.last_resource_offset + 1,
                limit: 1,
                max_bytes: None,
            },
            Some(&filter),
        )
        .expect("resume filtered page boundary");
    let second_records = event_records(second_page);

    // Assert
    assert_eq!(first_page.len(), 1);
    assert!(matches!(
        first_page[0],
        StreamReadItem::Filtered { offset, .. } if offset == boundary_offset
    ));
    assert!(first_cursor.has_more);
    assert_eq!(first_cursor.last_resource_offset, boundary_offset);
    assert_eq!(second_records.len(), 1);
    assert_eq!(second_records[0].body, Bytes::from_static(b"keep"));
    assert_eq!(
        second_records[0].resource_offset,
        REALM_PAGE_RECORD_LIMIT as u64
    );
    assert!(!second_cursor.has_more);
}

#[test]
fn should_apply_filter_clause_variants_given_discriminated_resource_records() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"alpha"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("alpha.created")),
        },
        EventPayload {
            body: Bytes::from_static(b"beta"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("beta.created")),
        },
        EventPayload {
            body: Bytes::from_static(b"missing"),
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
        .expect("commit discriminated records");

    // Act
    let starts_with = StreamFilterSet {
        clauses: vec![StreamFilterClause::StartsWith("alpha".to_string())],
    };
    let any_of = StreamFilterSet {
        clauses: vec![StreamFilterClause::AnyOf(vec![
            "beta.created".to_string(),
            "missing".to_string(),
        ])],
    };
    let not_equals = StreamFilterSet {
        clauses: vec![StreamFilterClause::NotEquals("alpha.created".to_string())],
    };
    let starts_with_records = event_records(
        store
            .read_resource_with_filter(
                &ReadResourceParams {
                    family: 1,
                    realm: "test",
                    area: "events",
                    resource: "orders",
                    from_offset: 0,
                    limit: 10,
                    max_bytes: None,
                },
                Some(&starts_with),
            )
            .expect("read starts-with filter")
            .0,
    );
    let any_of_records = event_records(
        store
            .read_resource_with_filter(
                &ReadResourceParams {
                    family: 1,
                    realm: "test",
                    area: "events",
                    resource: "orders",
                    from_offset: 0,
                    limit: 10,
                    max_bytes: None,
                },
                Some(&any_of),
            )
            .expect("read any-of filter")
            .0,
    );
    let not_equals_records = event_records(
        store
            .read_resource_with_filter(
                &ReadResourceParams {
                    family: 1,
                    realm: "test",
                    area: "events",
                    resource: "orders",
                    from_offset: 0,
                    limit: 10,
                    max_bytes: None,
                },
                Some(&not_equals),
            )
            .expect("read not-equals filter")
            .0,
    );

    // Assert
    assert_eq!(starts_with_records.len(), 1);
    assert_eq!(starts_with_records[0].body, Bytes::from_static(b"alpha"));
    assert_eq!(any_of_records.len(), 1);
    assert_eq!(any_of_records[0].body, Bytes::from_static(b"beta"));
    assert_eq!(not_equals_records.len(), 2);
    assert_eq!(not_equals_records[0].body, Bytes::from_static(b"beta"));
    assert_eq!(not_equals_records[1].body, Bytes::from_static(b"missing"));
}

#[test]
fn should_return_next_available_resource_record_given_trimmed_compact_resource_page_on_ttl_store() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::with_config(
        db.clone(),
        BatchLimits::default(),
        StreamTTL::with_seconds(1),
    );
    let first_page_events = vec![
        EventPayload {
            body: Bytes::from_static(b"first-page"),
            metadata: None,
            discriminator: None,
        };
        REALM_PAGE_RECORD_LIMIT
    ];
    let second_page_events = single_event(b"second-page");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &first_page_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("first page commit");
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: REALM_PAGE_RECORD_LIMIT as u64,
            events: &second_page_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second page commit");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin ttl trim tx");
    txn.delete(encode_compact_resource_page_key(
        "test", "events", "orders", 0,
    ))
    .expect("delete trimmed resource page");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit ttl trim simulation");

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
        .expect("read ttl-trimmed resource stream");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].resource_offset, REALM_PAGE_RECORD_LIMIT as u64);
    assert_eq!(records[0].body, Bytes::from_static(b"second-page"));
    assert_eq!(cursor.last_resource_offset, REALM_PAGE_RECORD_LIMIT as u64);
    assert!(!cursor.has_more);
}

#[test]
fn should_peek_resource_given_missing_offset_counter_and_present_resource_meta() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    let events = single_event(b"first");
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
        .expect("commit record");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin cleanup tx");
    txn.delete(encode_offset_counter_key("test", "events", "orders"))
        .expect("delete legacy offset counter");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit legacy offset counter removal");

    // Act
    let record = store
        .peek_resource(1, "test", "events", "orders")
        .expect("peek exact resource")
        .expect("expected tail record");

    // Assert
    assert_eq!(record.resource_offset, 0);
    assert_eq!(record.body, Bytes::from_static(b"first"));
}

#[test]
fn should_not_report_has_more_given_area_read_at_end() {
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
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 0, 2, None)
        .expect("read area stream");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].area_offset, Some(0));
    assert_eq!(records[1].area_offset, Some(1));
    assert_eq!(cursor.last_area_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_return_record_given_area_read_at_watermark_boundary() {
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
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 1, 1, None)
        .expect("read area at watermark boundary");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].area_offset, Some(1));
    assert_eq!(records[0].body, Bytes::from_static(b"second"));
    assert_eq!(cursor.last_area_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_truncate_area_read_given_max_bytes() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"abcd");
    let second_events = single_event(b"efgh");
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
            resource: "audits",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 0, 10, Some(4))
        .expect("read area with max_bytes");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, Bytes::from_static(b"abcd"));
    assert_eq!(records[0].area_offset, Some(0));
    assert_eq!(cursor.last_area_offset, Some(0));
    assert!(cursor.has_more);
}

#[test]
fn should_return_first_area_record_given_max_bytes_below_record_size() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event(b"abcde");
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
        .expect("commit record");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 0, 10, Some(4))
        .expect("read area with tight max_bytes");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, Bytes::from_static(b"abcde"));
    assert_eq!(cursor.last_area_offset, Some(0));
    assert!(!cursor.has_more);
}

#[test]
fn should_not_report_has_more_given_realm_read_at_end() {
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
            area: "audit",
            resource: "entries",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 0, 2, None)
        .expect("read realm stream");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].realm_offset, Some(0));
    assert_eq!(records[1].realm_offset, Some(1));
    assert_eq!(cursor.last_realm_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_return_record_given_realm_read_at_watermark_boundary() {
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
            area: "audit",
            resource: "entries",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 1, 1, None)
        .expect("read realm at watermark boundary");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].realm_offset, Some(1));
    assert_eq!(records[0].body, Bytes::from_static(b"second"));
    assert_eq!(cursor.last_realm_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_truncate_realm_read_given_max_bytes() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let first_events = single_event(b"abcd");
    let second_events = single_event(b"efgh");
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
            area: "audit",
            resource: "entries",
            expected_resource_next_offset: 0,
            events: &second_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("second commit");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 0, 10, Some(4))
        .expect("read realm with max_bytes");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, Bytes::from_static(b"abcd"));
    assert_eq!(records[0].realm_offset, Some(0));
    assert_eq!(cursor.last_realm_offset, Some(0));
    assert!(cursor.has_more);
}

#[test]
fn should_return_first_realm_record_given_max_bytes_below_record_size() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = single_event(b"abcde");
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
        .expect("commit record");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 0, 10, Some(4))
        .expect("read realm with tight max_bytes");
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, Bytes::from_static(b"abcde"));
    assert_eq!(cursor.last_realm_offset, Some(0));
    assert!(!cursor.has_more);
}

#[test]
fn should_read_realm_records_given_recreated_partial_compact_page() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let first_store = StreamStore::new(db.clone());
    let second_store = StreamStore::new(db);
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
        .expect("second commit");

    // Act
    let records = second_store
        .read_realm(1, "test", 1, 10, None)
        .expect("read realm from offset one")
        .0;
    let records = event_records(records);

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].realm_offset, Some(1));
    assert_eq!(records[0].body, Bytes::from_static(b"second"));
}

#[test]
fn should_return_error_given_malformed_compact_realm_page_when_reading_realm() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    store
        .set_realm_watermark(1, "test", 0)
        .expect("seed realm watermark");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    txn.put(
        encode_compressed_compact_realm_page_key("test", 0),
        vec![0, 0xB2, 1, 0, 0, 0],
        None,
    )
    .expect("write malformed compact realm page");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit malformed compact realm page");

    // Act
    let result = store.read_realm(1, "test", 0, 10, None);

    // Assert
    let error = result.expect_err("malformed compact realm page should fail read");
    assert!(error.contains("ERR_INVALID_COMPACT_REALM_PAGE"));
}
