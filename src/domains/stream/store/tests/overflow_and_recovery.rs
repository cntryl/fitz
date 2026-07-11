use super::*;

#[test]
fn should_reject_commit_given_resource_offset_exhaustion() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    store
        .ensure_layout_activation_for_family(1)
        .expect("activate stream layout");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin metadata transaction");
    txn.put(
        encode_resource_meta_key("test", "events", "orders"),
        ResourceMetaValue {
            next_offset: u64::MAX,
            committed_size_bytes: 0,
        }
        .encode(),
        None,
    )
    .expect("write exhausted resource metadata");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit exhausted resource metadata");
    let events = single_event(b"overflow");

    // Act
    let result = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "events",
        resource: "orders",
        expected_resource_next_offset: u64::MAX,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });

    // Assert
    assert_eq!(
        result.expect_err("offset exhaustion should reject commit"),
        "ERR_STREAM_OFFSET_EXHAUSTED"
    );
    assert_eq!(
        store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("read exhausted resource metadata"),
        u64::MAX
    );
}

#[test]
fn should_reject_commit_given_committed_size_exhaustion() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    store
        .ensure_layout_activation_for_family(1)
        .expect("activate stream layout");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin metadata transaction");
    txn.put(
        encode_resource_meta_key("test", "events", "orders"),
        ResourceMetaValue {
            next_offset: 0,
            committed_size_bytes: u64::MAX,
        }
        .encode(),
        None,
    )
    .expect("write exhausted resource metadata");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit exhausted resource metadata");
    let events = single_event(b"overflow");

    // Act
    let result = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "events",
        resource: "orders",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });

    // Assert
    assert_eq!(
        result.expect_err("size exhaustion should reject commit"),
        "ERR_STREAM_SIZE_EXHAUSTED"
    );
    assert_eq!(
        store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("read exhausted resource metadata"),
        0
    );
}

#[test]
fn should_isolate_resource_offsets_between_route_families() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1, 2]));
    let events = single_event(b"family-one");
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
        .expect("commit family one event");

    // Act
    let family_two_next = store
        .get_next_resource_offset(2, "test", "events", "orders")
        .expect("read family two next offset");
    let (family_two_records, family_two_cursor) = store
        .read_resource(&ReadResourceParams {
            family: 2,
            realm: "test",
            area: "events",
            resource: "orders",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        })
        .expect("read family two resource");

    // Assert
    assert_eq!(family_two_next, 0);
    assert!(family_two_records.is_empty());
    assert_eq!(family_two_cursor.last_resource_offset, 0);
}

#[test]
fn should_ignore_empty_tail_page_during_resource_recovery() {
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
        .expect("commit resource event");
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin sparse page transaction");
    txn.delete(encode_resource_meta_key("test", "events", "orders"))
        .expect("delete resource metadata");
    txn.put(
        encode_compact_resource_page_key(
            "test",
            "events",
            "orders",
            REALM_PAGE_RECORD_LIMIT as u64,
        ),
        CompactResourcePageValue {
            records: Vec::new(),
        }
        .encode(),
        None,
    )
    .expect("write empty tail page");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit sparse page transaction");

    // Act
    let last_offset = store
        .get_last_resource_offset(1, "test", "events", "orders")
        .expect("read resource tail");
    let next_offset = store
        .get_next_resource_offset(1, "test", "events", "orders")
        .expect("recover next resource offset");

    // Assert
    assert_eq!(last_offset, Some(0));
    assert_eq!(next_offset, 1);
}
