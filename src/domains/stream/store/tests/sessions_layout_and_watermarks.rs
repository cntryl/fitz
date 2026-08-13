use super::*;
pub(super) use crate::domains::stream::protocol::StreamFilterClause;
pub(super) use crate::domains::stream::storage::{encode_offset_counter_key, OffsetCounterValue};
pub(super) use crate::testkit::create_test_engine_with_cfs;
pub(super) use bytes::Bytes;

pub(super) fn read_layout_marker(
    engine: &cntryl_midge::Engine,
    family: u32,
) -> Option<StreamStorageLayout> {
    let txn = engine
        .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    txn.get(&encode_stream_layout_marker_key())
        .expect("read layout marker")
        .map(|bytes| {
            StreamLayoutMarkerValue::decode(&bytes)
                .expect("decode layout marker")
                .layout
        })
}

pub(super) fn read_layout_marker_bytes(
    engine: &cntryl_midge::Engine,
    family: u32,
) -> Option<Bytes> {
    let txn = engine
        .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    txn.get(&encode_stream_layout_marker_key())
        .expect("read layout marker")
}

pub(super) fn write_layout_marker(
    engine: &cntryl_midge::Engine,
    family: u32,
    layout: StreamStorageLayout,
) {
    let mut txn = engine
        .begin_tx(family, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    txn.put(
        encode_stream_layout_marker_key(),
        StreamLayoutMarkerValue::new(layout).encode(),
        None,
    )
    .expect("write layout marker");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit layout marker");
}

pub(super) fn single_event(body: &'static [u8]) -> Vec<EventPayload> {
    vec![EventPayload {
        body: Bytes::from_static(body),
        metadata: None,
        discriminator: None,
    }]
}

pub(super) fn single_event_with_discriminator(
    body: &'static [u8],
    discriminator: &'static str,
) -> Vec<EventPayload> {
    vec![EventPayload {
        body: Bytes::from_static(body),
        metadata: None,
        discriminator: Some(StreamDiscriminator::from(discriminator)),
    }]
}

pub(super) fn event_record(item: StreamReadItem) -> Option<StreamRecord> {
    match item {
        StreamReadItem::Event(record) => Some(record),
        _ => None,
    }
}

pub(super) fn event_records(items: Vec<StreamReadItem>) -> Vec<StreamRecord> {
    items.into_iter().filter_map(event_record).collect()
}

#[test]
pub(super) fn should_commit_stream_and_watermarks_with_background_cloud_write_options() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path(),
                "fitz-stream-writes",
                "background",
            )
            .build()
            .expect("build cloud-simulated options"),
        )
        .expect("open cloud-simulated engine"),
    );
    engine
        .create_column_family("tenant_default")
        .expect("create route-family column family");
    let store = StreamStore::with_storage_layout(
        crate::storage::FitzStorageEngine::new(Arc::clone(&engine)),
        StreamStorageLayout::PromotionFrontier,
    )
    .with_write_options(
        cntryl_midge::WriteOptions::cloud_async(),
        cntryl_midge::WriteOptions::cloud_async(),
    );
    let events = single_event(b"cloud-event");

    // Act
    let commit = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "acme",
        area: "events",
        resource: "orders",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });
    let area_watermark = store.set_watermark(1, "acme", "events", 1);
    let realm_watermark = store.set_realm_watermark(1, "acme", 1);

    // Assert
    assert!(commit.is_ok(), "cloud stream commit failed: {commit:?}");
    assert!(
        area_watermark.is_ok(),
        "cloud area-watermark commit failed: {area_watermark:?}"
    );
    assert!(
        realm_watermark.is_ok(),
        "cloud realm-watermark commit failed: {realm_watermark:?}"
    );
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
pub(super) fn should_reject_append_given_stream_session_route_family_mismatch() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1, 2]));
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");

    // Act
    let result = store.append_to_session(
        2,
        session_id,
        EventPayload {
            body: Bytes::from_static(b"wrong-family"),
            metadata: None,
            discriminator: None,
        },
    );

    // Assert
    assert_eq!(
        result.expect_err("family mismatch append should fail"),
        "ERR_SESSION_ROUTE_FAMILY_MISMATCH"
    );
    assert_eq!(store.session_event_count(session_id), Some(0));
}

#[test]
pub(super) fn should_count_discriminator_bytes_against_legacy_session_limit() {
    // Arrange
    let store = StreamStore::with_limits(
        create_test_engine_with_cfs(vec![1]),
        BatchLimits {
            max_batch_events: 4,
            max_batch_bytes: 5,
        },
    );
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");

    // Act
    let result = store.append_to_session(
        1,
        session_id,
        EventPayload {
            body: Bytes::from_static(b"a"),
            metadata: None,
            discriminator: Some(StreamDiscriminator::from("12345")),
        },
    );

    // Assert
    assert_eq!(
        result.expect_err("discriminator should count toward batch bytes"),
        "ERR_BATCH_TOO_LARGE: total 0 + event 6 exceeds max_batch_bytes 5"
    );
    assert_eq!(store.session_event_count(session_id), Some(0));
}

#[test]
pub(super) fn should_preserve_session_given_commit_route_family_mismatch() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1, 2]));
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"right-family"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append in original family");

    // Act
    let result = store.commit_session(2, session_id, 0, 0, 0, StreamWriteMode::Buffered);

    // Assert
    assert_eq!(
        result.expect_err("family mismatch commit should fail"),
        "ERR_SESSION_ROUTE_FAMILY_MISMATCH"
    );
    assert_eq!(store.session_event_count(session_id), Some(1));
    assert_eq!(read_layout_marker(store.db.as_ref(), 2), None);
    let commit = store
        .commit_session(1, session_id, 0, 0, 0, StreamWriteMode::Buffered)
        .expect("commit preserved session in original family");
    assert_eq!(commit.first_resource_offset, 0);
}

#[test]
pub(super) fn should_reject_stale_resource_offset_given_session_commit() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &single_event(b"existing"),
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit existing record");
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"retry"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append retry record");

    // Act
    let stale = store.commit_session(1, session_id, 0, 1, 1, StreamWriteMode::Buffered);
    let retry = store
        .commit_session(1, session_id, 1, 1, 1, StreamWriteMode::Buffered)
        .expect("retry with durable next offsets");

    // Assert
    assert_eq!(
        stale.expect_err("stale resource offset should fail"),
        "ERR_CONCURRENCY_CONFLICT"
    );
    assert_eq!(retry.first_resource_offset, 1);
    assert_eq!(retry.first_area_offset, 1);
    assert_eq!(retry.first_realm_offset, 1);
    assert_eq!(store.session_event_count(session_id), None);
}

#[test]
pub(super) fn should_retry_stale_area_offset_given_session_commit() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "seed",
            expected_resource_next_offset: 0,
            events: &single_event(b"existing"),
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit existing area record");
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"retry"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append retry record");

    // Act
    let committed = store
        .commit_session(1, session_id, 0, 0, 1, StreamWriteMode::Buffered)
        .expect("retry stale area offset internally");

    // Assert
    assert_eq!(committed.first_resource_offset, 0);
    assert_eq!(committed.first_area_offset, 1);
    assert_eq!(committed.first_realm_offset, 1);
    assert_eq!(store.session_event_count(session_id), None);
}

#[test]
pub(super) fn should_retry_stale_realm_offset_given_session_commit() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "seed",
            resource: "seed",
            expected_resource_next_offset: 0,
            events: &single_event(b"existing"),
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .expect("commit existing realm record");
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"retry"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append retry record");

    // Act
    let committed = store
        .commit_session(1, session_id, 0, 0, 0, StreamWriteMode::Buffered)
        .expect("retry stale realm offset internally");

    // Assert
    assert_eq!(committed.first_resource_offset, 0);
    assert_eq!(committed.first_area_offset, 0);
    assert_eq!(committed.first_realm_offset, 1);
    assert_eq!(store.session_event_count(session_id), None);
}

#[test]
pub(super) fn should_preserve_session_given_injected_session_commit_failure() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let session_id = store
        .begin_session(1, "test", "events", "orders", None)
        .expect("begin stream session");
    store
        .append_to_session(
            1,
            session_id,
            EventPayload {
                body: Bytes::from_static(b"retry"),
                metadata: None,
                discriminator: None,
            },
        )
        .expect("append retry record");
    store.fail_next_promotion_frontier_commit_for_tests();

    // Act
    let failed = store.commit_session(1, session_id, 0, 0, 0, StreamWriteMode::Buffered);
    let session_count_after_failure = store.session_event_count(session_id);
    let next_offset_after_failure = store
        .get_next_resource_offset(1, "test", "events", "orders")
        .expect("read next offset after failed commit");
    let retry = store
        .commit_session(1, session_id, 0, 0, 0, StreamWriteMode::Buffered)
        .expect("retry preserved stream session");

    // Assert
    assert_eq!(
        failed.expect_err("injected commit failure should fail"),
        "Injected stream commit failure"
    );
    assert_eq!(session_count_after_failure, Some(1));
    assert_eq!(store.session_event_count(session_id), None);
    assert_eq!(next_offset_after_failure, 0);
    assert_eq!(retry.first_resource_offset, 0);
    assert_eq!(retry.first_area_offset, 0);
    assert_eq!(retry.first_realm_offset, 0);
}

#[test]
pub(super) fn should_return_empty_direct_store_reads_given_zero_limit_with_committed_data() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
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
        .expect("commit stream record");

    // Act
    let (resource_items, resource_cursor) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            from_offset: 0,
            limit: 0,
            max_bytes: None,
        })
        .expect("read resource with zero limit");
    let (area_items, area_cursor) = store
        .read_area(1, "test", "events", 0, 0, None)
        .expect("read area with zero limit");
    let (realm_items, realm_cursor) = store
        .read_realm(1, "test", 0, 0, None)
        .expect("read realm with zero limit");

    // Assert
    assert!(resource_items.is_empty());
    assert!(!resource_cursor.has_more);
    assert_eq!(resource_cursor.last_resource_offset, 0);
    assert!(area_items.is_empty());
    assert!(!area_cursor.has_more);
    assert_eq!(area_cursor.last_area_offset, Some(0));
    assert!(realm_items.is_empty());
    assert!(!realm_cursor.has_more);
    assert_eq!(realm_cursor.last_realm_offset, Some(0));
}

#[test]
pub(super) fn should_reuse_sequence_guard_given_same_resource() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

    // Act
    let first = store.resource_sequence_guard(1, "test", "events", "orders");
    let second = store.resource_sequence_guard(1, "test", "events", "orders");

    // Assert
    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
pub(super) fn should_create_distinct_sequence_guards_given_different_resources() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

    // Act
    let left = store.resource_sequence_guard(1, "test", "events", "orders");
    let right = store.resource_sequence_guard(1, "test", "events", "audits");

    // Assert
    assert!(!Arc::ptr_eq(&left, &right));
}

#[test]
pub(super) fn should_use_promotion_frontier_stream_storage_layout_by_default() {
    // Arrange

    // Act
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

    // Assert
    assert_eq!(
        store.storage_layout(),
        StreamStorageLayout::PromotionFrontier
    );
}

#[test]
pub(super) fn should_use_selected_stream_storage_layout_given_explicit_layout() {
    // Arrange

    // Act
    let store = StreamStore::with_layout(
        create_test_engine_with_cfs(vec![1]),
        StreamStorageLayout::PromotionFrontier,
    );

    // Assert
    assert_eq!(
        store.storage_layout(),
        StreamStorageLayout::PromotionFrontier
    );
}

#[test]
pub(super) fn should_persist_promotion_frontier_stream_layout_marker_given_first_real_store_write()
{
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    let events = single_event(b"first");

    // Act
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
        .expect("commit records");

    // Assert
    assert_eq!(
        read_layout_marker(db.as_ref(), 1),
        Some(StreamStorageLayout::PromotionFrontier)
    );
    assert!(read_layout_marker_bytes(db.as_ref(), 1)
        .expect("fresh store layout marker")
        .starts_with(&[0, 0xD4]));
}

#[test]
pub(super) fn should_mark_existing_families_given_promotion_boot_scan() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1, 2]);
    let store = StreamStore::new(db.clone());

    // Act
    store
        .ensure_layout_activation_for_existing_families()
        .expect("boot scan should succeed for existing families");

    // Assert
    assert_eq!(
        read_layout_marker(db.as_ref(), 1),
        Some(StreamStorageLayout::PromotionFrontier)
    );
    assert_eq!(
        read_layout_marker(db.as_ref(), 2),
        Some(StreamStorageLayout::PromotionFrontier)
    );
}

#[test]
pub(super) fn should_return_error_given_unmarked_stream_data_on_default_promotion_layout() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    txn.put(
        encode_offset_counter_key("test", "events", "orders"),
        OffsetCounterValue { next_offset: 1 }.encode(),
        None,
    )
    .expect("write unmarked stream metadata");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit unmarked stream metadata");
    let store = StreamStore::new(db);

    // Act
    let result = store.get_next_resource_offset(1, "test", "events", "orders");

    // Assert
    let error = result.expect_err("promotion frontier should reject unmarked legacy data");
    assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED"));
}

#[test]
fn should_reject_d3_layout_with_export_or_reset_guidance() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin D3 marker transaction");
    txn.put(encode_stream_layout_marker_key(), vec![0, 0xD3, 1], None)
        .expect("write D3 marker");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit D3 marker");
    let store = StreamStore::new(db);

    // Act
    let result = store.get_next_resource_offset(1, "test", "events", "orders");

    // Assert
    let error = result.expect_err("D3 must be a clean on-disk break");
    assert!(error.contains("stored=D3 requested=D4"));
    assert!(error.contains("export/replay"));
    assert!(error.contains("reset"));
}

#[test]
pub(super) fn should_reject_corrupt_persisted_rows_for_each_stream_decoder_family() {
    // Arrange
    let stream_key = |prefix| {
        crate::utils::storage_key::prefixed_key(
            "test",
            crate::utils::storage_key::DomainKeyspace::Stream,
            &[prefix],
        )
    };
    let corrupt_rows = [
        (encode_stream_layout_marker_key(), "layout_marker"),
        (stream_key(KeyPrefix::Resource as u8), "resource"),
        (stream_key(KeyPrefix::Area as u8), "area"),
        (stream_key(KeyPrefix::Realm as u8), "realm"),
        (stream_key(KeyPrefix::Watermark as u8), "watermark"),
        (stream_key(KeyPrefix::OffsetCounter as u8), "offset_counter"),
        (
            stream_key(KeyPrefix::RealmWatermark as u8),
            "realm_watermark",
        ),
        (stream_key(KeyPrefix::ResourceMeta as u8), "resource_meta"),
        (stream_key(KeyPrefix::AreaCounter as u8), "area_counter"),
        (stream_key(KeyPrefix::RealmCounter as u8), "realm_counter"),
        (
            vec![KeyPrefix::CanonicalResource as u8],
            "canonical_resource",
        ),
        (stream_key(KeyPrefix::AreaLocator as u8), "area_locator"),
        (stream_key(KeyPrefix::RealmLocator as u8), "realm_locator"),
        (
            stream_key(KeyPrefix::CompactAreaPage as u8),
            "compact_area_page",
        ),
        (
            stream_key(KeyPrefix::CompressedCompactRealmPage as u8),
            "compressed_compact_realm_page",
        ),
        (
            stream_key(KeyPrefix::CompactResourcePage as u8),
            "compact_resource_page",
        ),
        (
            crate::domains::stream::storage::encode_staging_key(1, 1),
            "staging",
        ),
    ];

    // Act
    let errors = corrupt_rows
        .into_iter()
        .map(|(key, expected_category)| {
            (
                StreamStore::validate_persisted_row(&key, b"broken")
                    .expect_err("corrupt stream row should fail validation")
                    .0,
                expected_category,
            )
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(errors
        .into_iter()
        .all(|(actual, expected)| actual == expected));
}

#[test]
pub(super) fn should_encode_compact_resource_scan_prefix_with_typed_segments() {
    // Arrange
    let expected = {
        let mut bytes = b"test\0st\0".to_vec();
        bytes.push(KeyPrefix::CompactResourcePage as u8);
        bytes.extend_from_slice(b"events\0orders\0");
        bytes
    };

    // Act
    let prefix = StreamStore::build_compact_resource_page_prefix("test", "events", "orders");

    // Assert
    assert_eq!(prefix, expected);
}

#[test]
pub(super) fn should_fail_existing_family_validation_given_corrupt_stream_watermark() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    write_layout_marker(db.as_ref(), 1, StreamStorageLayout::PromotionFrontier);
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    txn.put(
        encode_watermark_key("test", "events"),
        b"broken".to_vec(),
        None,
    )
    .expect("write corrupt watermark");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit corrupt watermark");
    let store = StreamStore::new(db);

    // Act
    let result = store.validate_persisted_state_for_existing_families();

    // Assert
    let error = result.expect_err("corrupt watermark should fail family validation");
    assert!(error.contains("family=1"));
    assert!(error.contains("key_category=watermark"));
}

#[test]
pub(super) fn should_return_error_when_area_watermark_guard_read_fails() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .set_watermark(1, "test", "events", 10)
        .expect("seed area watermark");
    StreamStore::fail_next_area_watermark_guard_read_for_tests();

    // Act
    let result = store.set_watermark(1, "test", "events", 11);

    // Assert
    assert!(result.is_err());
    assert_eq!(
        store
            .get_watermark(1, "test", "events")
            .expect("read area watermark"),
        10
    );
}

#[test]
pub(super) fn should_preserve_area_watermark_given_same_value_update() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .set_watermark(1, "test", "events", 10)
        .expect("seed area watermark");

    // Act
    store
        .set_watermark(1, "test", "events", 10)
        .expect("rewrite same area watermark");

    // Assert
    assert_eq!(
        store
            .get_watermark(1, "test", "events")
            .expect("read area watermark"),
        10
    );
}

#[test]
pub(super) fn should_preserve_area_watermark_given_lower_value_update() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .set_watermark(1, "test", "events", 10)
        .expect("seed area watermark");

    // Act
    store
        .set_watermark(1, "test", "events", 9)
        .expect("rewrite lower area watermark");

    // Assert
    assert_eq!(
        store
            .get_watermark(1, "test", "events")
            .expect("read area watermark"),
        10
    );
}

#[test]
pub(super) fn should_not_hide_committed_area_records_behind_stale_persisted_watermark() {
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
        .expect("seed committed area watermark");

    // Act
    store
        .set_watermark(1, "test", "events", 0)
        .expect("first explicit area watermark should persist");

    // Assert
    assert_eq!(
        store
            .get_watermark(1, "test", "events")
            .expect("read area watermark"),
        1
    );
    assert_eq!(
        store
            .get_persisted_area_watermark(1, "test", "events")
            .expect("read persisted area watermark"),
        Some(0)
    );
}

#[test]
pub(super) fn should_return_error_when_realm_watermark_guard_read_fails() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .set_realm_watermark(1, "test", 10)
        .expect("seed realm watermark");
    StreamStore::fail_next_realm_watermark_guard_read_for_tests();

    // Act
    let result = store.set_realm_watermark(1, "test", 11);

    // Assert
    assert!(result.is_err());
    assert_eq!(
        store
            .get_realm_watermark(1, "test")
            .expect("read realm watermark"),
        10
    );
}

#[test]
pub(super) fn should_preserve_realm_watermark_given_same_value_update() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .set_realm_watermark(1, "test", 10)
        .expect("seed realm watermark");

    // Act
    store
        .set_realm_watermark(1, "test", 10)
        .expect("rewrite same realm watermark");

    // Assert
    assert_eq!(
        store
            .get_realm_watermark(1, "test")
            .expect("read realm watermark"),
        10
    );
}

#[test]
pub(super) fn should_preserve_realm_watermark_given_lower_value_update() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .set_realm_watermark(1, "test", 10)
        .expect("seed realm watermark");

    // Act
    store
        .set_realm_watermark(1, "test", 9)
        .expect("rewrite lower realm watermark");

    // Assert
    assert_eq!(
        store
            .get_realm_watermark(1, "test")
            .expect("read realm watermark"),
        10
    );
}
