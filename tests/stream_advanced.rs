//! Stream advanced regression tests for promotion-frontier metadata, watermarks,
//! and layout activation failure behavior.

use bytes::Bytes;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::storage::{
    encode_offset_counter_key, encode_stream_layout_marker_key, OffsetCounterValue,
    StreamLayoutMarkerValue,
};
use fitz::domains::stream::store::{
    CommitRecordsParams, EventPayload, StreamAdminRecord, StreamStorageLayout, StreamStore,
};
use fitz::testkit::create_test_engine_with_cfs;

fn commit_record(
    store: &StreamStore,
    family: u64,
    realm: &str,
    area: &str,
    resource: &str,
    expected_resource_next_offset: u64,
    body: &[u8],
) {
    let events = [EventPayload {
        body: Bytes::copy_from_slice(body),
        metadata: None,
    }];

    store
        .commit_records(CommitRecordsParams {
            family,
            realm,
            area,
            resource,
            expected_resource_next_offset,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit stream record");
}

fn seed_unmarked_stream_data(engine: &cntryl_midge::Engine, family: u32) {
    let mut tx = engine
        .begin_tx(family, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(
        encode_offset_counter_key("test", "events", "orders"),
        OffsetCounterValue { next_offset: 1 }.encode(),
        None,
    )
    .expect("write unmarked stream metadata");
    tx.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit unmarked stream metadata");
}

fn seed_layout_marker(engine: &cntryl_midge::Engine, family: u32, layout: StreamStorageLayout) {
    let mut tx = engine
        .begin_tx(family, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(
        encode_stream_layout_marker_key(),
        StreamLayoutMarkerValue::new(layout).encode(),
        None,
    )
    .expect("write layout marker");
    tx.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit layout marker");
}

#[test]
fn should_list_stream_metadata_given_committed_records() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");
    commit_record(&store, 1, "test", "events", "audits", 0, b"two");
    commit_record(&store, 1, "test", "events", "orders", 1, b"three");

    // Act
    let records = store
        .list_resource_metadata(1)
        .expect("list committed stream metadata");

    // Assert
    assert_eq!(
        records,
        vec![
            StreamAdminRecord {
                realm: "test".to_string(),
                area: "events".to_string(),
                resource: "audits".to_string(),
                next_offset: 1,
                committed_size_bytes: 3,
            },
            StreamAdminRecord {
                realm: "test".to_string(),
                area: "events".to_string(),
                resource: "orders".to_string(),
                next_offset: 2,
                committed_size_bytes: 8,
            },
        ]
    );
}

#[test]
fn should_return_reset_required_given_unmarked_stream_data_when_listing_metadata() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    seed_unmarked_stream_data(engine.as_ref(), 1);
    let store = StreamStore::new(engine);

    // Act
    let result = store.list_resource_metadata(1);

    // Assert
    let error = result.expect_err("unmarked stream data should fail layout activation");
    assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED"));
    assert!(error.contains("promotion-frontier"));
}

#[test]
fn should_return_layout_mismatch_given_legacy_layout_marker_when_listing_metadata() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    seed_layout_marker(engine.as_ref(), 1, StreamStorageLayout::LegacyCovering);
    let store = StreamStore::new(engine);

    // Act
    let result = store.list_resource_metadata(1);

    // Assert
    let error = result.expect_err("legacy marker should fail layout activation");
    assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_MISMATCH"));
    assert!(error.contains("legacy-covering"));
    assert!(error.contains("promotion-frontier"));
}

#[test]
fn should_preserve_monotonic_area_offsets_given_cross_resource_commits() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");
    commit_record(&store, 1, "test", "events", "audits", 0, b"two");
    commit_record(&store, 1, "test", "events", "orders", 1, b"three");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read area history");

    // Assert
    let area_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.area_offset.expect("area offset"))
        .collect();
    let bodies: Vec<Bytes> = records.into_iter().map(|record| record.body).collect();
    assert_eq!(area_offsets, vec![0, 1, 2]);
    assert_eq!(
        bodies,
        vec![
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
            Bytes::from_static(b"three"),
        ]
    );
    assert_eq!(cursor.last_area_offset, Some(2));
    assert!(!cursor.has_more);
}

#[test]
fn should_preserve_monotonic_realm_offsets_given_cross_area_commits() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");
    commit_record(&store, 1, "test", "audits", "entries", 0, b"two");
    commit_record(&store, 1, "test", "events", "orders", 1, b"three");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 0, 10, None)
        .expect("read realm history");

    // Assert
    let realm_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.realm_offset.expect("realm offset"))
        .collect();
    let bodies: Vec<Bytes> = records.into_iter().map(|record| record.body).collect();
    assert_eq!(realm_offsets, vec![0, 1, 2]);
    assert_eq!(
        bodies,
        vec![
            Bytes::from_static(b"one"),
            Bytes::from_static(b"two"),
            Bytes::from_static(b"three"),
        ]
    );
    assert_eq!(cursor.last_realm_offset, Some(2));
    assert!(!cursor.has_more);
}

#[test]
fn should_return_empty_success_given_read_past_area_watermark() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 1, 10, None)
        .expect("read past area watermark");

    // Assert
    assert!(records.is_empty());
    assert_eq!(cursor.last_area_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_return_record_given_read_at_area_watermark_boundary() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");

    // Act
    let (records, cursor) = store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read at area watermark boundary");

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].area_offset, Some(0));
    assert_eq!(records[0].body, Bytes::from_static(b"one"));
    assert_eq!(cursor.last_area_offset, Some(0));
    assert!(!cursor.has_more);
}

#[test]
fn should_return_empty_success_given_read_past_realm_watermark() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 1, 10, None)
        .expect("read past realm watermark");

    // Assert
    assert!(records.is_empty());
    assert_eq!(cursor.last_realm_offset, Some(1));
    assert!(!cursor.has_more);
}

#[test]
fn should_return_record_given_read_at_realm_watermark_boundary() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");

    // Act
    let (records, cursor) = store
        .read_realm(1, "test", 0, 10, None)
        .expect("read at realm watermark boundary");

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].realm_offset, Some(0));
    assert_eq!(records[0].body, Bytes::from_static(b"one"));
    assert_eq!(cursor.last_realm_offset, Some(0));
    assert!(!cursor.has_more);
}

#[test]
fn should_restore_watermarks_given_store_reopen() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(engine.clone());
    commit_record(&store, 1, "test", "events", "orders", 0, b"one");

    // Act
    let reopened = StreamStore::new(engine);
    let area_watermark = reopened
        .get_watermark(1, "test", "events")
        .expect("read reopened area watermark");
    let realm_watermark = reopened
        .get_realm_watermark(1, "test")
        .expect("read reopened realm watermark");

    // Assert
    assert_eq!(area_watermark, 0);
    assert_eq!(realm_watermark, 0);
}
