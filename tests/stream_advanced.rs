//! Stream advanced regression tests for durable metadata and legacy upgrade paths.

use bytes::Bytes;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::storage::{
    encode_area_key, encode_offset_counter_key, encode_realm_key, encode_resource_key,
    OffsetCounterValue,
};
use fitz::domains::stream::store::{CommitRecordsParams, EventPayload, StreamStore};
use fitz::testkit::create_test_engine_with_cfs;

#[derive(serde::Serialize)]
struct LegacyAreaValue {
    realm: String,
    area: String,
    resource: String,
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(serde::Serialize)]
struct LegacyResourceValue {
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
    area_offset: Option<u64>,
    realm_offset: Option<u64>,
}

#[derive(serde::Serialize)]
struct LegacyRealmValue {
    realm: String,
    area: String,
    area_offset: u64,
    resource: String,
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

struct LegacyRecordRef<'a> {
    realm: &'a str,
    area: &'a str,
    resource: &'a str,
    resource_offset: u64,
    area_offset: u64,
    realm_offset: u64,
}

fn write_legacy_record(
    engine: &cntryl_midge::Engine,
    family: u32,
    record: LegacyRecordRef<'_>,
    body: &[u8],
) {
    use cntryl_midge::{TransactionMode, WriteOptions};

    let LegacyRecordRef {
        realm,
        area,
        resource,
        resource_offset,
        area_offset,
        realm_offset,
    } = record;

    let mut tx = engine
        .begin_tx(family, TransactionMode::ReadWrite)
        .expect("begin write tx");
    let body = Bytes::copy_from_slice(body);
    let created_at = resource_offset + 1;

    tx.put(
        encode_resource_key(realm, area, resource, resource_offset),
        bincode::serialize(&LegacyResourceValue {
            resource_offset,
            body: body.clone(),
            metadata: None,
            created_at,
            area_offset: Some(area_offset),
            realm_offset: Some(realm_offset),
        })
        .expect("encode legacy resource value"),
        None,
    )
    .expect("write resource record");

    tx.put(
        encode_area_key(realm, area, area_offset),
        bincode::serialize(&LegacyAreaValue {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            resource_offset,
            body: body.clone(),
            metadata: None,
            created_at,
        })
        .expect("encode legacy area value"),
        None,
    )
    .expect("write area record");

    tx.put(
        encode_realm_key(realm, realm_offset),
        bincode::serialize(&LegacyRealmValue {
            realm: realm.to_string(),
            area: area.to_string(),
            area_offset,
            resource: resource.to_string(),
            resource_offset,
            body,
            metadata: None,
            created_at,
        })
        .expect("encode legacy realm value"),
        None,
    )
    .expect("write realm record");

    tx.put(
        encode_offset_counter_key(realm, area, resource),
        OffsetCounterValue {
            next_offset: resource_offset + 1,
        }
        .encode(),
        None,
    )
    .expect("write legacy offset counter");

    tx.commit(WriteOptions::buffered())
        .expect("commit legacy stream rows");
}

#[test]
fn should_list_stream_metadata_from_legacy_resource_counters() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    write_legacy_record(
        &engine,
        1,
        LegacyRecordRef {
            realm: "test",
            area: "events",
            resource: "orders",
            resource_offset: 0,
            area_offset: 0,
            realm_offset: 0,
        },
        b"legacy",
    );

    let store = StreamStore::new(engine);

    // Act
    let records = store
        .list_resource_metadata(1)
        .expect("list legacy metadata");

    // Assert
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].realm, "test");
    assert_eq!(records[0].area, "events");
    assert_eq!(records[0].resource, "orders");
    assert_eq!(records[0].next_offset, 1);
    assert_eq!(records[0].committed_size_bytes, 6);
}

#[test]
fn should_backfill_stream_counters_from_legacy_indexes_on_commit() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    write_legacy_record(
        &engine,
        1,
        LegacyRecordRef {
            realm: "test",
            area: "events",
            resource: "orders",
            resource_offset: 0,
            area_offset: 0,
            realm_offset: 0,
        },
        b"one",
    );
    write_legacy_record(
        &engine,
        1,
        LegacyRecordRef {
            realm: "test",
            area: "events",
            resource: "audits",
            resource_offset: 0,
            area_offset: 1,
            realm_offset: 1,
        },
        b"two",
    );

    let store = StreamStore::new(engine);

    // Act
    let response = store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 1,
            events: &[EventPayload {
                body: Bytes::from_static(b"three"),
                metadata: None,
            }],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit should backfill counters");

    // Assert
    assert_eq!(response.first_resource_offset, 1);
    assert_eq!(response.first_area_offset, 2);
    assert_eq!(response.first_realm_offset, 2);
    assert_eq!(
        store
            .get_next_resource_offset(1, "test", "events", "orders")
            .expect("resource next offset"),
        2
    );
    assert_eq!(
        store
            .get_watermark(1, "test", "events")
            .expect("area watermark"),
        2
    );
    assert_eq!(
        store
            .get_realm_watermark(1, "test")
            .expect("realm watermark"),
        2
    );

    let area_records = store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read area")
        .0;
    let area_offsets: Vec<u64> = area_records
        .iter()
        .map(|record| record.area_offset.expect("area offset"))
        .collect();
    assert_eq!(area_offsets, vec![0, 1, 2]);

    let realm_records = store
        .read_realm(1, "test", 0, 10, None)
        .expect("read realm")
        .0;
    let realm_offsets: Vec<u64> = realm_records
        .iter()
        .map(|record| record.realm_offset.expect("realm offset"))
        .collect();
    assert_eq!(realm_offsets, vec![0, 1, 2]);
}

#[test]
fn should_return_empty_success_when_reading_past_committed_stream_watermark() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    write_legacy_record(
        &engine,
        1,
        LegacyRecordRef {
            realm: "test",
            area: "events",
            resource: "orders",
            resource_offset: 0,
            area_offset: 0,
            realm_offset: 0,
        },
        b"one",
    );

    let store = StreamStore::new(engine);

    // Act
    let area_records = store
        .read_area(1, "test", "events", 99, 10, None)
        .expect("read past area watermark")
        .0;
    let realm_records = store
        .read_realm(1, "test", 99, 10, None)
        .expect("read past realm watermark")
        .0;

    // Assert
    assert!(area_records.is_empty());
    assert!(realm_records.is_empty());
}

#[test]
fn should_preserve_watermark_when_set_watermark_regresses() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(engine);
    store
        .set_watermark(1, "test", "events", 10)
        .expect("set initial watermark");

    // Act
    store
        .set_watermark(1, "test", "events", 5)
        .expect("ignore regressed watermark");
    let watermark = store
        .get_watermark(1, "test", "events")
        .expect("read guarded watermark");

    // Assert
    assert_eq!(watermark, 10);
}

#[test]
fn should_restore_watermark_after_reopening_store() {
    // Arrange
    let engine = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(engine.clone());
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &[EventPayload {
                body: Bytes::from_static(b"one"),
                metadata: None,
            }],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit initial record");

    // Act
    let reopened = StreamStore::new(engine);
    let area_watermark = reopened
        .get_watermark(1, "test", "events")
        .expect("read reopened area watermark");
    let realm_watermark = reopened
        .get_realm_watermark(1, "test")
        .expect("read reopened realm watermark");
    let area_records = reopened
        .read_area(1, "test", "events", 1, 10, None)
        .expect("read past reopened area watermark")
        .0;
    let realm_records = reopened
        .read_realm(1, "test", 1, 10, None)
        .expect("read past reopened realm watermark")
        .0;

    // Assert
    assert_eq!(area_watermark, 0);
    assert_eq!(realm_watermark, 0);
    assert!(area_records.is_empty());
    assert!(realm_records.is_empty());
}
