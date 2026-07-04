use super::*;
use crate::testkit::create_test_engine_with_cfs;

fn make_store() -> (ScheduleStore, Arc<cntryl_midge::Engine>) {
    let db = create_test_engine_with_cfs(vec![1]);
    (ScheduleStore::new(db.clone()), db)
}

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn put_raw(
    db: &Arc<cntryl_midge::Engine>,
    cf_id: u64,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), String> {
    let mut txn = db
        .begin_tx(
            u64_to_u32_saturating(cf_id),
            cntryl_midge::TransactionMode::ReadWrite,
        )
        .map_err(|e| format!("begin_tx failed: {e:?}"))?;
    txn.put(key, value, None)
        .map_err(|e| format!("put failed: {e:?}"))?;
    txn.commit(WriteOptions::buffered())
        .map_err(|e| format!("commit failed: {e:?}"))
}

fn read_raw_value(db: &Arc<cntryl_midge::Engine>, cf_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    let txn = db
        .begin_tx(
            u64_to_u32_saturating(cf_id),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .expect("begin read tx");

    txn.get(key)
        .expect("read raw key")
        .map(|value| value.to_vec())
}

fn count_prefix(db: &Arc<cntryl_midge::Engine>, cf_id: u64, prefix: &'static [u8]) -> usize {
    let txn = db
        .begin_tx(
            u64_to_u32_saturating(cf_id),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .expect("begin read tx");
    ScheduleStore::scan_schedule_rows(&txn, prefix)
        .expect("scan prefix")
        .len()
}

#[test]
fn should_encode_schedule_definition_key_with_typed_segments() {
    // Arrange
    let mut expected = storage_key::domain_marker_encoder(
        "acme",
        DomainKeyspace::Schedule,
        DEFINITION_PREFIX[0],
        13,
    );
    storage_key::encode_segment_into(&mut expected, "jobs");
    storage_key::encode_segment_into(&mut expected, "backup");
    expected.encode_string_into("run");

    // Act
    let key = ScheduleStore::encode_definition_key("schedule://acme/jobs/backup/run");

    // Assert
    assert_eq!(key, expected.into_vec());
}

#[test]
fn should_order_schedule_due_keys_by_fire_time_before_route_segments() {
    // Arrange
    let first = ScheduleStore::encode_due_key(1_700_000_000_001, "schedule://acme/jobs/a/run");
    let second = ScheduleStore::encode_due_key(1_700_000_000_010, "schedule://acme/jobs/a/run");

    // Act
    let ordered = first < second;

    // Assert
    assert!(ordered);
}

fn encode_inline_definition_value_v2(
    next_fire_ms: u64,
    cron: &str,
    payload: &Bytes,
    last_fire_ms: Option<u64>,
    executions_total: u64,
) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + 8 + 8 + 8 + 4 + cron.len() + 4 + payload.len());
    value.push(DEFINITION_VALUE_VERSION_V2);
    value.extend_from_slice(&next_fire_ms.to_be_bytes());
    value.extend_from_slice(&last_fire_ms.unwrap_or(0).to_be_bytes());
    value.extend_from_slice(&executions_total.to_be_bytes());
    value.extend_from_slice(&usize_to_u32_saturating(cron.len()).to_be_bytes());
    value.extend_from_slice(cron.as_bytes());
    value.extend_from_slice(&usize_to_u32_saturating(payload.len()).to_be_bytes());
    value.extend_from_slice(payload);
    value
}

#[test]
fn should_persist_definition_without_due_index_for_inserted_schedule() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/backup/run";
    let payload = Bytes::from_static(b"payload");
    let next_fire_ms = 1_700_000_001_000_u64;
    let expected_due_key = ScheduleStore::encode_due_key(next_fire_ms, route);

    // Act
    let stored_due_key = store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            WriteOptions::buffered(),
        )
        .expect("insert schedule");

    let definition_key = ScheduleStore::encode_definition_key(route);
    let definition_value = read_raw_value(&db, 1, &definition_key).expect("definition row");
    let body_key = ScheduleStore::encode_body_key(route);
    let body_value = read_raw_value(&db, 1, &body_key).expect("body row");

    // Assert
    assert_eq!(stored_due_key, expected_due_key);
    assert_eq!(
        ScheduleStore::decode_definition_value(&definition_value).unwrap(),
        DecodedDefinitionRow::Metadata {
            next_fire_ms,
            last_fire_ms: None,
            executions_total: 0,
        }
    );
    assert_eq!(
        ScheduleStore::decode_definition_body_value(&body_value).unwrap(),
        ("* * * * *".to_string(), Bytes::from_static(b"payload"),)
    );
    assert_eq!(
        count_prefix(&db, 1, BODY_PREFIX),
        1,
        "body row should be persisted alongside metadata"
    );
    assert_eq!(
        count_prefix(&db, 1, DEFINITION_PREFIX),
        1,
        "definition metadata row should be persisted"
    );
    assert!(
        read_raw_value(&db, 1, &expected_due_key).is_none(),
        "live insert should not persist the derived due index"
    );
}

#[test]
fn should_rebuild_due_index_from_inserted_schedule_definitions_on_load() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/rebuild/run";
    let payload = Bytes::from_static(b"payload");
    let next_fire_ms = 1_700_000_001_500_u64;
    let expected_due_key = ScheduleStore::encode_due_key(next_fire_ms, route);

    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "*/5 * * * *",
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            WriteOptions::buffered(),
        )
        .expect("insert schedule");
    assert!(
        read_raw_value(&db, 1, &expected_due_key).is_none(),
        "live insert should not persist the derived due index"
    );

    // Act
    let loaded = store
        .load_all(1, WriteOptions::buffered())
        .expect("load schedules");

    // Assert
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].route, route);
    assert_eq!(loaded[0].cron, "*/5 * * * *");
    assert_eq!(loaded[0].payload, payload);
    assert_eq!(loaded[0].next_fire_ms, next_fire_ms);
    assert_eq!(
        read_raw_value(&db, 1, &expected_due_key),
        Some(DUE_INDEX_VALUE.to_vec())
    );
}

#[test]
fn should_rebuild_due_index_from_definitions_after_legacy_import() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/legacy/run";
    let legacy_due_key = {
        let minute_epoch = 1_700_000_002_000_u64 / 60_000;
        let ms_offset = 1_700_000_002_000_u64 % 60_000;
        let mut key = Vec::new();
        key.extend_from_slice(LEGACY_PREFIX);
        key.extend_from_slice(minute_epoch.to_be_bytes().as_slice());
        key.push(b'/');
        key.extend_from_slice(ms_offset.to_be_bytes().as_slice());
        key.push(b':');
        key.extend_from_slice(route.as_bytes());
        key
    };
    put_raw(&db, 1, legacy_due_key, b"*/5 * * * *|legacy".to_vec()).expect("write legacy row");
    put_raw(
        &db,
        1,
        ScheduleStore::encode_due_key(9_999_999_999_999, "schedule://stale/index/only/run"),
        DUE_INDEX_VALUE.to_vec(),
    )
    .expect("write stale due row");

    // Act
    let loaded = store
        .load_all(1, WriteOptions::buffered())
        .expect("load schedules");

    // Assert
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].route, route);
    assert_eq!(loaded[0].cron, "*/5 * * * *");
    assert_eq!(loaded[0].payload, Bytes::from_static(b"legacy"));
    assert_eq!(count_prefix(&db, 1, LEGACY_PREFIX), 0);
    assert_eq!(count_prefix(&db, 1, LEGACY_INDEX_PREFIX), 0);
    assert!(
        read_raw_value(
            &db,
            1,
            &ScheduleStore::encode_due_key(loaded[0].next_fire_ms, route),
        )
        .is_some(),
        "rebuilt due index should exist for the imported schedule"
    );
    assert!(
        read_raw_value(
            &db,
            1,
            &ScheduleStore::encode_due_key(9_999_999_999_999, "schedule://stale/index/only/run",),
        )
        .is_none(),
        "stale due index rows should be removed during rebuild"
    );
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_some(),
        "definition row should exist after legacy import"
    );
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route)).is_some(),
        "body row should exist after legacy import"
    );
}

#[test]
fn should_split_inline_definition_rows_on_load() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/migrate/run";
    let payload = Bytes::from_static(b"payload");
    let next_fire_ms = 1_700_000_003_000_u64;
    let last_fire_ms = Some(1_700_000_002_500_u64);
    let executions_total = 7;

    put_raw(
        &db,
        1,
        ScheduleStore::encode_definition_key(route),
        encode_inline_definition_value_v2(
            next_fire_ms,
            "*/10 * * * *",
            &payload,
            last_fire_ms,
            executions_total,
        ),
    )
    .expect("write inline definition row");
    put_raw(
        &db,
        1,
        ScheduleStore::encode_due_key(next_fire_ms, route),
        DUE_INDEX_VALUE.to_vec(),
    )
    .expect("write due row");

    // Act
    let loaded = store
        .load_all(1, WriteOptions::buffered())
        .expect("load schedules");
    let metadata = read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route))
        .expect("definition metadata row");
    let body = read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route)).expect("body row");

    // Assert
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].route, route);
    assert_eq!(loaded[0].cron, "*/10 * * * *");
    assert_eq!(loaded[0].payload, payload);
    assert_eq!(loaded[0].next_fire_ms, next_fire_ms);
    assert_eq!(loaded[0].last_fire_ms, last_fire_ms);
    assert_eq!(loaded[0].executions_total, executions_total);
    assert_eq!(
        ScheduleStore::decode_definition_value(&metadata).unwrap(),
        DecodedDefinitionRow::Metadata {
            next_fire_ms,
            last_fire_ms,
            executions_total,
        }
    );
    assert_eq!(
        ScheduleStore::decode_definition_body_value(&body).unwrap(),
        ("*/10 * * * *".to_string(), Bytes::from_static(b"payload"))
    );
}

#[test]
fn should_remove_definition_with_due_index_when_canceling_schedule() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/delete/run";
    let payload = Bytes::from_static(b"payload");
    let next_fire_ms = 1_700_000_010_000_u64;
    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "0 * * * *",
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            WriteOptions::buffered(),
        )
        .expect("insert schedule");

    // Act
    store
        .delete_current(1, route, next_fire_ms, WriteOptions::buffered())
        .expect("delete schedule");

    // Assert
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_none(),
        "definition row should be deleted"
    );
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route)).is_none(),
        "body row should be deleted"
    );
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_due_key(next_fire_ms, route)).is_none(),
        "due index should be deleted"
    );
}

#[test]
fn should_persist_pending_fire_given_claimed_due_schedule() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/claim/run";
    let payload = Bytes::from_static(b"payload");
    let original_fire_ms = 1_700_000_020_000_u64;
    let next_fire_ms = 1_700_000_080_000_u64;
    let route_parts = parse_concrete_schedule_route(route).expect("valid schedule route");

    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                payload: &payload,
                next_fire_ms: original_fire_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            WriteOptions::buffered(),
        )
        .expect("insert schedule");

    // Act
    store
        .claim_due_batch(
            1,
            &[ScheduleFireClaim {
                route,
                route_parts: &route_parts,
                cron: "* * * * *",
                payload: &payload,
                claimed_at_ms: 1_700_000_020_500_u64,
                next_fire_ms,
                previous_fire_ms: original_fire_ms,
                last_fire_ms: None,
                executions_total: 0,
            }],
            WriteOptions::buffered(),
        )
        .expect("claim due schedule");
    let pending = store
        .load_pending_fire_claims(1)
        .expect("load pending fire claims");

    // Assert
    assert_eq!(
        pending,
        vec![PersistedPendingFireClaim {
            route: route.to_string(),
            payload: payload.clone(),
            claimed_at_ms: 1_700_000_020_500_u64,
            fire_ms: original_fire_ms,
        }]
    );
    assert!(
        read_raw_value(
            &db,
            1,
            &ScheduleStore::encode_due_key(original_fire_ms, route),
        )
        .is_none(),
        "original due key should be removed after claim"
    );
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_due_key(next_fire_ms, route)).is_none(),
        "live claim should not persist the next due key"
    );
    assert!(
        read_raw_value(
            &db,
            1,
            &ScheduleStore::encode_pending_fire_key(original_fire_ms, route),
        )
        .is_some(),
        "pending fire should be persisted after claim"
    );
    assert_eq!(
        ScheduleStore::decode_definition_body_value(
            &read_raw_value(&db, 1, &ScheduleStore::encode_body_key(route))
                .expect("body row should remain after claim"),
        )
        .unwrap(),
        ("* * * * *".to_string(), Bytes::from_static(b"payload"))
    );
}

#[test]
fn should_record_acknowledgement_state_given_acknowledged_claimed_due_schedule() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/claim/run";
    let payload = Bytes::from_static(b"payload");
    let original_fire_ms = 1_700_000_020_000_u64;
    let next_fire_ms = 1_700_000_080_000_u64;
    let acknowledged_at_ms = 1_700_000_021_500_u64;
    let route_parts = parse_concrete_schedule_route(route).expect("valid schedule route");

    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                payload: &payload,
                next_fire_ms: original_fire_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            WriteOptions::buffered(),
        )
        .expect("insert schedule");
    store
        .claim_due_batch(
            1,
            &[ScheduleFireClaim {
                route,
                route_parts: &route_parts,
                cron: "* * * * *",
                payload: &payload,
                claimed_at_ms: 1_700_000_020_500_u64,
                next_fire_ms,
                previous_fire_ms: original_fire_ms,
                last_fire_ms: None,
                executions_total: 0,
            }],
            WriteOptions::buffered(),
        )
        .expect("claim due schedule");

    // Act
    store
        .ack_pending_fire_claims(
            1,
            &[SchedulePendingFireClaimAck {
                route,
                fire_ms: original_fire_ms,
                acknowledged_at_ms,
                definition: Some(ScheduleAckDefinition {
                    next_fire_ms,
                    cron: "* * * * *",
                    payload: &payload,
                    executions_total: 1,
                }),
            }],
            WriteOptions::buffered(),
        )
        .expect("ack pending fire claim");
    let pending = store
        .load_pending_fire_claims(1)
        .expect("load pending fire claims");
    let schedules = store
        .load_all(1, WriteOptions::buffered())
        .expect("load schedules");

    // Assert
    assert!(
        pending.is_empty(),
        "pending fire should be removed after ack"
    );
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].last_fire_ms, Some(acknowledged_at_ms));
    assert_eq!(schedules[0].executions_total, 1);
    assert!(
        read_raw_value(
            &db,
            1,
            &ScheduleStore::encode_pending_fire_key(original_fire_ms, route),
        )
        .is_none(),
        "pending fire row should be deleted after ack"
    );
}

#[test]
fn should_remove_pending_fire_without_recreating_definition_given_missing_schedule_state() {
    // Arrange
    let (store, db) = make_store();
    let route = "schedule://acme/jobs/claim/run";
    let payload = Bytes::from_static(b"payload");
    let original_fire_ms = 1_700_000_020_000_u64;
    let next_fire_ms = 1_700_000_080_000_u64;
    let acknowledged_at_ms = 1_700_000_021_500_u64;
    let route_parts = parse_concrete_schedule_route(route).expect("valid schedule route");

    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                payload: &payload,
                next_fire_ms: original_fire_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            WriteOptions::buffered(),
        )
        .expect("insert schedule");
    store
        .claim_due_batch(
            1,
            &[ScheduleFireClaim {
                route,
                route_parts: &route_parts,
                cron: "* * * * *",
                payload: &payload,
                claimed_at_ms: 1_700_000_020_500_u64,
                next_fire_ms,
                previous_fire_ms: original_fire_ms,
                last_fire_ms: None,
                executions_total: 0,
            }],
            WriteOptions::buffered(),
        )
        .expect("claim due schedule");
    store
        .delete_current(1, route, next_fire_ms, WriteOptions::buffered())
        .expect("delete schedule definition");

    // Act
    store
        .ack_pending_fire_claims(
            1,
            &[SchedulePendingFireClaimAck {
                route,
                fire_ms: original_fire_ms,
                acknowledged_at_ms,
                definition: None,
            }],
            WriteOptions::buffered(),
        )
        .expect("ack pending fire claim");
    let pending = store
        .load_pending_fire_claims(1)
        .expect("load pending fire claims");
    let schedules = store
        .load_all(1, WriteOptions::buffered())
        .expect("load schedules");

    // Assert
    assert!(
        pending.is_empty(),
        "pending fire should be removed after ack"
    );
    assert!(
        schedules.is_empty(),
        "schedule definition should stay deleted"
    );
    assert!(
        read_raw_value(
            &db,
            1,
            &ScheduleStore::encode_pending_fire_key(original_fire_ms, route),
        )
        .is_none(),
        "pending fire row should be deleted after ack"
    );
    assert!(
        read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_none(),
        "schedule definition should not be recreated"
    );
}

#[test]
fn should_reject_malformed_persisted_schedule_definition_on_load() {
    // Arrange
    let (store, db) = make_store();
    put_raw(
        &db,
        1,
        ScheduleStore::encode_definition_key("schedule://acme/jobs/bad/run"),
        b"broken".to_vec(),
    )
    .expect("write malformed definition");

    // Act
    let result = store.load_all(1, WriteOptions::buffered());

    // Assert
    assert!(result
        .expect_err("malformed definition should fail recovery")
        .contains("decode persisted schedule definition failed"));
}

#[test]
fn should_reject_missing_schedule_body_on_load() {
    // Arrange
    let (store, db) = make_store();
    put_raw(
        &db,
        1,
        ScheduleStore::encode_definition_key("schedule://acme/jobs/missing/run"),
        ScheduleStore::encode_definition_metadata_value(1_700_000_000_000, None, 0),
    )
    .expect("write metadata without body");

    // Act
    let result = store.load_all(1, WriteOptions::buffered());

    // Assert
    assert!(result
        .expect_err("missing body should fail recovery")
        .contains("missing schedule body row"));
}

#[test]
fn should_reject_malformed_pending_fire_claim_on_load() {
    // Arrange
    let (store, db) = make_store();
    put_raw(
        &db,
        1,
        ScheduleStore::encode_pending_fire_key(1_700_000_000_000, "schedule://acme/jobs/bad/run"),
        b"broken".to_vec(),
    )
    .expect("write malformed pending fire claim");

    // Act
    let result = store.load_pending_fire_claims(1);

    // Assert
    assert!(result
        .expect_err("malformed pending claim should fail recovery")
        .contains("decode pending schedule fire failed"));
}
