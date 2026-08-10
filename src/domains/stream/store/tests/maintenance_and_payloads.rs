use super::*;

fn queue_synthetic_maintenance_group(store: &StreamStore, group: u8) {
    let mut fragment_key = vec![0xFA, group];
    fragment_key.extend_from_slice(&[0; 16]);
    store.queue_maintenance_key(1, &fragment_key);
}

fn large_resource_fragment(first_offset: u64, body: &Bytes) -> CompactResourcePageValue {
    CompactResourcePageValue {
        records: (0..7)
            .map(|index| CompactResourcePageRecord {
                area_offset: first_offset + index,
                realm_offset: first_offset + index,
                body: body.clone(),
                metadata: None,
                created_at: 1,
                expires_at: None,
            })
            .collect(),
    }
}

fn marker_collision_body(length: usize) -> Bytes {
    let mut body = PAYLOAD_BLOB_REF_MARKER.to_vec();
    body.resize(
        length,
        u8::try_from(length).expect("test payload length fits u8"),
    );
    Bytes::from(body)
}

#[test]
fn should_bound_non_compactable_maintenance_groups_by_buckets_examined() {
    // Arrange
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]))
        .with_maintenance_metrics_for_tests(metrics.clone());
    store
        .run_maintenance(1)
        .expect("initialize maintenance discovery");
    for group in 0..9 {
        queue_synthetic_maintenance_group(&store, group);
    }

    // Act
    let first = store
        .run_maintenance(1)
        .expect("examine first maintenance slice");
    let attempts_after_first =
        metrics.counter_get(crate::domains::stream::metrics::METRIC_MAINTENANCE_ATTEMPTS_TOTAL);
    let pending_after_first = store.has_pending_maintenance(1);
    let second = store
        .run_maintenance(1)
        .expect("examine remaining maintenance group");

    // Assert
    assert_eq!(first.buckets_compacted, 0);
    assert_eq!(attempts_after_first, 8);
    assert!(pending_after_first);
    assert_eq!(second.buckets_compacted, 0);
    assert_eq!(
        metrics.counter_get(crate::domains::stream::metrics::METRIC_MAINTENANCE_ATTEMPTS_TOTAL),
        9
    );
    assert!(!store.has_pending_maintenance(1));
}

#[test]
fn should_count_bytes_examined_for_non_compactable_group() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    store
        .run_maintenance(1)
        .expect("initialize maintenance discovery");
    let fragment_key = encode_compact_resource_page_key("north", "orders", "created", 0);
    let fragment_value = CompactResourcePageValue {
        records: vec![CompactResourcePageRecord {
            area_offset: 0,
            realm_offset: 0,
            body: Bytes::from_static(b"not-compactable"),
            metadata: None,
            created_at: 1,
            expires_at: None,
        }],
    }
    .encode();
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin non-compactable fragment write");
    txn.put(fragment_key.clone(), fragment_value.clone(), None)
        .expect("write non-compactable fragment");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit non-compactable fragment");
    store.queue_maintenance_key(1, &fragment_key);

    // Act
    let result = store
        .run_maintenance(1)
        .expect("examine non-compactable fragment");

    // Assert
    assert_eq!(result.buckets_compacted, 0);
    assert_eq!(result.bytes_examined, fragment_value.len());
    assert!(!store.has_pending_maintenance(1));
}

#[test]
fn should_count_plus_requeue_over_budget_maintenance_group() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    store
        .run_maintenance(1)
        .expect("initialize maintenance discovery");
    let body = Bytes::from(vec![0xA5; INLINE_PAYLOAD_LIMIT]);
    let mut fragment_keys = Vec::new();
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin over-budget fragment write");
    for resource_index in 0..5 {
        let resource = format!("resource-{resource_index}");
        for fragment_index in 0..9 {
            let first_offset = fragment_index * 7;
            let key = encode_compact_resource_page_key("north", "orders", &resource, first_offset);
            txn.put(
                key.clone(),
                large_resource_fragment(first_offset, &body).encode(),
                None,
            )
            .expect("write over-budget fragment");
            fragment_keys.push(key);
        }
    }
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit over-budget fragments");
    for key in &fragment_keys {
        store.queue_maintenance_key(1, key);
    }

    // Act
    let first = store
        .run_maintenance(1)
        .expect("run byte-bounded maintenance slice");
    let pending_after_first = store.has_pending_maintenance(1);
    let second = store
        .run_maintenance(1)
        .expect("run requeued maintenance group");

    // Assert
    assert_eq!(first.buckets_compacted, 4);
    assert!(first.bytes_examined > 4 * 1024 * 1024);
    assert!(pending_after_first);
    assert_eq!(second.buckets_compacted, 1);
    assert!(second.bytes_examined > 0);
    assert!(!store.has_pending_maintenance(1));
}

#[test]
fn should_roundtrip_every_reserved_blob_marker_prefix_length_with_metadata() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    let bodies = [
        marker_collision_body(42),
        marker_collision_body(43),
        marker_collision_body(44),
    ];
    let events: Vec<_> = bodies
        .iter()
        .enumerate()
        .map(|(index, body)| EventPayload {
            body: body.clone(),
            metadata: Some(Bytes::from(vec![
                0,
                0xD4,
                u8::try_from(index).expect("test event index fits u8"),
                0xFF,
            ])),
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
        .expect("commit marker-prefixed payloads");

    // Act
    let resource = event_records(
        store
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                from_offset: 0,
                limit: 3,
                max_bytes: None,
            })
            .expect("read marker-prefixed resource payloads")
            .0,
    );
    let area = event_records(
        store
            .read_area(1, "north", "orders", 0, 3, None)
            .expect("read marker-prefixed area payloads")
            .0,
    );
    let realm = event_records(
        store
            .read_realm(1, "north", 0, 3, None)
            .expect("read marker-prefixed realm payloads")
            .0,
    );
    let global = event_records(
        store
            .read_global(1, 0, 3, None, None)
            .expect("read marker-prefixed global payloads")
            .0,
    );
    let blob_txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin marker blob read");
    let stored_blobs: Vec<_> = (0..3)
        .map(|offset| {
            blob_txn
                .get(&encode_payload_blob_key(offset))
                .expect("read marker blob")
                .expect("marker payload must be blob-backed")
        })
        .collect();

    // Assert
    for records in [&resource, &area, &realm, &global] {
        assert_eq!(records.len(), events.len());
        for (record, event) in records.iter().zip(&events) {
            assert_eq!(record.body, event.body);
            assert_eq!(record.metadata, event.metadata);
        }
    }
    for (stored, event) in stored_blobs.iter().zip(&events) {
        let (body, metadata, checksum) = decode_payload_blob(stored).expect("decode marker blob");
        assert_eq!(body, event.body);
        assert_eq!(metadata, event.metadata);
        assert_eq!(
            checksum,
            payload_checksum(&event.body, event.metadata.as_ref())
        );
    }
}
