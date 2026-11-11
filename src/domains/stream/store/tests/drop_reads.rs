use super::*;
use crate::domains::stream::StreamDropScope;
use crate::runtime::routing::RouteFamily;

#[derive(Clone, Copy, Debug)]
enum ReadView {
    Resource,
    Area,
    Realm,
    RealmResource,
    Global,
    GlobalArea,
    GlobalResource,
    GlobalAreaResource,
}

impl ReadView {
    fn read(
        self,
        store: &StreamStore,
    ) -> (
        Vec<StreamReadItem>,
        super::super::super::protocol::ReadCursor,
    ) {
        match self {
            Self::Resource => store.read_resource(&ReadResourceParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "deleted",
                from_offset: 0,
                limit: 64,
                max_bytes: None,
            }),
            Self::Area => store.read_area(1, "north", "orders", 0, 64, None),
            Self::Realm => store.read_realm(1, "north", 0, 64, None),
            Self::RealmResource => store.read_realm_resource_posting(
                &ReadRealmPostingParams {
                    family: 1,
                    realm: "north",
                    resource: "deleted",
                    from_offset: 0,
                    limit: 64,
                    max_bytes: None,
                },
                None,
            ),
            Self::Global => store.read_global(1, 0, 64, None, None),
            Self::GlobalArea | Self::GlobalResource | Self::GlobalAreaResource => store
                .read_global_posting(
                    &ReadGlobalPostingParams {
                        family: 1,
                        from_offset: 0,
                        limit: 64,
                        max_bytes: None,
                        area: matches!(self, Self::GlobalArea | Self::GlobalAreaResource)
                            .then_some("orders"),
                        resource: matches!(self, Self::GlobalResource | Self::GlobalAreaResource)
                            .then_some("deleted"),
                    },
                    None,
                ),
        }
        .unwrap_or_else(|error| panic!("{self:?} read failed: {error}"))
    }
}

fn append(
    store: &StreamStore,
    realm: &str,
    area: &str,
    resource: &str,
    expected: u64,
    body: Bytes,
) -> CommitResponse {
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm,
            area,
            resource,
            expected_resource_next_offset: expected,
            events: &[EventPayload {
                body,
                metadata: None,
                discriminator: None,
            }],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit replay fixture")
}

fn drop_resource(store: &StreamStore) {
    store
        .drop_scope(
            &StreamDropScope::parse(RouteFamily::new(1), "stream://north/orders/deleted")
                .expect("valid DROP scope"),
            StreamWriteMode::Sync,
        )
        .expect("drop resource");
}

fn seeded_store() -> StreamStore {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for (realm, area, resource) in [
        ("north", "orders", "deleted"),
        ("north", "orders", "retained"),
        ("north", "payments", "deleted"),
        ("south", "orders", "deleted"),
    ] {
        append(&store, realm, area, resource, 0, Bytes::from_static(b"old"));
    }
    store
}

#[test]
fn should_omit_a_dropped_resource_from_every_replay_view() {
    // Arrange
    let store = seeded_store();
    let cases = [
        (ReadView::Resource, 0),
        (ReadView::Area, 1),
        (ReadView::Realm, 2),
        (ReadView::RealmResource, 1),
        (ReadView::Global, 3),
        (ReadView::GlobalArea, 2),
        (ReadView::GlobalResource, 2),
        (ReadView::GlobalAreaResource, 1),
    ];
    drop_resource(&store);

    // Act
    let results: Vec<_> = cases
        .into_iter()
        .map(|(view, count)| (view, count, view.read(&store)))
        .collect();

    // Assert
    for (view, count, (items, cursor)) in results {
        assert_eq!(items.len(), count, "{view:?}");
        assert!(!cursor.has_more, "{view:?}");
        assert!(
            items.into_iter().all(|item| matches!(
                item,
                StreamReadItem::Event(record)
                    if record.route.as_str() != "stream://north/orders/deleted"
            )),
            "{view:?}"
        );
    }
}

#[test]
fn should_skip_dropped_locators_before_loading_reclaimed_payload_blobs() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    let committed = append(
        &store,
        "north",
        "orders",
        "deleted",
        0,
        Bytes::from(vec![0xA5; INLINE_PAYLOAD_LIMIT + 1]),
    );
    drop_resource(&store);
    let mut txn = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin payload reclamation");
    txn.delete(encode_payload_blob_key(committed.first_global_offset))
        .expect("remove deleted blob");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit payload reclamation");

    // Act
    let results: Vec<_> = [
        ReadView::Area,
        ReadView::Realm,
        ReadView::RealmResource,
        ReadView::Global,
        ReadView::GlobalArea,
        ReadView::GlobalResource,
        ReadView::GlobalAreaResource,
    ]
    .into_iter()
    .map(|view| (view, view.read(&store)))
    .collect();

    // Assert
    for (view, (items, cursor)) in results {
        assert!(items.is_empty(), "{view:?}");
        assert!(!cursor.has_more, "{view:?}");
    }
}

#[test]
fn should_keep_recreated_resource_offsets_distinct_in_the_enclosing_realm() {
    // Arrange
    let store = seeded_store();
    drop_resource(&store);
    append(
        &store,
        "north",
        "orders",
        "deleted",
        0,
        Bytes::from_static(b"new"),
    );

    // Act
    let (items, _) = ReadView::RealmResource.read(&store);

    // Assert
    let records = event_records(items);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].route.as_str(), "stream://north/payments/deleted");
    assert_eq!(records[1].body.as_ref(), b"new");
    assert_eq!(records[1].resource_offset, 0);
    assert_eq!(records[1].realm_offset, Some(3));
}

#[test]
fn should_not_advance_past_unexamined_live_records_before_a_compacted_deleted_tail() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"retained"),
            metadata: None,
            discriminator: None,
        };
        65
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "retained",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit retained history");
    for offset in 0..8 {
        append(
            &store,
            "north",
            "orders",
            "deleted",
            offset,
            Bytes::from_static(b"old"),
        );
    }
    drain_maintenance(&store, 1);
    drop_resource(&store);

    // Act
    let (items, cursor) = store
        .read_area(1, "north", "orders", 0, 64, None)
        .expect("read the first full page");

    // Assert
    assert_eq!(items.len(), 64);
    assert_eq!(cursor.last_area_offset, Some(63));
    assert!(cursor.has_more);
}

#[test]
fn should_keep_global_postings_safe_when_a_realm_is_recreated_from_zero() {
    // Arrange
    let store = seeded_store();
    store
        .drop_scope(
            &StreamDropScope::parse(RouteFamily::new(1), "stream://north/*")
                .expect("valid realm DROP"),
            StreamWriteMode::Sync,
        )
        .expect("drop realm");
    append(
        &store,
        "north",
        "orders",
        "deleted",
        0,
        Bytes::from_static(b"new"),
    );

    // Act
    let (items, cursor) = ReadView::GlobalAreaResource.read(&store);

    // Assert
    let records = event_records(items);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].route.as_str(), "stream://south/orders/deleted");
    assert_eq!(records[1].body.as_ref(), b"new");
    assert_eq!(records[1].resource_offset, 0);
    assert_eq!(records[1].realm_offset, Some(0));
    assert_eq!(records[1].global_offset, Some(4));
    assert!(!cursor.has_more);
}

#[test]
fn should_keep_retained_postings_readable_after_dropped_payloads_are_reclaimed() {
    // Arrange
    let store = seeded_store();
    drop_resource(&store);
    drain_maintenance(&store, 1);

    // Act
    let (items, cursor) = ReadView::GlobalAreaResource.read(&store);

    // Assert
    let records = event_records(items);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].route.as_str(), "stream://south/orders/deleted");
    assert_eq!(records[0].global_offset, Some(3));
    assert!(!cursor.has_more);
}

#[test]
fn should_skip_dropped_records_before_decoding_filter_discriminators() {
    // Arrange
    let store = seeded_store();
    drop_resource(&store);
    let mut transaction = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    transaction
        .put(encode_global_discriminator_key(0), vec![0xFF], None)
        .unwrap();
    transaction
        .commit(cntryl_midge::WriteOptions::sync())
        .unwrap();
    let filter = StreamFilterSet {
        clauses: vec![
            crate::domains::stream::protocol::StreamFilterClause::Equals("unmatched".to_string()),
        ],
    };

    // Act
    let (items, cursor) = store.read_global(1, 0, 64, None, Some(&filter)).unwrap();

    // Assert
    assert_eq!(items.len(), 3);
    assert!(items.into_iter().all(|item| matches!(item, StreamReadItem::Filtered { route, .. } if route.as_str() != "stream://north/orders/deleted")));
    assert_eq!(cursor.last_global_offset, Some(3));
    assert!(!cursor.has_more);
}

#[test]
fn should_reject_a_missing_live_payload_after_a_neighboring_stream_drop() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = StreamStore::new(db.clone());
    append(
        &store,
        "north",
        "orders",
        "deleted",
        0,
        Bytes::from_static(b"old"),
    );
    let retained = append(
        &store,
        "north",
        "orders",
        "retained",
        0,
        Bytes::from(vec![0xA5; INLINE_PAYLOAD_LIMIT + 1]),
    );
    drop_resource(&store);
    let mut transaction = db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    transaction
        .delete(encode_payload_blob_key(retained.first_global_offset))
        .unwrap();
    transaction
        .commit(cntryl_midge::WriteOptions::sync())
        .unwrap();

    // Act
    let result = store.read_global_posting(
        &ReadGlobalPostingParams {
            family: 1,
            from_offset: 0,
            limit: 64,
            max_bytes: None,
            area: Some("orders"),
            resource: None,
        },
        None,
    );

    // Assert
    assert!(result
        .unwrap_err()
        .contains("ERR_STREAM_CORRUPT_BLOB: missing payload blob"));
}
