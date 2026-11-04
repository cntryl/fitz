use super::*;
use crate::domains::stream::StreamDropScope;
use crate::runtime::routing::RouteFamily;

#[test]
fn should_finish_dropped_payload_reclamation_despite_unrelated_appends() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = vec![
        EventPayload {
            body: Bytes::from(vec![0xA5; INLINE_PAYLOAD_LIMIT + 1]),
            metadata: None,
            discriminator: None,
        };
        130
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "acme",
            area: "deleted",
            resource: "events",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
    let scope =
        StreamDropScope::parse(RouteFamily::new(1), "stream://acme/deleted/events").unwrap();
    store.drop_scope(&scope, StreamWriteMode::Sync).unwrap();
    store.run_maintenance(1).unwrap();
    let initial_progress = store
        .db
        .read_tx(1)
        .get(&super::super::drop_reclamation::progress_key())
        .unwrap()
        .map(|bytes| super::super::drop_reclamation::decode_progress(&bytes).unwrap());
    assert_eq!(initial_progress, Some((64, 130)));
    let mut progress_after_drop = Vec::new();

    // Act
    for expected in 0..3 {
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "acme",
                area: "retained",
                resource: "events",
                expected_resource_next_offset: expected,
                events: &single_event(b"retained"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .unwrap();
        store.drop_scope(&scope, StreamWriteMode::Sync).unwrap();
        progress_after_drop.push(
            store
                .db
                .read_tx(1)
                .get(&super::super::drop_reclamation::progress_key())
                .unwrap()
                .map(|bytes| super::super::drop_reclamation::decode_progress(&bytes).unwrap()),
        );
        store.run_maintenance(1).unwrap();
    }

    // Assert
    assert!(
        store
            .db
            .read_tx(1)
            .get(&encode_payload_blob_key(128))
            .unwrap()
            .is_none(),
        "unrelated appends plus empty DROP rewound reclamation to {progress_after_drop:?}"
    );
    let (items, _) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "acme",
            area: "retained",
            resource: "events",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        })
        .unwrap();
    let retained = event_records(items);
    assert_eq!(retained.len(), 3);
    assert!(retained
        .iter()
        .all(|record| record.body.as_ref() == b"retained"));
}

#[test]
fn should_resume_past_a_full_work_budget_of_dropped_realm_postings() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let deleted_count = crate::domains::stream::MAX_POSTING_ENTRIES_EXAMINED + 1;
    let events = vec![
        EventPayload {
            body: Bytes::from_static(b"deleted"),
            metadata: None,
            discriminator: None,
        };
        deleted_count
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "acme",
            area: "deleted",
            resource: "events",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "acme",
            area: "retained",
            resource: "events",
            expected_resource_next_offset: 0,
            events: &[EventPayload {
                body: Bytes::from_static(b"retained"),
                metadata: None,
                discriminator: None,
            }],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
    let scope = StreamDropScope::parse(RouteFamily::new(1), "stream://acme/deleted/*").unwrap();
    store.drop_scope(&scope, StreamWriteMode::Sync).unwrap();
    let params = ReadRealmPostingParams {
        family: 1,
        realm: "acme",
        resource: "events",
        from_offset: 0,
        limit: 1,
        max_bytes: None,
    };

    // Act
    let (first_items, first_cursor) = store.read_realm_resource_posting(&params, None).unwrap();
    let (second_items, second_cursor) = store
        .read_realm_resource_posting(
            &ReadRealmPostingParams {
                from_offset: first_cursor.last_realm_offset.unwrap() + 1,
                ..params
            },
            None,
        )
        .unwrap();

    // Assert
    assert_eq!(first_items.len(), 0);
    assert!(first_cursor.has_more);
    assert_eq!(
        first_cursor.last_realm_offset,
        Some(crate::domains::stream::MAX_POSTING_ENTRIES_EXAMINED as u64 - 1)
    );
    let records = event_records(second_items);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].route.as_str(), "stream://acme/retained/events");
    assert_eq!(records[0].realm_offset, Some(deleted_count as u64));
    assert_eq!(records[0].body.as_ref(), b"retained");
    assert!(!second_cursor.has_more);
}
