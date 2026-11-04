use super::*;

fn seeded_store() -> StreamStore {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: 0,
            events: &vec![
                EventPayload {
                    body: Bytes::from_static(b"event"),
                    metadata: None,
                    discriminator: None,
                };
                64
            ],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
    store.set_watermark(1, "north", "orders", 63).unwrap();
    store.set_realm_watermark(1, "north", 63).unwrap();
    store
}

#[test]
fn should_discover_single_fragments_without_loading_unused_deletion_fences() {
    // Arrange
    let store = seeded_store();
    super::super::drop_scope::take_cutoff_gets();

    // Act
    let buckets = super::super::maintenance::scan_maintenance_buckets(&store, 1).unwrap();
    let gets = super::super::drop_scope::take_cutoff_gets();

    // Assert
    assert!(!buckets.is_empty());
    assert!(buckets.values().all(|count| *count == 1));
    assert_eq!(gets, 0, "discovery loaded {gets} unused deletion fences");
}

#[test]
fn should_read_deletion_fences_once_per_scope_in_each_replay_snapshot() {
    // Arrange
    let store = seeded_store();
    let mut results = Vec::new();

    // Act
    for view in 0..5 {
        super::super::drop_scope::take_cutoff_gets();
        let (items, _) = match view {
            0 => store.read_area(1, "north", "orders", 0, 64, None),
            1 => store.read_realm(1, "north", 0, 64, None),
            2 => store.read_global(1, 0, 64, None, None),
            3 => store.read_realm_resource_posting(
                &ReadRealmPostingParams {
                    family: 1,
                    realm: "north",
                    resource: "created",
                    from_offset: 0,
                    limit: 64,
                    max_bytes: None,
                },
                None,
            ),
            _ => store.read_global_posting(
                &ReadGlobalPostingParams {
                    family: 1,
                    from_offset: 0,
                    limit: 64,
                    max_bytes: None,
                    area: Some("orders"),
                    resource: Some("created"),
                },
                None,
            ),
        }
        .unwrap();
        results.push((
            view,
            items.len(),
            super::super::drop_scope::take_cutoff_gets(),
        ));
    }

    // Assert
    for (view, items, gets) in results {
        assert_eq!(items, 64, "replay view {view}");
        assert!(gets <= 3, "replay view {view} performed {gets} cutoff gets");
    }
}
