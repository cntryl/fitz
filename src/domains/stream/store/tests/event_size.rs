use super::*;

fn store_with_large_event() -> StreamStore {
    store_with_events(
        1,
        crate::protocol::wire2::DEFAULT_MAX_MESSAGE_BYTES as usize,
    )
}

fn store_with_events(count: usize, max_message_bytes: usize) -> StreamStore {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]))
        .with_max_message_bytes(max_message_bytes);
    let events = (0..count)
        .map(|_| EventPayload {
            body: Bytes::from(vec![42; 400 * 1024 - 16]),
            metadata: Some(Bytes::from(vec![7; 16])),
            discriminator: None,
        })
        .collect::<Vec<_>>();
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "sized",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
    store
        .set_watermark(1, "test", "events", u64::try_from(count - 1).unwrap())
        .unwrap();
    store
        .set_realm_watermark(1, "test", u64::try_from(count - 1).unwrap())
        .unwrap();
    store
}

fn assert_event(items: Vec<StreamReadItem>) {
    let records = event_records(items);
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].body.as_ref(),
        vec![42; 400 * 1024 - 16].as_slice()
    );
    assert_eq!(records[0].metadata.as_ref().unwrap().as_ref(), &[7; 16]);
}

#[test]
fn should_replay_a_400_kib_event_from_the_resource_plane() {
    // Arrange
    let store = store_with_large_event();
    // Act
    let (items, _) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "sized",
            from_offset: 0,
            limit: 1,
            max_bytes: None,
        })
        .unwrap();
    // Assert
    assert_event(items);
}

#[test]
fn should_replay_a_400_kib_event_from_the_area_plane() {
    // Arrange
    let store = store_with_large_event();
    // Act
    let (items, _) = store.read_area(1, "test", "events", 0, 1, None).unwrap();
    // Assert
    assert_event(items);
}

#[test]
fn should_replay_a_400_kib_event_from_the_realm_plane() {
    // Arrange
    let store = store_with_large_event();
    // Act
    let (items, _) = store.read_realm(1, "test", 0, 1, None).unwrap();
    // Assert
    assert_event(items);
}

#[test]
fn should_replay_a_400_kib_event_from_the_global_plane() {
    // Arrange
    let store = store_with_large_event();
    // Act
    let (items, _) = store.read_global(1, 0, 1, None, None).unwrap();
    // Assert
    assert_event(items);
}

#[derive(Clone, Copy)]
enum Plane {
    Resource,
    Area,
    Realm,
    RealmResource,
    Global,
    GlobalResource,
    GlobalArea,
    GlobalAreaResource,
}

fn assert_configured_page(plane: Plane) {
    // Arrange
    let store = store_with_events(3, 512 * 1024);
    // Act
    let (items, cursor) = match plane {
        Plane::Resource => store.read_resource(&ReadResourceParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "sized",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        }),
        Plane::Area => store.read_area(1, "test", "events", 0, 10, None),
        Plane::Realm => store.read_realm(1, "test", 0, 10, None),
        Plane::RealmResource => store.read_realm_resource_posting(
            &ReadRealmPostingParams {
                family: 1,
                realm: "test",
                resource: "sized",
                from_offset: 0,
                limit: 10,
                max_bytes: None,
            },
            None,
        ),
        Plane::Global => store.read_global(1, 0, 10, None, None),
        Plane::GlobalResource | Plane::GlobalArea | Plane::GlobalAreaResource => store
            .read_global_posting(
                &ReadGlobalPostingParams {
                    family: 1,
                    from_offset: 0,
                    limit: 10,
                    max_bytes: None,
                    area: (!matches!(plane, Plane::GlobalResource)).then_some("events"),
                    resource: (!matches!(plane, Plane::GlobalArea)).then_some("sized"),
                },
                None,
            ),
    }
    .unwrap();
    // Assert
    assert_event(items);
    assert!(cursor.has_more);
}

#[test]
fn should_bound_the_resource_page_by_the_configured_message_size() {
    assert_configured_page(Plane::Resource);
}

#[test]
fn should_bound_the_area_page_by_the_configured_message_size() {
    assert_configured_page(Plane::Area);
}

#[test]
fn should_bound_the_realm_page_by_the_configured_message_size() {
    assert_configured_page(Plane::Realm);
}

#[test]
fn should_bound_the_realm_resource_page_by_the_configured_message_size() {
    assert_configured_page(Plane::RealmResource);
}

#[test]
fn should_bound_the_global_page_by_the_configured_message_size() {
    assert_configured_page(Plane::Global);
}

#[test]
fn should_bound_the_global_resource_page_by_the_configured_message_size() {
    assert_configured_page(Plane::GlobalResource);
}

#[test]
fn should_bound_the_global_area_page_by_the_configured_message_size() {
    assert_configured_page(Plane::GlobalArea);
}

#[test]
fn should_bound_the_global_area_resource_page_by_the_configured_message_size() {
    assert_configured_page(Plane::GlobalAreaResource);
}

#[test]
fn should_return_one_400_kib_event_with_a_tiny_client_byte_budget() {
    // Arrange
    let store = store_with_events(1, 512 * 1024);
    // Act
    let (items, cursor) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "sized",
            from_offset: 0,
            limit: 10,
            max_bytes: Some(1),
        })
        .unwrap();
    // Assert
    assert_event(items);
    assert!(!cursor.has_more);
}
