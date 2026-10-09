use super::*;

#[derive(Clone, Copy)]
enum Scope {
    Resource,
    Area,
    Realm,
    Global,
}

impl Scope {
    fn read(
        self,
        store: &StreamStore,
        from: u64,
        limit: u64,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        match self {
            Self::Resource => store.read_resource(&ReadResourceParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                from_offset: from,
                limit,
                max_bytes: None,
            }),
            Self::Area => store.read_area(1, "north", "orders", from, limit, None),
            Self::Realm => store.read_realm(1, "north", from, limit, None),
            Self::Global => store.read_global(1, from, limit, None, None),
        }
    }
}

fn fixture() -> (StreamStore, Vec<EventPayload>) {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    let events = (0..64)
        .map(|offset| EventPayload {
            body: Bytes::from(vec![u8::try_from(offset).unwrap(); 17 * 1024]),
            metadata: Some(Bytes::from_static(b"event-kind=OrderChanged")),
            discriminator: Some("OrderChanged".into()),
        })
        .collect::<Vec<_>>();
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
        .expect("commit full blob-backed fragment");
    store.set_watermark(1, "north", "orders", 63).unwrap();
    store.set_realm_watermark(1, "north", 63).unwrap();
    (store, events)
}

fn delete_blob(store: &StreamStore, offset: u64) {
    let mut txn = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    txn.delete(encode_payload_blob_key(offset)).unwrap();
    txn.commit(cntryl_midge::WriteOptions::sync()).unwrap();
}

fn assert_unrequested_blob_is_not_loaded(scope: Scope, missing: u64) {
    // Arrange
    let (store, events) = fixture();
    delete_blob(&store, missing);

    // Act
    let (items, cursor) = scope.read(&store, 31, 1).expect("read checkpoint event");

    // Assert
    let records = event_records(items);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, events[31].body);
    assert_eq!(records[0].metadata, events[31].metadata);
    assert_eq!(records[0].route.as_str(), "stream://north/orders/created");
    assert_eq!(records[0].area_offset, Some(31));
    assert_eq!(
        records[0].realm_offset,
        (!matches!(scope, Scope::Area)).then_some(31)
    );
    assert_eq!(
        records[0].global_offset,
        matches!(scope, Scope::Global).then_some(31)
    );
    match scope {
        Scope::Resource => assert_eq!(cursor.last_resource_offset, 31),
        Scope::Area => assert_eq!(cursor.last_area_offset, Some(31)),
        Scope::Realm => assert_eq!(cursor.last_realm_offset, Some(31)),
        Scope::Global => assert_eq!(cursor.last_global_offset, Some(31)),
    }
    assert!(cursor.has_more);
}

fn assert_selected_blob_is_validated(scope: Scope) {
    // Arrange
    let (store, _) = fixture();
    delete_blob(&store, 31);

    // Act
    let result = scope.read(&store, 31, 1);

    // Assert
    assert!(result
        .expect_err("validate requested payload")
        .contains("missing payload blob"));
}

macro_rules! boundary_test {
    ($name:ident, $scope:ident, $missing:expr) => {
        #[test]
        fn $name() {
            assert_unrequested_blob_is_not_loaded(Scope::$scope, $missing);
        }
    };
}

boundary_test!(should_skip_resource_blobs_before_checkpoint, Resource, 0);
boundary_test!(should_skip_area_blobs_before_checkpoint, Area, 0);
boundary_test!(should_skip_realm_blobs_before_checkpoint, Realm, 0);
boundary_test!(should_skip_global_blobs_before_checkpoint, Global, 0);
boundary_test!(should_skip_resource_blobs_after_item_limit, Resource, 32);
boundary_test!(should_skip_area_blobs_after_item_limit, Area, 32);
boundary_test!(should_skip_realm_blobs_after_item_limit, Realm, 32);
boundary_test!(should_skip_global_blobs_after_item_limit, Global, 32);

macro_rules! validation_test {
    ($name:ident, $scope:ident) => {
        #[test]
        fn $name() {
            assert_selected_blob_is_validated(Scope::$scope);
        }
    };
}

validation_test!(should_validate_selected_resource_blob, Resource);
validation_test!(should_validate_selected_area_blob, Area);
validation_test!(should_validate_selected_realm_blob, Realm);
validation_test!(should_validate_selected_global_blob, Global);

fn assert_selected_blob_checksum_is_validated(scope: Scope) {
    // Arrange
    let (store, _) = fixture();
    let mut txn = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    let key = encode_payload_blob_key(31);
    let mut blob = txn.get(&key).unwrap().unwrap().to_vec();
    *blob.last_mut().unwrap() ^= 1;
    txn.put(key, blob, None).unwrap();
    txn.commit(cntryl_midge::WriteOptions::sync()).unwrap();

    // Act
    let result = scope.read(&store, 31, 1);

    // Assert
    assert!(result
        .expect_err("validate requested payload checksum")
        .contains("checksum"));
}

macro_rules! checksum_test {
    ($name:ident, $scope:ident) => {
        #[test]
        fn $name() {
            assert_selected_blob_checksum_is_validated(Scope::$scope);
        }
    };
}

checksum_test!(should_validate_selected_resource_blob_checksum, Resource);
checksum_test!(should_validate_selected_area_blob_checksum, Area);
checksum_test!(should_validate_selected_realm_blob_checksum, Realm);
checksum_test!(should_validate_selected_global_blob_checksum, Global);

#[test]
fn should_validate_filter_excluded_blob_in_examined_range() {
    // Arrange
    let (store, _) = fixture();
    delete_blob(&store, 31);
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("OtherKind".into())],
    };

    // Act
    let result = store.read_resource_with_filter(
        &ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 31,
            limit: 1,
            max_bytes: None,
        },
        Some(&filter),
    );

    // Assert
    assert!(result
        .expect_err("examined filtered payload must still be validated")
        .contains("missing payload blob"));
}

#[derive(Clone, Copy)]
enum Projection {
    Global,
    GlobalArea,
    RealmResource,
}

impl Projection {
    fn read(
        self,
        store: &StreamStore,
        filter: Option<&StreamFilterSet>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        match self {
            Self::Global => store.read_global(1, 31, 1, None, filter),
            Self::GlobalArea => store.read_global_posting(
                &ReadGlobalPostingParams {
                    family: 1,
                    from_offset: 31,
                    limit: 1,
                    max_bytes: None,
                    area: Some("orders"),
                    resource: None,
                },
                filter,
            ),
            Self::RealmResource => store.read_realm_resource_posting(
                &ReadRealmPostingParams {
                    family: 1,
                    realm: "north",
                    resource: "created",
                    from_offset: 31,
                    limit: 1,
                    max_bytes: None,
                },
                filter,
            ),
        }
    }
}

fn fixture_with_invalid_discriminator() -> (StreamStore, Vec<EventPayload>) {
    let (store, events) = fixture();
    let mut txn = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    txn.put(encode_global_discriminator_key(31), vec![0xff], None)
        .unwrap();
    txn.put(
        crate::domains::stream::storage::encode_realm_discriminator_key("north", 31),
        vec![0xff],
        None,
    )
    .unwrap();
    txn.commit(cntryl_midge::WriteOptions::sync()).unwrap();
    (store, events)
}

fn assert_unfiltered_projection_ignores_unused_discriminator(scope: Projection) {
    // Arrange
    let (store, events) = fixture_with_invalid_discriminator();

    // Act
    let (items, _) = scope
        .read(&store, None)
        .expect("read without a discriminator filter");

    // Assert
    let records = event_records(items);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, events[31].body);
    assert_eq!(records[0].metadata, events[31].metadata);
    assert_eq!(records[0].resource_offset, 31);
}

fn assert_filtered_projection_validates_discriminator(scope: Projection) {
    // Arrange
    let (store, _) = fixture_with_invalid_discriminator();
    let filter = StreamFilterSet {
        clauses: vec![StreamFilterClause::Equals("OrderChanged".into())],
    };

    // Act
    let result = scope.read(&store, Some(&filter));

    // Assert
    assert!(result
        .expect_err("validate filter data")
        .contains("invalid stream discriminator bytes"));
}

macro_rules! discriminator_tests {
    ($unfiltered:ident, $filtered:ident, $scope:ident) => {
        #[test]
        fn $unfiltered() {
            assert_unfiltered_projection_ignores_unused_discriminator(Projection::$scope);
        }
        #[test]
        fn $filtered() {
            assert_filtered_projection_validates_discriminator(Projection::$scope);
        }
    };
}

discriminator_tests!(
    should_skip_unused_global_discriminator,
    should_validate_filtered_global_discriminator,
    Global
);
discriminator_tests!(
    should_skip_unused_global_posting_discriminator,
    should_validate_filtered_global_posting_discriminator,
    GlobalArea
);
discriminator_tests!(
    should_skip_unused_realm_posting_discriminator,
    should_validate_filtered_realm_posting_discriminator,
    RealmResource
);
