use super::*;

#[test]
fn should_map_inventory_write_options_to_matching_local_or_cloud_class() {
    // Arrange
    let local_options = [
        cntryl_midge::WriteOptions::sync(),
        cntryl_midge::WriteOptions::buffered(),
        cntryl_midge::WriteOptions::best_effort(),
    ];
    let cloud_options = [
        cntryl_midge::WriteOptions::cloud_async(),
        cntryl_midge::WriteOptions::cloud_strict(),
    ];

    // Act
    let local_inventory_options = local_options.map(KvActor::inventory_write_options);
    let cloud_inventory_options = cloud_options.map(KvActor::inventory_write_options);

    // Assert
    assert_eq!(
        local_inventory_options,
        [cntryl_midge::WriteOptions::buffered(); 3]
    );
    assert_eq!(
        cloud_inventory_options,
        [cntryl_midge::WriteOptions::cloud_async(); 2]
    );
}

#[test]
fn should_persist_inventory_estimate_after_commit_in_cloud_mode() {
    // Arrange: a cloud-backed engine only accepts cloud_async()/cloud_strict()
    // commits; sync()/buffered() are rejected as local-only.
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path(),
                "fitz-kv-inventory-test",
                "background",
            )
            .build()
            .expect("build cloud-simulated options"),
        )
        .expect("open cloud-simulated engine"),
    );
    store
        .create_column_family("cf_1")
        .expect("create route-family column family");
    let mut actor = KvActor::new(store.clone());
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "cloud-shared");

    let KvResponse::BeginOk { tx_id } = actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::cloud_async().into(),
    }) else {
        panic!("transaction should begin");
    };
    assert!(matches!(
        actor.handle(KvMessage::Insert {
            tx_id,
            scope: scope.clone(),
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        }),
        KvResponse::InsertOk
    ));

    // Act
    let commit = actor.handle(KvMessage::Commit {
        tx_id,
        scope: scope.clone(),
    });

    // Assert: the primary write always succeeds regardless of the inventory
    // bug, so the real assertion is that the inventory estimate is actually
    // persisted afterward.
    assert!(matches!(commit, KvResponse::CommitOk));
    let inventory_key = KvActor::inventory_metadata_key(&scope.realm, &scope.area, &scope.resource);
    let read_tx = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin inventory read transaction");
    let stored = read_tx
        .get(&inventory_key)
        .expect("read inventory metadata");
    let estimate = crate::domains::kv::inventory::decode_estimate(
        stored
            .as_deref()
            .expect("inventory estimate should be persisted even in cloud mode"),
    )
    .expect("decode persisted inventory estimate");
    assert_eq!(estimate.estimated_record_count, 1);
}

#[test]
fn should_commit_disjoint_writes_without_inventory_conflict() {
    // Arrange
    let mut actor = test_actor();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "shared");
    let begin = |actor: &mut KvActor| {
        let KvResponse::BeginOk { tx_id } = actor.handle(KvMessage::Begin {
            scope: scope.clone(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered().into(),
        }) else {
            panic!("transaction should begin");
        };
        tx_id
    };
    let first = begin(&mut actor);
    let second = begin(&mut actor);
    for (tx_id, key) in [(first, "first"), (second, "second")] {
        assert!(matches!(
            actor.handle(KvMessage::Put {
                tx_id,
                scope: scope.clone(),
                key: Bytes::from(key),
                value: Bytes::from_static(b"value"),
            }),
            KvResponse::PutOk
        ));
    }

    // Act
    let first_commit = actor.handle(KvMessage::Commit {
        tx_id: first,
        scope: scope.clone(),
    });
    let second_commit = actor.handle(KvMessage::Commit {
        tx_id: second,
        scope,
    });

    // Assert
    assert!(matches!(first_commit, KvResponse::CommitOk));
    assert!(matches!(second_commit, KvResponse::CommitOk));
}

#[test]
fn should_mark_inventory_incomplete_for_put_without_adding_a_hot_path_read() {
    // Arrange
    let mut actor = test_actor();
    let store = actor.store.clone();
    let scope = KvResourceScope::new(RouteFamily::new(1), "test", "kv", "conservative");
    let tx_id = begin_with_scope(&mut actor, scope.clone());
    assert!(matches!(
        actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        }),
        KvResponse::PutOk
    ));

    // Act
    let response = actor.handle(KvMessage::Commit {
        tx_id,
        scope: scope.clone(),
    });

    // Assert
    assert!(matches!(response, KvResponse::CommitOk));
    let read_tx = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin inventory read transaction");
    let encoded = read_tx
        .get(&KvActor::inventory_metadata_key(
            &scope.realm,
            &scope.area,
            &scope.resource,
        ))
        .expect("read inventory estimate")
        .expect("incomplete estimate should be persisted");
    let estimate = crate::domains::kv::inventory::decode_estimate(&encoded)
        .expect("decode inventory estimate");
    assert!(!estimate.estimate_complete);
}

#[test]
pub(super) fn should_encode_kv_scope_prefix_with_typed_segments() {
    // Arrange
    let expected = {
        let mut bytes = b"acme\0kv\0".to_vec();
        bytes.push(KV_KEY_SCOPE_MARKER);
        bytes.extend_from_slice(b"users\0profiles\0");
        bytes
    };

    // Act
    let prefix = KvActor::realm_resource_prefix("acme", "users", "profiles");

    // Assert
    assert_eq!(prefix, expected);
}
