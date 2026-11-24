// Advanced KV tests — durability & recovery (moved from in-source migration)
// These are integration-style KV tests; they belong under `tests/`.

use bytes::Bytes;
use fitz_core::runtime::routing::RouteFamily;
use fitz_kv::domain::{KvMessage, KvResourceScope, KvResponse, ScanQuery, TxMode};
use fitz_kv::testkit::create_local_bench_store;
use fitz_kv::testkit::domain_internals::KvActor;
use fitz_kv::testkit::{create_test_engine_with_cfs, scaled_test_timeout};
use std::sync::Arc;
use std::time::Duration;

fn kv_scope(family: RouteFamily, realm: &str, resource: &str) -> KvResourceScope {
    KvResourceScope::new(family, realm, "kv", resource)
}

fn shutdown_local_bench_store(store: Arc<cntryl_midge::Engine>) {
    let mut engine = Arc::try_unwrap(store).unwrap_or_else(|store| {
        panic!(
            "local bench store has {} remaining owners",
            Arc::strong_count(&store)
        );
    });
    engine
        .shutdown(scaled_test_timeout(Duration::from_secs(2)))
        .expect("shutdown local bench store");
}

fn reopen_local_bench_store(temp_path: &std::path::Path) -> Arc<cntryl_midge::Engine> {
    Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(temp_path.to_string_lossy().as_ref())
                .build()
                .expect("build local reopen options"),
        )
        .expect("reopen engine"),
    )
}

fn write_committed_value_for_family(
    actor: &mut KvActor,
    family: RouteFamily,
    value: &'static [u8],
) {
    let begin = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            family,
            "tenant".to_string(),
            "kv".to_string(),
            "shared".to_string(),
        ),
        mode: TxMode::ReadWrite,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Begin failed");
    };

    let put = actor.handle(KvMessage::Put {
        tx_id,
        scope: kv_scope(family, "tenant", "shared"),
        key: Bytes::from_static(b"same-key"),
        value: Bytes::from_static(value),
    });
    assert!(matches!(put, KvResponse::PutOk));

    let commit = actor.handle(KvMessage::Commit {
        tx_id,
        scope: kv_scope(family, "tenant", "shared"),
        persistence: fitz_storage::domains::CommitPersistence::Buffered,
    });
    assert!(matches!(commit, KvResponse::CommitOk));
}

fn read_committed_value_for_family(actor: &mut KvActor, family: RouteFamily) -> Bytes {
    let begin = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            family,
            "tenant".to_string(),
            "kv".to_string(),
            "shared".to_string(),
        ),
        mode: TxMode::ReadOnly,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Begin failed");
    };

    let response = actor.handle(KvMessage::Get {
        tx_id,
        scope: kv_scope(family, "tenant", "shared"),
        key: Bytes::from_static(b"same-key"),
    });

    actor.handle(KvMessage::Rollback {
        tx_id,
        scope: kv_scope(family, "tenant", "shared"),
    });

    match response {
        KvResponse::GetResult {
            found: true,
            value: Some(value),
        } => value,
        other => panic!("Expected stored value, got {other:?}"),
    }
}

#[test]
fn should_show_committed_value_before_restart() {
    // Arrange
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store.clone());

    // Act
    let begin = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "dur".to_string(),
            "kv".to_string(),
            "r".to_string(),
        ),
        mode: TxMode::ReadWrite,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Begin failed");
    };

    actor.handle(KvMessage::Put {
        tx_id,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
        key: Bytes::from_static(b"k1"),
        value: Bytes::from_static(b"v1"),
    });

    let c = actor.handle(KvMessage::Commit {
        tx_id,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
        persistence: fitz_storage::domains::CommitPersistence::Sync,
    });

    // Assert (verify visibility)
    let b_read = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "dur".to_string(),
            "kv".to_string(),
            "r".to_string(),
        ),
        mode: TxMode::ReadOnly,
    });
    let KvResponse::BeginOk { tx_id: tx_read } = b_read else {
        panic!("Expected BeginOk");
    };

    // Assert
    assert!(matches!(c, KvResponse::CommitOk));
    let got_now = actor.handle(KvMessage::Get {
        tx_id: tx_read,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
        key: Bytes::from_static(b"k1"),
    });
    match got_now {
        KvResponse::GetResult {
            found: true,
            value: Some(v),
        } => assert_eq!(&*v, b"v1"),
        _ => panic!("Expected persisted value *before* restart"),
    }

    // Cleanup
    actor.handle(KvMessage::Rollback {
        tx_id: tx_read,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
    });
}

#[test]
fn should_restore_committed_kv_value_on_engine_restart() {
    // Arrange
    let (store, temp_dir) = create_local_bench_store();
    let scope = kv_scope(RouteFamily::new(1), "dur", "r");
    let key = Bytes::from_static(b"k1");
    let value = Bytes::from_static(b"v1");
    let mut writer = KvActor::new(Arc::clone(&store));

    let begin = writer.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Begin failed");
    };

    let put = writer.handle(KvMessage::Put {
        tx_id,
        scope: scope.clone(),
        key: key.clone(),
        value: value.clone(),
    });
    assert!(matches!(put, KvResponse::PutOk), "{put:?}");

    let commit = writer.handle(KvMessage::Commit {
        tx_id,
        scope: scope.clone(),
        persistence: fitz_storage::domains::CommitPersistence::Sync,
    });
    assert!(matches!(commit, KvResponse::CommitOk), "{commit:?}");

    drop(writer);
    shutdown_local_bench_store(store);

    // Act
    let mut reader = KvActor::new(reopen_local_bench_store(temp_dir.path()));
    let begin = reader.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadOnly,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Expected BeginOk, got {begin:?}");
    };
    let restored = reader.handle(KvMessage::Get {
        tx_id,
        scope: scope.clone(),
        key,
    });
    reader.handle(KvMessage::Rollback { tx_id, scope });

    // Assert
    let KvResponse::GetResult {
        found: true,
        value: Some(restored_value),
    } = restored
    else {
        panic!("Expected persisted value after restart, got {restored:?}");
    };
    assert_eq!(restored_value, value);
}

#[test]
fn should_discard_uncommitted_kv_write_on_engine_restart() {
    // Arrange
    let (store, temp_dir) = create_local_bench_store();
    let temp_path = temp_dir.path().to_path_buf();
    let mut actor = KvActor::new(store.clone());

    let begin = actor.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "dur".to_string(),
            "kv".to_string(),
            "r".to_string(),
        ),
        mode: TxMode::ReadWrite,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Begin failed");
    };

    let put = actor.handle(KvMessage::Put {
        tx_id,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
        key: Bytes::from_static(b"k1"),
        value: Bytes::from_static(b"v1"),
    });
    assert!(matches!(put, KvResponse::PutOk));

    drop(actor);
    shutdown_local_bench_store(store);

    let reopened = reopen_local_bench_store(&temp_path);
    let mut actor2 = KvActor::new(reopened);
    let begin2 = actor2.handle(KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "dur".to_string(),
            "kv".to_string(),
            "r".to_string(),
        ),
        mode: TxMode::ReadOnly,
    });
    let KvResponse::BeginOk { tx_id: tx2 } = begin2 else {
        panic!("Expected BeginOk");
    };

    // Act
    let got = actor2.handle(KvMessage::Get {
        tx_id: tx2,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
        key: Bytes::from_static(b"k1"),
    });

    // Assert
    match got {
        KvResponse::GetResult {
            found: false,
            value: None,
        } => {}
        other => panic!("Expected uncommitted value to be lost after restart, got {other:?}"),
    }

    let rollback = actor2.handle(KvMessage::Rollback {
        tx_id: tx2,
        scope: kv_scope(RouteFamily::new(1), "dur", "r"),
    });
    assert!(matches!(rollback, KvResponse::RollbackOk));
}

#[test]
fn should_return_family_one_value_given_same_key_in_multiple_route_families() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);

    write_committed_value_for_family(&mut actor, family_one, b"family-1");
    write_committed_value_for_family(&mut actor, family_two, b"family-2");

    // Act
    let value = read_committed_value_for_family(&mut actor, family_one);

    // Assert
    assert_eq!(value, Bytes::from_static(b"family-1"));
}

#[test]
fn should_return_family_two_value_given_same_key_in_multiple_route_families() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1, 2]);
    let mut actor = KvActor::new(store);
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);

    write_committed_value_for_family(&mut actor, family_one, b"family-1");
    write_committed_value_for_family(&mut actor, family_two, b"family-2");

    // Act
    let value = read_committed_value_for_family(&mut actor, family_two);

    // Assert
    assert_eq!(value, Bytes::from_static(b"family-2"));
}

#[test]
fn should_return_family_one_value_after_engine_restart_given_same_key_in_multiple_route_families() {
    // Arrange
    let (store, temp_dir) = create_local_bench_store();
    let temp_path = temp_dir.path().to_path_buf();
    let _ = store.create_column_family("cf_2");

    let mut actor = KvActor::new(store.clone());
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);

    write_committed_value_for_family(&mut actor, family_one, b"family-1");
    write_committed_value_for_family(&mut actor, family_two, b"family-2");

    drop(actor);
    shutdown_local_bench_store(store);

    let reopened = reopen_local_bench_store(&temp_path);
    let mut actor2 = KvActor::new(reopened);

    // Act
    let value = read_committed_value_for_family(&mut actor2, family_one);

    // Assert
    assert_eq!(value, Bytes::from_static(b"family-1"));
}

#[test]
fn should_return_family_two_value_after_engine_restart_given_same_key_in_multiple_route_families() {
    // Arrange
    let (store, temp_dir) = create_local_bench_store();
    let temp_path = temp_dir.path().to_path_buf();
    let _ = store.create_column_family("cf_2");

    let mut actor = KvActor::new(store.clone());
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);

    write_committed_value_for_family(&mut actor, family_one, b"family-1");
    write_committed_value_for_family(&mut actor, family_two, b"family-2");

    drop(actor);
    shutdown_local_bench_store(store);

    let reopened = reopen_local_bench_store(&temp_path);
    let mut actor2 = KvActor::new(reopened);

    // Act
    let value = read_committed_value_for_family(&mut actor2, family_two);

    // Assert
    assert_eq!(value, Bytes::from_static(b"family-2"));
}

#[test]
fn should_read_all_values_after_repeated_buffered_commits() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let scope = kv_scope(RouteFamily::new(1), "scale", "batch");
    let expected = (0..200)
        .map(|i| {
            (
                Bytes::from(format!("k{i:04}")),
                Bytes::from(format!("v{i:04}")),
            )
        })
        .collect::<Vec<_>>();

    for (key, value) in &expected {
        let begin = actor.handle(KvMessage::Begin {
            scope: scope.clone(),
            mode: TxMode::ReadWrite,
        });
        let KvResponse::BeginOk { tx_id } = begin else {
            panic!("Expected BeginOk for {key:?}, got {begin:?}");
        };

        let put = actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: key.clone(),
            value: value.clone(),
        });
        assert!(matches!(put, KvResponse::PutOk), "put {key:?}: {put:?}");

        let commit = actor.handle(KvMessage::Commit {
            tx_id,
            scope: scope.clone(),
            persistence: fitz_storage::domains::CommitPersistence::Buffered,
        });
        assert!(
            matches!(commit, KvResponse::CommitOk),
            "commit {key:?}: {commit:?}"
        );
    }

    let begin = actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadOnly,
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("Expected BeginOk, got {begin:?}");
    };

    // Act
    let result = actor.handle(KvMessage::Scan {
        tx_id,
        scope: scope.clone(),
        query: ScanQuery {
            start: None,
            end: None,
            limit: None,
            reverse: false,
            start_exclusive: false,
        },
    });
    actor.handle(KvMessage::Rollback { tx_id, scope });

    // Assert
    let KvResponse::ScanResult { items, has_more } = result else {
        panic!("Expected ScanResult, got {result:?}");
    };
    let actual = items
        .into_iter()
        .map(|item| (item.key, item.value))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert!(!has_more);
}
