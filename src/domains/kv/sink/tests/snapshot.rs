use super::*;
use crate::domains::kv::KvResponse;

fn commit_value(
    store: crate::domains::kv::store::KvStore,
    scope: KvResourceScope,
    key: &'static [u8],
    value: &'static [u8],
) {
    let mut actor = crate::domains::kv::KvActor::new(store);
    let KvResponse::BeginOk { tx_id } = actor.handle(crate::domains::kv::KvMessage::Begin {
        scope: scope.clone(),
        mode: crate::domains::kv::TxMode::ReadWrite,
    }) else {
        panic!("begin KV transaction");
    };
    assert!(matches!(
        actor.handle(crate::domains::kv::KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        }),
        KvResponse::PutOk
    ));
    assert!(matches!(
        actor.handle(crate::domains::kv::KvMessage::Commit {
            tx_id,
            scope,
            persistence: crate::domains::CommitPersistence::Sync,
        }),
        KvResponse::CommitOk
    ));
}

#[test]
fn should_capture_committed_values_from_one_exact_kv_resource() {
    // Arrange
    let family = RouteFamily::new(1);
    let scope = KvResourceScope::new(family, "acme", "jobs", "orders");
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let write_scope = scope.clone();
    sink.run_on_family_for_tests(family, move |runtime| {
        let column_family =
            crate::domains::kv::KvActor::resolve_column_family(family).expect("valid route family");
        let resource_prefix = crate::domains::kv::KvActor::realm_resource_prefix(
            &write_scope.realm,
            &write_scope.area,
            &write_scope.resource,
        );
        let mut tx = runtime
            .core
            .store
            .begin(column_family, crate::domains::kv::TxMode::ReadWrite)
            .expect("begin committed write");
        tx.put(
            crate::domains::kv::KvActor::encode_scoped_key(&resource_prefix, b"key"),
            b"value".to_vec(),
        )
        .expect("write KV row");
        tx.commit(crate::domains::WritePolicy::Sync)
            .expect("commit KV row");
    });
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://acme/jobs/orders",
    )
    .expect("valid snapshot selector");

    // Act
    let artifact = sink
        .capture_kv_snapshot(&selector)
        .expect("capture committed resource");
    let decoded = crate::snapshot::SnapshotArtifact::from_bytes(
        &artifact.to_bytes().expect("encode artifact"),
    )
    .expect("decode captured artifact");
    let destination = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    destination
        .restore_kv_snapshot(&decoded.to_bytes().expect("persisted artifact bytes"))
        .expect("restore artifact read back into a fresh domain");
    let restored_value = destination
        .admin_get_committed_value(family, "acme", "jobs", "orders", b"key")
        .expect("read restored value");

    // Assert
    assert_eq!(decoded.domain(), crate::snapshot::SnapshotDomain::Kv);
    assert_eq!(decoded.route_family(), family.as_u64());
    assert_eq!(decoded.selector(), "kv://acme/jobs/orders");
    assert_eq!(decoded.record_count(), 1);
    assert_eq!(
        decoded.kv_resources()[0].entries,
        [crate::snapshot::SnapshotKvEntry {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }]
    );
    assert_eq!(restored_value.as_deref(), Some(&b"value"[..]));
}

#[test]
fn should_capture_only_realm_matches_in_the_selected_route_family() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    for (route_family, realm, resource) in [
        (family, "acme", "orders"),
        (family, "acme", "shipments"),
        (family, "other", "orders"),
        (RouteFamily::new(2), "acme", "orders"),
    ] {
        sink.run_on_family_for_tests(route_family, move |runtime| {
            commit_value(
                runtime.core.store.clone(),
                KvResourceScope::new(route_family, realm, "jobs", resource),
                b"key",
                b"value",
            );
        });
    }
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://acme/**",
    )
    .expect("valid realm selector");

    // Act
    let artifact = sink
        .capture_kv_snapshot(&selector)
        .expect("capture realm-matched resources");
    let routes = artifact
        .kv_resources()
        .iter()
        .map(|resource| resource.route.as_str())
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(
        routes,
        ["kv://acme/jobs/orders", "kv://acme/jobs/shipments"]
    );
    assert_eq!(artifact.record_count(), 2);
}

#[test]
fn should_capture_empty_realm_pattern_without_inventing_resources() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://empty/**",
    )
    .expect("valid empty realm selector");

    // Act
    let artifact = sink
        .capture_kv_snapshot(&selector)
        .expect("capture empty realm selector");

    // Assert
    assert_eq!(artifact.kv_resources(), []);
    assert_eq!(artifact.record_count(), 0);
}

#[test]
fn should_reject_kv_restore_given_wildcard_resource_route_before_mutating_destination() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://acme/**",
    )
    .expect("valid KV selector");
    let artifact = crate::snapshot::SnapshotArtifact::from_kv_resources(
        &selector,
        vec![
            crate::snapshot::SnapshotKvResource {
                route: "kv://acme/jobs/orders".to_string(),
                entries: vec![crate::snapshot::SnapshotKvEntry {
                    key: b"hidden".to_vec(),
                    value: b"value".to_vec(),
                }],
            },
            crate::snapshot::SnapshotKvResource {
                route: "kv://acme/jobs/zzorders".to_string(),
                entries: vec![crate::snapshot::SnapshotKvEntry {
                    key: b"hidden".to_vec(),
                    value: b"value".to_vec(),
                }],
            },
        ],
    )
    .expect("valid concrete artifact");

    // Act
    let restored = sink.restore_kv_snapshot(&crate::snapshot::test_support::with_resource_route(
        &artifact,
        1,
        "kv://acme/jobs/zz*",
    ));
    let value = sink
        .admin_get_committed_value(family, "acme", "jobs", "orders", b"hidden")
        .expect("read valid earlier resource directly");

    // Assert
    assert!(
        restored.is_err() && value.is_none(),
        "restore must reject wildcard resource routes before writing even an earlier valid resource; result={restored:?}, value={value:?}"
    );
}

#[test]
fn should_reject_corrupt_snapshot_before_restore_mutates_destination() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let destination_scope = KvResourceScope::new(family, "acme", "jobs", "orders");
    sink.run_on_family_for_tests(family, move |runtime| {
        commit_value(
            runtime.core.store.clone(),
            destination_scope,
            b"key",
            b"original",
        );
    });
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://acme/jobs/orders",
    )
    .expect("valid snapshot selector");
    let artifact = crate::snapshot::SnapshotArtifact::from_kv_resource(
        &selector,
        "kv://acme/jobs/orders",
        vec![crate::snapshot::SnapshotKvEntry {
            key: b"key".to_vec(),
            value: b"replacement".to_vec(),
        }],
    )
    .expect("valid snapshot artifact");
    let mut encoded: serde_json::Value =
        serde_json::from_slice(&artifact.to_bytes().expect("encode artifact"))
            .expect("artifact JSON");
    encoded["payload"]["resources"][0]["entries"][0]["value"][0] = 88.into();
    let corrupt = serde_json::to_vec(&encoded).expect("encode corrupt artifact");

    // Act
    let result = sink.restore_kv_snapshot(&corrupt);
    let value = sink
        .admin_get_committed_value(family, "acme", "jobs", "orders", b"key")
        .expect("read destination value");

    // Assert
    assert!(result.is_err());
    assert_eq!(value.as_deref(), Some(&b"original"[..]));
}

#[test]
fn should_replace_selected_kv_resources_and_remove_post_capture_values() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://acme/jobs/**",
    )
    .expect("valid realm selector");
    commit_value(
        sink.config.store.clone(),
        KvResourceScope::new(family, "acme", "jobs", "orders"),
        b"stable",
        b"captured",
    );
    let artifact = sink
        .capture_kv_snapshot(&selector)
        .expect("capture committed values");
    commit_value(
        sink.config.store.clone(),
        KvResourceScope::new(family, "acme", "jobs", "orders"),
        b"later",
        b"remove",
    );
    commit_value(
        sink.config.store.clone(),
        KvResourceScope::new(family, "acme", "jobs", "created-after"),
        b"later",
        b"remove resource",
    );
    let current = sink
        .capture_kv_snapshot(&selector)
        .expect("capture current destination values");
    assert_eq!(
        current
            .kv_resources()
            .iter()
            .find(|resource| resource.route.ends_with("created-after"))
            .expect("new destination resource")
            .entries[0]
            .key,
        b"later"
    );

    // Act
    let completed = sink
        .restore_kv_snapshot(&artifact.to_bytes().expect("encode snapshot"))
        .expect("restore snapshot");
    let stable = sink
        .admin_get_committed_value(family, "acme", "jobs", "orders", b"stable")
        .expect("read restored key");
    let later = sink
        .admin_get_committed_value(family, "acme", "jobs", "orders", b"later")
        .expect("read removed key");
    let created = sink
        .admin_get_committed_value(family, "acme", "jobs", "created-after", b"later")
        .expect("read removed resource");

    // Assert
    assert_eq!(stable.as_deref(), Some(&b"captured"[..]));
    assert_eq!(later, None);
    assert_eq!(created, None, "completed resources: {completed:?}");
    assert_eq!(completed.len(), 2);
}

#[test]
fn should_capture_atomic_commits_without_partial_rows_during_concurrent_writes() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let scope = KvResourceScope::new(family, "acme", "jobs", "orders");
    commit_value(
        sink.config.store.clone(),
        scope.clone(),
        b"stable",
        b"value",
    );
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Kv,
        family,
        "kv://acme/jobs/orders",
    )
    .expect("valid exact-resource selector");
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let store = sink.config.store.clone();

    // Act
    std::thread::scope(|scope_thread| {
        let writer_barrier = barrier.clone();
        let writer = scope_thread.spawn(move || {
            writer_barrier.wait();
            let mut actor = crate::domains::kv::KvActor::new(store);
            let KvResponse::BeginOk { tx_id } =
                actor.handle(crate::domains::kv::KvMessage::Begin {
                    scope: scope.clone(),
                    mode: crate::domains::kv::TxMode::ReadWrite,
                })
            else {
                panic!("begin concurrent KV transaction");
            };
            for key in [b"atomic-a".as_slice(), b"atomic-b".as_slice()] {
                assert!(matches!(
                    actor.handle(crate::domains::kv::KvMessage::Put {
                        tx_id,
                        scope: scope.clone(),
                        key: Bytes::copy_from_slice(key),
                        value: Bytes::from_static(b"value"),
                    }),
                    KvResponse::PutOk
                ));
            }
            assert!(matches!(
                actor.handle(crate::domains::kv::KvMessage::Commit {
                    tx_id,
                    scope,
                    persistence: crate::domains::CommitPersistence::Sync,
                }),
                KvResponse::CommitOk
            ));
        });
        barrier.wait();
        for _ in 0..20 {
            let artifact = sink
                .capture_kv_snapshot(&selector)
                .expect("capture during concurrent commit");
            let rows = artifact.kv_resources()[0].entries.len();
            assert!(rows == 1 || rows == 3, "observed a partial transaction");
        }
        writer.join().expect("finish concurrent KV writer");
    });

    // Assert
    let final_artifact = sink
        .capture_kv_snapshot(&selector)
        .expect("capture committed KV rows");
    assert_eq!(final_artifact.record_count(), 3);
}
