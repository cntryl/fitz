use super::*;
use crate::domains::kv::inventory::{encode_estimate, KvInventoryEstimate};
use crate::domains::kv::KvActor;

fn write_fixture(sink: &KvDomain, family: RouteFamily, metadata: Option<Vec<u8>>) {
    sink.run_on_family_for_tests(family, move |runtime| {
        let mut tx = runtime
            .core
            .store
            .begin(family.id(), crate::domains::kv::TxMode::ReadWrite)
            .unwrap();
        let prefix = KvActor::realm_resource_prefix("acme", "jobs", "orders");
        tx.put(
            KvActor::encode_scoped_key(&prefix, b"private-key"),
            vec![7; 1024 * 1024],
        )
        .unwrap();
        if let Some(metadata) = metadata {
            tx.put(
                KvActor::inventory_metadata_key("acme", "jobs", "orders"),
                metadata,
            )
            .unwrap();
        }
        tx.commit(crate::domains::WritePolicy::Sync).unwrap();
    });
}

fn sink() -> KvDomain {
    KvDomain::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

#[test]
fn should_read_incomplete_kv_inventory_metadata_without_refreshing_user_values() {
    // Arrange
    let sink = sink();
    let family = RouteFamily::new(1);
    let metadata = encode_estimate(KvInventoryEstimate {
        estimated_record_count: 99,
        estimated_storage_bytes: 123,
        estimate_complete: false,
    });
    write_fixture(&sink, family, Some(metadata.clone()));

    // Act
    let result = sink
        .admin_inventory_metadata_resource(family, "acme", "jobs", "orders")
        .unwrap()
        .unwrap();
    let persisted = sink.run_on_family_for_tests(family, |runtime| {
        runtime
            .core
            .store
            .begin(1, crate::domains::kv::TxMode::ReadOnly)
            .unwrap()
            .get(&KvActor::inventory_metadata_key("acme", "jobs", "orders"))
            .unwrap()
            .unwrap()
    });

    // Assert
    assert_eq!(result.estimated_record_count, 99);
    assert_eq!(result.estimated_storage_bytes, 123);
    assert!(!result.estimate_complete);
    assert_eq!(persisted, metadata);
}

#[test]
fn should_report_missing_kv_inventory_metadata_without_scanning_user_values() {
    // Arrange
    let sink = sink();
    let family = RouteFamily::new(1);
    write_fixture(&sink, family, None);

    // Act
    let result = sink
        .admin_inventory_metadata_resource(family, "acme", "jobs", "orders")
        .unwrap();
    let persisted = sink.run_on_family_for_tests(family, |runtime| {
        runtime
            .core
            .store
            .begin(1, crate::domains::kv::TxMode::ReadOnly)
            .unwrap()
            .get(&KvActor::inventory_metadata_key("acme", "jobs", "orders"))
            .unwrap()
    });

    // Assert
    assert!(result.is_none());
    assert!(persisted.is_none());
}

#[test]
fn should_read_kv_inventory_metadata_only_from_selected_family() {
    // Arrange
    let sink = sink();
    let metadata = encode_estimate(KvInventoryEstimate {
        estimated_record_count: 3,
        estimated_storage_bytes: 100,
        estimate_complete: true,
    });
    write_fixture(&sink, RouteFamily::new(1), Some(metadata));
    write_fixture(
        &sink,
        RouteFamily::new(2),
        Some(b"malformed sibling estimate".to_vec()),
    );

    // Act
    let result = sink
        .admin_inventory_metadata_resource(RouteFamily::new(1), "acme", "jobs", "orders")
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(result.route_family, 1);
    assert_eq!(result.estimated_record_count, 3);
}

#[test]
fn should_read_exact_kv_metadata_while_the_live_transaction_index_is_locked() {
    // Arrange
    let sink = Arc::new(sink());
    let family = RouteFamily::new(1);
    write_fixture(
        &sink,
        family,
        Some(encode_estimate(KvInventoryEstimate {
            estimated_record_count: 3,
            estimated_storage_bytes: 100,
            estimate_complete: true,
        })),
    );
    let resource = KvResourceLockKey::new(1, "acme", "jobs", "orders");
    sink.config.projection.record_read_latency(&resource, 12.0);
    sink.config.projection.record_write_latency(&resource, 24.0);
    let locked = sink.config.active_transactions.lock_for_tests();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    let worker_sink = Arc::clone(&sink);

    // Act
    let worker = std::thread::spawn(move || {
        let result = worker_sink
            .admin_inventory_metadata_resource(family, "acme", "jobs", "orders")
            .unwrap()
            .unwrap();
        done_tx.send(result).unwrap();
    });
    let completed_while_locked = done_rx.recv_timeout(Duration::from_secs(2));
    drop(locked);
    worker.join().unwrap();

    // Assert
    let result = completed_while_locked.expect("metadata must not lock the live transaction index");
    assert_eq!(result.estimated_record_count, 3);
    assert_eq!(result.transactions_active, 0);
    assert!(result.read_latency_avg_ms.abs() < f64::EPSILON);
    assert!(result.read_latency_p95_ms.abs() < f64::EPSILON);
    assert!(result.write_latency_avg_ms.abs() < f64::EPSILON);
    assert!(result.write_latency_p95_ms.abs() < f64::EPSILON);
}

#[test]
fn should_page_kv_metadata_while_the_live_transaction_index_is_locked() {
    // Arrange
    let sink = Arc::new(sink());
    let family = RouteFamily::new(1);
    write_fixture(
        &sink,
        family,
        Some(encode_estimate(KvInventoryEstimate {
            estimated_record_count: 3,
            estimated_storage_bytes: 100,
            estimate_complete: true,
        })),
    );
    let locked = sink.config.active_transactions.lock_for_tests();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    let worker_sink = Arc::clone(&sink);

    // Act
    let worker = std::thread::spawn(move || {
        let result = worker_sink
            .admin_inventory_page(family, "acme", Some("jobs"), None, 1)
            .unwrap();
        done_tx.send(result).unwrap();
    });
    let completed_while_locked = done_rx.recv_timeout(Duration::from_secs(2));
    drop(locked);
    worker.join().unwrap();

    // Assert
    let (entries, has_more) =
        completed_while_locked.expect("metadata page must not lock the live transaction index");
    assert!(!has_more);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].estimated_record_count, 3);
    assert_eq!(entries[0].transactions_active, 0);
}

#[test]
fn should_preserve_live_transaction_and_latency_fields_in_full_kv_inventory() {
    // Arrange
    let sink = sink();
    let family = RouteFamily::new(1);
    write_fixture(
        &sink,
        family,
        Some(encode_estimate(KvInventoryEstimate {
            estimated_record_count: 3,
            estimated_storage_bytes: 100,
            estimate_complete: true,
        })),
    );
    let resource = KvResourceLockKey::new(1, "acme", "jobs", "orders");
    sink.config.projection.record_read_latency(&resource, 12.0);
    sink.config.projection.record_write_latency(&resource, 24.0);
    sink.config.active_transactions.upsert(
        9,
        &crate::control::admin::KvTransaction::snapshot(
            1,
            7,
            9,
            "acme",
            "jobs",
            "orders",
            "2026-10-04T00:00:00Z",
        ),
        None,
    );

    // Act
    let result = sink
        .admin_inventory_resource(family, "acme", "jobs", "orders")
        .unwrap()
        .unwrap();

    // Assert
    assert_eq!(result.transactions_active, 1);
    assert!((result.read_latency_avg_ms - 12.0).abs() < f64::EPSILON);
    assert!((result.read_latency_p95_ms - 12.0).abs() < f64::EPSILON);
    assert!((result.write_latency_avg_ms - 24.0).abs() < f64::EPSILON);
    assert!((result.write_latency_p95_ms - 24.0).abs() < f64::EPSILON);
}
