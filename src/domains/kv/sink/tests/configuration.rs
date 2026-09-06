use super::*;

#[test]
fn should_create_kv_domain_sink() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = KvDomainSink::new(store, router, admin_read_model);

    // Assert
    assert!(sink.is_active_for_tests());
    assert!(sink.is_actor_running());
}

#[test]
fn should_route_kv_counters_to_configured_collector() {
    // Arrange
    const TEST_COUNTER: &str = "fitz_kv_test_scoped_counter_total";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let configured = crate::observability::metrics::MetricsCollector::new();
    let global = crate::observability::metrics();
    let global_before = global.counter_get(TEST_COUNTER);
    let sink = KvDomainSink::new(store, router, admin_read_model).with_metrics(configured.clone());

    // Act
    sink.state.runtime().counter_inc(TEST_COUNTER);

    // Assert
    assert_eq!(configured.counter_get(TEST_COUNTER), 1);
    assert_eq!(global.counter_get(TEST_COUNTER), global_before);
}

#[test]
fn should_record_kv_latency_samples_by_operation_kind() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(source_address.clone(), mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        source_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        source_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&mailbox, "put ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        source_address,
        kv_address,
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&mailbox, "commit ack envelope");
    let resource_key = KvResourceLockKey::new(1, "acme", "app", "users");
    let (reads_before, writes_before) = sink.latency_snapshots(&resource_key);
    assert!(reads_before.avg_ms.abs() < f64::EPSILON);
    assert!(writes_before.avg_ms > 0.0);
    let value = sink
        .admin_get_committed_value(family, "acme", "app", "users", b"user:1")
        .expect("read committed KV value");

    // Assert
    assert_eq!(value.as_deref(), Some(&b"alice"[..]));
    let (reads_after, writes_after) = sink.latency_snapshots(&resource_key);
    assert!(reads_after.avg_ms > 0.0);
    assert!(writes_after.avg_ms > 0.0);
}

#[test]
fn should_map_sync_begin_to_cloud_strict_given_strict_cloud_sync_policy() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model)
        .with_sync_write_options(cntryl_midge::WriteOptions::cloud_strict());
    let message = crate::domains::kv::KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "acme".to_string(),
            "app".to_string(),
            "users".to_string(),
        ),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::sync().into(),
    };

    // Act
    let mapped = sink.apply_write_options(message);

    // Assert
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert_eq!(write_options, crate::domains::WritePolicy::CloudStrict);
        }
        _ => panic!("expected KV begin message"),
    }
}

#[test]
fn should_map_buffered_begin_to_cloud_async_given_cloud_storage() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model).with_write_options(
        cntryl_midge::WriteOptions::cloud_strict(),
        cntryl_midge::WriteOptions::cloud_async(),
    );
    let message = crate::domains::kv::KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "acme".to_string(),
            "app".to_string(),
            "users".to_string(),
        ),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    };

    // Act
    let mapped = sink.apply_write_options(message);

    // Assert
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert_eq!(write_options, crate::domains::WritePolicy::CloudAsync);
        }
        _ => panic!("expected KV begin message"),
    }
}

#[test]
fn should_derive_cloud_async_buffered_policy_given_strict_cloud_sync_builder() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model)
        .with_sync_write_options(cntryl_midge::WriteOptions::cloud_strict());
    let message = crate::domains::kv::KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "acme".to_string(),
            "app".to_string(),
            "users".to_string(),
        ),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    };

    // Act
    let mapped = sink.apply_write_options(message);

    // Assert
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert_eq!(write_options, crate::domains::WritePolicy::CloudAsync);
        }
        _ => panic!("expected KV begin message"),
    }
}

#[test]
fn should_derive_cloud_async_buffered_policy_given_background_cloud_sync_builder() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model)
        .with_sync_write_options(cntryl_midge::WriteOptions::cloud_async());
    let message = crate::domains::kv::KvMessage::Begin {
        scope: KvResourceScope::new(
            RouteFamily::new(1),
            "acme".to_string(),
            "app".to_string(),
            "users".to_string(),
        ),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    };

    // Act
    let mapped = sink.apply_write_options(message);

    // Assert
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert_eq!(write_options, crate::domains::WritePolicy::CloudAsync);
        }
        _ => panic!("expected KV begin message"),
    }
}
