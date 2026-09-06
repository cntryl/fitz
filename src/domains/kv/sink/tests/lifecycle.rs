use super::*;

#[test]
fn should_confirm_kv_session_cleanup_before_reporting_delivery() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let sink = KvDomainSink::new(
        store,
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("KV actor should block");
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);

    // Act
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let result = sink.deliver_high_priority(Envelope::new(
                RouteAddress::new(family, Route::new("kv://cleanup")),
                crate::runtime::SessionCleanup { session_id: 7 },
            ));
            let _ = result_tx.send(result);
        });
        let early_result = result_rx.recv_timeout(Duration::from_millis(50));
        let returned_early = early_result.is_ok();
        release_tx.send(()).expect("release KV actor");
        let final_result = early_result.unwrap_or_else(|_| {
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("KV cleanup result")
        });

        // Assert
        assert!(!returned_early, "cleanup returned before it executed");
        assert_eq!(final_result, Ok(()));
    });
}

#[test]
fn should_reject_queued_begin_after_cleanup_without_recreating_session_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(source_address.clone(), mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model.clone());
    let previously_queued_begin = Envelope::from_route(
        source_address,
        kv_address,
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    );

    // Act
    sink.cleanup_session(session_id)
        .expect("cleanup KV session");
    sink.deliver(previously_queued_begin)
        .expect("deliver queued BEGIN after cleanup");
    let response = receive_frame(&mailbox, "queued BEGIN rejection");

    // Assert
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_INVALID_ROUTE
    );
    assert!(sink.actors_are_empty_for_tests());
    assert_eq!(sink.active_transaction_count(), 0);
    assert!(sink.resource_locks_are_empty_for_tests());
    assert!(sink.watch_registries_are_empty_for_tests());
    assert!(admin_read_model.kv_transactions(None).is_empty());
}

#[test]
fn should_release_resource_lock_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let first_session_id = 7;
    let second_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let first_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let second_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let first_mailbox = Arc::new(Mailbox::new(8));
    let second_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(first_address.clone(), first_mailbox.clone());
    router.register(second_address.clone(), second_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        first_address,
        kv_address.clone(),
        FrameContext::new(
            first_session_id,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin first KV transaction");
    let first_begin_frame = receive_frame(&first_mailbox, "first begin ack envelope");
    let first_tx_id = decode_kv_begin_tx_id(&first_begin_frame.payload);
    assert_eq!(first_begin_frame.payload[0], 0);
    assert!(first_tx_id > 0);
    assert_eq!(sink.active_transaction_count(), 1);
    drain_mailbox(&first_mailbox);

    // Act
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("kv://cleanup")),
        crate::runtime::SessionCleanup {
            session_id: first_session_id,
        },
    ))
    .expect("cleanup first KV session");
    wait_for_active_transaction_count(&sink, 0);

    sink.deliver(Envelope::from_route(
        second_address,
        kv_address,
        FrameContext::new(
            second_session_id,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin second KV transaction");

    // Assert
    let second_begin_frame = receive_frame(&second_mailbox, "second begin ack envelope");
    let second_tx_id = decode_kv_begin_tx_id(&second_begin_frame.payload);
    assert_eq!(second_begin_frame.payload[0], 0);
    assert!(second_tx_id > 0);
    assert_eq!(sink.active_transaction_count(), 1);
    assert_no_envelope(&first_mailbox);
}

#[test]
fn should_reject_conflicting_read_write_begin_given_active_transaction_in_other_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let first_session_id = 7;
    let second_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let first_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let second_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let first_mailbox = Arc::new(Mailbox::new(8));
    let second_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(first_address.clone(), first_mailbox.clone());
    router.register(second_address.clone(), second_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        first_address,
        kv_address.clone(),
        FrameContext::new(
            first_session_id,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin first KV transaction");
    let _ = receive_envelope(&first_mailbox, "first begin ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        second_address,
        kv_address,
        FrameContext::new(
            second_session_id,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin second KV transaction");

    // Assert
    let second_begin_frame = receive_frame(&second_mailbox, "second begin response envelope");
    assert_eq!(
        decode_error_code(&second_begin_frame.payload),
        error_codes::kv::ERR_ISOLATION_CONFLICT
    );
    assert_eq!(sink.active_transaction_count(), 1);
}

#[test]
fn should_rebuild_kv_admin_transactions_from_actor_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(source_address.clone(), mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        source_address,
        kv_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let _ = receive_envelope(&mailbox, "begin ack envelope");

    // Act
    sink.sync_admin_snapshot();
    let before_cleanup = admin_read_model.kv_transactions(None);
    sink.cleanup_session(session_id)
        .expect("cleanup KV session");
    let after_cleanup = admin_read_model.kv_transactions(None);

    // Assert
    assert_eq!(before_cleanup.len(), 1);
    assert_eq!(before_cleanup[0].route_family, 1);
    assert_eq!(before_cleanup[0].realm, "acme");
    assert_eq!(before_cleanup[0].area, "app");
    assert_eq!(before_cleanup[0].resource, "users");
    assert!(after_cleanup.is_empty());
}

#[test]
fn should_route_kv_cleanup_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store.clone(), router, admin_read_model.clone());
    let mut actor = crate::domains::kv::KvActor::new(store);
    let begin_response = actor.handle(crate::domains::kv::KvMessage::Begin {
        scope: KvResourceScope::new(
            family,
            "acme".to_string(),
            "app".to_string(),
            "users".to_string(),
        ),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    assert!(matches!(
        begin_response,
        crate::domains::kv::KvResponse::BeginOk { .. }
    ));
    sink.insert_actor_for_tests(session_id, actor);
    sink.sync_admin_snapshot();
    assert_eq!(sink.active_transaction_count(), 1);
    assert_eq!(admin_read_model.kv_transactions(None).len(), 1);

    // Act
    sink.stop_actor_for_tests();
    let cleanup_result = sink.cleanup_session(session_id);
    sink.sync_admin_snapshot();
    let after_cleanup = admin_read_model.kv_transactions(None);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(
        cleanup_result,
        Err(crate::runtime::DeliveryError::ActorStopped)
    ));
    assert_eq!(after_cleanup.len(), 1);
}

#[test]
fn should_route_kv_live_transaction_count_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(source_address.clone(), mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);
    sink.deliver(Envelope::from_route(
        source_address,
        kv_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(100),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let _ = receive_envelope(&mailbox, "begin ack envelope");
    assert_eq!(sink.active_transaction_count(), 1);

    // Act
    sink.stop_actor_for_tests();
    let active_transaction_count = sink.active_transaction_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(active_transaction_count, 0);
}

#[test]
fn should_route_kv_admin_snapshot_sync_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store.clone(), router, admin_read_model.clone());
    let mut actor = crate::domains::kv::KvActor::new(store);
    let begin_response = actor.handle(crate::domains::kv::KvMessage::Begin {
        scope: KvResourceScope::new(
            family,
            "acme".to_string(),
            "app".to_string(),
            "users".to_string(),
        ),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    });
    assert!(matches!(
        begin_response,
        crate::domains::kv::KvResponse::BeginOk { .. }
    ));
    sink.insert_actor_for_tests(session_id, actor);

    // Act
    sink.stop_actor_for_tests();
    sink.sync_admin_snapshot();
    let transactions = admin_read_model.kv_transactions(None);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(transactions.is_empty());
}

#[test]
fn should_route_kv_latency_snapshot_query_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(8));
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

    // Act
    sink.stop_actor_for_tests();
    let (reads, writes) = sink.latency_snapshots(&resource_key);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(reads.avg_ms.abs() < f64::EPSILON);
    assert!(reads.p95_ms.abs() < f64::EPSILON);
    assert!(writes.avg_ms.abs() < f64::EPSILON);
    assert!(writes.p95_ms.abs() < f64::EPSILON);
}

#[test]
fn should_route_kv_sync_write_options_mapping_through_managed_actor() {
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
    sink.stop_actor_for_tests();
    let mapped = sink.apply_write_options(message);

    // Assert
    assert!(!sink.is_actor_running());
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert_ne!(write_options, crate::domains::WritePolicy::CloudStrict);
        }
        _ => panic!("expected KV begin message"),
    }
}
