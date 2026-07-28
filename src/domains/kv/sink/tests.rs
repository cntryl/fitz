use super::*;
use crate::dispatch::protocol::error_codes;
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::tlv::MessageType;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::Mailbox;
use bytes::{BufMut, Bytes};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod correctness;

#[inline]
fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn encode_kv_begin(route: &str, mode: u8, durability: u8) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u8(mode);
    payload.put_u8(durability);
    Bytes::from(payload)
}

fn encode_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u64(tx_id);
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    payload.put_u32(usize_to_u32_saturating(key.len()));
    payload.put_slice(key);
    payload.put_u32(usize_to_u32_saturating(value.len()));
    payload.put_slice(value);
    Bytes::from(payload)
}

fn encode_kv_commit(tx_id: u64, route: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u64(tx_id);
    payload.put_u32(usize_to_u32_saturating(route.len()));
    payload.put_slice(route.as_bytes());
    Bytes::from(payload)
}

fn encode_kv_subscribe(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

fn encode_kv_unsubscribe(pattern: &str) -> Bytes {
    let mut payload = Vec::new();
    payload.put_u32(usize_to_u32_saturating(pattern.len()));
    payload.put_slice(pattern.as_bytes());
    Bytes::from(payload)
}

fn decode_kv_begin_tx_id(payload: &[u8]) -> u64 {
    let tx_id_bytes: [u8; 8] = payload[1..9]
        .try_into()
        .expect("begin response tx_id bytes");
    u64::from_be_bytes(tx_id_bytes)
}

fn decode_kv_subscription_id(payload: &[u8]) -> u64 {
    let subscription_id_bytes: [u8; 8] = payload[1..9]
        .try_into()
        .expect("subscribe response subscription_id bytes");
    u64::from_be_bytes(subscription_id_bytes)
}

fn decode_kv_watch_delivery(frame: &FrameContext) -> (u64, String, u64) {
    let subscription_id = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
    let route_len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap()) as usize;
    let route = String::from_utf8(frame.payload[12..12 + route_len].to_vec())
        .expect("KV watch route should be utf-8");
    let mutation_offset = 12 + route_len;
    let mutation_count = u64::from_be_bytes(
        frame.payload[mutation_offset..mutation_offset + 8]
            .try_into()
            .unwrap(),
    );
    (subscription_id, route, mutation_count)
}

fn decode_error_code(payload: &[u8]) -> u16 {
    error_codes::decode_error_body(payload)
        .expect("error payload")
        .0
}

fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

fn receive_envelope(mailbox: &Mailbox, label: &str) -> Envelope {
    mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
}

fn receive_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
    receive_envelope(mailbox, label)
        .into_payload::<FrameContext>()
        .unwrap_or_else(|| panic!("{label} frame"))
}

fn assert_no_envelope(mailbox: &Mailbox) {
    assert!(mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(50))
        .is_err());
}

fn wait_for_active_transaction_count(sink: &KvDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.active_transaction_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.active_transaction_count(), expected);
}

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
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::sync(),
    };

    // Act
    let mapped = sink.apply_write_options(message);

    // Assert
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert!(write_options.is_cloud_strict());
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
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    // Act
    let mapped = sink.apply_write_options(message);

    // Assert
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert!(write_options.is_cloud_async());
        }
        _ => panic!("expected KV begin message"),
    }
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
    sink.cleanup_session(session_id);
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
        route_family: family,
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
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
    sink.cleanup_session(session_id);
    sink.sync_admin_snapshot();
    let after_cleanup = admin_read_model.kv_transactions(None);

    // Assert
    assert!(!sink.is_actor_running());
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
        route_family: family,
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
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
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: crate::domains::kv::TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::sync(),
    };

    // Act
    sink.stop_actor_for_tests();
    let mapped = sink.apply_write_options(message);

    // Assert
    assert!(!sink.is_actor_running());
    match mapped {
        crate::domains::kv::KvMessage::Begin { write_options, .. } => {
            assert!(!write_options.is_cloud_strict());
        }
        _ => panic!("expected KV begin message"),
    }
}

#[test]
fn should_notify_kv_subscriber_given_committed_put() {
    // Arrange
    let family = RouteFamily::new(1);
    let watch_session_id = 7;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_mailbox = Arc::new(Mailbox::new(16));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    // Act
    sink.deliver(Envelope::from_route(
        watcher_address,
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe to KV route");
    let subscribe_frame = receive_frame(&watcher_mailbox, "subscribe ack envelope");
    let subscription_id = decode_kv_subscription_id(&subscribe_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&writer_mailbox, "put ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    let notify_frame = receive_frame(&watcher_mailbox, "KV notify envelope");
    assert_eq!(
        notify_frame.msg_type.as_u16(),
        crate::dispatch::protocol::kv::msg_type::NOTIFY
    );
    let (delivered_subscription_id, delivered_route, mutation_count) =
        decode_kv_watch_delivery(&notify_frame);
    assert_eq!(delivered_subscription_id, subscription_id);
    assert_eq!(delivered_route, kv_route);
    assert_eq!(mutation_count, 1);
}

#[test]
fn should_not_notify_kv_subscriber_given_empty_commit() {
    // Arrange
    let family = RouteFamily::new(1);
    let watch_session_id = 7;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_mailbox = Arc::new(Mailbox::new(16));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        watcher_address,
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe to KV route");
    let _ = receive_envelope(&watcher_mailbox, "subscribe ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit empty KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    assert_no_envelope(&watcher_mailbox);
}

#[test]
fn should_remove_kv_subscription_given_unsubscribe() {
    // Arrange
    let family = RouteFamily::new(1);
    let watch_session_id = 7;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let watcher_mailbox = Arc::new(Mailbox::new(16));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        watcher_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe to KV route");
    let _ = receive_envelope(&watcher_mailbox, "subscribe ack envelope");

    // Act
    sink.deliver(Envelope::from_route(
        watcher_address,
        kv_address.clone(),
        FrameContext::new(
            watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::UNSUBSCRIBE),
            encode_kv_unsubscribe(kv_route),
            family,
        ),
    ))
    .expect("unsubscribe from KV route");
    let _ = receive_envelope(&watcher_mailbox, "unsubscribe ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&writer_mailbox, "put ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    assert_no_envelope(&watcher_mailbox);
    assert!(sink.watch_actors_are_empty_for_tests());
}
