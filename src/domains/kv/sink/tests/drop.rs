//! DROP deletes a selected namespace without enumerating its committed rows.

use super::*;
use crate::domains::kv::{KvActor, TxMode};
use crate::domains::WritePolicy;

const RESOURCE: &str = "kv://acme/jobs/orders";
const RAW_KEYS: &[&[u8]] = &[b"", b"key", b"\xff", b"\xff\xff\0"];

struct DropFixture {
    sink: KvDomain,
    engine: Arc<cntryl_midge::Engine>,
}

impl DropFixture {
    fn new() -> Self {
        Self::with_engine(crate::testkit::midge::create_in_memory_test_engine_with_cfs(&[1, 2]))
    }

    fn with_engine(engine: Arc<cntryl_midge::Engine>) -> Self {
        let sink = KvDomain::new(
            engine.clone(),
            Arc::new(Router::new()),
            crate::control::admin::read_model::AdminReadModel::new(),
        );
        Self { sink, engine }
    }

    fn request(&self, family: u32, session_id: u64, message: u16, payload: Bytes) -> FrameContext {
        let family = RouteFamily::new(family);
        let address =
            RouteAddress::new(family, Route::new(format!("inbox://session/{session_id}")));
        let mailbox = Arc::new(Mailbox::new(16));
        self.sink
            .config
            .router
            .register(address.clone(), mailbox.clone());
        self.sink
            .deliver(Envelope::from_route(
                address,
                RouteAddress::new(family, Route::new("kv://inbound")),
                FrameContext::new(
                    session_id,
                    ChannelId::Pub,
                    MessageType::new(message),
                    payload,
                    family,
                ),
            ))
            .unwrap();
        receive_frame(&mailbox, "KV response")
    }

    fn drop_scope(
        &self,
        family: u32,
        session_id: u64,
        route: &str,
        persistence: u8,
    ) -> FrameContext {
        self.request(family, session_id, 112, encode_kv_begin(route, persistence))
    }

    fn begin(&self, family: u32, session_id: u64, route: &str, mode: u8) -> u64 {
        let frame = self.request(family, session_id, 100, encode_kv_begin(route, mode));
        assert_eq!(frame.payload[0], 0, "{frame:?}");
        decode_kv_begin_tx_id(&frame.payload)
    }

    fn seed(&self, family: u32, realm: &str, area: &str, resource: &str) {
        let prefix = KvActor::realm_resource_prefix(realm, area, resource);
        let mut tx = self
            .sink
            .config
            .store
            .begin(family, TxMode::ReadWrite)
            .unwrap();
        for key in RAW_KEYS {
            tx.put(KvActor::encode_scoped_key(&prefix, key), b"old".to_vec())
                .unwrap();
        }
        tx.put(
            KvActor::inventory_metadata_key(realm, area, resource),
            b"estimate".to_vec(),
        )
        .unwrap();
        tx.commit(WritePolicy::Sync).unwrap();
    }

    fn resource_exists(&self, family: u32, realm: &str, area: &str, resource: &str) -> bool {
        let tx = self
            .sink
            .config
            .store
            .begin(family, TxMode::ReadOnly)
            .unwrap();
        let prefix = KvActor::realm_resource_prefix(realm, area, resource);
        let rows = RAW_KEYS
            .iter()
            .map(|key| tx.get(&KvActor::encode_scoped_key(&prefix, key)).unwrap());
        let metadata = tx
            .get(&KvActor::inventory_metadata_key(realm, area, resource))
            .unwrap();
        let present = metadata.is_some();
        for row in rows {
            assert_eq!(row.is_some(), present, "data and inventory disagree");
        }
        present
    }
}

fn assert_scope_isolation(route: &str, sibling_resource: bool, sibling_area: bool) {
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    fixture.seed(1, "acme", "jobs", "orders-long");
    fixture.seed(1, "acme", "jobs-long", "orders");
    fixture.seed(1, "acme-long", "jobs", "orders");
    fixture.seed(2, "acme", "jobs", "orders");
    let mut tx = fixture
        .sink
        .config
        .store
        .begin(1, TxMode::ReadWrite)
        .unwrap();
    let other_domain = crate::utils::storage_key::domain_prefix(
        "acme",
        crate::utils::storage_key::DomainKeyspace::Stream,
    );
    tx.put(other_domain.clone(), b"stream sentinel".to_vec())
        .unwrap();
    tx.commit(WritePolicy::Sync).unwrap();

    let response = fixture.drop_scope(1, 7, route, 1);

    assert_eq!(response.payload.as_ref(), &[0]);
    assert!(!fixture.resource_exists(1, "acme", "jobs", "orders"));
    assert_eq!(
        fixture.resource_exists(1, "acme", "jobs", "orders-long"),
        sibling_resource
    );
    assert_eq!(
        fixture.resource_exists(1, "acme", "jobs-long", "orders"),
        sibling_area
    );
    assert!(fixture.resource_exists(1, "acme-long", "jobs", "orders"));
    assert!(fixture.resource_exists(2, "acme", "jobs", "orders"));
    assert_eq!(
        fixture
            .sink
            .config
            .store
            .begin(1, TxMode::ReadOnly)
            .unwrap()
            .get(&other_domain)
            .unwrap(),
        Some(Bytes::from_static(b"stream sentinel"))
    );
}

#[test]
fn should_drop_one_resource_without_touching_neighboring_namespaces() {
    // Arrange
    let route = RESOURCE;
    // Act
    assert_scope_isolation(route, true, true);
    // Assert
    // The shared scope assertion checks data, metadata, raw keys and isolation.
}

#[test]
fn should_drop_one_area_without_touching_neighboring_namespaces() {
    // Arrange
    let route = "kv://acme/jobs/*";
    // Act
    assert_scope_isolation(route, false, true);
    // Assert
    // The shared scope assertion checks data, metadata, raw keys and isolation.
}

#[test]
fn should_drop_one_realm_without_touching_neighboring_namespaces() {
    // Arrange
    let route = "kv://acme/*";
    // Act
    assert_scope_isolation(route, false, false);
    // Assert
    // The shared scope assertion checks data, metadata, raw keys and isolation.
}

fn assert_writer_conflict(route: &str, drop_session: u64) {
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    fixture.begin(1, 7, RESOURCE, 1);
    let response = fixture.drop_scope(1, drop_session, route, 1);
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_ISOLATION_CONFLICT
    );
    assert!(fixture.resource_exists(1, "acme", "jobs", "orders"));
    assert_eq!(fixture.sink.active_transaction_count(), 1);
}

#[test]
fn should_reject_resource_drop_while_another_session_holds_a_write_transaction() {
    // Arrange
    let route = RESOURCE;
    // Act
    assert_writer_conflict(route, 8);
    // Assert
    // The conflicting transaction and committed data remain intact.
}

#[test]
fn should_reject_area_drop_while_a_child_resource_has_a_write_transaction() {
    // Arrange
    let route = "kv://acme/jobs/*";
    // Act
    assert_writer_conflict(route, 8);
    // Assert
    // The conflicting transaction and committed data remain intact.
}

#[test]
fn should_reject_realm_drop_while_a_child_resource_has_a_write_transaction() {
    // Arrange
    let route = "kv://acme/*";
    // Act
    assert_writer_conflict(route, 8);
    // Assert
    // The conflicting transaction and committed data remain intact.
}

#[test]
fn should_reject_drop_while_the_same_session_holds_an_overlapping_write_transaction() {
    // Arrange
    let route = "kv://acme/*";
    // Act
    assert_writer_conflict(route, 7);
    // Assert
    // Session ownership does not exempt an overlapping transaction.
}

#[test]
fn should_allow_drop_with_write_transactions_outside_its_scope() {
    // Arrange
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    fixture.begin(1, 7, "kv://acme/jobs-long/orders", 1);
    fixture.begin(2, 9, RESOURCE, 1);

    // Act
    let response = fixture.drop_scope(1, 8, "kv://acme/jobs/*", 1);

    // Assert
    assert_eq!(response.payload.as_ref(), &[0]);
    assert!(!fixture.resource_exists(1, "acme", "jobs", "orders"));
    assert_eq!(fixture.sink.active_transaction_count(), 2);
}

#[test]
fn should_preserve_an_open_read_snapshot_when_drop_removes_current_values() {
    // Arrange
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    let tx_id = fixture.begin(1, 7, RESOURCE, 0);
    let mut get = crate::protocol::payload_codec::PayloadEncoder::new();
    get.put_u64(tx_id);
    get.put_string(RESOURCE);
    get.put_bytes(b"key");

    // Act
    let dropped = fixture.drop_scope(1, 8, RESOURCE, 1);
    let snapshot = fixture.request(1, 7, 103, Bytes::from(get.finish()));

    // Assert
    assert_eq!(dropped.payload.as_ref(), &[0]);
    assert_eq!(&snapshot.payload[..2], &[0, 1]);
    assert_eq!(&snapshot.payload[6..], b"old");
    assert!(!fixture.resource_exists(1, "acme", "jobs", "orders"));
}

#[test]
fn should_scan_the_original_read_snapshot_after_drop() {
    // Arrange
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    let tx_id = fixture.begin(1, 7, RESOURCE, 0);
    let mut scan = crate::protocol::payload_codec::PayloadEncoder::new();
    scan.put_u64(tx_id);
    scan.put_string(RESOURCE);
    scan.put_u8(0);
    scan.put_u8(0);
    scan.put_u8(0);
    scan.put_u8(0);
    assert_eq!(fixture.drop_scope(1, 8, RESOURCE, 1).payload.as_ref(), &[0]);

    // Act
    let response = fixture.request(1, 7, 108, Bytes::from(scan.finish()));

    // Assert
    let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(&response.payload);
    assert_eq!(decoder.get_u8().unwrap(), 0);
    assert_eq!(
        decoder.get_u32().unwrap(),
        u32::try_from(RAW_KEYS.len()).unwrap()
    );
    for expected_key in RAW_KEYS {
        assert_eq!(decoder.get_bytes().unwrap().as_ref(), *expected_key);
        assert_eq!(decoder.get_bytes().unwrap().as_ref(), b"old");
    }
    assert_eq!(decoder.get_u8().unwrap(), 0);
    assert!(decoder.is_complete());
    assert!(!fixture.resource_exists(1, "acme", "jobs", "orders"));
}

#[test]
fn should_succeed_when_dropping_an_already_empty_scope() {
    // Arrange
    let fixture = DropFixture::new();
    assert_eq!(
        fixture.drop_scope(1, 7, "kv://acme/*", 1).payload.as_ref(),
        &[0]
    );

    // Act
    let response = fixture.drop_scope(1, 7, "kv://acme/*", 1);

    // Assert
    assert_eq!(response.payload.as_ref(), &[0]);
}

#[test]
fn should_allow_a_new_transaction_to_recreate_a_dropped_resource() {
    // Arrange
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    assert_eq!(fixture.drop_scope(1, 7, RESOURCE, 1).payload.as_ref(), &[0]);

    // Act
    let tx_id = fixture.begin(1, 7, RESOURCE, 1);
    let put = fixture.request(
        1,
        7,
        104,
        encode_kv_put(tx_id, RESOURCE, b"new", b"replacement"),
    );
    let committed = fixture.request(1, 7, 101, encode_kv_commit(tx_id, RESOURCE, 1));
    let replacement = fixture
        .sink
        .admin_get_committed_value(RouteFamily::new(1), "acme", "jobs", "orders", b"new")
        .unwrap();
    let old = fixture
        .sink
        .admin_get_committed_value(RouteFamily::new(1), "acme", "jobs", "orders", b"key")
        .unwrap();

    // Assert
    assert_eq!(put.payload.as_ref(), &[0]);
    assert_eq!(committed.payload.as_ref(), &[0]);
    assert_eq!(replacement, Some(b"replacement".to_vec()));
    assert!(old.is_none());
}

#[test]
fn should_discard_deferred_inventory_repairs_for_a_dropped_scope() {
    // Arrange
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    fixture
        .sink
        .run_on_family_for_tests(RouteFamily::new(1), |runtime| {
            let scope = KvResourceScope::new(RouteFamily::new(1), "acme", "jobs", "orders");
            runtime.core.pending_inventory_repairs.insert(
                KvResourceLockKey::new(1, "acme", "jobs", "orders"),
                (1, scope),
            );
            runtime.core.next_inventory_repair_at = Some(Instant::now() + Duration::from_secs(60));
        });

    // Act
    let response = fixture.drop_scope(1, 7, "kv://acme/*", 1);
    fixture
        .sink
        .run_on_family_for_tests(RouteFamily::new(1), |runtime| {
            runtime.core.next_inventory_repair_at = None;
            runtime.retry_inventory_repairs();
        });

    // Assert
    assert_eq!(response.payload.as_ref(), &[0]);
    assert!(!fixture.resource_exists(1, "acme", "jobs", "orders"));
}

#[test]
fn should_keep_dropped_inventory_absent_when_a_late_repair_is_retried() {
    // Arrange
    let fixture = DropFixture::new();
    fixture.seed(1, "acme", "jobs", "orders");
    assert_eq!(fixture.drop_scope(1, 7, RESOURCE, 1).payload.as_ref(), &[0]);
    fixture
        .sink
        .run_on_family_for_tests(RouteFamily::new(1), |runtime| {
            let scope = KvResourceScope::new(RouteFamily::new(1), "acme", "jobs", "orders");
            runtime.core.pending_inventory_repairs.insert(
                KvResourceLockKey::new(1, "acme", "jobs", "orders"),
                (1, scope),
            );
        });

    // Act
    fixture
        .sink
        .run_on_family_for_tests(RouteFamily::new(1), |runtime| {
            runtime.retry_inventory_repairs();
        });

    // Assert
    assert!(!fixture.resource_exists(1, "acme", "jobs", "orders"));
}

#[test]
fn should_reject_drop_without_expiring_its_callers_idle_write_transaction() {
    // Arrange
    let mut fixture = DropFixture::new();
    fixture.sink = fixture.sink.with_idle_transaction_ttl(Duration::ZERO);
    fixture.begin(1, 7, RESOURCE, 1);

    // Act
    let response = fixture.drop_scope(1, 7, "kv://acme/*", 1);

    // Assert
    assert_eq!(
        decode_error_code(&response.payload),
        error_codes::kv::ERR_ISOLATION_CONFLICT
    );
    assert_eq!(fixture.sink.active_transaction_count(), 1);
}

#[test]
fn should_persist_drop_across_a_local_engine_restart() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let options = || {
        cntryl_midge::OpenOptions::local(directory.path())
            .build()
            .unwrap()
    };
    let engine = Arc::new(cntryl_midge::Engine::open(options()).unwrap());
    engine.create_column_family("cf_1").unwrap();
    engine.create_column_family("cf_2").unwrap();
    let fixture = DropFixture::with_engine(engine);
    fixture.seed(1, "acme", "jobs", "orders");
    fixture.seed(1, "acme-long", "jobs", "orders");

    // Act
    let response = fixture.drop_scope(1, 7, "kv://acme/*", 1);
    fixture.sink.stop();
    let DropFixture { sink, engine } = fixture;
    drop(sink);
    crate::testkit::midge::shutdown_test_engine(engine);
    let reopened =
        DropFixture::with_engine(Arc::new(cntryl_midge::Engine::open(options()).unwrap()));

    // Assert
    assert_eq!(response.payload.as_ref(), &[0]);
    assert!(!reopened.resource_exists(1, "acme", "jobs", "orders"));
    assert!(reopened.resource_exists(1, "acme-long", "jobs", "orders"));
    reopened.sink.stop();
    let DropFixture { sink, engine } = reopened;
    drop(sink);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_recreate_a_resource_after_recovering_its_drop() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let options = || {
        cntryl_midge::OpenOptions::local(directory.path())
            .build()
            .unwrap()
    };
    let engine = Arc::new(cntryl_midge::Engine::open(options()).unwrap());
    engine.create_column_family("cf_1").unwrap();
    engine.create_column_family("cf_2").unwrap();
    let fixture = DropFixture::with_engine(engine);
    fixture.seed(1, "acme", "jobs", "orders");
    assert_eq!(fixture.drop_scope(1, 7, RESOURCE, 1).payload.as_ref(), &[0]);
    fixture.sink.stop();
    let DropFixture { sink, engine } = fixture;
    drop(sink);
    crate::testkit::midge::shutdown_test_engine(engine);
    let reopened =
        DropFixture::with_engine(Arc::new(cntryl_midge::Engine::open(options()).unwrap()));

    // Act
    let tx_id = reopened.begin(1, 7, RESOURCE, 1);
    let put = reopened.request(
        1,
        7,
        104,
        encode_kv_put(tx_id, RESOURCE, b"new", b"replacement"),
    );
    let committed = reopened.request(1, 7, 101, encode_kv_commit(tx_id, RESOURCE, 1));
    let rows = reopened
        .sink
        .admin_scan_committed_prefix(RouteFamily::new(1), "acme", "jobs", "orders", b"", 10)
        .unwrap();

    // Assert
    assert_eq!(put.payload.as_ref(), &[0]);
    assert_eq!(committed.payload.as_ref(), &[0]);
    assert!(!rows.has_more);
    assert_eq!(rows.items.len(), 1);
    assert_eq!(rows.items[0].key.as_slice(), b"new");
    assert_eq!(rows.items[0].value.as_slice(), b"replacement");
    reopened.sink.stop();
    let DropFixture { sink, engine } = reopened;
    drop(sink);
    crate::testkit::midge::shutdown_test_engine(engine);
}

fn observed_drop_policy(persistence: u8, cloud: bool) -> (Bytes, Vec<cntryl_midge::WriteOptions>) {
    let directory = tempfile::tempdir().unwrap();
    let options = if cloud {
        cntryl_midge::OpenOptions::cloud_simulated(directory.path(), "kv", "drop")
    } else {
        cntryl_midge::OpenOptions::local(directory.path())
    }
    .build()
    .unwrap();
    let shutdown_budget = options.runtime_response_timeout();
    let engine = Arc::new(cntryl_midge::Engine::open(options).unwrap());
    engine.create_column_family("cf_1").unwrap();
    engine.create_column_family("cf_2").unwrap();
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let store = crate::domains::kv::store::KvStore::new(engine.clone())
        .with_commit_observer(observed.clone());
    let mut sink = KvDomain::new(
        store,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    if cloud {
        sink = sink.with_write_policies(WritePolicy::CloudAsync, WritePolicy::CloudAsync);
    }
    let fixture = DropFixture { sink, engine };
    let response = fixture.drop_scope(1, 7, "kv://acme/*", persistence).payload;
    let options = observed.lock().clone();
    fixture.sink.stop();
    let DropFixture { sink, engine } = fixture;
    drop(sink);
    crate::testkit::midge::shutdown_test_engine_with_timeout(engine, shutdown_budget);
    (response, options)
}

#[test]
fn should_use_buffered_local_persistence_when_drop_selects_buffered() {
    // Arrange
    let persistence = 0;
    // Act
    let (response, options) = observed_drop_policy(persistence, false);
    // Assert
    assert_eq!(response.as_ref(), &[0]);
    assert_eq!(options, vec![cntryl_midge::WriteOptions::buffered()]);
}

#[test]
fn should_use_synced_local_persistence_when_drop_selects_sync() {
    // Arrange
    let persistence = 1;
    // Act
    let (response, options) = observed_drop_policy(persistence, false);
    // Assert
    assert_eq!(response.as_ref(), &[0]);
    assert_eq!(options, vec![cntryl_midge::WriteOptions::sync()]);
}

#[test]
fn should_use_cloud_async_persistence_when_drop_selects_buffered() {
    // Arrange
    let persistence = 0;
    // Act
    let (response, options) = observed_drop_policy(persistence, true);
    // Assert
    assert_eq!(response.as_ref(), &[0]);
    assert_eq!(options, vec![cntryl_midge::WriteOptions::cloud_async()]);
}

#[test]
fn should_keep_cloud_strict_persistence_when_drop_selects_sync_in_background_cloud_mode() {
    // Arrange
    let persistence = 1;
    // Act
    let (response, options) = observed_drop_policy(persistence, true);
    // Assert
    assert_eq!(response.as_ref(), &[0]);
    assert_eq!(options, vec![cntryl_midge::WriteOptions::cloud_strict()]);
}
