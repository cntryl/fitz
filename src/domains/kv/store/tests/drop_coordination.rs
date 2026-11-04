//! DROP coordination shares one deadline with write admission.

use super::*;
use crate::domains::kv::{KvActor, KvDropScope};
use crate::runtime::routing::RouteFamily;
use std::time::{Duration, Instant};

fn seed_scope(store: &KvStore) -> (Vec<u8>, Vec<u8>) {
    let data_key = KvActor::encode_scoped_key(
        &KvActor::realm_resource_prefix("acme", "area", "resource"),
        b"key",
    );
    let metadata_key = KvActor::inventory_metadata_key("acme", "area", "resource");
    let mut tx = store.begin(1, TxMode::ReadWrite).unwrap();
    tx.put(data_key.clone(), b"value".to_vec()).unwrap();
    tx.put(metadata_key.clone(), b"inventory".to_vec()).unwrap();
    tx.commit(WritePolicy::Sync).unwrap();
    (data_key, metadata_key)
}

fn assert_scope_unchanged(store: &KvStore, data_key: &[u8], metadata_key: &[u8]) {
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    assert_eq!(
        tx.get(data_key).unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(
        tx.get(metadata_key).unwrap(),
        Some(Bytes::from_static(b"inventory")),
    );
}

#[test]
fn should_preserve_scope_when_drop_cannot_acquire_its_family_gate_before_deadline() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let (data_key, metadata_key) = seed_scope(&store);
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let store = store.with_commit_observer(observed.clone());
    let gate = store
        .clone()
        .inventory_gate_for(1, Instant::now() + Duration::from_secs(5))
        .unwrap();
    let held = gate.lock();
    let scope = KvDropScope::parse(RouteFamily::new(1), "kv://acme/*").unwrap();

    // Act
    let result = store.drop_scope_until(
        &scope,
        WritePolicy::Sync,
        Instant::now() + Duration::from_millis(25),
    );
    drop(held);

    // Assert
    assert!(matches!(result, Err(KvError::BackendUnavailable(_))));
    assert!(observed.lock().is_empty());
    assert_scope_unchanged(&store, &data_key, &metadata_key);
    drop(gate);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_preserve_scope_when_drop_deadline_has_already_expired() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let (data_key, metadata_key) = seed_scope(&store);
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let store = store.with_commit_observer(observed.clone());
    let scope = KvDropScope::parse(RouteFamily::new(1), "kv://acme/*").unwrap();
    let _hook = write_admission::inject_admission(Box::new(|_, _| {
        panic!("an expired DROP must not reach storage admission")
    }));

    // Act
    let result = store.drop_scope_until(&scope, WritePolicy::Sync, Instant::now());

    // Assert
    assert!(matches!(result, Err(KvError::BackendUnavailable(_))));
    assert!(observed.lock().is_empty());
    assert_scope_unchanged(&store, &data_key, &metadata_key);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_reduce_drop_admission_budget_by_time_spent_waiting_for_its_family_gate() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let (data_key, metadata_key) = seed_scope(&store);
    let gate = store
        .inventory_gate_for(1, Instant::now() + Duration::from_secs(5))
        .unwrap();
    let held = gate.lock();
    let budget = Duration::from_secs(5);
    let gate_wait = Duration::from_millis(50);
    let deadline = Instant::now() + budget;
    let drop_store = store.clone();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (budget_tx, budget_rx) = crossbeam_channel::bounded(1);
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        let _hook = write_admission::inject_admission(Box::new(move |family, remaining| {
            assert_eq!(family, 1);
            budget_tx.send(remaining).unwrap();
            Ok(false)
        }));
        let scope = KvDropScope::parse(RouteFamily::new(1), "kv://acme/*").unwrap();
        entered_tx.send(()).unwrap();
        let result = drop_store.drop_scope_until(&scope, WritePolicy::Sync, deadline);
        result_tx.send(result).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    // Act
    let before_release = result_rx.recv_timeout(gate_wait);
    let waited_for_gate = before_release.is_err();
    drop(held);
    let result =
        before_release.unwrap_or_else(|_| result_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    worker.join().unwrap();
    let remaining = budget_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    // Assert
    assert!(waited_for_gate);
    assert!(remaining > Duration::ZERO);
    assert!(remaining <= budget.saturating_sub(gate_wait));
    assert!(matches!(result, Err(KvError::BackendUnavailable(_))));
    assert_scope_unchanged(&store, &data_key, &metadata_key);
    drop(gate);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}
