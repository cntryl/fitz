use super::*;
use crate::domains::kv::{KvActor, KvDropScope};
use crate::runtime::routing::RouteFamily;

#[test]
fn should_preserve_scope_when_drop_admission_fails() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let data_key = KvActor::encode_scoped_key(
        &KvActor::realm_resource_prefix("acme", "area", "resource"),
        b"key",
    );
    let metadata_key = KvActor::inventory_metadata_key("acme", "area", "resource");
    let mut tx = store.begin(1, TxMode::ReadWrite).unwrap();
    tx.put(data_key.clone(), b"value".to_vec()).unwrap();
    tx.put(metadata_key.clone(), b"inventory".to_vec()).unwrap();
    tx.commit(WritePolicy::Sync).unwrap();
    let scope = KvDropScope::parse(RouteFamily::new(1), "kv://acme/*").unwrap();
    let _hook = write_admission::inject_admission(Box::new(|_, _| Ok(false)));

    // Act
    let result = store.drop_scope(&scope, WritePolicy::Sync);

    // Assert
    assert!(matches!(result, Err(KvError::BackendUnavailable(_))));
    let tx = store.begin(1, TxMode::ReadOnly).unwrap();
    assert_eq!(
        tx.get(&data_key).unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(
        tx.get(&metadata_key).unwrap(),
        Some(Bytes::from_static(b"inventory"))
    );
    drop(tx);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_submit_drop_as_one_atomic_storage_commit() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let store = store.with_commit_observer(observed.clone());
    let scope = KvDropScope::parse(RouteFamily::new(1), "kv://acme/area/*").unwrap();

    // Act
    let result = store.drop_scope(&scope, WritePolicy::Sync);

    // Assert
    assert!(result.is_ok());
    assert_eq!(observed.lock().len(), 1);
    assert_eq!(observed.lock()[0], cntryl_midge::WriteOptions::sync());
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}
