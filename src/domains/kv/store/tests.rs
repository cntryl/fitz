use super::*;
use std::sync::Arc;

fn local_store() -> (Arc<cntryl_midge::Engine>, KvStore, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    assert_eq!(engine.create_column_family("cf_1").unwrap().id(), 1);
    (engine.clone(), KvStore::new(engine), directory)
}

#[test]
fn should_refuse_submission_when_write_admission_exhausts_its_budget() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let mut transaction = store.begin(1, TxMode::ReadWrite).unwrap();
    transaction.put(b"key".to_vec(), b"value".to_vec()).unwrap();
    let _hook = write_admission::inject_admission(Box::new(|_, _| Ok(false)));
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .wal_append_count;

    // Act
    let result = transaction.commit(WritePolicy::Sync);

    // Assert
    assert!(matches!(result, Err(KvError::BackendUnavailable(_))));
    assert_eq!(
        engine
            .metrics()
            .get_runtime_metrics()
            .unwrap()
            .wal_append_count,
        before
    );
    assert_eq!(
        store
            .begin(1, TxMode::ReadOnly)
            .unwrap()
            .get(b"key")
            .unwrap(),
        None
    );
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_preserve_sync_policy_after_waiting_for_family_write_admission() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let mut transaction = store.begin(1, TxMode::ReadWrite).unwrap();
    transaction.put(b"key".to_vec(), b"value".to_vec()).unwrap();
    let admitted = std::rc::Rc::new(std::cell::Cell::new(false));
    let observed = admitted.clone();
    let _hook = write_admission::inject_admission(Box::new(move |family, budget| {
        assert_eq!(family, 1);
        assert_eq!(budget, std::time::Duration::from_secs(30));
        observed.set(true);
        Ok(true)
    }));
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .wal_fsync_count;

    // Act
    let result = transaction.commit(WritePolicy::Sync);

    // Assert
    assert!(result.is_ok());
    assert!(admitted.get());
    assert!(
        engine
            .metrics()
            .get_runtime_metrics()
            .unwrap()
            .wal_fsync_count
            > before
    );
    assert_eq!(
        store
            .begin(1, TxMode::ReadOnly)
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_preserve_original_snapshot_conflict_checks_after_admission_wait() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let mut initial = store.begin(1, TxMode::ReadWrite).unwrap();
    initial.put(b"key".to_vec(), b"old".to_vec()).unwrap();
    initial.commit(WritePolicy::Sync).unwrap();
    let mut transaction = store.begin(1, TxMode::ReadWrite).unwrap();
    transaction
        .inner
        .set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);
    transaction
        .inner
        .assert_value(b"key".to_vec(), Some(b"old".to_vec()))
        .unwrap();
    transaction.put(b"key".to_vec(), b"new".to_vec()).unwrap();
    let concurrent_store = store.clone();
    let _hook = write_admission::inject_admission(Box::new(move |family, _| {
        let mut concurrent = concurrent_store.begin(family, TxMode::ReadWrite).unwrap();
        concurrent
            .put(b"key".to_vec(), b"concurrent".to_vec())
            .unwrap();
        concurrent
            .inner
            .commit(cntryl_midge::WriteOptions::sync())?;
        Ok(true)
    }));

    // Act
    let result = transaction.commit(WritePolicy::Sync);

    // Assert
    assert!(matches!(result, Err(KvError::Conflict(_))));
    assert_eq!(
        store
            .begin(1, TxMode::ReadOnly)
            .unwrap()
            .get(b"key")
            .unwrap(),
        Some(Bytes::from_static(b"concurrent"))
    );
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_complete_read_only_transaction_without_waiting_for_write_admission() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let transaction = store.begin(1, TxMode::ReadOnly).unwrap();
    let _hook = write_admission::inject_admission(Box::new(|_, _| {
        panic!("read-only commit must not wait")
    }));

    // Act
    let result = transaction.commit(WritePolicy::Sync);

    // Assert
    assert!(result.is_ok());
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_refuse_submission_when_admission_reports_an_unknown_error() {
    // Arrange
    let (engine, store, _directory) = local_store();
    let mut transaction = store.begin(1, TxMode::ReadWrite).unwrap();
    transaction.put(b"key".to_vec(), b"value".to_vec()).unwrap();
    let _hook = write_admission::inject_admission(Box::new(|_, _| {
        Err(cntryl_midge::MidgeError::Timeout(
            "unknown admission outcome".into(),
        ))
    }));
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .wal_append_count;

    // Act
    let result = transaction.commit(WritePolicy::Sync);

    // Assert
    assert!(matches!(result, Err(KvError::BackendUnavailable(_))));
    assert_eq!(
        engine
            .metrics()
            .get_runtime_metrics()
            .unwrap()
            .wal_append_count,
        before
    );
    assert_eq!(
        store
            .begin(1, TxMode::ReadOnly)
            .unwrap()
            .get(b"key")
            .unwrap(),
        None
    );
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}
