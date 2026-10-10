use super::*;

mod cross_domain;
use crate::domains::queue::actor::recovery_store::QueueStore;

fn local_engine(path: &std::path::Path) -> Arc<cntryl_midge::Engine> {
    local_engine_with_ttl(path, Duration::from_secs(1))
}

fn local_engine_with_ttl(path: &std::path::Path, ttl: Duration) -> Arc<cntryl_midge::Engine> {
    Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(path)
                .lease_ttl(ttl)
                .lease_clock_skew_tolerance(Duration::ZERO)
                .build()
                .unwrap(),
        )
        .unwrap(),
    )
}

#[test]
fn should_keep_local_queue_writes_out_of_wal() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let engine = local_engine(directory.path());
    let family = engine.create_column_family("queue").unwrap();
    let store = QueueStore::new(engine.clone());
    let initial = engine
        .metrics()
        .get_runtime_metrics_with_timeout(Duration::from_secs(2))
        .unwrap();
    let mut write = store
        .begin(
            family.id(),
            super::super::recovery_store::QueueTransactionMode::ReadWrite,
        )
        .unwrap();
    write
        .put(b"ready".to_vec(), b"body".to_vec(), None)
        .unwrap();

    // Act
    write.commit().unwrap();

    // Assert
    let after = engine
        .metrics()
        .get_runtime_metrics_with_timeout(Duration::from_secs(2))
        .unwrap();
    assert_eq!(after.wal_append_count, initial.wal_append_count);
    assert_eq!(after.wal_fsync_count, initial.wal_fsync_count);
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}

fn queue_key() -> QueueKey {
    QueueKey {
        family: RouteFamily::new(1),
        realm: "test".into(),
        area: "queue".into(),
        resource: "wal-window".into(),
    }
}

fn run_child(directory: &std::path::Path) {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "domains::queue::actor::tests::fast_flush::should_write_fast_queue_child_without_shutdown", "--nocapture"])
        .env("FITZ_QUEUE_FAST_WAL_CHILD_PATH", directory)
        .spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "Queue child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("Queue child exceeded its 30-second deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn should_preserve_fast_queue_backlog_and_ack_after_process_exit() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    run_child(directory.path());
    let (acked, remaining, published): (u64, u64, u64) =
        serde_json::from_slice(&std::fs::read(directory.path().join("receipt.json")).unwrap())
            .unwrap();
    // The child's 30-second deadline precedes the first renewal at 40 seconds.
    // This isolates persisted SST recovery from a lease-file mutation.
    let deadline = Instant::now() + Duration::from_secs(130);
    let engine = loop {
        let options = cntryl_midge::OpenOptions::local(directory.path().join("db"))
            .lease_ttl(Duration::from_secs(120))
            .lease_clock_skew_tolerance(Duration::ZERO)
            .build()
            .unwrap();
        match cntryl_midge::Engine::open(options) {
            Ok(engine) => break Arc::new(engine),
            Err(cntryl_midge::MidgeError::LeaseHeld(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("reopen after child exit failed: {error}"),
        }
    };
    let store = QueueStore::new(engine.clone());

    // Act
    let mut recovered = QueueActor::new(
        RouteFamily::new(1),
        queue_key(),
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let delivery = recovered.handle_receive_for_session(2, 30, Some(10));

    // Assert
    assert!(
        published > 0,
        "child persisted Queue state through an SST flush"
    );
    let QueueResponse::Received { messages } = delivery else {
        panic!("missing recovered backlog")
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id.as_u64(), remaining);
    assert_ne!(messages[0].id.as_u64(), acked);
    assert_eq!(messages[0].body, Bytes::from_static(b"remaining"));
    drop(recovered);
    crate::testkit::midge::shutdown_test_engine(engine);
}

#[test]
fn should_write_fast_queue_child_without_shutdown() {
    // Arrange
    let Some(directory) = std::env::var_os("FITZ_QUEUE_FAST_WAL_CHILD_PATH") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let engine = local_engine_with_ttl(&directory.join("db"), Duration::from_secs(120));
    engine.create_column_family("queue").unwrap();
    let store = QueueStore::new(engine.clone());
    let mut actor = QueueActor::new(
        RouteFamily::new(1),
        queue_key(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let QueueResponse::Sent { id: acked } = actor.handle_send(Bytes::from_static(b"acked"), None)
    else {
        panic!("enqueue failed")
    };
    let QueueResponse::Received { messages } = actor.handle_receive_for_session(1, 30, Some(1))
    else {
        panic!("reserve failed")
    };
    assert_eq!(
        actor.handle_ack_for_session(1, acked, messages[0].token),
        QueueResponse::Acked
    );
    let QueueResponse::Sent { id: remaining } =
        actor.handle_send(Bytes::from_static(b"remaining"), None)
    else {
        panic!("enqueue failed")
    };

    // Act
    assert!(store.flush_family(1).unwrap());

    // Assert
    let metrics = engine
        .metrics()
        .get_runtime_metrics_with_timeout(Duration::from_secs(2))
        .unwrap();
    std::fs::write(
        directory.join("receipt.json"),
        serde_json::to_vec(&(acked, remaining, metrics.sst_count)).unwrap(),
    )
    .unwrap();
    // Exit without running actor/engine destructors or orderly storage shutdown.
    std::process::exit(0);
}

#[test]
fn should_persist_best_effort_queue_state_through_background_flush() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let engine = local_engine(directory.path());
    let family = engine.create_column_family("queue").unwrap();
    let store = QueueStore::new(engine.clone());
    let mut write = store
        .begin(
            family.id(),
            super::super::recovery_store::QueueTransactionMode::ReadWrite,
        )
        .unwrap();
    write
        .put(b"ready".to_vec(), b"body".to_vec(), None)
        .unwrap();
    write.commit().unwrap();

    // Act
    let flushed = store.flush_family(family.id()).unwrap();

    // Assert
    assert!(flushed);
    assert_eq!(
        engine
            .metrics()
            .get_runtime_metrics_with_timeout(Duration::from_secs(2))
            .unwrap()
            .sst_count,
        1
    );
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);
}
