use crate::domains::queue::actor::recovery_store::{QueueStore, QueueTransactionMode};
use std::sync::Arc;
use std::time::Duration;

fn storage_under_pressure() -> (Arc<cntryl_midge::Engine>, u32, u32) {
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .with_memtable_size_limit(64 * 1024)
                .build()
                .expect("memory options"),
        )
        .expect("memory engine"),
    );
    let queue = engine.create_column_family("queue").expect("Queue family");
    let pressure = engine
        .create_column_family("pressure")
        .expect("pressure family");
    let mut fill = engine
        .begin_tx(pressure.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("pressure transaction");
    fill.put(b"pressure".to_vec(), vec![7; 256 * 1024], None)
        .expect("stage pressure");
    fill.commit(cntryl_midge::WriteOptions::buffered())
        .expect("fill memory");
    let mut prime = engine
        .begin_tx(queue.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("prime Queue pressure hint");
    prime
        .put(b"seed".to_vec(), b"seed".to_vec(), None)
        .expect("stage seed");
    prime
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("prime Queue hint");
    (engine, queue.id(), pressure.id())
}

#[test]
fn should_wait_for_queue_storage_pressure_to_clear_before_committing() {
    // Arrange
    let (engine, queue, pressure) = storage_under_pressure();
    let store = QueueStore::new(engine.clone());
    let mut transaction = store
        .begin(queue, QueueTransactionMode::ReadWrite)
        .expect("Queue transaction");
    transaction
        .put(b"pending".to_vec(), b"work".to_vec(), None)
        .expect("stage Queue work");
    let (started_tx, started_rx) = crossbeam_channel::bounded(1);
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
    let commit = std::thread::spawn(move || {
        started_tx.send(()).expect("report commit start");
        result_tx
            .send(transaction.commit())
            .expect("report commit result");
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("commit starts");

    // Act
    let early = result_rx.recv_timeout(Duration::from_millis(200));
    let returned_early = early.is_ok();
    engine
        .drop_column_family_discarding_unflushed(pressure)
        .expect("release test pressure");
    // Midge 0.3.1 refreshes its cached total on write, rather than CF drop.
    let refreshed = engine
        .create_column_family("refresh")
        .expect("refresh family");
    let mut refresh = engine
        .begin_tx(refreshed.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("refresh cached memory accounting");
    refresh
        .put(b"refresh".to_vec(), b"refresh".to_vec(), None)
        .expect("stage accounting refresh");
    refresh
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("refresh memory accounting");
    let outcome = early.unwrap_or_else(|_| {
        result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("commit after relief")
    });
    commit.join().expect("join commit");

    // Assert
    assert!(
        !returned_early,
        "Queue should wait while storage admission is stalled"
    );
    assert!(outcome.is_ok(), "Queue commit failed: {outcome:?}");
    let read = store
        .begin(queue, QueueTransactionMode::ReadOnly)
        .expect("read committed work");
    assert_eq!(
        read.get(b"pending").expect("read work").as_deref(),
        Some(b"work".as_slice())
    );
}

#[test]
fn should_leave_queue_storage_unchanged_when_pressure_wait_expires() {
    // Arrange
    let (engine, queue, _) = storage_under_pressure();
    let store = QueueStore::new(engine);
    let mut transaction = store
        .begin(queue, QueueTransactionMode::ReadWrite)
        .expect("Queue transaction");
    transaction
        .put(b"pending".to_vec(), b"work".to_vec(), None)
        .expect("stage Queue work");

    // Act
    let result = transaction.commit_with_pressure_wait(Duration::from_millis(20));

    // Assert
    assert!(result
        .expect_err("pressure remains blocked")
        .to_string()
        .contains("storage admission"));
    let read = store
        .begin(queue, QueueTransactionMode::ReadOnly)
        .expect("read Queue state");
    assert!(read.get(b"pending").expect("read pending work").is_none());
}

#[test]
fn should_commit_read_only_queue_work_without_waiting_for_storage_pressure() {
    // Arrange
    let (engine, queue, _) = storage_under_pressure();
    let store = QueueStore::new(engine);
    let transaction = store
        .begin(queue, QueueTransactionMode::ReadOnly)
        .expect("read-only Queue transaction");

    // Act
    let result = transaction.commit_with_pressure_wait(Duration::ZERO);

    // Assert
    assert!(result.is_ok(), "read-only commit failed: {result:?}");
}
