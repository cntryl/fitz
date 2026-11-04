use super::*;
use std::time::{Duration, Instant};

fn spawn_drop(
    store: Arc<StreamStore>,
) -> (
    std::thread::JoinHandle<()>,
    crossbeam_channel::Receiver<Result<(), String>>,
) {
    let (started_tx, started_rx) = crossbeam_channel::bounded(1);
    let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_millis(20);
        let scope = crate::domains::stream::StreamDropScope::parse(
            crate::runtime::routing::RouteFamily::new(1),
            "stream://acme/*",
        )
        .unwrap();
        started_tx.send(()).unwrap();
        let result = store.drop_scope_with_deadline(&scope, StreamWriteMode::Sync, Some(deadline));
        reply_tx.send(result).unwrap();
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("DROP worker started");
    (worker, reply_rx)
}

#[test]
fn should_bound_stream_drop_waiting_for_the_family_guard_registry() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
    store.ensure_layout_activation_for_family(1).unwrap();
    let held_registry = store.family_sequence_guards.lock();
    let (worker, replies) = spawn_drop(store.clone());

    // Act
    let initial = replies.recv_timeout(Duration::from_secs(1));
    let finished_with_registry_held = initial.is_ok();
    drop(held_registry);
    let outcome = initial.unwrap_or_else(|_| {
        replies
            .recv_timeout(Duration::from_secs(5))
            .expect("DROP worker exits after watchdog releases the registry")
    });
    worker.join().expect("DROP worker exits");

    // Assert
    assert!(
        finished_with_registry_held,
        "DROP must enforce its deadline before obtaining the family guard registry"
    );
    assert!(outcome.unwrap_err().contains("deadline"));
}

#[test]
fn should_bound_stream_drop_waiting_for_cold_family_recovery() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
    let guard = store.family_sequence_guard(1);
    let held_family = guard.lock();
    let (worker, replies) = spawn_drop(store.clone());

    // Act
    let initial = replies.recv_timeout(Duration::from_secs(1));
    let finished_with_family_held = initial.is_ok();
    drop(held_family);
    let outcome = initial.unwrap_or_else(|_| {
        replies
            .recv_timeout(Duration::from_secs(5))
            .expect("DROP worker exits after watchdog releases the family")
    });
    worker.join().expect("DROP worker exits");

    // Assert
    assert!(
        finished_with_family_held,
        "DROP must enforce its deadline during cold family recovery"
    );
    assert!(outcome.unwrap_err().contains("deadline"));
}

#[test]
fn should_bound_stream_drop_waiting_for_the_recovered_family_registry() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
    store.ensure_layout_activation_for_family(1).unwrap();
    let held_registry = store.recovered_global_families.lock();
    let (worker, replies) = spawn_drop(store.clone());

    // Act
    let initial = replies.recv_timeout(Duration::from_secs(1));
    let finished_with_registry_held = initial.is_ok();
    drop(held_registry);
    let outcome = initial.unwrap_or_else(|_| {
        replies
            .recv_timeout(Duration::from_secs(5))
            .expect("DROP worker exits after watchdog releases the recovered registry")
    });
    worker.join().expect("DROP worker exits");

    // Assert
    assert!(
        finished_with_registry_held,
        "DROP must enforce its deadline before obtaining the recovered family registry"
    );
    assert!(outcome.unwrap_err().contains("deadline"));
}
