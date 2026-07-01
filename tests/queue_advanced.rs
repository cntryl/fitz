//! Queue domain advanced tests
//!
//! Tests multi-consumer scenarios, crash recovery with in-flight messages,
//! and atomicity guarantees across process restarts.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;

use fitz::domains::queue::{
    protocol::{QueueKey, QueueResponse},
    Clock, QueueActor,
};
use fitz::runtime::routing::RouteFamily;
use uuid::Uuid;

const TEST_SESSION_ID: u64 = 1;

fn unique_queue_key(resource_prefix: &str) -> QueueKey {
    QueueKey {
        family: RouteFamily::new(0),
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: format!("{}-{}", resource_prefix, Uuid::new_v4()),
    }
}

#[derive(Clone)]
struct TestClock {
    state: Arc<Mutex<TestClockState>>,
}

#[derive(Clone, Copy)]
struct TestClockState {
    instant: Instant,
    epoch_ms: u64,
}

impl TestClock {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TestClockState {
                instant: Instant::now(),
                epoch_ms: 1_700_000_000_000,
            })),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().expect("clock state lock");
        state.instant += duration;
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    }
}

impl Clock for TestClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().expect("clock state lock").instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().expect("clock state lock").epoch_ms
    }
}

/// Test that multiple consumers can reserve messages fairly (competing consumer semantics)
#[test]
fn should_distribute_messages_fairly_among_competing_consumers() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("competing");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    // Enqueue 30 messages (10 per consumer)
    let mut message_ids = Vec::new();
    for i in 0..30 {
        let body = Bytes::from(format!("task {i}"));
        match actor.handle_send(body, None) {
            QueueResponse::Sent { id } => message_ids.push(id),
            _ => panic!("Expected Enqueued"),
        }
    }

    // Act
    let mut reservation_batches = Vec::new();
    for _ in 0..3 {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(10)) {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 10);
                reservation_batches.push(messages);
            }
            _ => panic!("Expected Reserved"),
        }
    }

    // Assert
    assert_eq!(actor.ready_len(), 0);
    assert!(!actor.inflight.is_empty(), "Should have in-flight messages");

    // Verify no message was reserved twice
    let mut all_ids: Vec<_> = reservation_batches[0]
        .iter()
        .chain(reservation_batches[1].iter())
        .chain(reservation_batches[2].iter())
        .map(|m| m.id)
        .collect();
    all_ids.sort_by_key(fitz::domains::queue::MessageId::as_u64);
    all_ids.dedup();
    assert_eq!(all_ids.len(), 30, "Should have 30 unique message IDs");
}

/// Test crash recovery: in-flight messages are automatically redelivered
#[test]
fn should_redeliver_messages_after_crash() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("crash-recovery");

    // Pre-populate with messages
    let mut original_ids = Vec::new();
    let mut original_bodies = Vec::new();
    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );

        for i in 0..10 {
            let body = format!("task {i}");
            original_bodies.push(body.clone());
            match actor.handle_send(Bytes::from(body), None) {
                QueueResponse::Sent { id } => original_ids.push(id),
                _ => panic!("Expected Enqueued"),
            }
        }

        // Consumer reserves 5 messages (simulating in-flight state)
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(5)) {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 5);
            }
            _ => panic!("Expected Reserved"),
        }

        // At this point:
        // - 5 messages in inflight (in-memory, will be lost)
        // - 5 messages in ready queue (will be persisted)
        assert_eq!(actor.ready_len(), 5);
        assert_eq!(actor.inflight.len(), 5);
    }

    // Act
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(
        actor.ready_len(),
        10,
        "All 10 messages should be in ready queue after restart"
    );
    assert_eq!(
        actor.inflight.len(),
        0,
        "Inflight map should be empty after restart"
    );

    // Verify recovery can deliver all original messages
    let mut recovered_count = 0;
    let mut recovered_bodies = Vec::new();
    loop {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(5)) {
            QueueResponse::Received { messages } => {
                if messages.is_empty() {
                    break;
                }
                recovered_count += messages.len();
                recovered_bodies.extend(messages.into_iter().map(|message| {
                    String::from_utf8(message.body.to_vec()).expect("queue body should be utf-8")
                }));
            }
            QueueResponse::NotFound => {
                break;
            }
            _ => panic!("Expected Reserved or NotFound"),
        }
    }
    assert_eq!(recovered_count, 10, "Should recover all 10 messages");
    original_bodies.sort();
    recovered_bodies.sort();
    assert_eq!(
        recovered_bodies, original_bodies,
        "Recovered messages should retain their original bodies"
    );
}

/// Test crash recovery preserves FIFO order across multi-digit IDs.
#[test]
fn should_preserve_fifo_order_after_recovery() {
    // Arrange
    let reference_store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let reference_queue_key = unique_queue_key("live-order");
    let mut reference_actor = QueueActor::new(
        RouteFamily::new(0),
        reference_queue_key,
        reference_store,
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    for i in 0..12 {
        match reference_actor.handle_send(Bytes::from(format!("task {i}")), None) {
            QueueResponse::Sent { .. } => {}
            _ => panic!("Expected Enqueued"),
        }
    }

    let mut expected_order = Vec::new();
    loop {
        match reference_actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(4)) {
            QueueResponse::Received { messages } => {
                if messages.is_empty() {
                    break;
                }
                expected_order.extend(messages.into_iter().map(|message| {
                    String::from_utf8(message.body.to_vec()).expect("queue body should be utf-8")
                }));
            }
            _ => panic!("Expected Received response"),
        }
    }

    let recovery_store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let recovery_queue_key = unique_queue_key("crash-order");

    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            recovery_queue_key.clone(),
            recovery_store.clone(),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );

        for i in 0..12 {
            match actor.handle_send(Bytes::from(format!("task {i}")), None) {
                QueueResponse::Sent { .. } => {}
                _ => panic!("Expected Enqueued"),
            }
        }
    }

    // Act
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        recovery_queue_key,
        recovery_store,
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    let mut recovered_bodies = Vec::new();
    loop {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(4)) {
            QueueResponse::Received { messages } => {
                if messages.is_empty() {
                    break;
                }
                recovered_bodies.extend(messages.into_iter().map(|message| {
                    String::from_utf8(message.body.to_vec()).expect("queue body should be utf-8")
                }));
            }
            _ => panic!("Expected Received response"),
        }
    }

    // Assert
    assert_eq!(recovered_bodies, expected_order);
}

/// Test delayed message visibility survives crash (V-002: time semantics fix)
#[test]
fn should_preserve_delayed_visibility_across_restart() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("delayed-crash");

    // Enqueue messages with delay
    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );

        // Message 1: immediately visible
        actor.handle_send(Bytes::from("immediate"), None);

        // Message 2: delayed 1 hour
        actor.handle_send(Bytes::from("delayed_1h"), Some(3600));

        // Verify in-memory state
        assert_eq!(actor.ready_len(), 1, "Should have 1 ready message");
        // delayed field is private, verify through reserve behavior
    }

    // Act
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.ready_len(), 1, "Ready message should be recovered");
    // Note: delayed is private field, so verify behavior through reserve instead

    // Verify immediately visible
    match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].body, Bytes::from("immediate"));
        }
        _ => panic!("Expected to reserve the immediate message"),
    }
}

/// Test atomic batch enqueue: no ID collisions after crash (V-001: atomicity fix)
#[test]
fn should_prevent_id_collisions_across_crash() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("atomic-batch");
    let mut first_batch_ids = Vec::new();

    // Enqueue first batch
    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );

        for i in 0..10 {
            let body = Bytes::from(format!("batch1-{i}"));
            match actor.handle_send(body, None) {
                QueueResponse::Sent { id } => first_batch_ids.push(id.as_u64()),
                _ => panic!("Expected Enqueued"),
            }
        }

        // Verify sequential IDs
        assert_eq!(first_batch_ids[0], 1);
        assert_eq!(first_batch_ids[9], 10);
    }

    // Act
    let mut second_batch_ids = Vec::new();
    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );

        for i in 0..10 {
            let body = Bytes::from(format!("batch2-{i}"));
            match actor.handle_send(body, None) {
                QueueResponse::Sent { id } => second_batch_ids.push(id.as_u64()),
                _ => panic!("Expected Enqueued"),
            }
        }

        // Verify crash-safe monotonic allocation: no reuse, gaps allowed.
        assert!(second_batch_ids[0] > 10);
        assert_eq!(second_batch_ids.len(), 10);
        assert!(second_batch_ids
            .windows(2)
            .all(|pair| pair[1] == pair[0] + 1));
    }

    // Assert
    let mut all_ids = first_batch_ids.clone();
    all_ids.extend(&second_batch_ids);
    all_ids.sort_unstable();
    all_ids.dedup();
    assert_eq!(
        all_ids.len(),
        20,
        "Should have 20 unique IDs across crashes (no collisions)"
    );
}

/// Test lease expiration with redelivery (automatic)
#[test]
fn should_redeliver_message_on_lease_expiration() {
    // Arrange
    let clock = TestClock::new();
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("lease-expire");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        fitz::utils::idempotency::default_dedup_store(),
    );

    // Act
    actor.handle_send(Bytes::from("work"), None);
    let first = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            messages[0].clone()
        }
        _ => panic!("Expected Reserved"),
    };
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();
    let second = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            messages[0].clone()
        }
        _ => panic!("Expected redelivered message"),
    };

    // Assert
    assert_eq!(actor.inflight.len(), 1);
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(second.id, first.id);
    assert_eq!(second.body, first.body);
    assert_ne!(second.token, first.token);
    assert_eq!(second.attempts, 2);
}

/// Test dead letter queue (DLQ) for max attempts threshold
#[test]
fn should_dlq_message_after_max_attempts() {
    // Arrange
    let clock = TestClock::new();
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("crash-dlq");

    let msg_id = {
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            Some(3),
            fitz::utils::idempotency::default_dedup_store(),
        );

        let send_response = actor.handle_send(Bytes::from("task"), None);
        let QueueResponse::Sent { id: msg_id } = send_response else {
            panic!("Expected Sent response");
        };

        for expected_attempt in 1..=3 {
            match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
                QueueResponse::Received { messages } => {
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].attempts, expected_attempt);
                }
                _ => panic!("Expected Received response"),
            }

            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();
        }

        msg_id
    };

    // Act
    let mut recovered = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock),
        Some(3),
        fitz::utils::idempotency::default_dedup_store(),
    );
    let reserve_response = recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    match reserve_response {
        QueueResponse::NotFound => {}
        QueueResponse::Received { messages } => assert!(messages.is_empty()),
        _ => panic!("Expected empty queue after retained DLQ transition"),
    }

    let dead_letters = recovered.admin_dead_letters();
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].message_id, msg_id.as_u64());

    let replayed = recovered
        .replay_dead_letter(msg_id)
        .expect("replay retained dead letter after restart");
    assert!(replayed);

    match recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].body, Bytes::from("task"));
        }
        _ => panic!("Expected replayed DLQ message to become receivable"),
    }
}
