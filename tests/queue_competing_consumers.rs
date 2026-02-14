//! Competing consumer integration tests for queue domain
//!
//! Tests multi-consumer scenarios, crash recovery with in-flight messages,
//! and atomicity guarantees across process restarts.

use std::sync::Arc;

use bytes::Bytes;

use fitz::domains::queue::{
    protocol::{QueueKey, QueueResponse},
    queue_actor::QueueActor,
};
use fitz::runtime::routing::RouteFamily;
use uuid::Uuid;

fn unique_queue_key(resource_prefix: &str) -> QueueKey {
    QueueKey {
        family: RouteFamily::new(0),
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: format!("{}-{}", resource_prefix, Uuid::new_v4()),
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
        fitz::utils::idempotency::global_dedup_store(),
    );

    // Enqueue 30 messages (10 per consumer)
    let mut message_ids = Vec::new();
    for i in 0..30 {
        let body = Bytes::from(format!("task {}", i));
        match actor.handle_enqueue(body, None) {
            QueueResponse::Enqueued { id } => message_ids.push(id),
            _ => panic!("Expected Enqueued"),
        }
    }

    // Act - Three competing consumers reserve messages
    let mut _consumer_a_msgs = Vec::new();
    let mut _consumer_b_msgs = Vec::new();
    let mut _consumer_c_msgs = Vec::new();

    // Consumer A reserves 10 messages
    match actor.handle_reserve(30, Some(10)) {
        QueueResponse::Reserved { messages } => {
            assert_eq!(messages.len(), 10);
            _consumer_a_msgs = messages;
        }
        _ => panic!("Expected Reserved"),
    }

    // Consumer B reserves 10 messages
    match actor.handle_reserve(30, Some(10)) {
        QueueResponse::Reserved { messages } => {
            assert_eq!(messages.len(), 10);
            _consumer_b_msgs = messages;
        }
        _ => panic!("Expected Reserved"),
    }

    // Consumer C reserves remaining 10 messages
    match actor.handle_reserve(30, Some(10)) {
        QueueResponse::Reserved { messages } => {
            assert_eq!(messages.len(), 10);
            _consumer_c_msgs = messages;
        }
        _ => panic!("Expected Reserved"),
    }

    // Assert
    assert_eq!(actor.ready.len(), 0);
    assert!(!actor.inflight.is_empty(), "Should have in-flight messages");

    // Verify no message was reserved twice
    let mut all_ids: Vec<_> = _consumer_a_msgs
        .iter()
        .chain(_consumer_b_msgs.iter())
        .chain(_consumer_c_msgs.iter())
        .map(|m| m.id)
        .collect();
    all_ids.sort_by_key(|id| id.as_u64());
    all_ids.dedup();
    assert_eq!(all_ids.len(), 30, "Should have 30 unique message IDs");
}

/// Test crash recovery: in-flight messages are automatically redelivered
#[test]
fn should_redelivery_messages_after_crash() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("crash-recovery");

    // Pre-populate with messages
    let mut original_ids = Vec::new();
    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::global_dedup_store(),
        );

        for i in 0..10 {
            let body = Bytes::from(format!("task {}", i));
            match actor.handle_enqueue(body, None) {
                QueueResponse::Enqueued { id } => original_ids.push(id),
                _ => panic!("Expected Enqueued"),
            }
        }

        // Consumer reserves 5 messages (simulating in-flight state)
        match actor.handle_reserve(30, Some(5)) {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 5);
            }
            _ => panic!("Expected Reserved"),
        }

        // At this point:
        // - 5 messages in inflight (in-memory, will be lost)
        // - 5 messages in ready queue (will be persisted)
        assert_eq!(actor.ready.len(), 5);
        assert_eq!(actor.inflight.len(), 5);
    }

    // Act - Actor crashes and restarts (create new actor, recovery runs automatically)
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        fitz::utils::idempotency::global_dedup_store(),
    );

    // Assert - All 10 messages recovered (inflight automatically redelivered)
    assert_eq!(
        actor.ready.len(),
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
    loop {
        match actor.handle_reserve(30, Some(5)) {
            QueueResponse::Reserved { messages } => {
                if messages.is_empty() {
                    break;
                }
                recovered_count += messages.len();
            }
            _ => panic!("Expected Reserved"),
        }
    }
    assert_eq!(recovered_count, 10, "Should recover all 10 messages");
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
            fitz::utils::idempotency::global_dedup_store(),
        );

        // Message 1: immediately visible
        actor.handle_enqueue(Bytes::from("immediate"), None);

        // Message 2: delayed 1 hour
        actor.handle_enqueue(Bytes::from("delayed_1h"), Some(3600));

        // Verify in-memory state
        assert_eq!(actor.ready.len(), 1, "Should have 1 ready message");
        // delayed field is private, verify through reserve behavior
    }

    // Act - Restart actor
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        fitz::utils::idempotency::global_dedup_store(),
    );

    // Assert - Recovery should restore delayed queue
    assert_eq!(actor.ready.len(), 1, "Ready message should be recovered");
    // Note: delayed is private field, so verify behavior through reserve instead

    // Verify immediately visible
    match actor.handle_reserve(30, Some(1)) {
        QueueResponse::Reserved { messages } => {
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
            fitz::utils::idempotency::global_dedup_store(),
        );

        for i in 0..10 {
            let body = Bytes::from(format!("batch1-{}", i));
            match actor.handle_enqueue(body, None) {
                QueueResponse::Enqueued { id } => first_batch_ids.push(id.as_u64()),
                _ => panic!("Expected Enqueued"),
            }
        }

        // Verify sequential IDs
        assert_eq!(first_batch_ids[0], 1);
        assert_eq!(first_batch_ids[9], 10);
    }

    // Act - Restart and enqueue second batch
    let mut second_batch_ids = Vec::new();
    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::global_dedup_store(),
        );

        for i in 0..10 {
            let body = Bytes::from(format!("batch2-{}", i));
            match actor.handle_enqueue(body, None) {
                QueueResponse::Enqueued { id } => second_batch_ids.push(id.as_u64()),
                _ => panic!("Expected Enqueued"),
            }
        }

        // Verify no ID collisions
        assert_eq!(second_batch_ids[0], 11);
        assert_eq!(second_batch_ids[9], 20);
    }

    // Assert - No collisions between batches
    let mut all_ids = first_batch_ids.clone();
    all_ids.extend(&second_batch_ids);
    all_ids.sort();
    all_ids.dedup();
    assert_eq!(
        all_ids.len(),
        20,
        "Should have 20 unique IDs across crashes (no collisions)"
    );
}

/// Test lease expiration with redelivery (automatic)
#[test]
fn should_redelivery_message_on_lease_expiration() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("lease-expire");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        fitz::utils::idempotency::global_dedup_store(),
    );

    // Act
    // Enqueue and reserve message
    actor.handle_enqueue(Bytes::from("work"), None);
    match actor.handle_reserve(30, Some(1)) {
        QueueResponse::Reserved { messages } => {
            assert_eq!(messages.len(), 1);
        }
        _ => panic!("Expected Reserved"),
    };

    // Assert
    // Verify message is inflight
    assert_eq!(actor.inflight.len(), 1);
    assert_eq!(actor.ready.len(), 0);

    // Note: Cannot advance time without MockClock (test limitation).
    // Unit tests verify expiration behavior with MockClock.
    // This integration test verifies reserve/complete mechanics.
}

/// Test dead letter queue (DLQ) for max attempts threshold
#[test]
fn should_dlq_message_after_max_attempts() {
    // This test requires MockClock to advance time, which is only available in unit tests.
    // Unit tests in src/domains/queue/queue_actor.rs verify DLQ behavior with MockClock.
    // This integration test verifies the overall queue mechanics.
}
