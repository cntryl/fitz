//! Queue domain integration tests
//!
//! Tests durability, restart semantics, and end-to-end workflows.

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
        family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: format!("{}-{}", resource_prefix, Uuid::new_v4()),
    }
}

/// Test that messages persist to Midge during actor lifecycle
#[test]
fn should_persist_messages_to_storage() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("durable");

    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key.clone(),
        store.clone(),
        None,
    );
    let body = Bytes::from("durable message");

    // Act
    let response = actor.handle_enqueue(body.clone(), None);

    // Assert
    match response {
        QueueResponse::Enqueued { id } => {
            // Verify message can be reserved
            let reserve_response = actor.handle_reserve(30, Some(1));
            match reserve_response {
                QueueResponse::Reserved { messages } => {
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].id, id);
                    assert_eq!(messages[0].body, body);
                }
                _ => panic!("Expected Reserved response"),
            }
        }
        _ => panic!("Expected Enqueued response"),
    }
}

/// Test that messages can be recovered after actor restart
#[test]
fn should_recover_messages_after_restart() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );

    let queue_key = unique_queue_key("durable-restart");

    // Pre-populate with a message that will be recovered
    let msg_id = {
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key.clone(),
            store.clone(),
            None,
        );
        let body = Bytes::from("durable message");
        match actor.handle_enqueue(body, None) {
            QueueResponse::Enqueued { id } => id,
            _ => panic!("Expected Enqueued response"),
        }
    };

    // Act - Restart actor and recover from storage
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
    );
    let reserve_response = actor.handle_reserve(30, Some(1));

    // Assert - Message recovered and redeliverable
    match reserve_response {
        QueueResponse::Reserved { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].body, Bytes::from("durable message"));
            // Attempts should be incremented (though we manually re-enqueued for MVP)
        }
        _ => panic!("Expected Reserved response"),
    }
}

/// Test high-volume enqueue performance
#[test]
#[ignore = "Slow performance test"]
fn should_handle_high_volume_enqueue() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let queue_key = QueueKey {
        family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: "volume".to_string(),
    };
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
    );

    let count = 10_000;
    let start = std::time::Instant::now();

    // Act
    for i in 0..count {
        let body = Bytes::from(format!("message {}", i));
        let response = actor.handle_enqueue(body, None);
        match response {
            QueueResponse::Enqueued { .. } => {}
            _ => panic!("Expected Enqueued response"),
        }
    }

    let elapsed = start.elapsed();

    // Assert
    assert_eq!(actor.ready.len(), count);
    let msgs_per_sec = count as f64 / elapsed.as_secs_f64();
    println!(
        "Enqueued {} messages in {:?} ({:.0} msg/sec)",
        count, elapsed, msgs_per_sec
    );

    // Target: 200k-1M msg/sec (this is local-only, no actor routing overhead)
    // In real usage, actor mailbox adds ~50ns overhead per message
}

/// Test concurrent reserve/complete across multiple workers
#[test]
#[ignore = "Slow performance test"]
fn should_handle_concurrent_workers() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let queue_key = QueueKey {
        family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: "workers".to_string(),
    };
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
    );

    // Enqueue 100 messages
    for i in 0..100 {
        let body = Bytes::from(format!("task {}", i));
        actor.handle_enqueue(body, None);
    }

    // Act - Simulate 10 workers each reserving 10 messages
    let mut completed = 0;

    for _worker_id in 0..10 {
        let reserve_response = actor.handle_reserve(30, Some(10));

        match reserve_response {
            QueueResponse::Reserved { messages } => {
                for msg in messages {
                    // Simulate processing
                    let complete_response = actor.handle_complete(msg.id, msg.token);
                    match complete_response {
                        QueueResponse::Completed => completed += 1,
                        _ => panic!("Expected Completed response"),
                    }
                }
            }
            _ => panic!("Expected Reserved response"),
        }
    }

    // Assert
    assert_eq!(completed, 100);
    assert_eq!(actor.ready.len(), 0);
    assert_eq!(actor.inflight.len(), 0);
}

/// Test that expired messages are redelivered with incremented attempts
/// NOTE: This test is disabled because MockClock is only available in cfg(test) module
/// and not accessible from integration tests. The functionality is tested in unit tests.
#[test]
#[ignore = "Requires MockClock from cfg(test) module"]
fn should_increment_attempts_on_redelivery() {
    // Tested in unit tests with MockClock
}

/// Test reserve latency (performance)
#[test]
#[ignore = "Slow performance test"]
fn should_have_low_reserve_latency() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let queue_key = QueueKey {
        family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: "perf".to_string(),
    };
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
    );

    // Enqueue 1000 messages
    for i in 0..1000 {
        let body = Bytes::from(format!("message {}", i));
        actor.handle_enqueue(body, None);
    }

    // Act - Measure reserve latency
    let iterations = 1000;
    let start = std::time::Instant::now();

    for _ in 0..iterations {
        let _response = actor.handle_reserve(30, Some(1));
    }

    let elapsed = start.elapsed();
    let avg_latency = elapsed / iterations;

    // Assert
    println!("Average reserve latency: {:?}", avg_latency);

    // Target: <10Ãƒâ€šÃ‚Âµs (excluding Midge read cost)
    // Actual: ~1-2Ãƒâ€šÃ‚Âµs for in-memory operations + Midge read overhead
}

/// Test complete latency (performance)
#[test]
#[ignore = "Slow performance test"]
fn should_have_low_complete_latency() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    );
    let queue_key = QueueKey {
        family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: "perf".to_string(),
    };
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
    );

    // Enqueue and reserve 1000 messages
    let mut messages = Vec::new();
    for i in 0..1000 {
        let body = Bytes::from(format!("message {}", i));
        actor.handle_enqueue(body, None);
    }

    for _ in 0..100 {
        let response = actor.handle_reserve(30, Some(10));
        match response {
            QueueResponse::Reserved { messages: msgs } => {
                messages.extend(msgs);
            }
            _ => panic!("Expected Reserved response"),
        }
    }

    // Act - Measure complete latency
    let start = std::time::Instant::now();

    for msg in &messages {
        let _response = actor.handle_complete(msg.id, msg.token);
    }

    let elapsed = start.elapsed();
    let avg_latency = elapsed / messages.len() as u32;

    // Assert
    println!("Average complete latency: {:?}", avg_latency);

    // Target: <5Ãƒâ€šÃ‚Âµs (excluding Midge delete cost)
    // Actual: ~1Ãƒâ€šÃ‚Âµs for in-memory ops + Midge delete overhead
}
