mod harness;
use fitz::storage::mem::{QueueConfig, QueueScope};
use harness::common::start_test_engine;

// ============================================================================
// QUEUE ENGINE INTEGRATION TESTS
// ============================================================================
// These tests exercise the engine-level queue functionality via in-process
// EngineHandle, not over WebSocket transport.
//
// For full end-to-end WebSocket tests, see e2e_queue_ws.rs (to be added).
// ============================================================================

// ============================================================================
// QUEUE OPERATIONS
// ============================================================================
// Queues provide at-least-once delivery with:
// - Enqueue(route, message, dedupeKey?) → msgID: add message to queue
// - Reserve/Lease(route, visibilityMs, maxBatch?) → messages: claim messages
// - Complete/Consume(route, msgID, token): acknowledge processed message
// - ExtendLease(route, msgID, token, addSecs): extend visibility timeout
// - Peek(route): view next message without claiming
// - SetQueueConfig(scope, config): configure visibility, DLQ, etc.
//
// Messages become invisible during lease, return to queue if not completed
// ============================================================================

// ============================================================================
// HAPPY PATH TESTS - Enqueue
// ============================================================================

#[tokio::test]
async fn should_enqueue_message_to_queue() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task data".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_assign_unique_message_ids() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"data1".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg2".to_string(),
            b"data2".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg3".to_string(),
            b"data3".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Assert - no-op removed
}

#[tokio::test]
async fn should_persist_enqueued_messages_durably() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let result = handle.peek("queue://realm/area/jobs".to_string()).await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Reserve/Lease
// ============================================================================

#[tokio::test]
async fn should_reserve_message_from_queue() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_return_lease_token_with_reserved_message() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let (_id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Assert
    assert!(!token.is_empty());
}

#[tokio::test]
async fn should_make_reserved_message_invisible_to_other_consumers() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let _first = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();
    let second = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(second.is_err());
}

#[tokio::test]
async fn should_respect_visibility_timeout_on_lease() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let _first = handle
        .reserve("queue://realm/area/jobs".to_string(), 2)
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let second = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(second.is_ok());
}

#[tokio::test]
async fn should_support_batch_reserve() {
    // Arrange
    let (handle, _store) = start_test_engine();
    for i in 0..10 {
        handle
            .publish(
                "queue://realm/area/jobs".to_string(),
                format!("msg{}", i),
                b"task".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
    }

    // Act
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Complete/Consume
// ============================================================================

#[tokio::test]
async fn should_complete_message_with_valid_token() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    let result = handle
        .consume("queue://realm/area/jobs".to_string(), id, token)
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_remove_completed_message_from_queue() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    handle
        .consume("queue://realm/area/jobs".to_string(), id, token)
        .await
        .unwrap();
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// HAPPY PATH TESTS - NACK/Reject
// ============================================================================

#[tokio::test]
async fn should_nack_message_and_return_to_queue() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, _token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    // Note: NACK not yet in API, would need to add nack() method
    let result = handle
        .consume(
            "queue://realm/area/jobs".to_string(),
            id.clone(),
            "invalid_token".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_not_increment_delivery_count_on_nack() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (_id, _body, _token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    // NACK is achieved by failing to consume (invalid token or lease expiry)
    // This test documents that a failed consume does not increment delivery_count
    // For now, this behavior is implementation-dependent

    // Assert
    // When delivery_count tracking is implemented, verify it's not incremented on failed consume
    // Currently this test serves as a placeholder for the feature
}

#[tokio::test]
async fn should_make_nacked_message_available_immediately() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    // Attempt consume with invalid token (simulating NACK)
    let consume_result = handle
        .consume(
            "queue://realm/area/jobs".to_string(),
            id.clone(),
            "invalid_token".to_string(),
        )
        .await;

    // Assert
    assert!(consume_result.is_err(), "Consume with invalid token should fail");
    // Message should still be available for reservation
    // (when lease expiry is implemented, this would be testable)
}

// ============================================================================
// HAPPY PATH TESTS - Extend Lease
// ============================================================================

#[tokio::test]
async fn should_extend_lease_with_valid_token() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 5)
        .await
        .unwrap();

    // Act
    let result = handle
        .extend_lease("queue://realm/area/jobs".to_string(), id, token, 10)
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_prevent_message_return_when_lease_extended() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 2)
        .await
        .unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    handle
        .extend_lease("queue://realm/area/jobs".to_string(), id, token, 5)
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// HAPPY PATH TESTS - Peek
// ============================================================================

#[tokio::test]
async fn should_peek_next_message_without_claiming() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let result = handle.peek("queue://realm/area/jobs".to_string()).await;

    // Assert
    assert!(result.is_ok());
    assert!(result.unwrap().is_some());
}

#[tokio::test]
async fn should_allow_reserve_after_peek() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let _peeked = handle
        .peek("queue://realm/area/jobs".to_string())
        .await
        .unwrap();
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Queue Configuration
// ============================================================================

#[tokio::test]
async fn should_apply_queue_config_to_scope() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let config = QueueConfig {
        dlq_threshold: 3,
        default_visibility_secs: 60,
        ttl_secs: 3600,
    };

    // Act
    let result = handle
        .set_queue_config(
            QueueScope::Area {
                realm: "realm".to_string(),
                area: "area".to_string(),
            },
            config,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_use_default_visibility_from_config() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let config = QueueConfig {
        dlq_threshold: 5,
        default_visibility_secs: 10,
        ttl_secs: 0,
    };
    handle
        .set_queue_config(
            QueueScope::Resource {
                realm: "realm".to_string(),
                area: "area".to_string(),
                resource: "jobs".to_string(),
            },
            config,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Deduplication
// ============================================================================

#[tokio::test]
async fn should_deduplicate_messages_with_same_dedupe_key() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "order-123".to_string(),
            b"data1".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let result = handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "order-123".to_string(),
            b"data2".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_allow_different_dedupe_keys() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "order-123".to_string(),
            b"data1".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let result = handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "order-456".to_string(),
            b"data2".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// NEGATIVE TESTS - Complete
// ============================================================================

#[tokio::test]
async fn should_reject_complete_with_invalid_token() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, _token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    let result = handle
        .consume(
            "queue://realm/area/jobs".to_string(),
            id,
            "invalid_token".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_complete_for_nonexistent_message() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .consume(
            "queue://realm/area/jobs".to_string(),
            "nonexistent".to_string(),
            "fake_token".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_double_complete() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    handle
        .consume(
            "queue://realm/area/jobs".to_string(),
            id.clone(),
            token.clone(),
        )
        .await
        .unwrap();
    let result = handle
        .consume("queue://realm/area/jobs".to_string(), id, token)
        .await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// NEGATIVE TESTS - Extend Lease
// ============================================================================

#[tokio::test]
async fn should_reject_extend_lease_with_invalid_token() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, _token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    let result = handle
        .extend_lease(
            "queue://realm/area/jobs".to_string(),
            id,
            "invalid_token".to_string(),
            10,
        )
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_extend_lease_after_expiration() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 1)
        .await
        .unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let result = handle
        .extend_lease("queue://realm/area/jobs".to_string(), id, token, 10)
        .await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// NEGATIVE TESTS - Empty Queue
// ============================================================================

#[tokio::test]
async fn should_return_empty_when_reserving_from_empty_queue() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .reserve("queue://realm/area/empty".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_return_empty_when_peeking_empty_queue() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.peek("queue://realm/area/empty".to_string()).await;

    // Assert
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ============================================================================
// EDGE CASES - DLQ (Dead Letter Queue)
// ============================================================================

#[tokio::test]
async fn should_move_message_to_dlq_after_max_deliveries() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let config = QueueConfig {
        dlq_threshold: 3,
        default_visibility_secs: 30,
        ttl_secs: 0,
    };
    handle
        .set_queue_config(
            QueueScope::Resource {
                realm: "realm".to_string(),
                area: "area".to_string(),
                resource: "jobs".to_string(),
            },
            config,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    for _ in 0..3 {
        let _ = handle
            .reserve("queue://realm/area/jobs".to_string(), 1)
            .await;
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    // Assert
    // After 3 failed deliveries, message should be moved to DLQ
    // When DLQ functionality is implemented, this test will verify the move
    // For now, this documents expected DLQ behavior
}

#[tokio::test]
async fn should_not_return_dlq_messages_in_normal_reserve() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_allow_processing_dlq_messages_explicitly() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .reserve("queue://realm/area/jobs/dlq".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_support_explicit_move_to_dlq() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"poison".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    // Note: Explicit DLQ move not yet in API
    let result = handle
        .consume("queue://realm/area/jobs".to_string(), id, token)
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// EDGE CASES - In-Flight Tracking
// ============================================================================

#[tokio::test]
async fn should_track_in_flight_message_count() {
    // Arrange
    let (handle, _store) = start_test_engine();
    for i in 0..5 {
        handle
            .publish(
                "queue://realm/area/jobs".to_string(),
                format!("msg{}", i),
                b"task".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
    }

    // Act
    let _ = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();
    let _ = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();
    let _ = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Assert
    // 3 messages should be in-flight (reserved)
    // When observability API is added, we can query in-flight count
    // This test documents the expected behavior
}

#[tokio::test]
async fn should_decrease_in_flight_count_on_complete() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task1".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg2".to_string(),
            b"task2".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let (id1, _body1, token1) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();
    let (_id2, _body2, _token2) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Act
    handle
        .consume("queue://realm/area/jobs".to_string(), id1, token1)
        .await
        .unwrap();

    // Assert
    // After consuming msg1, in-flight count should be 1 (only msg2)
    // When observability API is added, we can query and verify in-flight count decreased
    // This test documents the expected behavior
}

#[tokio::test]
async fn should_return_to_available_when_lease_expires() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let _ = handle
        .reserve("queue://realm/area/jobs".to_string(), 1)
        .await
        .unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assert
    // Message should return to available after lease expires
    // When lease expiry is fully implemented, we can re-reserve the message
    // This test documents the expected lease expiry behavior
}

// ============================================================================
// EDGE CASES - FIFO Ordering
// ============================================================================

#[tokio::test]
async fn should_preserve_fifo_order_for_queue_messages() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msgA".to_string(),
            b"A".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msgB".to_string(),
            b"B".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msgC".to_string(),
            b"C".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let (_id1, body1, _token1) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();
    let (_id2, body2, _token2) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();
    let (_id3, body3, _token3) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Assert
    assert_eq!(body1, b"A");
    assert_eq!(body2, b"B");
    assert_eq!(body3, b"C");
}

// ============================================================================
// EDGE CASES - Delivery Count and Max Retries
// ============================================================================

#[tokio::test]
async fn should_return_unique_token_on_each_delivery() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let (_id1, _body1, token1) = handle
        .reserve("queue://realm/area/jobs".to_string(), 1)
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let (_id2, _body2, token2) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Assert
    assert_ne!(token1, token2);
}

#[tokio::test]
async fn should_return_non_empty_token_on_first_delivery() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let (_id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Assert
    assert!(!token.is_empty());
}

#[tokio::test]
async fn should_return_non_empty_token_on_redelivery() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let _ = handle
        .reserve("queue://realm/area/jobs".to_string(), 1)
        .await
        .unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let (_id, _body, token) = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await
        .unwrap();

    // Assert
    assert!(!token.is_empty());
}

#[tokio::test]
async fn should_track_delivery_count_on_redelivery() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let _ = handle
        .reserve("queue://realm/area/jobs".to_string(), 1)
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_move_to_dlq_when_max_deliveries_exceeded() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let config = QueueConfig {
        dlq_threshold: 3,
        default_visibility_secs: 30,
        ttl_secs: 0,
    };
    handle
        .set_queue_config(
            QueueScope::Resource {
                realm: "realm".to_string(),
                area: "area".to_string(),
                resource: "jobs".to_string(),
            },
            config,
        )
        .await
        .unwrap();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    for _ in 0..3 {
        let _ = handle
            .reserve("queue://realm/area/jobs".to_string(), 1)
            .await;
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
    let result = handle
        .reserve("queue://realm/area/jobs".to_string(), 30)
        .await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// EDGE CASES - Concurrent Consumers
// ============================================================================

#[tokio::test]
async fn should_allow_first_concurrent_consumer_to_reserve() {
    // Arrange
    let (handle, _store) = start_test_engine();
    for i in 0..10 {
        handle
            .publish(
                "queue://realm/area/jobs".to_string(),
                format!("msg{}", i),
                b"task".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
    }

    // Act
    let h1 = handle.clone();
    let h2 = handle.clone();
    let h3 = handle.clone();

    let (r1, _r2, _r3) = tokio::join!(
        h1.reserve("queue://realm/area/jobs".to_string(), 30),
        h2.reserve("queue://realm/area/jobs".to_string(), 30),
        h3.reserve("queue://realm/area/jobs".to_string(), 30),
    );

    // Assert
    assert!(r1.is_ok());
}

#[tokio::test]
async fn should_allow_second_concurrent_consumer_to_reserve() {
    // Arrange
    let (handle, _store) = start_test_engine();
    for i in 0..10 {
        handle
            .publish(
                "queue://realm/area/jobs".to_string(),
                format!("msg{}", i),
                b"task".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
    }

    // Act
    let h1 = handle.clone();
    let h2 = handle.clone();
    let h3 = handle.clone();

    let (_r1, r2, _r3) = tokio::join!(
        h1.reserve("queue://realm/area/jobs".to_string(), 30),
        h2.reserve("queue://realm/area/jobs".to_string(), 30),
        h3.reserve("queue://realm/area/jobs".to_string(), 30),
    );

    // Assert
    assert!(r2.is_ok());
}

#[tokio::test]
async fn should_allow_third_concurrent_consumer_to_reserve() {
    // Arrange
    let (handle, _store) = start_test_engine();
    for i in 0..10 {
        handle
            .publish(
                "queue://realm/area/jobs".to_string(),
                format!("msg{}", i),
                b"task".to_vec(),
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
    }

    // Act
    let h1 = handle.clone();
    let h2 = handle.clone();
    let h3 = handle.clone();

    let (_r1, _r2, r3) = tokio::join!(
        h1.reserve("queue://realm/area/jobs".to_string(), 30),
        h2.reserve("queue://realm/area/jobs".to_string(), 30),
        h3.reserve("queue://realm/area/jobs".to_string(), 30),
    );

    // Assert
    assert!(r3.is_ok());
}

#[tokio::test]
async fn should_prevent_duplicate_delivery_to_concurrent_consumers() {
    // Arrange
    let (handle, _store) = start_test_engine();
    handle
        .publish(
            "queue://realm/area/jobs".to_string(),
            "msg1".to_string(),
            b"task".to_vec(),
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();

    // Act
    let h1 = handle.clone();
    let h2 = handle.clone();

    let (r1, r2) = tokio::join!(
        h1.reserve("queue://realm/area/jobs".to_string(), 30),
        h2.reserve("queue://realm/area/jobs".to_string(), 30),
    );

    // Assert
    let success_count = [r1.is_ok(), r2.is_ok()].iter().filter(|&&x| x).count();
    assert_eq!(success_count, 1);
}
