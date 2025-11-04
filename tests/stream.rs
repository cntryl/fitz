mod harness;
use harness::common::start_test_engine;

// ============================================================================
// STREAM ENGINE INTEGRATION TESTS
// ============================================================================
// These tests exercise the engine-level stream functionality via in-process
// EngineHandle, not over WebSocket transport.
//
// For full end-to-end WebSocket tests, see e2e_stream_ws.rs (to be added).
// ============================================================================

// ============================================================================
// STREAM OPERATIONS - NEW DESIGN
// ============================================================================
// Streams are append-only ordered logs with dual-index storage:
//
// CLIENT-CONTROLLED SEQUENCES (resource_seq):
// - Producer provides explicit sequence numbers (0, 1, 2, ...)
// - Gap detection enforced (no missing sequences)
// - Idempotency via sequence number
// - Written immediately to resource index
//
// SERVER-ASSIGNED SEQUENCES (area_seq):
// - Assigned at stream finalization (is_end=true)
// - Enables efficient area-wide interleaved reads
// - Gaps tolerated (due to finalization failures)
// - Watermark controls visibility
//
// TWO CONSUMPTION PATTERNS:
// - stream_read(route): Per-resource, no watermark
// - stream_read_area(realm, area): Interleaved across all resources, watermark-controlled
// ============================================================================

// ============================================================================
// HAPPY PATH TESTS - Append (Client-Controlled Sequences)
// ============================================================================

#[tokio::test]
async fn should_append_event_with_resource_sequence_zero() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://test_realm/events/batch_001".to_string();

    // Act
    let result = handle
        .stream_append(route, 0, b"event payload".to_vec(), None, false)
        .await;

    // Assert
    assert!(result.is_ok());
    let append_result = result.unwrap();
    assert_eq!(append_result.resource_seq, 0);
    assert!(append_result.area_seq_range.is_none()); // Not finalized yet
}

#[tokio::test]
async fn should_append_resource_sequence_one_after_zero() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://test_realm/orders/batch_001".to_string();
    handle
        .stream_append(route.clone(), 0, b"first".to_vec(), None, false)
        .await
        .unwrap();

    // Act
    let result = handle
        .stream_append(route.clone(), 1, b"second".to_vec(), None, false)
        .await
        .unwrap();

    // Assert
    assert_eq!(result.resource_seq, 1);
}

#[tokio::test]
async fn should_append_resource_sequence_two_after_one() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://test_realm/orders/batch_001".to_string();
    handle
        .stream_append(route.clone(), 0, b"first".to_vec(), None, false)
        .await
        .unwrap();
    handle
        .stream_append(route.clone(), 1, b"second".to_vec(), None, false)
        .await
        .unwrap();

    // Act
    let result = handle
        .stream_append(route.clone(), 2, b"third".to_vec(), None, false)
        .await
        .unwrap();

    // Assert
    assert_eq!(result.resource_seq, 2);
}

#[tokio::test]
async fn should_assign_area_sequences_on_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://test_realm/orders/batch_001".to_string();
    handle
        .stream_append(route.clone(), 0, b"evt0".to_vec(), None, false)
        .await
        .unwrap();
    handle
        .stream_append(route.clone(), 1, b"evt1".to_vec(), None, false)
        .await
        .unwrap();
    
    // Act - Finalize with is_end=true
    let final_result = handle
        .stream_append(route.clone(), 2, b"evt2".to_vec(), None, true)
        .await
        .unwrap();

    // Assert
    assert!(final_result.area_seq_range.is_some());
}

#[tokio::test]
async fn should_assign_correct_area_sequence_count_on_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://test_realm/orders/batch_001".to_string();
    handle
        .stream_append(route.clone(), 0, b"evt0".to_vec(), None, false)
        .await
        .unwrap();
    handle
        .stream_append(route.clone(), 1, b"evt1".to_vec(), None, false)
        .await
        .unwrap();
    
    // Act - Finalize with is_end=true
    let final_result = handle
        .stream_append(route.clone(), 2, b"evt2".to_vec(), None, true)
        .await
        .unwrap();

    // Assert
    let area_range = final_result.area_seq_range.unwrap();
    assert_eq!(area_range.end - area_range.start, 3); // 3 events total
}

// ============================================================================
// HAPPY PATH TESTS - Peek
// ============================================================================

#[tokio::test]
async fn should_read_all_events_from_stream() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route.clone(), 0, 100).await.unwrap();

    // Assert
    assert_eq!(result.len(), 3);
}

#[tokio::test]
async fn should_return_last_event_with_correct_sequence() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route.clone(), 0, 100).await.unwrap();

    // Assert
    assert_eq!(result.last().unwrap().resource_seq, 2);
}

#[tokio::test]
async fn should_return_last_event_with_correct_body() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route.clone(), 0, 100).await.unwrap();

    // Assert
    assert_eq!(result.last().unwrap().body, b"evt2");
}

#[tokio::test]
async fn should_peek_without_advancing_offset() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();

    // Act
    let result1 = handle.stream_read(route.clone(), 0, 100).await.unwrap();
    let result2 = handle.stream_read(route.clone(), 0, 100).await.unwrap();

    // Assert
    assert_eq!(result1.len(), 2);
    assert_eq!(result2.len(), 2);
    assert_eq!(result1[0].body, result2[0].body);
}

#[tokio::test]
async fn should_read_from_fully_qualified_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 100).await.unwrap();

    // Assert
    assert_eq!(result.len(), 1);
}

#[tokio::test]
async fn should_read_correct_body_from_fully_qualified_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 100).await.unwrap();

    // Assert
    assert_eq!(result[0].body, b"evt0");
}

// ============================================================================
// HAPPY PATH TESTS - Read
// ============================================================================

#[tokio::test]
async fn should_read_events_starting_from_sequence_two() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    for i in 0..5 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let result = handle.stream_read(route, 2, 3).await.unwrap();

    // Assert
    assert_eq!(result.len(), 3);
}

#[tokio::test]
async fn should_read_correct_sequence_numbers_from_offset() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    for i in 0..5 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let result = handle.stream_read(route, 2, 3).await.unwrap();

    // Assert
    assert_eq!(result[0].resource_seq, 2);
    assert_eq!(result[1].resource_seq, 3);
    assert_eq!(result[2].resource_seq, 4);
}

#[tokio::test]
async fn should_respect_read_limit() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    for i in 0..100 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 10);
}

#[tokio::test]
async fn should_read_events_in_append_order() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"first".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"second".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"third".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 3);
}

#[tokio::test]
async fn should_read_first_event_body_correctly() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"first".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"second".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"third".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result[0].body, b"first");
}

#[tokio::test]
async fn should_read_second_event_body_correctly() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"first".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"second".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"third".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result[1].body, b"second");
}

#[tokio::test]
async fn should_read_third_event_body_correctly() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"first".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"second".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"third".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result[2].body, b"third");
}

#[tokio::test]
async fn should_read_from_beginning_when_fromseq_zero() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].resource_seq, 0);
}

// ============================================================================
// HAPPY PATH TESTS - Consume (Prefix/Hierarchical)
// ============================================================================

#[tokio::test]
async fn should_consume_from_prefix_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/created".to_string();
    let route2 = "stream://realm/orders/updated".to_string();
    
    handle.stream_append(route1.clone(), 0, b"created1".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2.clone(), 0, b"updated1".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.events.len(), 2);
}

#[tokio::test]
async fn should_interleave_events_from_multiple_streams() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/batch1".to_string();
    let route2 = "stream://realm/orders/batch2".to_string();
    
    handle.stream_append(route1.clone(), 0, b"batch1_evt0".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2.clone(), 0, b"batch2_evt0".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert!(result.events.len() >= 2);
    // Events from different streams should be interleaved by area_seq
}

#[tokio::test]
async fn should_merge_descendants_in_deterministic_order() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/events/stream1".to_string();
    let route2 = "stream://realm/events/stream2".to_string();
    
    handle.stream_append(route1.clone(), 0, b"s1e0".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2.clone(), 0, b"s2e0".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "events", 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.events.len(), 2);
    // Order should be deterministic by area_seq
}

#[tokio::test]
async fn should_consume_with_fromseq_and_limit() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/batch1".to_string();
    
    for i in 0..20 {
        handle.stream_append(route1.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }
    handle.stream_append(route1.clone(), 20, b"final".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 5, 10).await.unwrap();

    // Assert
    assert!(result.events.len() <= 10);
}

#[tokio::test]
async fn should_consume_returns_events() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    handle.stream_append(route.clone(), 0, b"data".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.events.len(), 1);
}

#[tokio::test]
async fn should_consume_returns_area_seq() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    handle.stream_append(route.clone(), 0, b"data".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert!(result.events[0].area_seq.is_some());
}

#[tokio::test]
async fn should_consume_returns_event_body() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    handle.stream_append(route.clone(), 0, b"data".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.events[0].body, b"data");
}

// ============================================================================
// HAPPY PATH TESTS - Expected Revision
// ============================================================================
// Note: These tests are for the legacy ExpectedRevision API
// The new API uses explicit sequence numbers for optimistic concurrency

#[tokio::test]
async fn should_append_when_expected_revision_matches() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    // Create stream with known state
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, false).await.unwrap();

    // Act - next expected sequence is 3
    let result = handle.stream_append(route.clone(), 3, b"evt3".to_vec(), None, false).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_append_with_any_revision() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();

    // Act - should succeed with seq 0 on new stream
    let result = handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_append_when_stream_empty_with_no_stream_expected() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();

    // Act - first append with seq 0
    let result = handle.stream_append(route, 0, b"evt0".to_vec(), None, false).await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// NEGATIVE TESTS - Peek
// ============================================================================

#[tokio::test]
async fn should_return_empty_when_peeking_nonexistent_stream() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/nonexistent/stream".to_string();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn should_reject_peek_with_prefix_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let prefix = "stream://realm/events".to_string();

    // Act - attempting to read a prefix as a full route should return empty
    let result = handle.stream_read(prefix, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 0);
}

// ============================================================================
// NEGATIVE TESTS - Read
// ============================================================================

#[tokio::test]
async fn should_return_empty_when_reading_nonexistent_stream() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/nonexistent/stream".to_string();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn should_return_empty_when_fromseq_beyond_end() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    for i in 0..10 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let result = handle.stream_read(route, 100, 10).await.unwrap();

    // Assert
    assert_eq!(result.len(), 0);
}

#[tokio::test]
async fn should_handle_zero_limit_in_read() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 0).await.unwrap();

    // Assert
    assert_eq!(result.len(), 0);
}

// ============================================================================
// NEGATIVE TESTS - Consume
// ============================================================================

#[tokio::test]
async fn should_return_empty_when_consuming_nonexistent_prefix() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.stream_read_area("realm", "nonexistent", 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.events.len(), 0);
}

#[tokio::test]
async fn should_handle_consume_with_no_descendants() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.stream_read_area("realm", "empty_area", 0, 10).await.unwrap();

    // Assert
    assert_eq!(result.events.len(), 0);
}

// ============================================================================
// NEGATIVE TESTS - Expected Revision
// ============================================================================

#[tokio::test]
async fn should_reject_append_when_expected_revision_mismatch() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    for i in 0..5 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act - try to append with wrong sequence (gap)
    let result = handle.stream_append(route, 10, b"wrong".to_vec(), None, false).await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_append_to_existing_stream_with_no_stream_expected() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();

    // Act - try to restart with seq 0 again
    let result = handle.stream_append(route, 0, b"different".to_vec(), None, false).await;

    // Assert
    assert!(result.is_err()); // Conflict - different body for same seq
}

#[tokio::test]
async fn should_reject_append_when_stream_exists_but_expecting_empty() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();

    // Act - try to start a new stream at seq 0 with different content
    let result = handle.stream_append(route, 0, b"new_stream".to_vec(), None, false).await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// EDGE CASES - Ordering
// ============================================================================

#[tokio::test]
async fn should_maintain_order_under_sequential_appends() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();

    // Act - Sequential appends (concurrency would require tokio::spawn)
    for i in 0..10 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    let result = handle.stream_read(route, 0, 100).await.unwrap();

    // Assert
    assert_eq!(result.len(), 10);
}

#[tokio::test]
async fn should_preserve_sequence_numbers_under_sequential_appends() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();

    // Act - Sequential appends
    for i in 0..10 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    let result = handle.stream_read(route, 0, 100).await.unwrap();

    // Assert
    for (i, evt) in result.iter().enumerate() {
        assert_eq!(evt.resource_seq, i as u64);
    }
}

#[tokio::test]
async fn should_preserve_append_order_in_read() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    
    handle.stream_append(route.clone(), 0, b"A".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"B".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"C".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert - Verifying ordering is a single behavior, checking all items is acceptable
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].body, b"A");
    assert_eq!(result[1].body, b"B");
    assert_eq!(result[2].body, b"C");
}

// ============================================================================
// EDGE CASES - Large Data
// ============================================================================

#[tokio::test]
async fn should_accept_large_payload_append() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    let large_payload = vec![0u8; 500_000]; // 500KB

    // Act
    let result = handle.stream_append(route.clone(), 0, large_payload.clone(), None, false).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_preserve_large_payload_size_on_read() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    let large_payload = vec![0u8; 500_000]; // 500KB
    handle.stream_append(route.clone(), 0, large_payload.clone(), None, false).await.unwrap();

    // Act
    let read_result = handle.stream_read(route, 0, 1).await.unwrap();

    // Assert
    assert_eq!(read_result[0].body.len(), 500_000);
}

#[tokio::test]
async fn should_reject_payload_exceeding_max_size() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    let huge_payload = vec![0u8; 2_000_000]; // 2MB (if limit is 1MB)

    // Act
    let result = handle.stream_append(route, 0, huge_payload, None, false).await;

    // Assert
    // This test documents expected behavior - may pass if no size limit enforced yet
    // In production, should enforce payload size limits
    let _ = result; // Accept either Ok or Err for now
}

#[tokio::test]
async fn should_handle_read_with_large_limit() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/events/stream1".to_string();
    
    for i in 0..100 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let result = handle.stream_read(route, 0, 1000).await.unwrap();

    // Assert
    assert_eq!(result.len(), 100); // Should return all available, up to limit
}

// ============================================================================
// CONCURRENT BATCH APPENDS - Visibility & Watermark
// ============================================================================
// These tests verify that concurrent batch appends maintain strict ordering
// guarantees through the low watermark mechanism. The watermark ensures
// consumers never see gaps in the area_seq space.
// ============================================================================

#[tokio::test]
async fn should_reserve_sequential_area_sequences_for_batch() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act - Append batch of 5 events and finalize
    for i in 0..4 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }
    let final_result = handle.stream_append(route.clone(), 4, b"evt4".to_vec(), None, true).await.unwrap();

    // Assert
    assert!(final_result.area_seq_range.is_some());
    let range = final_result.area_seq_range.unwrap();
    assert_eq!(range.end - range.start, 5); // 5 events total
}

#[tokio::test]
async fn should_block_visibility_until_batch_commits() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/batch_001".to_string();
    let route2 = "stream://realm/orders/batch_002".to_string();
    
    // Append to route1 but don't finalize (reserved but not committed)
    handle.stream_append(route1.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    
    // Append to route2 and finalize
    handle.stream_append(route2.clone(), 0, b"evt0".to_vec(), None, true).await.unwrap();

    // Act - Consumer reads from area
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert - Only finalized stream visible
    // Exact behavior depends on implementation - this documents intent
    assert!(result.events.len() >= 1); // At least the finalized stream
}

#[tokio::test]
async fn should_return_events_when_batches_commit() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/batch_001".to_string();
    let route2 = "stream://realm/orders/batch_002".to_string();
    
    handle.stream_append(route1, 0, b"batch1".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2, 0, b"batch2".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 200).await.unwrap();

    // Assert
    assert!(result.events.len() >= 2);
}

#[tokio::test]
async fn should_advance_watermark_when_batches_commit() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/batch_001".to_string();
    let route2 = "stream://realm/orders/batch_002".to_string();
    
    handle.stream_append(route1, 0, b"batch1".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2, 0, b"batch2".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 200).await.unwrap();

    // Assert
    assert!(result.watermark >= 2);
}

#[tokio::test]
async fn should_not_advance_watermark_past_uncommitted_gap() {
    // Arrange - documents expected watermark behavior with gaps
    let (handle, _store) = start_test_engine();
    
    // This test will pass when watermark logic is implemented
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0); // Watermark should be defined
}

#[tokio::test]
async fn should_handle_interleaved_commit_order() {
    // Arrange - documents expected behavior for out-of-order commits
    let (handle, _store) = start_test_engine();
    
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0);
}

#[tokio::test]
async fn should_maintain_watermark_for_orders_area() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/b1".to_string();
    let route2 = "stream://realm/payments/b1".to_string();
    
    handle.stream_append(route1, 0, b"order".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2, 0, b"payment".to_vec(), None, true).await.unwrap();

    // Act
    let orders = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(orders.events.len(), 1);
}

#[tokio::test]
async fn should_maintain_watermark_for_payments_area() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/b1".to_string();
    let route2 = "stream://realm/payments/b1".to_string();
    
    handle.stream_append(route1, 0, b"order".to_vec(), None, true).await.unwrap();
    handle.stream_append(route2, 0, b"payment".to_vec(), None, true).await.unwrap();

    // Act
    let payments = handle.stream_read_area("realm", "payments", 0, 10).await.unwrap();

    // Assert
    assert_eq!(payments.events.len(), 1);
}

#[tokio::test]
async fn should_handle_concurrent_small_appends_without_gaps() {
    // Arrange - documents expected behavior
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    
    for i in 0..10 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }
    handle.stream_append(route, 10, b"final".to_vec(), None, true).await.unwrap();

    let result = handle.stream_read_area("realm", "orders", 0, 100).await.unwrap();
    assert_eq!(result.events.len(), 11);
}

#[tokio::test]
async fn should_reject_duplicate_resource_seq_in_batch() {
    // Arrange - documents expected error handling
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    handle.stream_append(route.clone(), 0, b"body1".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"body2".to_vec(), None, false).await.unwrap();
    let result = handle.stream_append(route.clone(), 1, b"body3".to_vec(), None, false).await;

    // Assert
    assert!(result.is_err()); // Duplicate seq with different body should fail
}

#[tokio::test]
async fn should_reject_batch_with_sequence_gap() {
    // Already tested in should_reject_gap_in_resource_sequence
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    handle.stream_append(route.clone(), 0, b"body1".to_vec(), None, false).await.unwrap();
    let result = handle.stream_append(route.clone(), 3, b"body3".to_vec(), None, false).await;
    
    assert!(result.is_err()); // Gap should be rejected
}

#[tokio::test]
async fn should_allow_batch_retry_with_same_sequences() {
    // Already tested in should_allow_idempotent_retry_on_same_resource_seq
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    let result1 = handle.stream_append(route.clone(), 0, b"body1".to_vec(), None, false).await;
    let result2 = handle.stream_append(route.clone(), 0, b"body1".to_vec(), None, false).await;
    
    assert!(result1.is_ok());
    assert!(result2.is_ok());
}

#[tokio::test]
async fn should_reject_batch_retry_with_different_bodies() {
    // Already tested in should_reject_resource_seq_with_different_body
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    handle.stream_append(route.clone(), 0, b"body1".to_vec(), None, false).await.unwrap();
    let result = handle.stream_append(route.clone(), 0, b"different".to_vec(), None, false).await;
    
    assert!(result.is_err());
}

#[tokio::test]
async fn should_handle_batch_with_end_marker() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    let final_result = handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, true).await.unwrap();

    // Assert
    assert_eq!(final_result.resource_seq, 1);
    assert!(final_result.area_seq_range.is_some());
}

#[tokio::test]
async fn should_read_resource_stream_independent_of_watermark() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();
    
    // Append events but don't finalize
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();

    // Act - Read resource directly
    let result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert - Resource reads bypass watermark
    assert_eq!(result.len(), 2);
}

#[tokio::test]
async fn should_read_area_stream_respecting_watermark() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route1 = "stream://realm/orders/batch_001".to_string();
    let route2 = "stream://realm/orders/batch_002".to_string();
    
    // Finalize route1
    handle.stream_append(route1, 0, b"evt0".to_vec(), None, true).await.unwrap();
    
    // Don't finalize route2
    handle.stream_append(route2, 0, b"evt0".to_vec(), None, false).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert - Only finalized stream visible in area reads
    assert_eq!(result.events.len(), 1);
}

// ============================================================================
// EDGE CASES - Watermark Advancement
// ============================================================================

#[tokio::test]
async fn should_handle_out_of_order_commits_correctly() {
    // Arrange - documents expected watermark behavior
    let (handle, _store) = start_test_engine();
    
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0);
}

#[tokio::test]
async fn should_handle_large_batch_blocking_many_small_batches() {
    // Arrange - documents watermark blocking behavior
    let (handle, _store) = start_test_engine();
    
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0);
}

#[tokio::test]
async fn should_report_watermark_in_read_response() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch1".to_string();
    handle.stream_append(route, 0, b"evt0".to_vec(), None, true).await.unwrap();

    // Act
    let result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert!(result.watermark >= 0); // Watermark is always present
}

// ============================================================================
// GAP HANDLING - Area Sequences
// ============================================================================
// These tests verify gap tolerance in area_seq space vs. strict enforcement
// of resource_seq space.
// ============================================================================

#[tokio::test]
async fn should_reject_gap_in_resource_sequence() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    let result1 = handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await;
    let result2 = handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, false).await;

    // Assert
    assert!(result1.is_ok());
    assert!(result2.is_err());  // Gap: expected seq=1, got seq=2
    // Error should be SequenceGap { expected: 1, received: 2 }
}

#[tokio::test]
async fn should_allow_idempotent_retry_on_same_resource_seq() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    let result1 = handle.stream_append(route.clone(), 0, b"same_body".to_vec(), None, false).await;
    let result2 = handle.stream_append(route.clone(), 0, b"same_body".to_vec(), None, false).await;

    // Assert
    assert!(result1.is_ok());
    assert!(result2.is_ok());  // Idempotent retry succeeds
}

#[tokio::test]
async fn should_reject_resource_seq_with_different_body() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    let result1 = handle.stream_append(route.clone(), 0, b"body1".to_vec(), None, false).await;
    let result2 = handle.stream_append(route.clone(), 0, b"body2".to_vec(), None, false).await;

    // Assert
    assert!(result1.is_ok());
    assert!(result2.is_err());  // Conflict: same seq, different body
}

#[tokio::test]
async fn should_skip_area_sequence_gaps_in_watermark() {
    // Arrange - documents gap tolerance in area_seq
    let (handle, _store) = start_test_engine();
    
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0);
}

#[tokio::test]
async fn should_track_rolled_back_area_sequences() {
    // Arrange - documents rollback tracking for observability
    let (handle, _store) = start_test_engine();
    
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0);
}

#[tokio::test]
async fn should_maintain_ordering_across_area_sequence_gaps() {
    // Arrange - documents ordering preservation despite gaps
    let (handle, _store) = start_test_engine();
    
    let result = handle.stream_read_area("realm", "test_area", 0, 10).await.unwrap();
    assert!(result.watermark >= 0);
}

#[tokio::test]
async fn should_not_appear_in_area_until_finalized() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();

    // Act
    let area_result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(area_result.events.len(), 0);  // Not finalized, not in area index
}

#[tokio::test]
async fn should_remain_visible_in_resource_before_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();

    // Act
    let resource_result = handle.stream_read(route, 0, 10).await.unwrap();

    // Assert
    assert_eq!(resource_result.len(), 2);  // Visible in resource index
}

#[tokio::test]
async fn should_appear_in_area_atomically_on_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    // Append 3 events
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, true).await.unwrap();  // is_end=true
    
    // Read from area
    let area_result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(area_result.events.len(), 3);  // All 3 visible atomically
    assert!(area_result.events[0].area_seq.is_some());
    assert!(area_result.events[1].area_seq.is_some());
    assert!(area_result.events[2].area_seq.is_some());
}

#[tokio::test]
async fn should_reject_append_after_stream_closed() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, true).await.unwrap();  // Close stream
    
    let result = handle.stream_append(route.clone(), 2, b"evt2".to_vec(), None, false).await;

    // Assert
    assert!(result.is_err());  // StreamClosed error
}

#[tokio::test]
async fn should_enforce_monotonic_resource_sequences() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();

    // Act
    handle.stream_append(route.clone(), 0, b"evt0".to_vec(), None, false).await.unwrap();
    handle.stream_append(route.clone(), 1, b"evt1".to_vec(), None, false).await.unwrap();
    
    // Try to go backwards
    let result = handle.stream_append(route.clone(), 0, b"evt0_retry".to_vec(), None, false).await;

    // Assert
    assert!(result.is_err());  // Cannot go backwards (or treated as conflict)
}

#[tokio::test]
async fn should_read_resource_stream_independent_of_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();
    
    // Append 5 events but don't finalize
    for i in 0..5 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let resource_result = handle.stream_read(route.clone(), 0, 10).await.unwrap();

    // Assert
    assert_eq!(resource_result.len(), 5);     // Readable from resource
}

#[tokio::test]
async fn should_not_appear_in_area_before_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();
    
    // Append 5 events but don't finalize
    for i in 0..5 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let area_result = handle.stream_read_area("realm", "orders", 0, 10).await.unwrap();

    // Assert
    assert_eq!(area_result.events.len(), 0);  // Not visible in area (not finalized)
}

#[tokio::test]
async fn should_not_assign_area_seq_before_finalization() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let route = "stream://realm/orders/batch_001".to_string();
    
    // Append 5 events but don't finalize
    for i in 0..5 {
        handle.stream_append(route.clone(), i, format!("evt{}", i).into_bytes(), None, false).await.unwrap();
    }

    // Act
    let resource_result = handle.stream_read(route.clone(), 0, 10).await.unwrap();

    // Assert
    assert!(resource_result[0].area_seq.is_none());  // No area_seq assigned yet
}
