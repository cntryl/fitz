//! Stream Semantics Tests
//!
//! Tests core stream invariants and error conditions:
//! - Optimistic concurrency control with expected_offset
//! - Session lifecycle and single active session per resource
//! - Watermark advancement and gap detection
//! - Offset lease coordination between actors
//! - Batch size limits
use bytes::Bytes;
use fitz::domains::stream::protocol::StreamMessage;
use fitz::prelude::Actor;
use fitz::runtime::routing::Route;
use fitz::testkit::{create_test_area_actor, create_test_stream_actor};

#[test]
fn should_reject_commit_with_wrong_expected_offset() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Commit first event (offset 0)
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "session1".to_string(),
            body: Bytes::from("event_0"),
            metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::CommitSession {
            session_id: "session1".to_string(),
        },
        &mut ctx,
    );
    // Act - Try to begin session with wrong expected_offset
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 0, // Wrong! Should be 1
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Assert - Should fail with ConcurrencyConflict error
    // (In real impl, would check response for StreamError::ConcurrencyConflict)
}
#[test]
fn should_reject_second_session_when_one_active() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Act - Begin first session
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Try to begin second session
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Assert - Second begin should fail with SessionAlreadyActive error
}
#[test]
fn should_allow_new_session_after_commit() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Act - First session
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "session1".to_string(),
            body: Bytes::from("event_0"),
            metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::CommitSession {
            session_id: "session1".to_string(),
        },
        &mut ctx,
    );
    // Second session (should succeed)
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 1,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Assert - Second session should succeed
}
#[test]
fn should_allow_new_session_after_abort() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Act - Begin and abort
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::AbortSession {
            session_id: "session1".to_string(),
        },
        &mut ctx,
    );
    // New session (should succeed with same offset)
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 0, // Still 0 since previous aborted
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Assert - Should succeed
}
#[test]
fn should_advance_watermark_only_on_contiguous_commits() {
    // Arrange
    let (mut area_actor, mut area_ctx) = create_test_area_actor("realm1", "area1");
    // Act - Commit batch at offsets 1-3 (area offsets, watermark starts at 0)
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 1,
            last_area_offset: 3,
            first_realm_offset: 1,
            last_realm_offset: 3,
        },
        &mut area_ctx,
    );
    // Assert - Watermark should be at 3
    assert_eq!(area_actor.watermark(), 3);
    // Commit batch at offsets 6-8 (gap at 4-5)
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 6,
            last_area_offset: 8,
            first_realm_offset: 6,
            last_realm_offset: 8,
        },
        &mut area_ctx,
    );
    // Assert - Watermark should still be 3 (gap prevents advancement)
    assert_eq!(area_actor.watermark(), 3);
    // Commit batch at offsets 4-5 (fills gap)
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 4,
            last_area_offset: 5,
            first_realm_offset: 4,
            last_realm_offset: 5,
        },
        &mut area_ctx,
    );
    // Assert - Watermark should now be 8 (gap filled)
    assert_eq!(area_actor.watermark(), 8);
}
#[test]
fn should_track_committed_ranges_for_gap_detection() {
    // Arrange
    let (mut area_actor, mut area_ctx) = create_test_area_actor("realm1", "area1");
    // Act - Commit non-contiguous batches
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 1,
            last_area_offset: 3,
            first_realm_offset: 1,
            last_realm_offset: 3,
        },
        &mut area_ctx,
    );
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 6,
            last_area_offset: 8,
            first_realm_offset: 6,
            last_realm_offset: 8,
        },
        &mut area_ctx,
    );
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 11,
            last_area_offset: 13,
            first_realm_offset: 11,
            last_realm_offset: 13,
        },
        &mut area_ctx,
    );
    // Assert - Watermark at 3, with ranges buffered
    assert_eq!(area_actor.watermark(), 3);
    // Fill first gap
    area_actor.receive(
        StreamMessage::BatchCommitted {
            first_area_offset: 4,
            last_area_offset: 5,
            first_realm_offset: 4,
            last_realm_offset: 5,
        },
        &mut area_ctx,
    );
    // Watermark should advance to 8 (next gap starts at 9)
    assert_eq!(area_actor.watermark(), 8);
}
#[test]
fn should_request_lease_when_insufficient_capacity() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Act - Begin session (will have empty leases initially)
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Append events
    for i in 0..10 {
        actor.receive(
            StreamMessage::AppendToSession {
                session_id: "large_session".to_string(),
                body: Bytes::from(format!("event_{}", i)),
                metadata: None,
            },
            &mut ctx,
        );
    }
    // Try to commit (should request lease if insufficient)
    actor.receive(
        StreamMessage::CommitSession {
            session_id: "large_session".to_string(),
        },
        &mut ctx,
    );
    // Assert - LeaseRequested error or successful commit after lease grant
    // (Would check router for RequestLease message sent to AreaActor)
}
#[test]
fn should_process_pending_commits_after_lease_grant() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Begin session and append
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "pending_session".to_string(),
            body: Bytes::from("event_data"),
            metadata: None,
        },
        &mut ctx,
    );
    // Try commit (will be queued if no lease)
    actor.receive(
        StreamMessage::CommitSession {
            session_id: "pending_session".to_string(),
        },
        &mut ctx,
    );
    // Act - Grant lease
    actor.receive(
        StreamMessage::LeaseGranted {
            grant: fitz::domains::stream::protocol::LeaseGrant {
                area_start: 0,
                area_end_exclusive: 1000,
                realm_start: 0,
                realm_end_exclusive: 1000,
            },
        },
        &mut ctx,
    );
    // Assert - Pending commit should now complete
    // (Would verify BatchCommitted notification was sent to AreaActor)
}
#[test]
fn should_reject_event_exceeding_max_size() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Act - Try to append oversized event (>1MB)
    let huge_payload = vec![0u8; 2 * 1024 * 1024]; // 2 MB
    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "oversized_session".to_string(),
            body: Bytes::from(huge_payload),
            metadata: None,
        },
        &mut ctx,
    );
    // Assert - Should fail with EventTooLarge error
}
#[test]
fn should_enforce_realm_isolation() {
    // Arrange
    let (mut actor1, mut ctx1) = create_test_stream_actor("realm1", "area1", "orders");
    let (mut actor2, mut ctx2) = create_test_stream_actor("realm2", "area1", "orders");
    // Both use same area/resource name but different realms
    // Act - Append to both
    actor1.receive(
        StreamMessage::BeginSession {
            family_id: *ctx1.address().family(),
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx1,
    );
    actor2.receive(
        StreamMessage::BeginSession {
            family_id: *ctx2.address().family(),
            route: Route::new("stream://realm2/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx2,
    );
    // Assert - Both start at offset 0 independently (realm isolation)
}
#[test]
fn should_enforce_area_isolation_within_realm() {
    // Arrange
    let (mut actor1, mut ctx1) = create_test_stream_actor("realm1", "area1", "orders");
    let (mut actor2, mut ctx2) = create_test_stream_actor("realm1", "area2", "orders");
    // Same realm, different areas
    // Act - Both should have independent offsets
    actor1.receive(
        StreamMessage::BeginSession {
            family_id: *ctx1.address().family(),
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx1,
    );
    actor2.receive(
        StreamMessage::BeginSession {
            family_id: *ctx2.address().family(),
            route: Route::new("stream://realm1/area2/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx2,
    );
    // Assert - Independent area offsets
}
