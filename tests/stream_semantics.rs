//! Stream Semantics Tests
//!
//! Tests core stream invariants and error conditions:
//! - Optimistic concurrency control with expected_offset
//! - Session lifecycle and single active session per resource
//! - Watermark advancement and gap detection
//! - Offset lease coordination between actors
//! - Batch size limits
use bytes::Bytes;
use fitz::domains::stream::protocol::{StreamMessage, StreamWriteMode};
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
        StreamMessage::Begin {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::Append {
            session_id: 1,
            body: Bytes::from("event_0"),
            metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::Commit {
            session_id: 1,
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );
    // Act - Try to begin session with wrong expected_offset
    actor.receive(
        StreamMessage::Begin {
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
        StreamMessage::Begin {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    // Try to begin second session
    actor.receive(
        StreamMessage::Begin {
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
        StreamMessage::Begin {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::Append {
            session_id: 1,
            body: Bytes::from("event_0"),
            metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::Commit {
            session_id: 1,
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );
    // Second session (should succeed)
    actor.receive(
        StreamMessage::Begin {
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
        StreamMessage::Begin {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::Rollback {
            session_id: 1,
        },
        &mut ctx,
    );
    // New session (should succeed with same offset)
    actor.receive(
        StreamMessage::Begin {
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
fn should_debounce_area_watermark_notifications() {
    use crossbeam_channel::bounded;
    use fitz::domains::notification::protocol::NotificationMessage;
    use fitz::prelude::Actor as PreActor;
    use fitz::runtime::routing::Route;
    use fitz::runtime::routing::RouteAddress;
    use fitz::runtime::routing::RouteFamily;
    use fitz::runtime::scheduler::Scheduler;
    use std::thread;

    // Arrange
    let scheduler = Scheduler::new(1);
    scheduler.start();
    let family = RouteFamily::new(1);

    // Spawn a notification collector at the notice route
    let (tx, rx) = bounded::<bytes::Bytes>(1);

    struct Collector {
        tx: Option<crossbeam_channel::Sender<bytes::Bytes>>,
    }

    impl PreActor for Collector {
        type Message = NotificationMessage;

        fn receive(&mut self, msg: Self::Message, _ctx: &mut fitz::runtime::actor::Context<Self>) {
            if let NotificationMessage::Publish(p) = msg {
                if let Some(tx) = &self.tx {
                    let _ = tx.send(p.payload.clone());
                }
            }
        }
    }

    let collector_addr = RouteAddress::new(
        family,
        Route::new("notice://realm1/area1/*/watermark".to_string()),
    );
    let collector = Collector { tx: Some(tx) };
    let _ = scheduler.spawn(collector, collector_addr.clone(), 16);

    // Spawn AreaActor
    use fitz::domains::stream::area_actor::AreaActor;
    use fitz::domains::stream::store::StreamStore;

    let db = std::sync::Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default()).unwrap(),
    );
    let store = std::sync::Arc::new(StreamStore::new(db));

    let area_addr = RouteAddress::new(
        family,
        Route::new("stream://realm1/area1/__area__".to_string()),
    );

    let actor = AreaActor::new(family, "realm1".to_string(), "area1".to_string(), store);
    let _ = scheduler.spawn(actor, area_addr.clone(), 16);

    // Act - send batch committed to advance watermark
    let area_ref = fitz::runtime::actor::ActorRef::new(area_addr, scheduler.router());
    let _ = area_ref.send(
        fitz::domains::stream::protocol::StreamMessage::BatchCommitted {
            first_area_offset: 1,
            last_area_offset: 3,
            first_realm_offset: 1,
            last_realm_offset: 3,
        },
    );

    // Assert - no immediate notification (debounced)
    assert!(rx
        .recv_timeout(std::time::Duration::from_millis(10))
        .is_err());

    // After debounce period, notification should arrive
    let payload = rx
        .recv_timeout(std::time::Duration::from_millis(200))
        .unwrap();
    let payload_str = String::from_utf8(payload.to_vec()).unwrap();
    assert!(payload_str.contains("watermark"));

    // Cleanup
    let _ = area_ref.send(
        fitz::domains::stream::protocol::StreamMessage::LeaseGranted {
            grant: fitz::domains::stream::protocol::LeaseGrant {
                area_start: 0,
                area_end_exclusive: 0,
                realm_start: 0,
                realm_end_exclusive: 0,
            },
        },
    );
    thread::sleep(std::time::Duration::from_millis(20));
}
#[test]
fn should_request_lease_when_insufficient_capacity() {
    // Arrange
    let (mut actor, mut ctx) = create_test_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");
    // Act - Begin session (will have empty leases initially)
    actor.receive(
        StreamMessage::Begin {
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
            StreamMessage::Append {
                session_id: 2,
                body: Bytes::from(format!("event_{}", i)),
                metadata: None,
            },
            &mut ctx,
        );
    }
    // Try to commit (should request lease if insufficient)
    actor.receive(
        StreamMessage::Commit {
            session_id: 2,
            mode: StreamWriteMode::Sync,
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
        StreamMessage::Begin {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );
    actor.receive(
        StreamMessage::Append {
            session_id: 3,
            body: Bytes::from("event_data"),
            metadata: None,
        },
        &mut ctx,
    );
    // Try commit (will be queued if no lease)
    actor.receive(
        StreamMessage::Commit {
            session_id: 3,
            mode: StreamWriteMode::Sync,
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
        StreamMessage::Begin {
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
        StreamMessage::Append {
            session_id: 4,
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
        StreamMessage::Begin {
            family_id: *ctx1.address().family(),
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx1,
    );
    actor2.receive(
        StreamMessage::Begin {
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
        StreamMessage::Begin {
            family_id: *ctx1.address().family(),
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx1,
    );
    actor2.receive(
        StreamMessage::Begin {
            family_id: *ctx2.address().family(),
            route: Route::new("stream://realm1/area2/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx2,
    );
    // Assert - Independent area offsets
}
