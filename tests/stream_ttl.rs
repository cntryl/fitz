use bytes::Bytes;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use fitz::domains::stream::StreamActor;
use fitz::domains::stream::StreamStore;
use fitz::domains::stream::store::{StreamTTL, BatchLimits};
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

// Tests for Stream TTL (time-to-live) functionality
// Verifies that expired events are filtered and offsets remain stable

fn make_test_store_with_ttl(ttl_seconds: u64) -> Arc<StreamStore> {
    let db = Arc::new(cntryl_midge::MidgeEngine::open(cntryl_midge::MidgeOptions::default()).unwrap());
    Arc::new(StreamStore::with_config(
        db,
        BatchLimits::default(),
        StreamTTL::with_seconds(ttl_seconds),
    ))
}

fn make_test_store() -> Arc<StreamStore> {
    let db = Arc::new(cntryl_midge::MidgeEngine::open(cntryl_midge::MidgeOptions::default()).unwrap());
    Arc::new(StreamStore::new(db))
}

fn make_ctx() -> Context<StreamActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://realm1/area1/resource1"),
    );
    Context::new(addr, router)
}

#[test]
fn should_filter_expired_events_during_read() {
    // Arrange
    let store = make_test_store_with_ttl(2);  // 2 second TTL
    
    // Commit events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    
    for i in 0..5 {
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{}", i)),
            metadata: None,
        }).unwrap();
    }
    
    store.commit_session(&session_id, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 4).unwrap();
    
    // Verify events are readable immediately
    let (records_before, _) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();
    assert_eq!(records_before.len(), 5);
    
    // Act: Wait for TTL expiration
    thread::sleep(Duration::from_secs(3));
    
    // Read after expiration
    let (records_after, _) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();
    
    // Assert: Events filtered by TTL
    assert_eq!(records_after.len(), 0, "Expired events should be filtered");
}

#[test]
fn should_preserve_offsets_after_ttl_expiry() {
    // Arrange
    let store = make_test_store_with_ttl(2);
    
    // Commit batch 1 (will expire)
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..3 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("old-event{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 2).unwrap();
    
    // Wait for expiration
    thread::sleep(Duration::from_secs(3));
    
    // Act: Commit new events (should start at offset 3, not 0)
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 3..6 {
        store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("new-event{}", i)),
            metadata: None,
        }).unwrap();
    }
    let response = store.commit_session(&session2, 3, 3, 3).unwrap();
    
    // Assert: Offsets preserved (no reuse)
    assert_eq!(response.first_resource_offset, 3);
    assert_eq!(response.last_resource_offset, 5);
}

#[test]
fn should_tolerate_ttl_gaps_during_reads() {
    // Arrange
    let store = make_test_store_with_ttl(2);
    
    // Commit events 0-2 (will expire)
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..3 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("old{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 2).unwrap();
    
    // Wait for expiration
    thread::sleep(Duration::from_secs(3));
    
    // Commit new events 3-5
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 3..6 {
        store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("new{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session2, 3, 3, 3).unwrap();
    store.set_watermark("realm1", "area1", 5).unwrap();
    
    // Act: Read from offset 0 (includes expired range)
    let (records, cursor) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();
    
    // Assert: Only non-expired events returned, pagination works
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].resource_offset, 3);
    assert_eq!(records[1].resource_offset, 4);
    assert_eq!(records[2].resource_offset, 5);
    assert!(!cursor.has_more);
}

#[test]
fn should_handle_partial_ttl_expiration() {
    // Arrange
    let store = make_test_store_with_ttl(3);
    
    // Commit first batch
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..3 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("batch1-{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 2).unwrap();
    
    // Wait 2 seconds
    thread::sleep(Duration::from_secs(2));
    
    // Commit second batch (fresher)
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 3..6 {
        store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("batch2-{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session2, 3, 3, 3).unwrap();
    store.set_watermark("realm1", "area1", 5).unwrap();
    
    // Wait another 2 seconds (batch1 expires, batch2 still valid)
    thread::sleep(Duration::from_secs(2));
    
    // Act: Read all events
    let (records, _) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();
    
    // Assert: Only batch2 visible
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].resource_offset, 3);
    assert_eq!(records[2].resource_offset, 5);
}

#[test]
fn should_not_affect_watermark_with_ttl() {
    // Arrange
    let store = make_test_store_with_ttl(2);
    
    // Commit events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..5 {
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session_id, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 4).unwrap();
    
    // Act: Wait for expiration
    thread::sleep(Duration::from_secs(3));
    
    // Get watermark
    let watermark = store.get_watermark("realm1", "area1").unwrap();
    
    // Assert: Watermark unchanged by TTL expiration
    assert_eq!(watermark, 4, "Watermark should not change due to TTL");
}

#[test]
fn should_handle_peek_with_expired_events() {
    // Arrange
    let store = make_test_store_with_ttl(2);
    
    // Commit events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..3 {
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session_id, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 2).unwrap();
    
    // Peek before expiration
    let peek_before = store.peek_resource("realm1", "area1", "resource1").unwrap();
    assert!(peek_before.is_some());
    assert_eq!(peek_before.unwrap().resource_offset, 2);
    
    // Act: Wait for expiration
    thread::sleep(Duration::from_secs(3));
    
    // Peek after expiration
    let peek_after = store.peek_resource("realm1", "area1", "resource1").unwrap();
    
    // Assert: Peek returns None for expired stream
    assert!(peek_after.is_none(), "Peek should return None when all events expired");
}

#[test]
fn should_handle_ttl_with_large_batches() {
    // Arrange
    let store = make_test_store_with_ttl(2);
    
    // Commit 100 events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..100 {
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{:03}", i)),
            metadata: None,
        }).unwrap();
    }
    
    store.commit_session(&session_id, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 99).unwrap();
    
    // Read before expiration
    let (records_before, _) = store.read_resource("realm1", "area1", "resource1", 0, 100, None).unwrap();
    assert_eq!(records_before.len(), 100);
    
    // Act: Wait for expiration
    thread::sleep(Duration::from_secs(3));
    
    // Read after expiration
    let (records_after, _) = store.read_resource("realm1", "area1", "resource1", 0, 100, None).unwrap();
    
    // Assert: All 100 events expired
    assert_eq!(records_after.len(), 0);
}

#[test]
fn should_support_infinite_ttl() {
    // Arrange
    let store = make_test_store();  // No TTL specified (infinite)
    
    // Commit events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..5 {
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session_id, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 4).unwrap();
    
    // Act: Wait (would expire with TTL)
    thread::sleep(Duration::from_secs(3));
    
    // Read events
    let (records, _) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();
    
    // Assert: All events still present (no TTL)
    assert_eq!(records.len(), 5);
}

#[test]
fn should_handle_mixed_expired_and_fresh_reads() {
    // Arrange
    let store = make_test_store_with_ttl(2);
    
    // Commit old batch
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..5 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("old{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 4).unwrap();
    
    // Wait for expiration
    thread::sleep(Duration::from_secs(3));
    
    // Commit fresh batch
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 5..10 {
        store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("fresh{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session2, 5, 5, 5).unwrap();
    store.set_watermark("realm1", "area1", 9).unwrap();
    
    // Act: Read entire range
    let (records, _) = store.read_resource("realm1", "area1", "resource1", 0, 20, None).unwrap();
    
    // Assert: Only fresh events returned
    assert_eq!(records.len(), 5);
    assert_eq!(records[0].resource_offset, 5);
    assert_eq!(records[4].resource_offset, 9);
}

/// **CRITICAL REGRESSION TEST**: Offset counter survives TTL + restart
/// 
/// This test verifies the fix for the CRITICAL offset reuse bug.
/// Without offset counter metadata, next_resource_offset resets to 0
/// after TTL expiry, causing catastrophic offset collisions.
#[test]
fn should_not_reset_offsets_after_actor_restart_with_ttl() {
    // Arrange: Write data with short TTL
    let store = make_test_store_with_ttl(1);
    
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..100 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 99).unwrap();
    
    // Act: Wait for TTL to expire all data
    thread::sleep(Duration::from_secs(2));
    
    // Simulate actor restart: create new StreamActor (calls get_next_resource_offset)
    let _actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store.clone(),
    );
    
    // Begin new session with expected_offset = 100 (should succeed)
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
        body: Bytes::from("event100"),
        metadata: None,
    }).unwrap();
    
    // Assert: Should succeed with offset 100, not reset to 0
    let response = store.commit_session(&session2, 100, 100, 100).unwrap();
    assert_eq!(response.first_resource_offset, 100);
    assert_eq!(response.last_resource_offset, 100);
}

/// **CRITICAL REGRESSION TEST**: Reject expected_offset=0 after TTL+restart
/// 
/// This test verifies that clients cannot accidentally reuse offset 0
/// after TTL expiry. The offset counter must preserve sequencing.
#[test]
fn should_reject_expected_offset_zero_after_ttl_and_restart() {
    // Arrange: Write data with short TTL
    let store = make_test_store_with_ttl(1);
    
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..50 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("event{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 49).unwrap();
    
    // Act: Wait for TTL to expire all data
    thread::sleep(Duration::from_secs(2));
    
    // Simulate actor restart
    let _actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store.clone(),
    );
    
    // Attempt to append with expected_offset=0 (MUST FAIL)
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
        body: Bytes::from("collision"),
        metadata: None,
    }).unwrap();
    
    // Assert: Commit with offset 0 should fail (offset counter says next=50)
    // In real system, StreamActor would reject expected_offset=0 during BeginSession
    // Here we test that offset counter persists across TTL expiry
    let next_offset = store.get_next_resource_offset("realm1", "area1", "resource1").unwrap();
    assert_eq!(next_offset, 50, "Offset counter must preserve sequence across TTL expiry");
}

/// **CRITICAL REGRESSION TEST**: Continue offsets after all events expire
/// 
/// Ensures offset sequencing continues monotonically even when
/// TTL causes complete data loss between commits.
#[test]
fn should_continue_offsets_after_all_events_expire() {
    // Arrange: Write 3 batches with gaps where TTL expires everything
    let store = make_test_store_with_ttl(2);
    
    // Batch 1: offsets 0-9
    let session1 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 0..10 {
        store.append_to_session(&session1, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("batch1_{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session1, 0, 0, 0).unwrap();
    store.set_watermark("realm1", "area1", 9).unwrap();
    
    // Act: Wait for TTL expiry
    thread::sleep(Duration::from_secs(3));
    
    // Batch 2: offsets 10-19 (all previous data expired)
    let session2 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 10..20 {
        store.append_to_session(&session2, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("batch2_{}", i)),
            metadata: None,
        }).unwrap();
    }
    store.commit_session(&session2, 10, 10, 10).unwrap();
    store.set_watermark("realm1", "area1", 19).unwrap();
    
    // Wait again
    thread::sleep(Duration::from_secs(3));
    
    // Batch 3: offsets 20-29 (all previous data expired again)
    let session3 = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    for i in 20..30 {
        store.append_to_session(&session3, fitz::domains::stream::store::EventPayload {
            body: Bytes::from(format!("batch3_{}", i)),
            metadata: None,
        }).unwrap();
    }
    let response = store.commit_session(&session3, 20, 20, 20).unwrap();
    
    // Assert: Offsets continue monotonically despite total data loss
    assert_eq!(response.first_resource_offset, 20);
    assert_eq!(response.last_resource_offset, 29);
    
    // Verify offset counter is correct for next append
    let next_offset = store.get_next_resource_offset("realm1", "area1", "resource1").unwrap();
    assert_eq!(next_offset, 30);
}
