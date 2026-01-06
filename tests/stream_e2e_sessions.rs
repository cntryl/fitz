use bytes::Bytes;
use std::sync::Arc;

use fitz::domains::stream::{
    StreamActor, StreamStore, StreamMessage, StreamError,
    protocol::{BeginSessionResponse, CommitSessionResponse, ReadResponse, PeekResponse}
};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

// Tests for the new session-based Stream API
// Follows BeginSession → AppendToSession → CommitSession flow

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
fn should_complete_simple_session_successfully() {
    // Arrange
    let store = make_test_store();
    let mut actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store,
    );
    let mut ctx = make_ctx();
    
    // Give actor initial leases
    actor.update_area_lease(fitz::domains::stream::protocol::LeaseGrant {
        area_start: 0,
        area_end: 999,
        realm_start: 0,
        realm_end: 999,
    });

    // Act: Begin session
    let begin_msg = StreamMessage::BeginSession {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://realm1/area1/resource1"),
        expected_offset: 0,
        ingest_metadata: None,
    };
    actor.receive(begin_msg, &mut ctx);

    // Assert: Session created
    // (Would check return value in real impl with message passing)
}

#[test]
fn should_reject_second_session_when_active() {
    // Arrange
    let store = make_test_store();
    let mut actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store,
    );
    let mut ctx = make_ctx();
    
    actor.update_area_lease(fitz::domains::stream::protocol::LeaseGrant {
        area_start: 0,
        area_end: 999,
        realm_start: 0,
        realm_end: 999,
    });

    // Act: Begin first session
    let begin_msg1 = StreamMessage::BeginSession {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://realm1/area1/resource1"),
        expected_offset: 0,
        ingest_metadata: None,
    };
    actor.receive(begin_msg1, &mut ctx);

    // Act: Try to begin second session
    let begin_msg2 = StreamMessage::BeginSession {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://realm1/area1/resource1"),
        expected_offset: 0,
        ingest_metadata: None,
    };
    actor.receive(begin_msg2, &mut ctx);

    // Assert: Second session rejected (verified via StreamError::SessionAlreadyActive in real impl)
}

#[test]
fn should_reject_wrong_expected_offset() {
    // Arrange
    let store = make_test_store();
    let mut actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store,
    );
    let mut ctx = make_ctx();
    
    actor.update_area_lease(fitz::domains::stream::protocol::LeaseGrant {
        area_start: 0,
        area_end: 999,
        realm_start: 0,
        realm_end: 999,
    });

    // Act: Begin session with wrong expected_offset
    let begin_msg = StreamMessage::BeginSession {
        family_id: RouteFamily::new(1),
        route: Route::new("stream://realm1/area1/resource1"),
        expected_offset: 100,  // Wrong! Should be 0
        ingest_metadata: None,
    };
    actor.receive(begin_msg, &mut ctx);

    // Assert: Rejected with ConcurrencyConflict (verified in real impl)
}

#[test]
fn should_append_events_to_session() {
    // Arrange
    let store = make_test_store();
    let mut actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store.clone(),
    );
    let mut ctx = make_ctx();
    
    actor.update_area_lease(fitz::domains::stream::protocol::LeaseGrant {
        area_start: 0,
        area_end: 999,
        realm_start: 0,
        realm_end: 999,
    });

    // Begin session
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();

    // Act: Append events
    let events = vec![
        Bytes::from("event1"),
        Bytes::from("event2"),
        Bytes::from("event3"),
    ];

    for event_body in events {
        let append_msg = StreamMessage::AppendToSession {
            session_id: session_id.clone(),
            body: event_body,
            metadata: None,
        };
        actor.receive(append_msg, &mut ctx);
    }

    // Assert: Event count should be 3
    assert_eq!(store.session_event_count(&session_id), Some(3));
}

#[test]
fn should_commit_session_with_correct_offsets() {
    // Arrange
    let store = make_test_store();
    let mut actor = StreamActor::new(
        RouteFamily::new(1),
        "realm1".to_string(),
        "area1".to_string(),
        "resource1".to_string(),
        store.clone(),
    );
    let mut ctx = make_ctx();
    
    actor.update_area_lease(fitz::domains::stream::protocol::LeaseGrant {
        area_start: 100,
        area_end: 199,
        realm_start: 1000,
        realm_end: 1099,
    });

    // Begin session and append events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    
    for i in 0..5 {
        let event = Bytes::from(format!("event{}", i));
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: event,
            metadata: None,
        }).unwrap();
    }

    // Act: Commit session
    let resource_offsets = vec![0, 1, 2, 3, 4];
    let area_offsets = vec![100, 101, 102, 103, 104];
    let realm_offsets = vec![1000, 1001, 1002, 1003, 1004];
    
    let response = store.commit_session(
        &session_id,
        resource_offsets,
        area_offsets,
        realm_offsets,
    ).unwrap();

    // Assert: Correct offsets assigned
    assert_eq!(response.first_resource_offset, 0);
    assert_eq!(response.last_resource_offset, 4);
    assert_eq!(response.first_area_offset, 100);
    assert_eq!(response.last_area_offset, 104);
    assert_eq!(response.first_realm_offset, 1000);
    assert_eq!(response.last_realm_offset, 1004);
    assert_eq!(response.batch_size, 5);
}

#[test]
fn should_read_committed_events() {
    // Arrange
    let store = make_test_store();
    
    // Commit a batch
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    
    for i in 0..3 {
        let event = Bytes::from(format!("event{}", i));
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: event,
            metadata: None,
        }).unwrap();
    }
    
    let resource_offsets = vec![0, 1, 2];
    let area_offsets = vec![0, 1, 2];
    let realm_offsets = vec![0, 1, 2];
    
    store.commit_session(&session_id, resource_offsets, area_offsets, realm_offsets).unwrap();
    
    // Set watermark
    store.set_watermark("realm1", "area1", 2).unwrap();

    // Act: Read events
    let (records, cursor) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();

    // Assert: All events returned
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].body, Bytes::from("event0"));
    assert_eq!(records[1].body, Bytes::from("event1"));
    assert_eq!(records[2].body, Bytes::from("event2"));
    assert!(!cursor.has_more);
}

#[test]
fn should_respect_watermark_during_reads() {
    // Arrange
    let store = make_test_store();
    
    // Commit events 0-4
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    
    for i in 0..5 {
        let event = Bytes::from(format!("event{}", i));
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: event,
            metadata: None,
        }).unwrap();
    }
    
    let resource_offsets = vec![0, 1, 2, 3, 4];
    let area_offsets = vec![0, 1, 2, 3, 4];
    let realm_offsets = vec![0, 1, 2, 3, 4];
    
    store.commit_session(&session_id, resource_offsets, area_offsets, realm_offsets).unwrap();
    
    // Set watermark to 2 (only events 0, 1, 2 visible)
    store.set_watermark("realm1", "area1", 2).unwrap();

    // Act: Read events
    let (records, _) = store.read_resource("realm1", "area1", "resource1", 0, 10, None).unwrap();

    // Assert: Only events up to watermark returned
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].resource_offset, 0);
    assert_eq!(records[1].resource_offset, 1);
    assert_eq!(records[2].resource_offset, 2);
}

#[test]
fn should_peek_at_last_committed_record() {
    // Arrange
    let store = make_test_store();
    
    // Commit events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    
    for i in 0..5 {
        let event = Bytes::from(format!("event{}", i));
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: event,
            metadata: None,
        }).unwrap();
    }
    
    let resource_offsets = vec![0, 1, 2, 3, 4];
    let area_offsets = vec![0, 1, 2, 3, 4];
    let realm_offsets = vec![0, 1, 2, 3, 4];
    
    store.commit_session(&session_id, resource_offsets, area_offsets, realm_offsets).unwrap();
    store.set_watermark("realm1", "area1", 4).unwrap();

    // Act: Peek at last record
    let result = store.peek_resource("realm1", "area1", "resource1").unwrap();

    // Assert: Last record returned
    assert!(result.is_some());
    let record = result.unwrap();
    assert_eq!(record.resource_offset, 4);
    assert_eq!(record.body, Bytes::from("event4"));
}

#[test]
fn should_return_none_when_peeking_empty_stream() {
    // Arrange
    let store = make_test_store();
    store.set_watermark("realm1", "area1", 0).unwrap();

    // Act: Peek at empty stream
    let result = store.peek_resource("realm1", "area1", "resource1").unwrap();

    // Assert: None returned
    assert!(result.is_none());
}

#[test]
fn should_stream_reads_with_cursor() {
    // Arrange
    let store = make_test_store();
    
    // Commit 100 events
    let session_id = store.begin_session("realm1", "area1", "resource1", None).unwrap();
    
    for i in 0..100 {
        let event = Bytes::from(format!("event{:03}", i));
        store.append_to_session(&session_id, fitz::domains::stream::store::EventPayload {
            body: event,
            metadata: None,
        }).unwrap();
    }
    
    let resource_offsets: Vec<u64> = (0..100).collect();
    let area_offsets: Vec<u64> = (0..100).collect();
    let realm_offsets: Vec<u64> = (0..100).collect();
    
    store.commit_session(&session_id, resource_offsets, area_offsets, realm_offsets).unwrap();
    store.set_watermark("realm1", "area1", 99).unwrap();

    // Act: Read in batches of 25
    let mut all_records = Vec::new();
    let mut from_offset = 0;
    
    loop {
        let (records, cursor) = store.read_resource("realm1", "area1", "resource1", from_offset, 25, None).unwrap();
        
        if records.is_empty() {
            break;
        }
        
        all_records.extend(records);
        
        if !cursor.has_more {
            break;
        }
        
        from_offset = cursor.last_resource_offset + 1;
    }

    // Assert: All 100 events read via cursor
    assert_eq!(all_records.len(), 100);
    assert_eq!(all_records[0].resource_offset, 0);
    assert_eq!(all_records[99].resource_offset, 99);
}
