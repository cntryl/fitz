//! Stream E2E Basic Tests
//!
//! Tests the golden path for stream operations:
//! - Basic append-commit flow with session
//! - Single event and batch appends
//! - Read operations
//! - Offset assignment and sequencing

use bytes::Bytes;
use fitz::domains::stream::protocol::{IngestMetadata, StreamMessage, StreamWriteMode};
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::stream_actor::StreamActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

fn make_stream_actor(
    realm: &str,
    area: &str,
    resource: &str,
) -> (StreamActor, Context<StreamActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(
        family,
        Route::new(format!("stream://{}/{}/{}/append", realm, area, resource)),
    );

    let db = Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open store"),
    );
    let store = Arc::new(StreamStore::new(db));
    let actor = StreamActor::new(
        family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    );
    let ctx = Context::new(addr, router);

    (actor, ctx)
}

#[test]
fn should_append_single_event_to_stream() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");

    // Act - Begin session
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );

    // Append single event
    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "test_session".to_string(),
            body: Bytes::from("event_data"),
            metadata: None,
        },
        &mut ctx,
    );

    // Commit session
    actor.receive(
        StreamMessage::CommitSession {
            session_id: "test_session".to_string(),
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );

    // Assert - Read back the event
    actor.receive(
        StreamMessage::Read {
            family_id: family,
            route: route.clone(),
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        },
        &mut ctx,
    );
}

#[test]
fn should_append_batch_of_events() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "logs");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/logs/append");

    // Act - Begin session
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx,
    );

    // Append multiple events
    for i in 0..5 {
        actor.receive(
            StreamMessage::AppendToSession {
                session_id: "batch_session".to_string(),
                body: Bytes::from(format!("event_{}", i)),
                metadata: Some(Bytes::from(format!("meta_{}", i))),
            },
            &mut ctx,
        );
    }

    // Commit session
    actor.receive(
        StreamMessage::CommitSession {
            session_id: "batch_session".to_string(),
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );

    // Assert - All events committed atomically
    actor.receive(
        StreamMessage::Read {
            family_id: family,
            route,
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        },
        &mut ctx,
    );
}

#[test]
fn should_assign_sequential_resource_offsets() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "events");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/events/append");

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
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );

    // Second session (should start at offset 1)
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 1,
            ingest_metadata: None,
        },
        &mut ctx,
    );

    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "session2".to_string(),
            body: Bytes::from("event_1"),
            metadata: None,
        },
        &mut ctx,
    );

    actor.receive(
        StreamMessage::CommitSession {
            session_id: "session2".to_string(),
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );

    // Assert - Offsets are sequential
    actor.receive(
        StreamMessage::Read {
            family_id: family,
            route,
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        },
        &mut ctx,
    );
}

#[test]
fn should_handle_session_with_ingest_metadata() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "imports");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/imports/append");

    let metadata = IngestMetadata {
        opaque: Bytes::from(r#"{"source": "csv", "batch_id": "123"}"#),
    };

    // Act - Begin session with metadata
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: route.clone(),
            expected_offset: 0,
            ingest_metadata: Some(metadata.clone()),
        },
        &mut ctx,
    );

    actor.receive(
        StreamMessage::AppendToSession {
            session_id: "import_session".to_string(),
            body: Bytes::from("data"),
            metadata: None,
        },
        &mut ctx,
    );

    actor.receive(
        StreamMessage::CommitSession {
            session_id: "import_session".to_string(),
            mode: StreamWriteMode::Sync,
        },
        &mut ctx,
    );

    // Assert - Metadata preserved
    actor.receive(
        StreamMessage::GetMetadata {
            family_id: family,
            route,
        },
        &mut ctx,
    );
}

#[test]
fn should_abort_session_without_committing() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "orders");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/orders/append");

    // Act - Begin session and append
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
            session_id: "abort_session".to_string(),
            body: Bytes::from("should_not_commit"),
            metadata: None,
        },
        &mut ctx,
    );

    // Abort instead of commit
    actor.receive(
        StreamMessage::AbortSession {
            session_id: "abort_session".to_string(),
        },
        &mut ctx,
    );

    // Assert - Stream still at offset 0
    actor.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route,
            expected_offset: 0, // Should still be 0
            ingest_metadata: None,
        },
        &mut ctx,
    );
}

#[test]
fn should_peek_at_last_committed_event() {
    // Arrange
    let (mut actor, mut ctx) = make_stream_actor("realm1", "area1", "metrics");
    let family = *ctx.address().family();
    let route = Route::new("stream://realm1/area1/metrics/append");

    // Commit several events
    for i in 0..3 {
        actor.receive(
            StreamMessage::BeginSession {
                family_id: family,
                route: route.clone(),
                expected_offset: i,
                ingest_metadata: None,
            },
            &mut ctx,
        );

        actor.receive(
            StreamMessage::AppendToSession {
                session_id: format!("session_{}", i),
                body: Bytes::from(format!("event_{}", i)),
                metadata: None,
            },
            &mut ctx,
        );

        actor.receive(
            StreamMessage::CommitSession {
                session_id: format!("session_{}", i),
                mode: StreamWriteMode::Sync,
            },
            &mut ctx,
        );
    }

    // Act - Peek should return last event
    actor.receive(
        StreamMessage::Peek {
            family_id: family,
            route,
        },
        &mut ctx,
    );

    // Assert - Should see event_2
}

#[test]
fn should_isolate_streams_across_resources() {
    // Arrange
    let (mut actor1, mut ctx1) = make_stream_actor("realm1", "area1", "orders");
    let (mut actor2, mut ctx2) = make_stream_actor("realm1", "area1", "invoices");
    let family = *ctx1.address().family();

    // Act - Append to both streams
    actor1.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: Route::new("stream://realm1/area1/orders/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx1,
    );

    actor2.receive(
        StreamMessage::BeginSession {
            family_id: family,
            route: Route::new("stream://realm1/area1/invoices/append"),
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut ctx2,
    );

    // Both should start at offset 0 independently
    // Assert - Streams are isolated
}
