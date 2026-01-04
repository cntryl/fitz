//! RPC ReplyInboxActor streaming and ordering tests
//!
//! Tests streaming chunk ordering, buffering, gap detection, and duplicate handling.

use fitz::domains::rpc::{ReplyInboxActor, InboxMessage};
use fitz::domains::rpc::protocol::RpcResponse;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::actor::{Actor, Context};

fn create_inbox() -> ReplyInboxActor {
    ReplyInboxActor::new(RouteFamily::new(1))
}

fn create_context() -> Context<ReplyInboxActor> {
    let router = std::sync::Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("inbox://session/123"),
    );
    Context::new(addr, router)
}

fn create_response(correlation_id: &str, seq: u64, stream_end: bool) -> RpcResponse {
    RpcResponse::chunk(
        correlation_id.to_string(),
        seq,
        vec![seq as u8],
        stream_end,
    )
}

#[test]
fn should_accept_single_chunk_response() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let response = RpcResponse::single("req-001".to_string(), vec![1, 2, 3]);
    
    // Act
    inbox.receive(InboxMessage::Response(response), &mut ctx);
    
    // Assert - stream should be cleaned up after stream_end
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_in_order_streaming_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - send chunks in order
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 1, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 2, true)), &mut ctx);
    
    // Assert - stream should be cleaned up
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_buffer_out_of_order_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - receive seq 2 before seq 0 and 1
    inbox.receive(InboxMessage::Response(create_response("req-001", 2, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 3, false)), &mut ctx);
    
    // Assert - chunks buffered, stream still active
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count("req-001"), 2);
}

#[test]
fn should_flush_buffer_when_gap_filled() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - receive out of order, then fill gaps
    inbox.receive(InboxMessage::Response(create_response("req-001", 3, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 2, false)), &mut ctx);
    assert_eq!(inbox.buffered_count("req-001"), 2);
    
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    assert_eq!(inbox.buffered_count("req-001"), 2); // Still buffered
    
    inbox.receive(InboxMessage::Response(create_response("req-001", 1, false)), &mut ctx);
    
    // Assert - all buffered chunks should be flushed
    assert_eq!(inbox.buffered_count("req-001"), 0);
}

#[test]
fn should_cleanup_stream_when_final_chunk_received() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - send chunks with stream_end on last
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    assert_eq!(inbox.active_streams(), 1);
    
    inbox.receive(InboxMessage::Response(create_response("req-001", 1, true)), &mut ctx);
    
    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_cleanup_when_buffered_final_chunk_flushed() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - receive final chunk before earlier chunks
    inbox.receive(InboxMessage::Response(create_response("req-001", 2, true)), &mut ctx);
    assert_eq!(inbox.active_streams(), 1);
    
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 1, false)), &mut ctx);
    
    // Assert - stream cleaned up when buffered final chunk flushed
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_drop_duplicate_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - send seq 0 twice
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 1, true)), &mut ctx);
    
    // Assert - should complete successfully (duplicate dropped)
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_multiple_concurrent_streams() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - start 3 different streams
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-002", 0, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-003", 0, false)), &mut ctx);
    
    // Assert
    assert_eq!(inbox.active_streams(), 3);
}

#[test]
fn should_isolate_streams_by_correlation_id() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - complete one stream, leave another active
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, true)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-002", 0, false)), &mut ctx);
    
    // Assert
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count("req-001"), 0);
    assert_eq!(inbox.buffered_count("req-002"), 0);
}

#[test]
fn should_handle_buffer_overflow_by_disconnecting() {
    // Arrange
    let mut inbox = ReplyInboxActor::with_buffer_size(RouteFamily::new(1), 5);
    let mut ctx = create_context();
    
    // Act - send 6 out-of-order chunks (buffer limit is 5)
    for seq in 1..=6 {
        inbox.receive(InboxMessage::Response(create_response("req-001", seq, false)), &mut ctx);
    }
    
    // Assert - stream should be disconnected (cleaned up)
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_cleanup_stream_on_explicit_cleanup_message() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    assert_eq!(inbox.active_streams(), 1);
    
    // Act
    inbox.receive(InboxMessage::Cleanup { correlation_id: "req-001".to_string() }, &mut ctx);
    
    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_large_sequence_gaps() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    
    // Act - receive seq 100 before seq 0
    inbox.receive(InboxMessage::Response(create_response("req-001", 100, false)), &mut ctx);
    inbox.receive(InboxMessage::Response(create_response("req-001", 0, false)), &mut ctx);
    
    // Assert - seq 100 should still be buffered
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count("req-001"), 1);
}
