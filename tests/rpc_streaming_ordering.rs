//! RPC ReplyInboxActor streaming and ordering tests
//!
//! Tests streaming chunk ordering, buffering, gap detection, and duplicate handling.

use bytes::Bytes;
use fitz::domains::rpc::protocol::RpcResponse;
use fitz::domains::rpc::{InboxMessage, ReplyInboxActor};
use fitz::runtime::actor::{Actor, Context};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use uuid::Uuid;

fn create_inbox() -> ReplyInboxActor {
    ReplyInboxActor::new(RouteFamily::new(1))
}

fn create_context() -> Context<ReplyInboxActor> {
    let router = std::sync::Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/123"));
    Context::new(addr, router)
}

fn create_response(correlation_id: Uuid, seq: u64, stream_end: bool) -> RpcResponse {
    RpcResponse::chunk(
        correlation_id,
        seq,
        Bytes::from(vec![seq as u8]),
        stream_end,
    )
}

#[test]
fn should_accept_single_chunk_response() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let _correlation_id = Uuid::new_v4();
    let correlation_id = Uuid::new_v4();
    let response = RpcResponse::single(correlation_id, Bytes::from(vec![1, 2, 3]));

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
    let correlation_id = Uuid::new_v4();

    // Act - send chunks in order
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, true)),
        &mut ctx,
    );

    // Assert - stream should be cleaned up
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_buffer_out_of_order_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - receive seq 2 before seq 0 and 1
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 3, false)),
        &mut ctx,
    );

    // Assert - chunks buffered, stream still active
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count(&correlation_id), 2);
}

#[test]
fn should_flush_buffer_when_gap_filled() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - receive out of order, then fill gaps
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 3, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, false)),
        &mut ctx,
    );
    assert_eq!(inbox.buffered_count(&correlation_id), 2);

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    assert_eq!(inbox.buffered_count(&correlation_id), 2); // Still buffered

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, false)),
        &mut ctx,
    );

    // Assert - all buffered chunks should be flushed
    assert_eq!(inbox.buffered_count(&correlation_id), 0);
}

#[test]
fn should_cleanup_stream_when_final_chunk_received() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - send chunks with stream_end on last
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    assert_eq!(inbox.active_streams(), 1);

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, true)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_cleanup_when_buffered_final_chunk_flushed() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - receive final chunk before earlier chunks
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, true)),
        &mut ctx,
    );
    assert_eq!(inbox.active_streams(), 1);

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, false)),
        &mut ctx,
    );

    // Assert - stream cleaned up when buffered final chunk flushed
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_drop_duplicate_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - send seq 0 twice
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 1, true)),
        &mut ctx,
    );

    // Assert - should complete successfully (duplicate dropped)
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_multiple_concurrent_streams() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();
    let correlation_id_3 = Uuid::new_v4();

    // Act - start 3 different streams
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_1, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_2, 0, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_3, 0, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 3);
}

#[test]
fn should_isolate_streams_by_correlation_id() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();

    // Act - complete one stream, leave another active
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_1, 0, true)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id_2, 0, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count(&correlation_id_1), 0);
    assert_eq!(inbox.buffered_count(&correlation_id_2), 0);
}

#[test]
fn should_handle_buffer_overflow_by_disconnecting() {
    // Arrange
    let mut inbox = ReplyInboxActor::with_buffer_size(RouteFamily::new(1), 5);
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - send 6 out-of-order chunks (buffer limit is 5)
    for seq in 1..=6 {
        inbox.receive(
            InboxMessage::Response(create_response(correlation_id, seq, false)),
            &mut ctx,
        );
    }

    // Assert - stream should be disconnected (cleaned up)
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_cleanup_stream_on_explicit_cleanup_message() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );
    assert_eq!(inbox.active_streams(), 1);

    // Act
    inbox.receive(InboxMessage::Cleanup { correlation_id }, &mut ctx);

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_large_sequence_gaps() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_context();
    let correlation_id = Uuid::new_v4();

    // Act - receive seq 100 before seq 0
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 100, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );

    // Assert - seq 100 should still be buffered
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count(&correlation_id), 1);
}
