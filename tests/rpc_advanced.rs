//! RPC domain advanced tests - Tier 2
//!
//! Advanced RPC scenarios covering:
//! - Lease management and tracking
//! - Fault tolerance and timeout handling
//! - Streaming response ordering and buffering
//! - Gap detection and duplicate handling

use bytes::Bytes;
use fitz::domains::rpc::{InboxMessage, RpcError, RpcErrorCode, RpcRequest, RpcResponse as RpcResponseMsg, RpcRouteActor, ReplyInboxActor};
use fitz::runtime::actor::{Actor, Context};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::time::Duration;
use uuid::Uuid;

// ============================================================================
//                         LEASE & FAULT TOLERANCE HELPERS
// ============================================================================

fn create_actor_with_timeout(timeout_ms: u64) -> RpcRouteActor {
    RpcRouteActor::with_timeout(RouteFamily::new(1), 1000, Duration::from_millis(timeout_ms))
}

fn create_request(correlation_id: Uuid) -> RpcRequest {
    RpcRequest::new(
        RouteFamily::new(1),
        correlation_id,
        Route::new("rpc://test/area/resource/operation"),
        Route::new("inbox://session/123"),
        Bytes::from(vec![1, 2, 3]),
    )
}

fn create_worker_addr(id: u64) -> RouteAddress {
    RouteAddress::new(
        RouteFamily::new(1),
        Route::new(format!("worker://test/worker{}", id)),
    )
}

// ============================================================================
//                         STREAMING & ORDERING HELPERS
// ============================================================================

fn create_inbox() -> ReplyInboxActor {
    ReplyInboxActor::new(RouteFamily::new(1))
}

fn create_response(correlation_id: Uuid, seq: u64, stream_end: bool) -> RpcResponseMsg {
    RpcResponseMsg::chunk(
        correlation_id,
        seq,
        Bytes::from(vec![seq as u8]),
        stream_end,
    )
}

fn create_rpc_context() -> Context<RpcRouteActor> {
    let router = std::sync::Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://test/area/resource/operation"),
    );
    Context::new(addr, router)
}

fn create_inbox_context() -> Context<ReplyInboxActor> {
    let router = std::sync::Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/123"));
    Context::new(addr, router)
}

// ============================================================================
//                      LEASE & FAULT TOLERANCE TESTS
// ============================================================================

#[test]
fn should_track_active_leases_when_dispatching_request() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_rpc_context();
    let worker_addr = create_worker_addr(1);

    actor.receive(
        fitz::domains::rpc::RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Act
    let request = create_request(Uuid::new_v4());
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);

    // Assert
    assert_eq!(actor.active_leases(), 1);
}

#[test]
fn should_release_lease_when_receiving_stream_end_response() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_rpc_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id = Uuid::new_v4();

    actor.receive(
        fitz::domains::rpc::RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );
    let request = create_request(correlation_id);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);

    // Act
    let response = RpcResponseMsg::single(correlation_id, Bytes::from(vec![4, 5, 6]));
    actor.receive(fitz::domains::rpc::RpcMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(actor.active_leases(), 0);
}

#[test]
fn should_release_lease_when_receiving_ack() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_rpc_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id = Uuid::new_v4();

    actor.receive(
        fitz::domains::rpc::RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );
    let request = create_request(correlation_id);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);

    // Act
    actor.receive(
        fitz::domains::rpc::RpcMessage::Ack { correlation_id },
        &mut ctx,
    );

    // Assert
    assert_eq!(actor.active_leases(), 0);
}

#[test]
fn should_allow_worker_to_take_next_request_after_lease_released() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_rpc_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();

    actor.receive(
        fitz::domains::rpc::RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );
    let req1 = create_request(correlation_id_1);
    let req2 = create_request(correlation_id_2);

    // Act
    actor.receive(fitz::domains::rpc::RpcMessage::Request(req1), &mut ctx);
    assert_eq!(actor.pending_count(), 0);

    // Second request should be queued
    actor.receive(fitz::domains::rpc::RpcMessage::Request(req2), &mut ctx);
    assert_eq!(actor.pending_count(), 1);

    // Release first request
    let response = RpcResponseMsg::single(correlation_id_1, Bytes::from(vec![]));
    actor.receive(fitz::domains::rpc::RpcMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_handle_backpressure_when_queue_full() {
    // Arrange
    let mut actor = RpcRouteActor::with_capacity(RouteFamily::new(1), 2);
    let mut ctx = create_rpc_context();

    // Act
    actor.receive(
        fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())),
        &mut ctx,
    );
    actor.receive(
        fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())),
        &mut ctx,
    );
    actor.receive(
        fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())),
        &mut ctx,
    );

    // Assert
    assert_eq!(actor.pending_count(), 2);
}

#[test]
fn should_drop_late_response_after_lease_expired() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_rpc_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id = Uuid::new_v4();

    actor.receive(
        fitz::domains::rpc::RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );
    let request = create_request(correlation_id);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);

    // Release lease manually
    actor.receive(
        fitz::domains::rpc::RpcMessage::Ack { correlation_id },
        &mut ctx,
    );

    // Act
    let response = RpcResponseMsg::single(correlation_id, Bytes::from(vec![]));
    actor.receive(fitz::domains::rpc::RpcMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(actor.active_leases(), 0);
}

#[test]
fn should_track_multiple_concurrent_leases() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_rpc_context();

    // Register 3 workers
    for i in 1..=3 {
        actor.receive(
            fitz::domains::rpc::RpcMessage::Subscribe {
                worker_addr: create_worker_addr(i),
            },
            &mut ctx,
        );
    }

    // Act
    for _i in 1..=3 {
        actor.receive(
            fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())),
            &mut ctx,
        );
    }

    // Assert
    assert_eq!(actor.active_leases(), 3);
}

#[test]
fn should_format_error_codes_correctly() {
    assert_eq!(RpcErrorCode::Timeout.as_str(), "RPC_TIMEOUT");
    assert_eq!(RpcErrorCode::Backpressure.as_str(), "RPC_BACKPRESSURE");
    assert_eq!(RpcErrorCode::Unauthorized.as_str(), "RPC_UNAUTHORIZED");
}

#[test]
fn should_create_backpressure_error_with_correlation_id() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let error = RpcError::backpressure(correlation_id);

    // Assert
    assert_eq!(error.correlation_id, correlation_id);
    assert_eq!(error.code, RpcErrorCode::Backpressure);
    assert!(error.message.contains("capacity"));
}

#[test]
fn should_create_timeout_error_with_correlation_id() {
    // Arrange
    let correlation_id = Uuid::new_v4();

    // Act
    let error = RpcError::timeout(correlation_id);

    // Assert
    assert_eq!(error.correlation_id, correlation_id);
    assert_eq!(error.code, RpcErrorCode::Timeout);
    assert!(error.message.contains("timeout"));
}

// ============================================================================
//                      STREAMING & ORDERING TESTS
// ============================================================================

#[test]
fn should_accept_single_chunk_response() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();
    let response = RpcResponseMsg::single(correlation_id, Bytes::from(vec![1, 2, 3]));

    // Act
    inbox.receive(InboxMessage::Response(response), &mut ctx);

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_in_order_streaming_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
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

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_buffer_out_of_order_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 2, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 3, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count(&correlation_id), 2);
}

#[test]
fn should_flush_buffer_when_gap_filled() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
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

    // Assert
    assert_eq!(inbox.buffered_count(&correlation_id), 0);
}

#[test]
fn should_cleanup_stream_when_final_chunk_received() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
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
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
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

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_drop_duplicate_chunks() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
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

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_handle_multiple_concurrent_streams() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();
    let correlation_id_3 = Uuid::new_v4();

    // Act
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
    let mut ctx = create_inbox_context();
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();

    // Act
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
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    for seq in 1..=6 {
        inbox.receive(
            InboxMessage::Response(create_response(correlation_id, seq, false)),
            &mut ctx,
        );
    }

    // Assert
    assert_eq!(inbox.active_streams(), 0);
}

#[test]
fn should_cleanup_stream_on_explicit_cleanup_message() {
    // Arrange
    let mut inbox = create_inbox();
    let mut ctx = create_inbox_context();
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
    let mut ctx = create_inbox_context();
    let correlation_id = Uuid::new_v4();

    // Act
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 100, false)),
        &mut ctx,
    );
    inbox.receive(
        InboxMessage::Response(create_response(correlation_id, 0, false)),
        &mut ctx,
    );

    // Assert
    assert_eq!(inbox.active_streams(), 1);
    assert_eq!(inbox.buffered_count(&correlation_id), 1);
}
