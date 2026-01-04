//! RPC domain lease and fault tolerance tests
//!
//! Tests the lease mechanism, timeout handling, re-enqueue logic, and error responses.

use fitz::domains::rpc::{RpcError, RpcErrorCode, RpcRouteActor, RpcRequest, RpcResponse};
use uuid::Uuid;
use bytes::Bytes;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::actor::{Actor, Context};
use std::time::Duration;

fn create_actor_with_timeout(timeout_ms: u64) -> RpcRouteActor {
    RpcRouteActor::with_timeout(
        RouteFamily::new(1),
        1000,
        Duration::from_millis(timeout_ms),
    )
}

fn create_request(correlation_id: Uuid) -> RpcRequest {
    RpcRequest::new(
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

fn create_context() -> Context<RpcRouteActor> {
    let router = std::sync::Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://test/area/resource/operation"),
    );
    Context::new(addr, router)
}

#[test]
fn should_track_active_leases_when_dispatching_request() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_context();
    let worker_addr = create_worker_addr(1);
    
    actor.receive(fitz::domains::rpc::RpcMessage::Subscribe { worker_addr: worker_addr.clone() }, &mut ctx);
    
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
    let mut ctx = create_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id = Uuid::new_v4();
    
    actor.receive(fitz::domains::rpc::RpcMessage::Subscribe { worker_addr: worker_addr.clone() }, &mut ctx);
    let request = create_request(correlation_id);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);
    
    // Act
    let response = RpcResponse::single(correlation_id, Bytes::from(vec![4, 5, 6]));
    actor.receive(fitz::domains::rpc::RpcMessage::Response(response), &mut ctx);
    
    // Assert
    assert_eq!(actor.active_leases(), 0);
}

#[test]
fn should_release_lease_when_receiving_ack() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id = Uuid::new_v4();
    
    actor.receive(fitz::domains::rpc::RpcMessage::Subscribe { worker_addr: worker_addr.clone() }, &mut ctx);
    let request = create_request(correlation_id);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);
    
    // Act
    actor.receive(fitz::domains::rpc::RpcMessage::Ack { correlation_id }, &mut ctx);
    
    // Assert
    assert_eq!(actor.active_leases(), 0);
}

#[test]
fn should_allow_worker_to_take_next_request_after_lease_released() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id_1 = Uuid::new_v4();
    let correlation_id_2 = Uuid::new_v4();
    
    actor.receive(fitz::domains::rpc::RpcMessage::Subscribe { worker_addr: worker_addr.clone() }, &mut ctx);
    let req1 = create_request(correlation_id_1);
    let req2 = create_request(correlation_id_2);
    
    // Act - dispatch first request
    actor.receive(fitz::domains::rpc::RpcMessage::Request(req1), &mut ctx);
    assert_eq!(actor.pending_count(), 0);
    
    // Second request should be queued
    actor.receive(fitz::domains::rpc::RpcMessage::Request(req2), &mut ctx);
    assert_eq!(actor.pending_count(), 1);
    
    // Release first request
    let response = RpcResponse::single(correlation_id_1, Bytes::from(vec![]));
    actor.receive(fitz::domains::rpc::RpcMessage::Response(response), &mut ctx);
    
    // Assert - second request should now be dispatched
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_handle_backpressure_when_queue_full() {
    // Arrange - actor with tiny capacity
    let mut actor = RpcRouteActor::with_capacity(RouteFamily::new(1), 2);
    let mut ctx = create_context();
    
    // Act - fill queue past capacity
    actor.receive(fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())), &mut ctx);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())), &mut ctx);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())), &mut ctx);
    
    // Assert - only 2 requests queued (third rejected with backpressure)
    assert_eq!(actor.pending_count(), 2);
}

#[test]
fn should_drop_late_response_after_lease_expired() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_context();
    let worker_addr = create_worker_addr(1);
    let correlation_id = Uuid::new_v4();
    
    actor.receive(fitz::domains::rpc::RpcMessage::Subscribe { worker_addr: worker_addr.clone() }, &mut ctx);
    let request = create_request(correlation_id);
    actor.receive(fitz::domains::rpc::RpcMessage::Request(request), &mut ctx);
    
    // Release lease manually
    actor.receive(fitz::domains::rpc::RpcMessage::Ack { correlation_id }, &mut ctx);
    
    // Act - send response after lease released
    let response = RpcResponse::single(correlation_id, Bytes::from(vec![]));
    actor.receive(fitz::domains::rpc::RpcMessage::Response(response), &mut ctx);
    
    // Assert - should not panic, just drop the late response
    assert_eq!(actor.active_leases(), 0);
}

#[test]
fn should_track_multiple_concurrent_leases() {
    // Arrange
    let mut actor = create_actor_with_timeout(5000);
    let mut ctx = create_context();
    
    // Register 3 workers
    for i in 1..=3 {
        actor.receive(
            fitz::domains::rpc::RpcMessage::Subscribe { 
                worker_addr: create_worker_addr(i) 
            }, 
            &mut ctx
        );
    }
    
    // Act - dispatch 3 requests to 3 workers
    for _i in 1..=3 {
        actor.receive(
            fitz::domains::rpc::RpcMessage::Request(create_request(Uuid::new_v4())), 
            &mut ctx
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





