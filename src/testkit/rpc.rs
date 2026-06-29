//! RPC test helpers

use crate::domains::rpc::protocol::{RpcRequest, RpcResponse};
use crate::domains::rpc::reply_inbox::ReplyInboxActor;
use crate::domains::rpc::RpcRouteActor;
use crate::runtime::actor::Context;
use crate::runtime::router::Router;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

/// Create an RPC route actor context for testing
///
/// # Arguments
/// * `route_str` - The RPC route string (e.g., "rpc://realm/service/operation")
#[must_use]
pub fn create_test_rpc_context(route_str: &str) -> Context<RpcRouteActor> {
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new(route_str));
    Context::new(addr, router)
}

/// Create a reply inbox actor for testing
#[must_use]
pub fn create_test_inbox() -> ReplyInboxActor {
    ReplyInboxActor::new(RouteFamily::new(1))
}

/// Create an inbox context for testing
#[must_use]
pub fn create_test_inbox_context() -> Context<ReplyInboxActor> {
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/1"));
    Context::new(addr, router)
}

/// Create a test RPC request
///
/// # Arguments
/// * `correlation_id` - Unique correlation ID
/// * `route` - RPC operation route
/// * `reply_route` - Reply inbox route
/// * `body` - Request body bytes
#[must_use]
pub fn create_test_rpc_request(
    correlation_id: Uuid,
    route: &str,
    reply_route: &str,
    body: &[u8],
) -> RpcRequest {
    RpcRequest {
        family_id: RouteFamily::new(1),
        correlation_id,
        route: Route::new(route),
        reply_route: Route::new(reply_route),
        body: Bytes::from(body.to_vec()),
    }
}

/// Create a test RPC response
///
/// # Arguments
/// * `correlation_id` - Correlation ID matching the request
/// * `seq` - Sequence number (for streaming)
/// * `stream_end` - Whether this is the final response
/// * `body` - Response body bytes
#[must_use]
pub fn create_test_rpc_response(
    correlation_id: Uuid,
    seq: u64,
    stream_end: bool,
    body: &[u8],
) -> RpcResponse {
    RpcResponse {
        correlation_id,
        seq,
        stream_end,
        body: Bytes::from(body.to_vec()),
    }
}

/// Create a worker address for testing
///
/// # Arguments
/// * `id` - Worker ID number
#[must_use]
pub fn create_test_worker_addr(id: u64) -> RouteAddress {
    RouteAddress::new(
        RouteFamily::new(1),
        Route::new(format!("worker://realm/service/worker{id}")),
    )
}

/// Create an RPC route actor with a timeout for testing
///
/// # Arguments
/// * `timeout_ms` - Timeout in milliseconds
#[must_use]
pub fn create_test_rpc_actor_with_timeout(timeout_ms: u64) -> RpcRouteActor {
    RpcRouteActor::with_timeout(
        RouteFamily::new(1),
        1000, // Default capacity for tests
        std::time::Duration::from_millis(timeout_ms),
    )
}
