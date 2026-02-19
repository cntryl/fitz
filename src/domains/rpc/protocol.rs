//! RPC protocol message types
//!
//! Defines the message types used for request/response operations:
//! - **Subscribe**: Worker registers to handle requests for a route
//! - **Unsubscribe**: Worker stops handling requests for a route
//! - **Request**: Client request routed to available worker
//! - **Response**: Worker response forwarded to client (supports streaming)
//! - **Ack**: Worker signals completion for cleanup
//!
//! # Correlation Protocol
//!
//! Every request carries a unique correlation_id that must be echoed in responses.
//! This enables clients to match responses to requests when multiple requests
//! are in flight.
//!
//! # Streaming Support
//!
//! Workers can send multi-chunk responses by setting seq (0-based) and stream_end:
//! - First chunk: seq=0, stream_end=false
//! - Middle chunks: seq=N, stream_end=false
//! - Final chunk: seq=M, stream_end=true

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

/// RPC request from client to route actor
///
/// Sent by clients to initiate request/response interaction with a worker.
/// Contains all information needed for routing, correlation, and reply delivery.
/// Uses `Arc<Route>` for route and reply_route to avoid cloning strings on dispatch.
#[derive(Debug, Clone)]
pub struct RpcRequest {
    /// Route family for isolation
    pub family_id: RouteFamily,

    /// Unique correlation ID for matching responses (UUID for distributed tracing)
    pub correlation_id: Uuid,

    /// Target RPC route (e.g., "rpc://acme/auth/user/create")
    pub route: Arc<Route>,

    /// Reply inbox route (e.g., "inbox://session/123")
    pub reply_route: Arc<Route>,

    /// Request payload (Bytes for zero-copy)
    pub body: Bytes,
}

impl RpcRequest {
    /// Create new RPC request (wraps route and reply_route in Arc once)
    pub fn new(
        family_id: RouteFamily,
        correlation_id: Uuid,
        route: Route,
        reply_route: Route,
        body: Bytes,
    ) -> Self {
        Self {
            family_id,
            correlation_id,
            route: Arc::new(route),
            reply_route: Arc::new(reply_route),
            body,
        }
    }
}

/// RPC response from worker back to client
///
/// Workers send responses with the same correlation_id as the request.
/// For streaming responses, workers send multiple chunks with incrementing
/// seq numbers and mark the final chunk with stream_end=true.
#[derive(Debug, Clone)]
pub struct RpcResponse {
    /// Correlation ID matching the request (UUID for distributed tracing)
    pub correlation_id: Uuid,

    /// Sequence number for streaming (starts at 0)
    pub seq: u64,

    /// Response payload chunk (Bytes for zero-copy)
    pub body: Bytes,

    /// True if this is the final chunk
    pub stream_end: bool,
}

impl RpcResponse {
    /// Create single-chunk response (non-streaming)
    pub fn single(correlation_id: Uuid, body: Bytes) -> Self {
        Self {
            correlation_id,
            seq: 0,
            body,
            stream_end: true,
        }
    }

    /// Create streaming response chunk
    pub fn chunk(correlation_id: Uuid, seq: u64, body: Bytes, stream_end: bool) -> Self {
        Self {
            correlation_id,
            seq,
            body,
            stream_end,
        }
    }
}

/// Messages handled by RpcRouteActor
///
/// These messages coordinate worker registration, request routing, and response delivery.
#[derive(Debug, Clone)]
pub enum RpcMessage {
    /// Worker subscribes to handle requests for this route
    ///
    /// Sent by workers to register as handlers for the route. Workers are
    /// assigned requests in round-robin fashion based on availability.
    Subscribe {
        /// Address of the worker actor
        worker_addr: RouteAddress,
    },

    /// Worker unsubscribes from this route
    ///
    /// Sent by workers to stop receiving requests. Cleans up worker registration
    /// and any in-flight tracking for this worker.
    Unsubscribe {
        /// Address of the worker actor
        worker_addr: RouteAddress,
    },

    /// Client request to be routed to a worker
    ///
    /// Queued until a worker becomes available. Dispatched in FIFO order
    /// to maintain request ordering guarantees.
    Request(RpcRequest),

    /// Worker response to be forwarded to client
    ///
    /// Contains correlation_id matching the original request. The route actor
    /// forwards this to the client's reply_route specified in the request.
    Response(RpcResponse),

    /// Worker acknowledges completion (for cleanup)
    ///
    /// Sent by workers after processing completes to decrement in-flight count
    /// and allow the worker to receive additional requests.
    Ack {
        /// Correlation ID of completed request
        correlation_id: Uuid,
    },

    /// Request delivery to worker (internal routing message)
    ///
    /// Sent from RpcRouteActor to worker session actor to deliver a request.
    /// The session actor encodes this as message type 302 (REQUEST) on the wire.
    RequestDelivery(RpcWorkItem),
}

impl RpcMessage {
    /// Create Subscribe message
    pub fn subscribe(worker_addr: RouteAddress) -> Self {
        Self::Subscribe { worker_addr }
    }

    /// Create Unsubscribe message
    pub fn unsubscribe(worker_addr: RouteAddress) -> Self {
        Self::Unsubscribe { worker_addr }
    }

    /// Create Request message
    pub fn request(req: RpcRequest) -> Self {
        Self::Request(req)
    }

    /// Create Response message
    pub fn response(resp: RpcResponse) -> Self {
        Self::Response(resp)
    }

    /// Create Ack message
    pub fn ack(correlation_id: Uuid) -> Self {
        Self::Ack { correlation_id }
    }

    /// Create RequestDelivery message
    pub fn request_delivery(work_item: RpcWorkItem) -> Self {
        Self::RequestDelivery(work_item)
    }
}

/// Work item dispatched to a worker
///
/// Contains the minimal information needed for a worker to process a request
/// and send responses back to the client. Created from RpcRequest before
/// dispatching to the worker pool. Uses Arc<Route> to avoid cloning route strings.
#[derive(Debug, Clone)]
pub struct RpcWorkItem {
    /// Correlation ID for tracking
    pub correlation_id: Uuid,

    /// Target route (for worker context/logging)
    pub route: Arc<Route>,

    /// Reply route for sending responses
    pub reply_route: Arc<Route>,

    /// Request payload
    pub body: Bytes,
}

impl RpcWorkItem {
    /// Create work item from request (Arc::clone only, no string alloc)
    pub fn from_request(req: &RpcRequest) -> Self {
        Self {
            correlation_id: req.correlation_id,
            route: Arc::clone(&req.route),
            reply_route: Arc::clone(&req.reply_route),
            body: req.body.clone(),
        }
    }

    /// Create work item directly
    pub fn new(
        correlation_id: Uuid,
        route: Arc<Route>,
        reply_route: Arc<Route>,
        body: Bytes,
    ) -> Self {
        Self {
            correlation_id,
            route,
            reply_route,
            body,
        }
    }
}
