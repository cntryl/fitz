//! RPC protocol message types
//!
//! Defines the message types used for request/response operations:
//! - **`RegisterWorker`**: Worker registers an exact route or wildcard pattern
//! - **`UnregisterWorker`**: Worker removes that registration pattern
//! - **Request**: Client request routed to available worker
//! - **Response**: Worker response forwarded to client (supports streaming)
//!
//! # Correlation Protocol
//!
//! Every request carries a unique `correlation_id` that must be echoed in responses.
//! This enables clients to match responses to requests when multiple requests
//! are in flight.
//!
//! # Streaming Support
//!
//! Workers can send multi-chunk responses only if sequence numbers are strict
//! and contiguous:
//! - First chunk: seq=0
//! - Next chunk: seq=previous+1
//! - Final chunk: `stream_end=true` on the last contiguous chunk
//!
//! Duplicate or out-of-order chunks are terminal protocol failures. Fitz does
//! not buffer or reorder RPC responses on the broker side.

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;
use std::fmt;
use uuid::Uuid;

/// Typed RPC decode and validation failure used to select the documented wire error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcDecodeError {
    StructurallyUndecodable(String),
    InvalidCallRoute(String),
    InvalidRegistrationPattern(String),
}

impl RpcDecodeError {
    pub(crate) fn invalid_call_route(message: impl Into<String>) -> Self {
        Self::InvalidCallRoute(message.into())
    }

    pub(crate) fn invalid_registration_pattern(message: impl Into<String>) -> Self {
        Self::InvalidRegistrationPattern(message.into())
    }
}

impl From<String> for RpcDecodeError {
    fn from(message: String) -> Self {
        Self::StructurallyUndecodable(message)
    }
}

impl fmt::Display for RpcDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::StructurallyUndecodable(message)
            | Self::InvalidCallRoute(message)
            | Self::InvalidRegistrationPattern(message) => message,
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RpcDecodeError {}

/// RPC request from client to route actor
///
/// Sent by clients to initiate request/response interaction with a worker.
/// Contains all information needed for routing and correlation.
#[derive(Debug, Clone)]
pub struct RpcRequest {
    /// Route family for isolation
    pub family_id: RouteFamily,

    /// Unique correlation ID for matching responses (UUID for distributed tracing)
    pub correlation_id: Uuid,

    /// Target RPC route (e.g., `<rpc://acme/auth/user/create>`)
    pub route: Route,

    /// Request payload (Bytes for zero-copy)
    pub body: Bytes,
}

impl RpcRequest {
    /// Create new RPC request
    pub fn new(family_id: RouteFamily, correlation_id: Uuid, route: Route, body: Bytes) -> Self {
        Self {
            family_id,
            correlation_id,
            route,
            body,
        }
    }
}

/// RPC response from worker back to client
///
/// Workers send responses with the same `correlation_id` as the request.
/// For streaming responses, workers must send chunks in strict contiguous
/// sequence order and mark the final chunk with `stream_end=true`.
#[derive(Debug, Clone)]
pub struct RpcResponse {
    /// Correlation ID matching the request (UUID for distributed tracing)
    pub correlation_id: Uuid,

    /// Sequence number for streaming (starts at 0 and must advance contiguously)
    pub seq: u64,

    /// Response payload chunk (Bytes for zero-copy)
    pub body: Bytes,

    /// True if this chunk completes a successful response
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

    /// Create streaming response chunk.
    ///
    /// Chunks must start at seq=0 and advance contiguously without duplicates
    /// or gaps.
    pub fn chunk(correlation_id: Uuid, seq: u64, body: Bytes, stream_end: bool) -> Self {
        Self {
            correlation_id,
            seq,
            body,
            stream_end,
        }
    }
}

/// Messages handled by RPC route state machines.
///
/// These messages coordinate worker registration, request routing, and response delivery.
#[derive(Debug, Clone)]
pub enum RpcMessage {
    /// Worker registers to handle requests matching this pattern
    ///
    /// Sent by workers to register as handlers for an exact route or wildcard
    /// pattern. Workers are assigned requests while registration-owned credit
    /// is available.
    RegisterWorker {
        /// Registered exact route or wildcard pattern
        worker_addr: RouteAddress,
        /// Maximum number of concurrent requests this worker accepts
        max_concurrent: usize,
    },

    /// Worker unregisters this exact route or wildcard pattern
    ///
    /// Sent by workers to stop receiving requests. Cleans up worker registration
    /// and any in-flight tracking for this worker.
    UnregisterWorker {
        /// Exact route or wildcard pattern to remove
        worker_addr: RouteAddress,
    },

    /// Client request to be routed to a worker
    ///
    /// Queued until a worker becomes available. Dispatched in FIFO order
    /// to maintain request ordering guarantees.
    Request(RpcRequest),

    /// Worker response to be forwarded to client
    ///
    /// Contains `correlation_id` matching the original request. Responses that do
    /// not follow strict contiguous sequence order are terminally rejected.
    Response(RpcResponse),

    /// Request delivery to worker (internal routing message)
    ///
    /// Sent from RPC routing state to a worker session to deliver a request.
    /// The session encodes this as message type 302 (REQUEST) on the wire.
    Deliver(RpcWorkItem),
}

impl RpcMessage {
    /// Create `RegisterWorker` message
    #[must_use]
    pub fn register_worker(worker_addr: RouteAddress, max_concurrent: usize) -> Self {
        Self::RegisterWorker {
            worker_addr,
            max_concurrent,
        }
    }

    /// Create `UnregisterWorker` message
    #[must_use]
    pub fn unregister_worker(worker_addr: RouteAddress) -> Self {
        Self::UnregisterWorker { worker_addr }
    }

    /// Create Request message
    pub fn request(req: RpcRequest) -> Self {
        Self::Request(req)
    }

    /// Create Response message
    pub fn response(resp: RpcResponse) -> Self {
        Self::Response(resp)
    }

    /// Create Deliver message
    pub fn deliver(work_item: RpcWorkItem) -> Self {
        Self::Deliver(work_item)
    }
}

/// Parsed client request delivered to the RPC domain sink.
#[derive(Debug, Clone)]
pub struct RpcClientRequest {
    pub meta: ClientFrameMeta,
    pub message: Result<RpcMessage, RpcDecodeError>,
    pub raw_payload: Bytes,
}

impl RpcClientRequest {
    pub fn new(meta: ClientFrameMeta, message: Result<RpcMessage, RpcDecodeError>) -> Self {
        Self {
            meta,
            message,
            raw_payload: Bytes::new(),
        }
    }

    pub fn new_with_payload(
        meta: ClientFrameMeta,
        message: Result<RpcMessage, RpcDecodeError>,
        raw_payload: Bytes,
    ) -> Self {
        Self {
            meta,
            message,
            raw_payload,
        }
    }
}

/// Immediate response routed from the RPC domain sink back to a client session.
#[derive(Debug, Clone)]
pub struct RpcClientResponse {
    pub meta: ClientFrameMeta,
    pub response: RpcClientResponseBody,
}

impl RpcClientResponse {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, response: RpcClientResponseBody) -> Self {
        Self { meta, response }
    }
}

/// Response from RPC control operations before transport encoding.
#[derive(Debug, Clone)]
pub enum RpcClientResponseBody {
    Ok { data: Vec<u8> },
    CodeError { code: u16, message: String },
    Error(String),
}

/// RPC request delivery routed from the domain sink to a worker session.
#[derive(Debug, Clone)]
pub struct RpcWorkerRequestDelivery {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub request: RpcRequest,
}

impl RpcWorkerRequestDelivery {
    pub fn new(session_id: u64, route_family: RouteFamily, request: RpcRequest) -> Self {
        Self {
            session_id,
            route_family,
            request,
        }
    }
}

/// RPC response forwarded from a worker to a caller session.
#[derive(Debug, Clone)]
pub struct RpcClientForwardedResponse {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub body: RpcClientForwardedResponseBody,
}

impl RpcClientForwardedResponse {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        body: RpcClientForwardedResponseBody,
    ) -> Self {
        Self {
            session_id,
            route_family,
            body,
        }
    }
}

/// Forwarded RPC response before transport encoding.
#[derive(Debug, Clone)]
pub enum RpcClientForwardedResponseBody {
    Response(RpcResponse),
    TerminalError {
        correlation_id: Uuid,
        code: u16,
        message: &'static str,
    },
}

/// Work item dispatched to a worker
///
/// Contains the minimal information needed for a worker to process a request.
/// Created from `RpcRequest` before dispatching to the worker pool.
#[derive(Debug, Clone)]
pub struct RpcWorkItem {
    /// Correlation ID for tracking
    pub correlation_id: Uuid,

    /// Target route (for worker context/logging)
    pub route: Route,

    /// Request payload
    pub body: Bytes,
}

impl RpcWorkItem {
    /// Create work item from request
    pub fn from_request(req: &RpcRequest) -> Self {
        Self {
            correlation_id: req.correlation_id,
            route: req.route.clone(),
            body: req.body.clone(),
        }
    }

    /// Create work item directly
    pub fn new(correlation_id: Uuid, route: Route, body: Bytes) -> Self {
        Self {
            correlation_id,
            route,
            body,
        }
    }
}
