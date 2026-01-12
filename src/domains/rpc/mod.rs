//! RPC domain: request/response messaging with worker pools
//!
//! # Architecture
//!
//! - **RpcRouteActor** ([rpc_route_actor]): Manages worker pool and request queue per route
//! - **SessionActor**: Enforces authentication/authorization before forwarding to RpcRouteActor
//! - Workers register with routes and receive requests via round-robin assignment
//!
//! # Performance Characteristics (Hardened v2)
//!
//! - **Dispatch latency**: ~140ns (zero-allocation hot path)
//! - **Worker lookup**: O(1) index-based (no linear search)
//! - **Lease expiration**: O(K) min-heap (K = expired count, not total leases)
//! - **Scaling**: Stable to 10k+ in-flight requests and 256+ workers
//! - **Throughput**: 7M+ dispatches/sec single-threaded
//!
//! # Semantics
//!
//! - **Exactly-once dispatch**: Each request is assigned to exactly one worker
//! - **Strict correlation**: Responses must include the original correlation ID
//! - **FIFO ordering**: Requests dispatched in arrival order per route
//! - **Bounded queue**: Backpressure when queue reaches capacity (default: 1000)
//! - **Streaming support**: Workers can send multi-chunk responses with sequence numbers
//! - **Non-durable**: All state is in-memory (no persistence for ultra-low latency)
//!
//! # Worker Model
//!
//! Workers register with specific routes (no wildcards). Each worker can handle
//! `max_concurrent` requests (default: 1). The actor maintains in-flight tracking
//! and assigns new requests only to available workers.
//!
//! # Route Format
//!
//! ```text
//! rpc://{realm}/{area}/{resource}/{operation}
//! ```
//!
//! The `{operation}` represents the business operation (create, update, authenticate, etc.),
//! not Fitz internal operations. Each unique route has its own actor and worker pool.
//!
//! Examples:
//! - `rpc://acme/auth/user/create`
//! - `rpc://acme/inventory/item/update`
//! - `rpc://acme/reports/monthly/generate`
//!
//! # Example
//!
//! ```ignore
//! // Worker registers for a route
//! send(actor, RpcMessage::Subscribe {
//!     worker: worker_addr,
//!     max_concurrent: 1
//! });
//!
//! // Client sends request
//! send(actor, RpcMessage::Request {
//!     request: RpcRequest {
//!         correlation_id: "req-001",
//!         route: "rpc://acme/auth/user/create",
//!         reply_route: client_addr,
//!         body: b"..."
//!     }
//! });
//!
//! // Worker processes and responds
//! send(actor, RpcMessage::Response {
//!     response: RpcResponse {
//!         correlation_id: "req-001",
//!         seq: 0,
//!         body: b"...",
//!         stream_end: true
//!     }
//! });
//! ```

pub mod errors;
pub mod protocol;
pub mod reply_inbox;
pub mod rpc_route_actor;

// Test helper - lightweight SessionActor stub for testing RPC authorization
// Available in tests (both unit tests and integration tests)
#[cfg_attr(not(test), doc(hidden))]
pub mod session;

// Re-export primary types
pub use errors::{RpcError, RpcErrorCode};
pub use protocol::{RpcMessage, RpcRequest, RpcResponse, RpcWorkItem};
pub use reply_inbox::{InboxMessage, ReplyInboxActor};
pub use rpc_route_actor::RpcRouteActor;
