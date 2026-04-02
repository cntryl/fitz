//! RPC domain: request/response messaging with worker pools
//!
//! # Architecture
//!
//! - **RpcRouteActor** ([actor]): Manages worker pool and request queue per route
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
//! - **Explicitly ephemeral**: Worker registrations and pending requests live only in memory
//! - **Restart loss**: Broker restart drops workers, pending requests, and reply routing state
//! - **Reconnect contract**: Workers must re-register and callers must retry lost work at the application layer
//!
//! # Worker Model
//!
//! Workers register with specific routes (no wildcards). Each worker can handle
//! `max_concurrent` requests (default: 1). The actor maintains in-flight tracking
//! and assigns new requests only to available workers. Disconnect or broker
//! restart clears the worker pool; there is no durable worker recovery.
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

pub mod actor;
pub mod errors;
pub mod protocol;
pub mod reply_inbox;
pub mod session;

// Re-export primary types
pub use actor::RpcRouteActor;
pub use errors::{RpcError, RpcErrorCode};
pub use protocol::{RpcMessage, RpcRequest, RpcResponse, RpcWorkItem};
pub use reply_inbox::{InboxMessage, ReplyInboxActor};
pub use session::SessionActor;
