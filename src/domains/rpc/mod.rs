//! RPC domain: request/response messaging with worker pools
//!
//! # Architecture
//!
//! - **`RpcDomainSink`** ([sink]): Production ingress path used by the live broker
//! - Workers register with routes and receive requests while declared credit is available
//!
//! Production request forwarding, terminal error delivery, and caller-disconnect
//! handling live in `RpcDomainSink`.
//!
//! # Semantics
//!
//! - **Single-worker dispatch**: Each accepted request is assigned to one live
//!   worker inside the current broker process
//! - **Strict correlation**: Responses must include the original correlation ID
//!   to match the live in-flight request
//! - **FIFO ordering**: Requests dispatched in arrival order per route
//! - **Fair wildcard dispatch**: Ready concrete routes rotate while sharing a
//!   wildcard registration's concurrency credit
//! - **Bounded queue**: Backpressure when queue reaches capacity (default: 1000)
//! - **Streaming support**: Workers can send multi-chunk responses with sequence numbers
//! - **Explicitly ephemeral**: Worker registrations and pending requests live only in memory
//! - **Restart loss**: Broker restart drops workers, pending requests, and reply routing state
//! - **No durable dedup**: Correlation IDs are matching keys, not broker-side
//!   replay or idempotency tokens
//! - **Reconnect contract**: Workers must re-register and callers must retry lost
//!   work at the application layer
//!
//! # Worker Model
//!
//! Each `(session, pattern)` registration owns one compiled exact or wildcard
//! pattern and one shared `max_concurrent` credit pool. Overlapping exact and
//! wildcard registrations are independent, equal candidates. A duplicate
//! registration is idempotent; workers must unregister before changing credit.
//! Disconnect or broker restart clears registrations; there is no durable
//! worker recovery.
//!
//! # Route Format
//!
//! RPC calls use concrete routes. Worker registrations may use strict
//! whole-segment `*` and `**` patterns. Operation-style routes commonly use:
//!
//! ```text
//! rpc://{realm}/{area}/{resource}/{operation}
//! ```
//!
//! The `{operation}` represents the business operation (create, update, authenticate, etc.),
//! not Fitz internal operations.
//!
//! Examples:
//! - `rpc://acme/auth/user/create`
//! - `rpc://acme/inventory/item/update`
//! - `rpc://acme/reports/monthly/generate`

pub mod errors;
pub mod events;
pub mod metrics;
pub mod projection;
pub mod protocol;
pub mod reply_inbox;
pub mod sink;

pub use errors::{RpcError, RpcErrorCode};
pub use metrics::RpcMetrics;
pub use protocol::{
    RpcClientForwardedResponse, RpcClientForwardedResponseBody, RpcClientRequest,
    RpcClientResponse, RpcClientResponseBody, RpcDecodeError, RpcMessage, RpcRequest, RpcResponse,
    RpcWorkItem, RpcWorkerRequestDelivery,
};
pub use reply_inbox::{InboxMessage, ReplyInboxActor};
