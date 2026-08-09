//! Notification domain: fire-and-forget pub/sub with wildcard routing
//!
//! # Architecture
//!
//! - **`NoticeDomainSink`** (`src/domains/notice/sink.rs`): Mailbox adapter for production Notice delivery
//! - **`NoticeDomainActor`** (`src/domains/notice/sink/actor_runtime.rs`): Managed production actor that owns broker-local subscription state for the current process
//! - Subscriptions are session-scoped and cleaned up on disconnect
//!
//! # Semantics
//!
//! - **Fire-and-forget**: Publish acknowledgement only means the broker accepted the request, not that any subscriber received it
//! - **Best-effort**: Messages are delivered only to subscribers alive at publish time
//! - **Broker-local**: Subscription IDs and admin views describe only the running broker process
//! - **Non-durable**: No replay, restart recovery, or persisted subscriber state
//! - **Session-scoped**: Subscriptions vanish on disconnect, and clients must re-subscribe after reconnect or broker restart
//! - **Isolated**: All messaging is scoped to `(RouteFamilyId, route)` pairs
//!
//! # Wildcard Routing
//!
//! - `*` matches a single path segment
//! - `**` matches zero or more path segments
//! - Wildcards apply only within the same `RouteFamily`

pub mod metrics;
pub mod protocol;
pub mod sink;

pub mod bench; // Zero-copy notification primitives for benchmarking

pub use metrics::NoticeMetrics;
pub use protocol::{
    DeliverMessage, NoticeClientNotification, NoticeClientRequest, NoticeClientResponse,
    NoticeError, NoticeResponse, NotificationMessage,
};
