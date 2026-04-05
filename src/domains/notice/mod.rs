//! Notification domain: fire-and-forget pub/sub with wildcard routing
//!
//! # Architecture
//!
//! - **NoticeDomainSink** (`src/boot/domains/notice_sink.rs`): Live production handler that keeps broker-local subscription state in memory for the current process
//! - **NoticeRouteActor** ([actor]): Sync actor model used for core matching and focused unit tests
//! - **SessionActor**: Enforces authentication/authorization before forwarding notice messages
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
//! - Wildcards apply only within the same RouteFamily

pub mod actor;
pub mod events;
pub mod metrics;
pub mod protocol;
pub mod session;

pub mod bench; // Zero-copy notification primitives for benchmarking

pub use actor::NoticeRouteActor;
pub use metrics::NoticeMetrics;
pub use protocol::{DeliverMessage, NoticeError, NotificationMessage};
pub use session::SessionActor;
