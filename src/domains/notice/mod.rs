//! Notification domain: fire-and-forget pub/sub with wildcard routing
//!
//! # Architecture
//!
//! - **NoticeRouteActor** ([actor]): Owns subscriptions per route, performs wildcard matching and fanout
//! - **SessionActor**: Enforces authentication/authorization before forwarding to NoticeRouteActor
//! - Subscriptions are session-scoped and cleaned up on disconnect
//!
//! # Semantics
//!
//! - **Fire-and-forget**: No acknowledgements, retries, or delivery guarantees
//! - **Best-effort**: Messages delivered only to subscribers alive at publish time
//! - **Isolated**: All messaging scoped to (RouteFamilyId, route) pairs
//! - **Session-scoped**: Subscriptions vanish on disconnect
//!
//! # Wildcard Routing
//!
//! - `*` matches a single path segment
//! - `**` matches zero or more path segments
//! - Wildcards apply only within the same RouteFamily

pub mod actor;
pub mod protocol;
pub mod session;

pub mod bench; // Zero-copy notification primitives for benchmarking

pub use actor::NoticeRouteActor;
pub use protocol::{DeliverMessage, NoticeError, NotificationMessage};
pub use session::SessionActor;
