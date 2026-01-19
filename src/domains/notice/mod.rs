//! Notification domain: fire-and-forget pub/sub with wildcard routing
//!
//! # Architecture
//!
//! - **NoticeRouteActor** ([route_actor]): Owns subscriptions per route, performs wildcard matching and fanout
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

pub mod protocol;
pub mod route_actor;

// Test helper - lightweight SessionActor stub for testing notification authorization
// Available in tests (both unit tests and integration tests)
#[cfg_attr(not(test), doc(hidden))]
pub mod session;

pub mod bench; // Zero-copy notification primitives for benchmarking
