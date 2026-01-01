//! Notification domain (pub/sub with wildcard fan-out)
//!
//! Fire-and-forget pub/sub messaging with NATS-like wildcard routing.
//!
//! # Architecture
//!
//! - **SessionActor**: Enforces all authentication and authorization
//! - **NoticeRouteActor**: Owns subscriptions per route, trusts SessionActor, performs fanout
//! - Subscriptions are session-scoped and cleaned up on disconnect
//! - Authorization is prefix-based (exact, area wildcard, realm wildcard, global)
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
//!
//! # Example
//!
//! ```ignore
//! // Subscribe to all orders
//! subscribe(family, "notice://acme/orders/*", subscriber);
//!
//! // Publish to create operation
//! publish(family, "notice://acme/orders/create", payload);
//! // Matches above subscription
//! ```

pub mod actor;
pub mod protocol;
