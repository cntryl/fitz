//! Notification domain (pub/sub with wildcard fan-out)
//!
//! Fire-and-forget pub/sub messaging with NATS-like wildcard routing.
//!
//! # Semantics
//!
//! - **Fire-and-forget**: No acknowledgements, retries, or delivery guarantees
//! - **Best-effort**: Messages delivered only to subscribers alive at publish time
//! - **Isolated**: All messaging scoped to (RouteFamilyId, route) pairs
//! - **Stateless**: No ordering, durability, or persistence
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
//! subscribe(family, "notify://acme/orders/*", subscriber);
//!
//! // Publish to create operation
//! publish(family, "notify://acme/orders/create", payload);
//! // Matches above subscription
//! ```

pub mod actor;
pub mod protocol;
