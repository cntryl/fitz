//! Notification protocol definitions//!
//! The notification protocol supports fire-and-forget pub/sub with wildcard routing.
//!
//! # Message Types
//!
//! - **Publish**: Send a message to all subscribers matching a route pattern
//! - **Subscribe**: Register to receive messages on a pattern (with wildcards)
//! - **Unsubscribe**: Unregister from a subscription
//!
//! # Semantics
//!
//! - Fire-and-forget: No acknowledgements, retries, or delivery guarantees
//! - Best-effort: Messages are delivered only to subscribers alive at publish time
//! - Isolated: All messaging is scoped to (RouteFamilyId, route) pairs
//! - Stateless: No ordering, no durability, no persistence

use crate::transport::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;

/// Messages for the notification domain
#[derive(Debug, Clone)]
pub enum NotificationMessage {
    /// Publish a message to all matching subscribers
    Publish(PublishMessage),
    /// Subscribe to messages matching a pattern
    Subscribe(SubscribeMessage),
    /// Unsubscribe from a pattern
    Unsubscribe(UnsubscribeMessage),
    /// Notify a subscriber of a published message (internal)
    Notify(NotifyMessage),
}

/// Publish a message to all subscribers matching the route
#[derive(Debug, Clone)]
pub struct PublishMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Route being published to
    pub route: Route,
    /// Raw byte payload
    pub payload: Bytes,
}

impl PublishMessage {
    pub fn new(family_id: RouteFamily, route: Route, payload: Bytes) -> Self {
        Self {
            family_id,
            route,
            payload,
        }
    }
}

/// Subscribe to messages matching a pattern (may include wildcards)
#[derive(Debug, Clone)]
pub struct SubscribeMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Pattern to match (may include * and ** wildcards)
    pub pattern: Route,
    /// Subscriber address (messages will be sent here)
    pub subscriber: RouteAddress,
}

impl SubscribeMessage {
    pub fn new(family_id: RouteFamily, pattern: Route, subscriber: RouteAddress) -> Self {
        Self {
            family_id,
            pattern,
            subscriber,
        }
    }
}

/// Unsubscribe from a pattern
#[derive(Debug, Clone)]
pub struct UnsubscribeMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Pattern being unsubscribed from
    pub pattern: Route,
    /// Subscriber to remove
    pub subscriber: RouteAddress,
}

impl UnsubscribeMessage {
    pub fn new(family_id: RouteFamily, pattern: Route, subscriber: RouteAddress) -> Self {
        Self {
            family_id,
            pattern,
            subscriber,
        }
    }
}

/// Notification delivered to a subscriber
#[derive(Debug, Clone)]
pub struct NotifyMessage {
    /// The route that was published to
    pub route: Route,
    /// The payload published
    pub payload: Bytes,
}

impl NotifyMessage {
    pub fn new(route: Route, payload: Bytes) -> Self {
        Self { route, payload }
    }
}
