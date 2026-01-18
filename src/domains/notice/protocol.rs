//! Notification protocol message types
//!
//! Defines the message types used for pub/sub operations:
//! - **Publish**: Send message to matching subscribers
//! - **Subscribe**: Register pattern subscription
//! - **Unsubscribe**: Remove specific subscription
//! - **UnsubscribeAll**: Clean up session subscriptions on disconnect
//! - **Notify**: Deliver published message to subscriber

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::session::SessionId;
use bytes::Bytes;
use std::sync::Arc;

/// Messages for the notification domain
#[derive(Debug, Clone)]
pub enum NotificationMessage {
    /// Publish a message to all matching subscribers (from any domain/client)
    Publish(PublishMessage),
    /// Subscribe to messages matching a pattern (from SessionActor)
    Subscribe(SubscribeMessage),
    /// Unsubscribe from a pattern (from SessionActor)
    Unsubscribe(UnsubscribeMessage),
    /// Unsubscribe all subscriptions for a session (called on disconnect)
    UnsubscribeAll(UnsubscribeAllMessage),
    /// Notify a subscriber of a published message (internal to NoticeRouteActor)
    Notify(NotifyMessage),
}

/// Publish a message to all subscribers matching the route pattern
#[derive(Debug, Clone)]
pub struct PublishMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Route being published to (exact path, no wildcards)
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

/// Subscribe to messages matching a pattern (may include wildcards * and **)
///
/// Sent from SessionActor to NoticeRouteActor after authorization is verified.
/// SessionActor has already enforced prefix-based auth rules.
#[derive(Debug, Clone)]
pub struct SubscribeMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Pattern to match (may include * and ** wildcards)
    pub pattern: Route,
    /// Session making the subscription
    pub session_id: SessionId,
    /// Address to send notifications to (typically the SessionActor)
    pub subscriber: RouteAddress,
}

impl SubscribeMessage {
    pub fn new(
        family_id: RouteFamily,
        pattern: Route,
        session_id: SessionId,
        subscriber: RouteAddress,
    ) -> Self {
        Self {
            family_id,
            pattern,
            session_id,
            subscriber,
        }
    }
}

/// Unsubscribe from a pattern
///
/// Sent from SessionActor to NoticeRouteActor.
#[derive(Debug, Clone)]
pub struct UnsubscribeMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Pattern being unsubscribed from
    pub pattern: Route,
    /// Session being unsubscribed
    pub session_id: SessionId,
    /// Address to remove
    pub subscriber: RouteAddress,
}

impl UnsubscribeMessage {
    pub fn new(
        family_id: RouteFamily,
        pattern: Route,
        session_id: SessionId,
        subscriber: RouteAddress,
    ) -> Self {
        Self {
            family_id,
            pattern,
            session_id,
            subscriber,
        }
    }
}

/// Unsubscribe all subscriptions for a session (called on disconnect)
///
/// SessionActor sends this to all NoticeRouteActors when the session terminates.
/// Cleanup is best-effort; NoticeRouteActor may not have received all subscribe messages yet.
#[derive(Debug, Clone)]
pub struct UnsubscribeAllMessage {
    /// Session being disconnected
    pub session_id: SessionId,
    /// Address to remove from all subscriptions
    pub subscriber: RouteAddress,
}

impl UnsubscribeAllMessage {
    pub fn new(session_id: SessionId, subscriber: RouteAddress) -> Self {
        Self {
            session_id,
            subscriber,
        }
    }
}

/// Notification delivered to a subscriber
///
/// Uses Arc for route and payload to enable zero-allocation fanout.
/// Multiple subscribers share the same Arc pointers, avoiding per-subscriber clones.
#[derive(Debug, Clone)]
pub struct NotifyMessage {
    /// The route that was published to (shared via Arc)
    pub route: Arc<Route>,
    /// The payload published (shared via Arc)
    pub payload: Arc<Bytes>,
}

impl NotifyMessage {
    /// Create notification with owned data (converts to Arc)
    pub fn new(route: Route, payload: Bytes) -> Self {
        Self {
            route: Arc::new(route),
            payload: Arc::new(payload),
        }
    }

    /// Create notification from Arc-shared data (zero-allocation fanout path)
    pub fn new_shared(route: Arc<Route>, payload: Arc<Bytes>) -> Self {
        Self { route, payload }
    }
}
