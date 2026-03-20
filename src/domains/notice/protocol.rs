//! Notification protocol message types
//!
//! Defines the message types used for pub/sub operations:
//! - **Publish**: Send message to matching subscribers
//! - **Subscribe**: Register pattern subscription
//! - **Unsubscribe**: Remove specific subscription
//! - **UnsubscribeAll**: Clean up session subscriptions on disconnect
//! - **Deliver**: Deliver published message to subscriber

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::session::SessionId;
use bytes::Bytes;

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
    /// Deliver a published message to a subscriber (internal to NoticeRouteActor)
    Deliver(DeliverMessage),
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
/// `Route` and `Bytes` are cheap to clone, so fanout can share delivery payloads
/// without layering extra `Arc` allocations inside the notice domain.
#[derive(Debug, Clone)]
pub struct DeliverMessage {
    /// The route that was published to.
    pub route: Route,
    /// The payload published.
    pub payload: Bytes,
}

impl DeliverMessage {
    /// Create notification from shareable route and payload values.
    pub fn new(route: Route, payload: Bytes) -> Self {
        Self { route, payload }
    }
}

/// Notice errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeError {
    /// Invalid realm format (3030)
    InvalidRealm,

    /// Realm mismatch - operation targets different realm than active subscription (3031)
    RealmMismatch,
}

impl NoticeError {
    pub fn code(&self) -> u16 {
        match self {
            NoticeError::InvalidRealm => 3030,
            NoticeError::RealmMismatch => 3031,
        }
    }
}
