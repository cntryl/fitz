//! Notification protocol definitions
//!
//! The notification protocol supports fire-and-forget pub/sub with wildcard routing.
//!
//! # Architecture
//!
//! - **SessionActor** enforces all authentication and authorization
//! - **NoticeRouteActor** owns subscriptions and performs fanout (trusts SessionActor)
//! - Subscriptions are session-scoped and cleaned up on disconnect
//! - Authorization is prefix-based and evaluated only at subscribe/publish time
//!
//! # Message Types
//!
//! - **Publish**: Send a message to all subscribers matching a pattern
//! - **Subscribe**: Register a session to receive messages on a pattern
//! - **Unsubscribe**: Unregister a session from a subscription
//! - **UnsubscribeAll**: Called on session disconnect to clean up all subscriptions
//! - **Notify**: Deliver a published message to a subscribed session
//!
//! # Semantics
//!
//! - Fire-and-forget: No acknowledgements, retries, or delivery guarantees
//! - Best-effort: Messages are delivered only to subscribers alive at publish time
//! - Isolated: All messaging is scoped to (RouteFamilyId, route) pairs
//! - Session-scoped: Subscriptions vanish on disconnect

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
