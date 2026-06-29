//! Notification protocol message types
//!
//! Defines the message types used for pub/sub operations:
//! - **Publish**: Send message to matching subscribers
//! - **Subscribe**: Register pattern subscription
//! - **Unsubscribe**: Remove specific subscription
//! - **UnsubscribeAll**: Clean up session subscriptions on disconnect
//! - **Deliver**: Deliver published message to subscriber

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use crate::session::session::SessionId;
use bytes::Bytes;

/// Messages for the notification domain
#[derive(Debug, Clone)]
pub enum NotificationMessage {
    /// Publish a message to all matching subscribers (from any domain/client)
    Publish(PublishMessage),
    /// Subscribe to messages matching a pattern (from SessionActor)
    Subscribe(SubscribeMessage),
    /// Unsubscribe from a subscription ID (from SessionActor)
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

/// Unsubscribe from a subscription
///
/// Sent from SessionActor to NoticeRouteActor.
#[derive(Debug, Clone)]
pub struct UnsubscribeMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Subscription being unsubscribed
    pub subscription_id: u64,
    /// Session being unsubscribed
    pub session_id: SessionId,
}

impl UnsubscribeMessage {
    pub fn new(family_id: RouteFamily, subscription_id: u64, session_id: SessionId) -> Self {
        Self {
            family_id,
            subscription_id,
            session_id,
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

/// Parsed client request delivered to the Notice domain sink.
#[derive(Debug, Clone)]
pub struct NoticeClientRequest {
    pub meta: ClientFrameMeta,
    pub message: Result<NotificationMessage, String>,
}

impl NoticeClientRequest {
    pub fn new(meta: ClientFrameMeta, message: Result<NotificationMessage, String>) -> Self {
        Self { meta, message }
    }
}

/// Typed Notice response to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct NoticeClientResponse {
    pub meta: ClientFrameMeta,
    pub response: NoticeResponse,
}

impl NoticeClientResponse {
    pub fn new(meta: ClientFrameMeta, response: NoticeResponse) -> Self {
        Self { meta, response }
    }
}

/// Typed Notice delivery notification to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct NoticeClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub route: Route,
    pub payload: Bytes,
}

impl NoticeClientNotification {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: Route,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
            payload,
        }
    }
}

/// Response from notice operations.
#[derive(Debug, Clone)]
pub enum NoticeResponse {
    /// Operation succeeded with no response payload.
    Ok,
    /// Subscribe succeeded and returns a subscription ID.
    SubscribeOk { subscription_id: u64 },
    /// Operation failed with error message.
    Error(String),
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
