//! Notification protocol message types
//!
//! Defines the message types used for pub/sub operations:
//! - **`Publish`**: Send message to matching subscribers
//! - **`Subscribe`**: Register pattern subscription
//! - **`Unsubscribe`**: Remove specific subscription
//! - **`UnsubscribeAll`**: Clean up session subscriptions on disconnect
//! - **`Deliver`**: Deliver published message to subscriber

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use crate::session::session::SessionId;
use bytes::Bytes;

/// Messages for the notification domain.
#[derive(Debug, Clone)]
pub enum NotificationMessage {
    /// Publish a message to all matching subscribers (from any domain/client)
    Publish(PublishMessage),
    /// Subscribe to messages matching a pattern after runtime-ingress authorization
    Subscribe(SubscribeMessage),
    /// Unsubscribe from a subscription ID after runtime-ingress authorization
    Unsubscribe(UnsubscribeMessage),
    /// Unsubscribe all subscriptions for a session (called on disconnect)
    UnsubscribeAll(UnsubscribeAllMessage),
    /// Deliver a published message to a subscriber (internal to Notice delivery)
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

/// Subscribe to messages matching a pattern (may include wildcards `*` and `**`)
///
/// Runtime ingress authorizes the complete concrete-route match set before the
/// production Notice family actor receives this message.
#[derive(Debug, Clone)]
pub struct SubscribeMessage {
    /// Route family for isolation
    pub family_id: RouteFamily,
    /// Pattern to match (may include * and ** wildcards)
    pub pattern: Route,
    /// Session making the subscription
    pub session_id: SessionId,
    /// Address to send notifications to (typically the session inbox)
    pub subscriber: RouteAddress,
}

impl SubscribeMessage {
    #[must_use]
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
/// Sent to the production Notice family actor after ingress authorization.
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
    #[must_use]
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
/// Runtime ingress sends this to the production Notice family actor when the
/// ephemeral session terminates. Cleanup is best-effort.
#[derive(Debug, Clone)]
pub struct UnsubscribeAllMessage {
    /// Session being disconnected
    pub session_id: SessionId,
    /// Address to remove from all subscriptions
    pub subscriber: RouteAddress,
}

impl UnsubscribeAllMessage {
    #[must_use]
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
    #[must_use]
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
    /// Operation failed; the kind selects the wire code, the message explains.
    Error(NoticeClientError),
}

/// The client-visible condition behind a Notice error. The wire codec maps
/// each kind to its numeric code; the domain never deals in numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeClientErrorKind {
    InvalidRoute,
    InvalidPattern,
    SubscriptionLimit,
    /// Any failure the domain cannot attribute to a client condition. It is
    /// fatal for the request and says nothing about whether it took effect.
    Backend,
}

/// A Notice error as the client will see it: a typed kind plus the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoticeClientError {
    pub kind: NoticeClientErrorKind,
    pub message: String,
}

impl NoticeClientError {
    pub fn new(kind: NoticeClientErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn invalid_pattern(message: impl Into<String>) -> Self {
        Self::new(NoticeClientErrorKind::InvalidPattern, message)
    }

    pub fn subscription_limit(message: impl Into<String>) -> Self {
        Self::new(NoticeClientErrorKind::SubscriptionLimit, message)
    }

    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(NoticeClientErrorKind::Backend, message)
    }

    /// Recover the kind from a free-text domain error. Shared validation
    /// helpers still report some conditions as strings; this is the one place
    /// that reads them, and `should_classify_every_known_notice_error_message`
    /// pins each wording so a reword fails a test instead of silently
    /// becoming `Backend`.
    #[must_use]
    pub fn from_message(message: String) -> Self {
        let kind = classify_notice_error_message(&message);
        Self { kind, message }
    }
}

fn classify_notice_error_message(message: &str) -> NoticeClientErrorKind {
    use NoticeClientErrorKind as Kind;

    match message {
        "empty pattern" | "route must not contain control characters" => Kind::InvalidPattern,
        message
            if message.starts_with("subscription pattern ")
                || message.starts_with("invalid subscription pattern:") =>
        {
            Kind::InvalidPattern
        }
        message
            if message.starts_with("notice subscription limit exceeded")
                || message.starts_with("wildcard subscription limit exceeded") =>
        {
            Kind::SubscriptionLimit
        }
        "route family mismatch" => Kind::InvalidRoute,
        message
            if message.starts_with("invalid notice publish route:")
                || message.starts_with("notice publish route ") =>
        {
            Kind::InvalidRoute
        }
        _ => Kind::Backend,
    }
}

impl From<String> for NoticeClientError {
    fn from(message: String) -> Self {
        Self::from_message(message)
    }
}

impl From<&str> for NoticeClientError {
    fn from(message: &str) -> Self {
        Self::from_message(message.to_owned())
    }
}

impl NoticeResponse {
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

#[cfg(test)]
mod response_tests {
    use super::NoticeResponse;

    #[test]
    fn should_classify_only_error_responses_as_failures() {
        // Arrange
        let responses = [
            (NoticeResponse::Ok, false),
            (NoticeResponse::SubscribeOk { subscription_id: 1 }, false),
            (NoticeResponse::Error("invalid".into()), true),
        ];

        // Act
        let classifications = responses
            .iter()
            .map(|(response, _)| response.is_failure())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(classifications, [false, false, true]);
    }

    #[test]
    fn should_classify_every_known_notice_error_message() {
        // Arrange
        use super::{NoticeClientError, NoticeClientErrorKind as Kind};
        let cases = [
            ("empty pattern", Kind::InvalidPattern),
            (
                "route must not contain control characters",
                Kind::InvalidPattern,
            ),
            (
                "subscription pattern must use notice://",
                Kind::InvalidPattern,
            ),
            ("invalid subscription pattern: bad", Kind::InvalidPattern),
            (
                "notice subscription limit exceeded (1024 per session)",
                Kind::SubscriptionLimit,
            ),
            (
                "wildcard subscription limit exceeded (128 per session)",
                Kind::SubscriptionLimit,
            ),
            ("route family mismatch", Kind::InvalidRoute),
            ("invalid notice publish route: x", Kind::InvalidRoute),
            (
                "notice publish route must have three segments",
                Kind::InvalidRoute,
            ),
            ("backend route cache unavailable", Kind::Backend),
        ];

        // Act
        let kinds = cases
            .iter()
            .map(|(message, _)| NoticeClientError::from(*message).kind)
            .collect::<Vec<_>>();

        // Assert
        let expected = cases.iter().map(|(_, kind)| *kind).collect::<Vec<_>>();
        assert_eq!(kinds, expected);
    }

    #[test]
    fn should_classify_real_pattern_compiler_errors_as_the_spec_documents() {
        // Arrange: the spec keeps 3005 for malformed wildcard shapes and 3002
        // for grammar errors, so the real producer must keep that split.
        use super::{NoticeClientError, NoticeClientErrorKind as Kind};
        let compile = |pattern: &str| {
            crate::runtime::DomainKind::Notice
                .descriptor()
                .compile_registration_pattern(pattern)
                .expect_err("pattern must be rejected")
        };

        // Act
        let partial_wildcard = NoticeClientError::from(compile("notice://r/ord*ers/x")).kind;
        let adjacent_double_star = NoticeClientError::from(compile("notice://r/**/**")).kind;
        let control_character = NoticeClientError::from(compile("notice://r/a/b\u{1}")).kind;
        let empty_segment = NoticeClientError::from(compile("notice://r//x")).kind;

        // Assert
        assert_eq!(partial_wildcard, Kind::Backend);
        assert_eq!(adjacent_double_star, Kind::Backend);
        assert_eq!(control_character, Kind::InvalidPattern);
        assert_eq!(empty_segment, Kind::InvalidPattern);
    }
}
