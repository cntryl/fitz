//! Queue protocol message types and responses
//!
//! Defines the message types for queue operations:
//! - **Send**: Add message to queue
//! - **Receive**: Reserve one or more messages for processing
//! - **Extend**: Extend inflight expiration for a reserved message
//! - **Ack**: Acknowledge and delete message
//!
//! # Queue Identity
//!
//! Queues are uniquely identified by (`RouteFamily`, realm, area, resource):
//! - `RouteFamily`: Routing isolation boundary (opaque u64)
//! - realm/area/resource: Logical identity within the family
//!
//! # Inflight Semantics
//!
//! - Messages are received with an inflight duration
//! - While inflight, messages are invisible to other consumers
//! - Inflight reservations expire automatically, returning messages to the ready queue
//! - Redelivered messages have incremented attempt counters
//!
//! # Token Protocol
//!
//! - Each received message gets a random u64 token
//! - Tokens must be provided for extend/ack operations
//! - Invalid tokens are rejected (prevents accidental duplicate operations)
//! - Tokens are ephemeral (not persisted, regenerated on actor restart)
//!
//! # Queue Watches
//!
//! Queue availability is surfaced through explicit watch notifications:
//! - `QueueActor` always returns immediately (never blocks)
//! - `QueueDomainSink` owns ephemeral watch state for the current broker process
//! - Watches target exact or wildcard `queue://{realm}/{area}/{resource}` routes
//! - Notifications are readiness signals only; they never carry queue message bodies

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use super::core::{MessageId, QueueKey, ReservedMessage};

/// Queue domain messages
///
/// All queue operations are asynchronous and return responses via
/// the actor messaging system.
#[derive(Debug, Clone)]
pub enum QueueMessage {
    /// Send a message to the queue
    ///
    /// Route format: `queue://{realm}/{area}/{resource}`
    ///
    /// Writes the message body according to the configured queue write policy and
    /// adds it to the ready queue.
    /// If `delay_seconds` is provided, message won't be visible until delay elapses.
    /// Returns the `MessageId` for tracking.
    Send {
        family_id: RouteFamily,
        route: Route,
        body: Bytes,
        delay_seconds: Option<u64>,
    },

    /// Receive messages for processing
    ///
    /// Route format: `queue://{realm}/{area}/{resource}`
    ///
    /// Pops up to `batch_size` messages from the ready queue, creates inflight reservations,
    /// and returns them with bodies loaded from storage.
    ///
    /// If `batch_size` is None, defaults to 1.
    ///
    Receive {
        family_id: RouteFamily,
        route: Route,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    },

    /// Extend inflight reservation
    ///
    /// Route format: `queue://{realm}/{area}/{resource}`
    ///
    /// Extends the expiration time for a reserved message.
    /// Requires valid token. Fails if token mismatches or inflight reservation expired.
    Extend {
        family_id: RouteFamily,
        route: Route,
        id: MessageId,
        token: u64,
        inflight_seconds: u64,
    },

    /// Acknowledge message processing
    ///
    /// Route format: `queue://{realm}/{area}/{resource}`
    ///
    /// Marks message as successfully processed and acknowledges delivery.
    /// Removes inflight entry and deletes the queue record according to the
    /// configured queue write policy.
    /// Requires valid token. Fails if token mismatches or inflight entry expired.
    Ack {
        family_id: RouteFamily,
        route: Route,
        id: MessageId,
        token: u64,
    },

    /// Internal timer expiration event
    ///
    /// Not exposed via external routes.
    /// Sent by the actor's timer system when an inflight reservation expires.
    /// Causes message to be re-enqueued to the ready queue.
    InflightExpired { id: MessageId },
}

/// Queue watch messages handled by `QueueDomainSink` before actor dispatch.
#[derive(Debug, Clone)]
pub enum QueueSubscriptionMessage {
    Watch {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    Unwatch {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Parsed client request delivered to the Queue domain sink.
#[derive(Debug, Clone)]
pub struct QueueClientRequest {
    pub meta: ClientFrameMeta,
    pub frame: Result<QueueClientFrame, String>,
}

impl QueueClientRequest {
    pub fn new(meta: ClientFrameMeta, frame: Result<QueueClientFrame, String>) -> Self {
        Self { meta, frame }
    }
}

/// Queue request classified after wire parsing.
#[derive(Debug, Clone)]
pub enum QueueClientFrame {
    Op(QueueMessage),
    Sub(QueueSubscriptionMessage),
}

/// Typed Queue response to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct QueueClientResponse {
    pub meta: ClientFrameMeta,
    pub response: QueueResponse,
}

impl QueueClientResponse {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, response: QueueResponse) -> Self {
        Self { meta, response }
    }
}

/// Typed Queue watch notification to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct QueueClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub route: Route,
    pub notification: QueueNotification,
}

impl QueueClientNotification {
    #[must_use]
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: Route,
        notification: QueueNotification,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
            notification,
        }
    }
}

/// Lightweight readiness notification payload for queue watchers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNotification {
    pub ready_messages: u64,
    pub delayed_messages: u64,
    pub inflight_messages: u64,
}

/// Queue errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    /// Invalid realm format (3010)
    InvalidRealm,

    /// Realm mismatch - operation targets different realm than active subscription (3011)
    RealmMismatch,
}

impl QueueError {
    #[must_use]
    pub fn code(&self) -> u16 {
        match self {
            QueueError::InvalidRealm => 3010,
            QueueError::RealmMismatch => 3011,
        }
    }
}

/// Queue operation responses
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueResponse {
    /// Message successfully sent
    Sent { id: MessageId },

    /// Queue watch successfully registered.
    WatchOk { subscription_id: u64 },

    /// Queue watch successfully removed.
    UnwatchOk,

    /// Multiple messages successfully sent in one batch (same semantics as N×Sent)
    SentBatch { ids: Vec<MessageId> },

    /// Messages successfully received
    Received { messages: Vec<ReservedMessage> },

    /// Inflight entry successfully extended
    Extended,

    /// Message successfully acknowledged
    Acked,

    /// Invalid token (mismatch with expected value)
    InvalidToken,

    /// Inflight reservation already expired before operation
    InflightExpired,

    /// Message not found (completed or never existed)
    NotFound,

    /// Bad request (malformed parameters)
    BadRequest { reason: String },

    /// Invalid exact or wildcard Queue subscription pattern.
    InvalidSubscriptionPattern { reason: String },

    /// Per-session wildcard subscription limit reached.
    SubscriptionLimit,

    /// Queue does not exist
    QueueNotFound,

    /// Internal error
    Error { message: String },
}
