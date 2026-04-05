//! Queue protocol message types and responses
//!
//! Defines the message types for queue operations:
//! - **Send**: Add message to queue
//! - **Receive**: Lease one or more messages for processing
//! - **Extend**: Extend lease expiration for a received message
//! - **Ack**: Acknowledge and delete message
//!
//! # Queue Identity
//!
//! Queues are uniquely identified by (RouteFamily, realm, area, resource):
//! - RouteFamily: Routing isolation boundary (opaque u64)
//! - realm/area/resource: Logical identity within the family
//!
//! # Lease Semantics
//!
//! - Messages are received with a lease duration
//! - While leased, messages are invisible to other consumers
//! - Leases expire automatically, returning messages to the ready queue
//! - Redelivered messages have incremented attempt counters
//!
//! # Token Protocol
//!
//! - Each received message gets a random u64 token
//! - Tokens must be provided for extend/ack operations
//! - Invalid tokens are rejected (prevents accidental duplicate operations)
//! - Tokens are ephemeral (not persisted, regenerated on actor restart)
//!
//! # Long Polling (RPC-Level Only)
//!
//! Receive operations support optional long polling via `wait_seconds`:
//! - QueueActor always returns immediately (never blocks)
//! - If empty and `wait_seconds > 0`, QueueDomainSink keeps an ephemeral waiter
//! - The waiter is resumed when the queue becomes ready or when the wait expires
//! - QueueActor never stores waiters or blocking state

use crate::runtime::routing::{Route, RouteFamily};
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
    /// Writes the message body to durable storage and adds it to the ready queue.
    /// If delay_seconds is provided, message won't be visible until delay elapses.
    /// Returns the MessageId for tracking.
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
    /// Pops up to `batch_size` messages from the ready queue, creates leases,
    /// and returns them with bodies loaded from storage.
    ///
    /// If `batch_size` is None, defaults to 1.
    ///
    /// # Long Polling (RPC-Level Only)
    ///
    /// If `wait_seconds` is provided and receive returns empty:
    /// - QueueDomainSink keeps the request parked inside the queue domain
    /// - The waiter is resumed when the queue becomes ready or the wait expires
    /// - QueueActor NEVER blocks or stores waiters
    Receive {
        family_id: RouteFamily,
        route: Route,
        lease_seconds: u64,
        batch_size: Option<usize>,
        wait_seconds: Option<u64>,
    },

    /// Extend message lease
    ///
    /// Route format: `queue://{realm}/{area}/{resource}`
    ///
    /// Extends the expiration time for a reserved message.
    /// Requires valid token. Fails if token mismatches or lease expired.
    Extend {
        family_id: RouteFamily,
        route: Route,
        id: MessageId,
        token: u64,
        lease_seconds: u64,
    },

    /// Acknowledge message processing
    ///
    /// Route format: `queue://{realm}/{area}/{resource}`
    ///
    /// Marks message as successfully processed and acknowledges delivery.
    /// Removes inflight entry and deletes durable record.
    /// Requires valid token. Fails if token mismatches or lease expired.
    Ack {
        family_id: RouteFamily,
        route: Route,
        id: MessageId,
        token: u64,
    },

    /// Internal timer expiration event
    ///
    /// Not exposed via external routes.
    /// Sent by the actor's timer system when a lease expires.
    /// Causes message to be re-enqueued to the ready queue.
    LeaseExpired { id: MessageId },
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

    /// Multiple messages successfully sent in one batch (same semantics as N×Sent)
    SentBatch { ids: Vec<MessageId> },

    /// Messages successfully received
    Received { messages: Vec<ReservedMessage> },

    /// Lease successfully extended
    Extended,

    /// Message successfully acknowledged
    Acked,

    /// Invalid token (mismatch with expected value)
    InvalidToken,

    /// Lease already expired before operation
    LeaseExpired,

    /// Message not found (completed or never existed)
    NotFound,

    /// Bad request (malformed parameters)
    BadRequest { reason: String },

    /// Queue does not exist
    QueueNotFound,

    /// Internal error
    Error { message: String },
}
