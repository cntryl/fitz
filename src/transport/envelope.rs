//! Domain-agnostic message envelope for routing, tracing, and observability
//!
//! # Purpose
//!
//! The `Envelope` type wraps actor messages with metadata needed for:
//! - **Routing**: Source and destination actor identification
//! - **Tracing**: Message causation chains (parent/child relationships)
//! - **Deadlines**: Time-bounded message processing
//! - **Observability**: Message correlation and tracking
//! - **Future remoting**: Network transparency (not yet implemented)
//!
//! # Design Principles
//!
//! 1. **Type Erasure**: Payloads are `Box<dyn Any>` so the envelope can carry
//!    any message type without knowing its structure
//! 2. **Immutability**: Envelopes are immutable once created
//! 3. **Zero Actor Impact**: Actors still process strongly-typed messages;
//!    envelope unwrapping happens in the runtime
//! 4. **In-Process Only**: Currently for local routing; remote delivery is future work
//!
//! # Usage
//!
//! Envelopes are created by the runtime when:
//! - An actor sends a message via `ActorRef::send()`
//! - A domain handler dispatches to another actor
//! - A reply is sent back to a caller
//!
//! Actors themselves never see envelopes directly—they receive typed messages.
//!
//! # Example
//!
//! ```ignore
//! use fitz::transport::envelope::Envelope;
//! use fitz::runtime::ActorId;
//! use std::time::{Duration, Instant};
//!
//! // Create an envelope with a deadline
//! let envelope = Envelope::new(
//!     ActorId::new(1),  // destination
//!     "Hello".to_string()  // payload
//! ).with_deadline(Instant::now() + Duration::from_secs(5));
//!
//! // Create a reply envelope
//! let reply = envelope.reply_to("World".to_string());
//! ```

use crate::runtime::ActorId;
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Unique message identifier for tracing and correlation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(u64);

impl MessageId {
    /// Create a new unique message ID
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    /// Get the underlying ID value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "msg:{}", self.0)
    }
}

/// Domain-agnostic message envelope with routing and tracing metadata
///
/// Wraps actor messages with metadata needed for routing, observability,
/// and future remoting capabilities. The payload is type-erased to allow
/// the runtime to handle any message type uniformly.
pub struct Envelope {
    /// Unique message identifier
    id: MessageId,

    /// Source actor (None for external/user-initiated messages)
    source: Option<ActorId>,

    /// Destination actor
    destination: ActorId,

    /// Parent message ID for causation tracking (request/reply chains)
    causation: Option<MessageId>,

    /// Optional deadline for time-bounded processing
    deadline: Option<Instant>,

    /// Type-erased message payload (must be Send + Sync)
    payload: Box<dyn Any + Send + Sync>,
}

impl Envelope {
    /// Create a new envelope with a destination and payload
    ///
    /// # Example
    ///
    /// ```ignore
    /// let envelope = Envelope::new(actor_id, MyMessage::DoWork);
    /// ```
    pub fn new<M: Any + Send + Sync>(destination: ActorId, payload: M) -> Self {
        Self {
            id: MessageId::new(),
            source: None,
            destination,
            causation: None,
            deadline: None,
            payload: Box::new(payload),
        }
    }

    /// Create an envelope with a known source actor
    pub fn from_actor<M: Any + Send + Sync>(
        source: ActorId,
        destination: ActorId,
        payload: M,
    ) -> Self {
        Self {
            id: MessageId::new(),
            source: Some(source),
            destination,
            causation: None,
            deadline: None,
            payload: Box::new(payload),
        }
    }

    /// Set a deadline for this message
    ///
    /// Messages past their deadline may be dropped or logged as warnings.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set the causation chain (parent message ID)
    ///
    /// Used for request/reply tracking and distributed tracing.
    pub fn with_causation(mut self, parent: MessageId) -> Self {
        self.causation = Some(parent);
        self
    }

    /// Create a reply envelope to the source of this message
    ///
    /// The reply inherits:
    /// - Source becomes destination (reply goes back to sender)
    /// - This message's ID becomes the causation ID
    /// - Deadline is inherited if present
    ///
    /// # Panics
    ///
    /// Panics if this envelope has no source (cannot reply to external messages)
    pub fn reply_to<M: Any + Send + Sync>(&self, payload: M) -> Envelope {
        let source = self
            .source
            .expect("Cannot reply to message with no source");

        Envelope {
            id: MessageId::new(),
            source: Some(self.destination),
            destination: source,
            causation: Some(self.id),
            deadline: self.deadline,
            payload: Box::new(payload),
        }
    }

    /// Get the message ID
    pub fn id(&self) -> MessageId {
        self.id
    }

    /// Get the source actor ID (if any)
    pub fn source(&self) -> Option<ActorId> {
        self.source
    }

    /// Get the destination actor ID
    pub fn destination(&self) -> ActorId {
        self.destination
    }

    /// Get the causation ID (parent message)
    pub fn causation(&self) -> Option<MessageId> {
        self.causation
    }

    /// Get the deadline (if any)
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Check if this message has expired past its deadline
    pub fn is_expired(&self) -> bool {
        self.deadline
            .map(|d| Instant::now() > d)
            .unwrap_or(false)
    }

    /// Extract the payload, downcasting to the expected type
    ///
    /// Returns `None` if the type doesn't match.
    pub fn into_payload<M: Any>(self) -> Option<M> {
        self.payload.downcast::<M>().ok().map(|b| *b)
    }

    /// Borrow the payload as the expected type
    ///
    /// Returns `None` if the type doesn't match.
    pub fn payload<M: Any>(&self) -> Option<&M> {
        self.payload.downcast_ref::<M>()
    }
}

impl fmt::Debug for Envelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Envelope")
            .field("id", &self.id)
            .field("source", &self.source)
            .field("destination", &self.destination)
            .field("causation", &self.causation)
            .field("deadline", &self.deadline)
            .field("payload", &"<type-erased>")
            .finish()
    }
}

// Envelope is Send + Sync because all fields are Send + Sync
unsafe impl Send for Envelope {}
unsafe impl Sync for Envelope {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn should_create_envelope_with_destination() {
        // Arrange
        let destination = ActorId::new(42);
        let payload = "test message";

        // Act
        let envelope = Envelope::new(destination, payload);

        // Assert
        assert_eq!(envelope.destination(), destination);
        assert_eq!(envelope.source(), None);
        assert_eq!(envelope.payload::<&str>(), Some(&"test message"));
    }

    #[test]
    fn should_create_envelope_with_source() {
        // Arrange
        let source = ActorId::new(1);
        let destination = ActorId::new(2);
        let payload = 42;

        // Act
        let envelope = Envelope::from_actor(source, destination, payload);

        // Assert
        assert_eq!(envelope.source(), Some(source));
        assert_eq!(envelope.destination(), destination);
        assert_eq!(envelope.payload::<i32>(), Some(&42));
    }

    #[test]
    fn should_set_deadline() {
        // Arrange
        let destination = ActorId::new(1);
        let deadline = Instant::now() + Duration::from_secs(10);

        // Act
        let envelope = Envelope::new(destination, "msg").with_deadline(deadline);

        // Assert
        assert_eq!(envelope.deadline(), Some(deadline));
        assert!(!envelope.is_expired());
    }

    #[test]
    fn should_detect_expired_deadline() {
        // Arrange
        let destination = ActorId::new(1);
        let past_deadline = Instant::now() - Duration::from_secs(1);

        // Act
        let envelope = Envelope::new(destination, "msg").with_deadline(past_deadline);

        // Assert
        assert!(envelope.is_expired());
    }

    #[test]
    fn should_set_causation() {
        // Arrange
        let destination = ActorId::new(1);
        let parent_id = MessageId::new();

        // Act
        let envelope = Envelope::new(destination, "msg").with_causation(parent_id);

        // Assert
        assert_eq!(envelope.causation(), Some(parent_id));
    }

    #[test]
    fn should_create_reply_envelope() {
        // Arrange
        let source = ActorId::new(1);
        let destination = ActorId::new(2);
        let original = Envelope::from_actor(source, destination, "request");

        // Act
        let reply = original.reply_to("response");

        // Assert
        assert_eq!(reply.source(), Some(destination));
        assert_eq!(reply.destination(), source);
        assert_eq!(reply.causation(), Some(original.id()));
        assert_eq!(reply.payload::<&str>(), Some(&"response"));
    }

    #[test]
    fn should_extract_payload() {
        // Arrange
        let destination = ActorId::new(1);
        let envelope = Envelope::new(destination, 42_i32);

        // Act
        let payload = envelope.into_payload::<i32>();

        // Assert
        assert_eq!(payload, Some(42));
    }

    #[test]
    fn should_return_none_for_wrong_type() {
        // Arrange
        let destination = ActorId::new(1);
        let envelope = Envelope::new(destination, "string");

        // Act
        let payload = envelope.payload::<i32>();

        // Assert
        assert_eq!(payload, None);
    }

    #[test]
    fn should_generate_unique_message_ids() {
        // Arrange
        let id1 = MessageId::new();

        // Act
        let id2 = MessageId::new();

        // Assert
        assert_ne!(id1, id2);
        assert!(id2.as_u64() > id1.as_u64());
    }

    #[test]
    fn should_format_message_id() {
        // Arrange
        let id = MessageId::new();

        // Act
        let formatted = format!("{}", id);

        // Assert
        assert!(formatted.starts_with("msg:"));
    }

    #[test]
    fn should_inherit_deadline_in_reply() {
        // Arrange
        let source = ActorId::new(1);
        let destination = ActorId::new(2);
        let deadline = Instant::now() + Duration::from_secs(5);
        let original = Envelope::from_actor(source, destination, "request")
            .with_deadline(deadline);

        // Act
        let reply = original.reply_to("response");

        // Assert
        assert_eq!(reply.deadline(), Some(deadline));
    }
}
