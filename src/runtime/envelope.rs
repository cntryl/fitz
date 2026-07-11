// LAYER: RUNTIME
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

use crate::runtime::routing::RouteAddress;
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Envelope metadata without the payload (for zero-copy causation tracking)
#[derive(Debug, Clone)]
pub struct EnvelopeMetadata {
    pub id: MessageId,
    pub source: Option<RouteAddress>,
    pub destination: RouteAddress,
    pub causation: Option<MessageId>,
    pub deadline: Option<Instant>,
    pub queued_at: Option<Instant>,
}

/// Unique message identifier for tracing and correlation
///
/// # Current Implementation
///
/// IDs are generated from a process-local atomic counter. This means:
/// - **Process-local**: IDs are unique within a single Fitz process
/// - **Not stable**: IDs reset on process restart
/// - **Monotonic**: IDs increase sequentially (useful for ordering)
///
/// # Future Evolution
///
/// This will be replaced with distributed ID generation when remoting is added:
/// - **UUID**: For globally unique, collision-resistant IDs
/// - **Snowflake**: For sortable distributed IDs with timestamp prefix
/// - **Midge-backed sequence**: For persistent, cluster-coordinated sequences
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(u64);

impl MessageId {
    /// Create a new unique message ID
    #[inline]
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Get the underlying ID value
    #[inline]
    #[must_use]
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

    /// Source route (None for external/user-initiated messages)
    source: Option<RouteAddress>,

    /// Destination route
    destination: RouteAddress,

    /// Parent message ID for causation tracking (request/reply chains)
    causation: Option<MessageId>,

    /// Optional deadline for time-bounded processing
    deadline: Option<Instant>,

    /// Mailbox enqueue time, set when the envelope is accepted by a mailbox.
    queued_at: Option<Instant>,

    /// Type-erased message payload (must be Send + Sync)
    payload: Box<dyn Any + Send + Sync>,
}

impl Envelope {
    /// Create a new envelope with a destination and payload
    pub fn new<M: Any + Send + Sync>(destination: RouteAddress, payload: M) -> Self {
        Self {
            id: MessageId::new(),
            source: None,
            destination,
            causation: None,
            deadline: None,
            queued_at: None,
            payload: Box::new(payload),
        }
    }

    /// Create an envelope with a known source route
    pub fn from_route<M: Any + Send + Sync>(
        source: RouteAddress,
        destination: RouteAddress,
        payload: M,
    ) -> Self {
        Self {
            id: MessageId::new(),
            source: Some(source),
            destination,
            causation: None,
            deadline: None,
            queued_at: None,
            payload: Box::new(payload),
        }
    }

    /// Set a deadline for this message
    ///
    /// Messages past their deadline may be dropped or logged as warnings.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set the causation chain (parent message ID)
    ///
    /// Used for request/reply tracking and distributed tracing.
    #[must_use]
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
    #[must_use]
    pub fn reply_to<M: Any + Send + Sync>(&self, payload: M) -> Envelope {
        let source = self
            .source
            .as_ref()
            .expect("Cannot reply to message with no source");

        Envelope {
            id: MessageId::new(),
            source: Some(self.destination.clone()),
            destination: source.clone(),
            causation: Some(self.id),
            deadline: self.deadline,
            queued_at: None,
            payload: Box::new(payload),
        }
    }

    /// Create a reply envelope to the source of this message (non-panicking)
    ///
    /// Returns `None` if this envelope has no source address.
    /// Prefer this over `reply_to()` in production paths.
    pub fn try_reply_to<M: Any + Send + Sync>(&self, payload: M) -> Option<Envelope> {
        let source = self.source.as_ref()?;

        Some(Envelope {
            id: MessageId::new(),
            source: Some(self.destination.clone()),
            destination: source.clone(),
            causation: Some(self.id),
            deadline: self.deadline,
            queued_at: None,
            payload: Box::new(payload),
        })
    }

    /// Get the message ID
    #[inline]
    #[must_use]
    pub fn id(&self) -> MessageId {
        self.id
    }

    /// Get the source route address (if any)
    #[inline]
    #[must_use]
    pub fn source(&self) -> Option<&RouteAddress> {
        self.source.as_ref()
    }

    /// Get the destination route address
    #[inline]
    #[must_use]
    pub fn destination(&self) -> &RouteAddress {
        &self.destination
    }

    /// Get the causation ID (parent message)
    #[inline]
    #[must_use]
    pub fn causation(&self) -> Option<MessageId> {
        self.causation
    }

    /// Get the deadline (if any)
    #[inline]
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Get the mailbox enqueue time, if this envelope has entered a mailbox.
    #[inline]
    #[must_use]
    pub fn queued_at(&self) -> Option<Instant> {
        self.queued_at
    }

    /// Stamp the envelope with the instant it entered a mailbox.
    #[inline]
    pub(crate) fn mark_queued(&mut self, queued_at: Instant) {
        self.queued_at = Some(queued_at);
    }

    /// Check if this message has expired past its deadline
    ///
    /// Hot path: no deadline (None) returns false without calling `Instant::now()`.
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        match self.deadline {
            None => false,
            Some(d) => Instant::now() > d,
        }
    }

    /// Extract metadata without consuming the envelope
    #[inline]
    #[must_use]
    pub fn metadata(&self) -> EnvelopeMetadata {
        EnvelopeMetadata {
            id: self.id,
            source: self.source.clone(),
            destination: self.destination.clone(),
            causation: self.causation,
            deadline: self.deadline,
            queued_at: self.queued_at,
        }
    }

    /// Extract metadata and payload together (zero-copy for metadata)
    #[must_use]
    pub fn into_parts<M: Any>(self) -> (EnvelopeMetadata, Option<M>) {
        let metadata = EnvelopeMetadata {
            id: self.id,
            source: self.source,
            destination: self.destination,
            causation: self.causation,
            deadline: self.deadline,
            queued_at: self.queued_at,
        };
        let payload = self.payload.downcast::<M>().ok().map(|b| *b);
        (metadata, payload)
    }

    /// Extract the payload, downcasting to the expected type
    ///
    /// Returns `None` if the type doesn't match.
    #[must_use]
    pub fn into_payload<M: Any>(self) -> Option<M> {
        self.payload.downcast::<M>().ok().map(|b| *b)
    }

    /// Borrow the payload as the expected type
    ///
    /// Returns `None` if the type doesn't match.
    #[must_use]
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
            .field("queued_at", &self.queued_at)
            .field("payload", &"<type-erased>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteFamily};
    use std::time::Duration;

    fn test_address(family: u64, route: &str) -> RouteAddress {
        RouteAddress::new(
            RouteFamily::try_from(family).expect("test family must fit in u32"),
            Route::new(route),
        )
    }

    #[test]
    fn should_create_envelope_with_destination() {
        // Arrange
        let destination = test_address(1, "/test/actor");
        let payload = "test message";

        // Act
        let envelope = Envelope::new(destination.clone(), payload);

        // Assert
        assert_eq!(envelope.destination(), &destination);
        assert_eq!(envelope.source(), None);
        assert_eq!(envelope.payload::<&str>(), Some(&"test message"));
    }

    #[test]
    fn should_create_envelope_with_source() {
        // Arrange
        let source = test_address(1, "/test/source");
        let destination = test_address(1, "/test/destination");
        let payload = 42;

        // Act
        let envelope = Envelope::from_route(source.clone(), destination.clone(), payload);

        // Assert
        assert_eq!(envelope.source(), Some(&source));
        assert_eq!(envelope.destination(), &destination);
        assert_eq!(envelope.payload::<i32>(), Some(&42));
    }

    #[test]
    fn should_set_deadline() {
        // Arrange
        let destination = test_address(1, "/test/actor");
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
        let destination = test_address(1, "/test/actor");
        let past_deadline = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();

        // Act
        let envelope = Envelope::new(destination, "msg").with_deadline(past_deadline);

        // Assert
        assert!(envelope.is_expired());
    }

    #[test]
    fn should_set_causation() {
        // Arrange
        let destination = test_address(1, "/test/actor");
        let parent_id = MessageId::new();

        // Act
        let envelope = Envelope::new(destination, "msg").with_causation(parent_id);

        // Assert
        assert_eq!(envelope.causation(), Some(parent_id));
    }

    #[test]
    fn should_create_reply_envelope() {
        // Arrange
        let source = test_address(1, "/test/source");
        let destination = test_address(1, "/test/destination");
        let original = Envelope::from_route(source.clone(), destination.clone(), "request");

        // Act
        let reply = original.reply_to("response");

        // Assert
        assert_eq!(reply.source(), Some(&destination));
        assert_eq!(reply.destination(), &source);
        assert_eq!(reply.causation(), Some(original.id()));
        assert_eq!(reply.payload::<&str>(), Some(&"response"));
    }

    #[test]
    fn should_extract_payload() {
        // Arrange
        let destination = test_address(1, "/test/actor");
        let envelope = Envelope::new(destination, 42_i32);

        // Act
        let payload = envelope.into_payload::<i32>();

        // Assert
        assert_eq!(payload, Some(42));
    }

    #[test]
    fn should_mark_envelope_as_queued() {
        // Arrange
        let mut envelope = Envelope::new(test_address(1, "/test/actor"), 42_i32);
        let queued_at = Instant::now();

        // Act
        envelope.mark_queued(queued_at);

        // Assert
        assert_eq!(envelope.queued_at(), Some(queued_at));
    }

    #[test]
    fn should_return_none_for_wrong_type() {
        // Arrange
        let destination = test_address(1, "/test/actor");
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
        let formatted = format!("{id}");

        // Assert
        assert!(formatted.starts_with("msg:"));
    }

    #[test]
    fn should_inherit_deadline_in_reply() {
        // Arrange
        let source = test_address(1, "/test/source");
        let destination = test_address(1, "/test/destination");
        let deadline = Instant::now() + Duration::from_secs(5);
        let original = Envelope::from_route(source, destination, "request").with_deadline(deadline);

        // Act
        let reply = original.reply_to("response");

        // Assert
        assert_eq!(reply.deadline(), Some(deadline));
    }
}
