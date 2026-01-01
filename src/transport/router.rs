//! Message routing and delivery infrastructure
//!
//! This module defines the routing layer that sits between message producers
//! and actor mailboxes. The router uses a trait-based sink interface to avoid
//! circular dependencies between the transport and runtime layers.
//!
//! # Architecture
//!
//! ```text
//! [Actor/Client]
//!       ↓
//!   Envelope
//!       ↓
//!    Router  ──→  MailboxSink (trait)
//!                      ↑
//!                      │ implements
//!                      │
//!                  Mailbox (runtime)
//! ```
//!
//! # Design Principles
//!
//! 1. **Narrow Interface**: Router depends only on `MailboxSink` trait, not concrete types
//! 2. **No Runtime Dependency**: Transport layer never imports runtime types
//! 3. **Best-Effort Delivery**: Router does not guarantee delivery or retry
//! 4. **In-Process Only**: No network transparency (future work)
//!
//! # Invariants
//!
//! - Routing is synchronous and fails fast
//! - Unknown destinations are dropped (logged at debug level)
//! - Sink implementations handle backpressure
//! - Router is thread-safe and cloneable

use crate::runtime::ActorId;
use crate::transport::envelope::Envelope;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Trait for delivering envelopes to actor mailboxes
///
/// This trait provides a narrow interface between the routing layer
/// and the execution layer (runtime). It must be object-safe to allow
/// dynamic dispatch and avoid circular dependencies.
///
/// # Implementation Notes
///
/// Implementers should:
/// - Handle backpressure appropriately (drop, block, or return error)
/// - Be thread-safe (Send + Sync)
/// - Fail fast rather than retry
pub trait MailboxSink: Send + Sync {
    /// Attempt to deliver an envelope to this mailbox
    ///
    /// Returns `Ok(())` if the envelope was accepted, or an error describing
    /// why delivery failed (full, disconnected, etc.).
    ///
    /// # Errors
    ///
    /// Returns `DeliveryError` if:
    /// - Mailbox is at capacity (backpressure)
    /// - Mailbox receiver has been dropped (actor stopped)
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError>;
}

/// Errors that can occur during envelope delivery
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryError {
    /// Mailbox is at capacity (backpressure)
    MailboxFull,
    /// Mailbox receiver has been dropped (actor stopped)
    ActorStopped,
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryError::MailboxFull => write!(f, "Mailbox is full"),
            DeliveryError::ActorStopped => write!(f, "Actor has stopped"),
        }
    }
}

impl std::error::Error for DeliveryError {}

/// Errors that can occur during routing
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// Destination actor not found in registry
    ActorNotFound(ActorId),
    /// Delivery failed (mailbox full or actor stopped)
    DeliveryFailed(ActorId, DeliveryError),
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteError::ActorNotFound(id) => write!(f, "Actor not found: {:?}", id),
            RouteError::DeliveryFailed(id, err) => {
                write!(f, "Delivery failed for actor {:?}: {}", id, err)
            }
        }
    }
}

impl std::error::Error for RouteError {}

/// Actor registry mapping ActorId to mailbox sinks
struct ActorRegistry {
    sinks: RwLock<HashMap<ActorId, Arc<dyn MailboxSink>>>,
}

impl ActorRegistry {
    fn new() -> Self {
        Self {
            sinks: RwLock::new(HashMap::new()),
        }
    }

    fn register(&self, id: ActorId, sink: Arc<dyn MailboxSink>) {
        let mut sinks = self.sinks.write().unwrap();
        sinks.insert(id, sink);
    }

    fn unregister(&self, id: ActorId) {
        let mut sinks = self.sinks.write().unwrap();
        sinks.remove(&id);
    }

    fn get(&self, id: ActorId) -> Option<Arc<dyn MailboxSink>> {
        let sinks = self.sinks.read().unwrap();
        sinks.get(&id).cloned()
    }

    fn len(&self) -> usize {
        let sinks = self.sinks.read().unwrap();
        sinks.len()
    }
}

/// Message router for in-process actor delivery
///
/// The router maintains a registry of actor mailbox sinks and delivers
/// envelopes to their destinations. It provides best-effort delivery
/// without guarantees or retries.
///
/// # Thread Safety
///
/// Router is `Clone` and all clones share the same registry. This allows
/// multiple threads to route messages concurrently.
///
/// # Example
///
/// ```ignore
/// use fitz::transport::router::{Router, MailboxSink};
/// use fitz::transport::envelope::Envelope;
/// use fitz::runtime::ActorId;
///
/// let router = Router::new();
/// let actor_id = ActorId::new(1);
///
/// // Register a mailbox sink
/// router.register(actor_id, Arc::new(my_sink));
///
/// // Route an envelope
/// let envelope = Envelope::new(actor_id, MyMessage::DoWork);
/// router.route(envelope)?;
/// ```
#[derive(Clone)]
pub struct Router {
    registry: Arc<ActorRegistry>,
}

impl Router {
    /// Create a new router with an empty registry
    pub fn new() -> Self {
        Self {
            registry: Arc::new(ActorRegistry::new()),
        }
    }

    /// Register an actor's mailbox sink
    ///
    /// After registration, envelopes addressed to this actor will be
    /// delivered to the provided sink.
    ///
    /// # Note
    ///
    /// If an actor with this ID is already registered, the old sink
    /// is replaced.
    pub fn register(&self, id: ActorId, sink: Arc<dyn MailboxSink>) {
        self.registry.register(id, sink);
    }

    /// Unregister an actor
    ///
    /// After unregistration, envelopes addressed to this actor will
    /// fail with `ActorNotFound` error.
    pub fn unregister(&self, id: ActorId) {
        self.registry.unregister(id);
    }

    /// Route an envelope to its destination actor
    ///
    /// Extracts the destination from the envelope, looks up the registered
    /// sink, and attempts delivery.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `ActorNotFound` if the destination is not registered
    /// - `DeliveryFailed` if the sink rejected the envelope
    ///
    /// # Invariants
    ///
    /// - Routing is synchronous and non-blocking (fails fast)
    /// - No retries or queuing on failure
    /// - Deadlines in envelope are not enforced (sink's responsibility)
    pub fn route(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination();

        let sink = self
            .registry
            .get(dest)
            .ok_or(RouteError::ActorNotFound(dest))?;

        sink.deliver(envelope)
            .map_err(|e| RouteError::DeliveryFailed(dest, e))
    }

    /// Check if an actor is registered
    pub fn contains(&self, id: ActorId) -> bool {
        self.registry.get(id).is_some()
    }

    /// Get the number of registered actors
    pub fn len(&self) -> usize {
        self.registry.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Mock sink for testing
    struct MockSink {
        delivered: Mutex<Vec<Envelope>>,
        should_fail: bool,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                delivered: Mutex::new(Vec::new()),
                should_fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                delivered: Mutex::new(Vec::new()),
                should_fail: true,
            }
        }

        fn count(&self) -> usize {
            self.delivered.lock().unwrap().len()
        }
    }

    impl MailboxSink for MockSink {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            if self.should_fail {
                return Err(DeliveryError::MailboxFull);
            }
            self.delivered.lock().unwrap().push(envelope);
            Ok(())
        }
    }

    #[test]
    fn should_register_actor() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let sink = Arc::new(MockSink::new());

        // Act
        router.register(actor_id, sink);

        // Assert
        assert!(router.contains(actor_id));
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn should_unregister_actor() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let sink = Arc::new(MockSink::new());
        router.register(actor_id, sink);

        // Act
        router.unregister(actor_id);

        // Assert
        assert!(!router.contains(actor_id));
        assert!(router.is_empty());
    }

    #[test]
    fn should_route_envelope_to_registered_actor() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let sink = Arc::new(MockSink::new());
        router.register(actor_id, sink.clone());
        let envelope = Envelope::new(actor_id, "test message");

        // Act
        let result = router.route(envelope);

        // Assert
        assert!(result.is_ok());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn should_return_error_for_unregistered_actor() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let envelope = Envelope::new(actor_id, "test message");

        // Act
        let result = router.route(envelope);

        // Assert
        assert_eq!(result, Err(RouteError::ActorNotFound(actor_id)));
    }

    #[test]
    fn should_return_error_for_failed_delivery() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let sink = Arc::new(MockSink::failing());
        router.register(actor_id, sink);
        let envelope = Envelope::new(actor_id, "test message");

        // Act
        let result = router.route(envelope);

        // Assert
        assert_eq!(
            result,
            Err(RouteError::DeliveryFailed(
                actor_id,
                DeliveryError::MailboxFull
            ))
        );
    }

    #[test]
    fn should_support_multiple_actors() {
        // Arrange
        let router = Router::new();
        let actor1 = ActorId::new(1);
        let actor2 = ActorId::new(2);
        let sink1 = Arc::new(MockSink::new());
        let sink2 = Arc::new(MockSink::new());
        router.register(actor1, sink1.clone());
        router.register(actor2, sink2.clone());

        // Act
        router.route(Envelope::new(actor1, "msg1")).unwrap();
        router.route(Envelope::new(actor2, "msg2")).unwrap();

        // Assert
        assert_eq!(sink1.count(), 1);
        assert_eq!(sink2.count(), 1);
        assert_eq!(router.len(), 2);
    }

    #[test]
    fn should_clone_router() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let sink = Arc::new(MockSink::new());
        router.register(actor_id, sink);

        // Act
        let cloned = router.clone();

        // Assert
        assert!(cloned.contains(actor_id));
        assert_eq!(cloned.len(), router.len());
    }

    #[test]
    fn should_handle_concurrent_routing() {
        // Arrange
        let router = Router::new();
        let actor_id = ActorId::new(1);
        let sink = Arc::new(MockSink::new());
        router.register(actor_id, sink.clone());

        let router_clone = router.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..10 {
                let envelope = Envelope::new(actor_id, i);
                router_clone.route(envelope).unwrap();
            }
        });

        // Act - route from main thread concurrently
        for i in 10..20 {
            let envelope = Envelope::new(actor_id, i);
            router.route(envelope).unwrap();
        }

        handle.join().unwrap();

        // Assert
        assert_eq!(sink.count(), 20);
    }
}
