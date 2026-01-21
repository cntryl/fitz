// LAYER: RUNTIME
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
//! - Route families are strictly isolated (no cross-family routing)

use crate::runtime::envelope::Envelope;
use crate::runtime::routing::RouteAddress;
use dashmap::DashMap;
use std::sync::Arc;

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

    /// Attempt to deliver an envelope to the high-priority lane
    ///
    /// **Runtime-internal use only**. This method is used by the runtime
    /// for control-plane operations (timers, supervision, leases) that must
    /// not be starved by data-plane message saturation.
    ///
    /// # Errors
    ///
    /// Returns `DeliveryError::HighLaneFull` if the high-priority lane is full.
    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError>;
}

/// Errors that can occur during envelope delivery
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryError {
    /// Mailbox is at capacity (backpressure)
    /// Includes capacity and current length for adaptive backoff
    MailboxFull { capacity: usize, current_len: usize },
    /// High-priority lane is at capacity
    /// This should be rare and indicates the control plane is saturated
    HighLaneFull { capacity: usize, current_len: usize },
    /// Mailbox receiver has been dropped (actor stopped)
    ActorStopped,
}

impl DeliveryError {
    /// Get occupancy ratio (0.0 to 1.0) for backpressure decisions
    pub fn occupancy(&self) -> f64 {
        match self {
            DeliveryError::MailboxFull {
                capacity,
                current_len,
            }
            | DeliveryError::HighLaneFull {
                capacity,
                current_len,
            } => *current_len as f64 / *capacity as f64,
            DeliveryError::ActorStopped => 1.0,
        }
    }
}

impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryError::MailboxFull {
                capacity,
                current_len,
            } => {
                write!(f, "Mailbox is full ({}/{} messages)", current_len, capacity)
            }
            DeliveryError::HighLaneFull {
                capacity,
                current_len,
            } => {
                write!(
                    f,
                    "High-priority lane is full ({}/{} messages)",
                    current_len, capacity
                )
            }
            DeliveryError::ActorStopped => write!(f, "Actor has stopped"),
        }
    }
}

impl std::error::Error for DeliveryError {}

/// Errors that can occur during routing
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteError {
    /// Destination route not found in registry
    RouteNotFound(RouteAddress),
    /// Delivery failed (mailbox full or actor stopped)
    DeliveryFailed(RouteAddress, DeliveryError),
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteError::RouteNotFound(addr) => write!(f, "Route not found: {}", addr),
            RouteError::DeliveryFailed(addr, err) => {
                write!(f, "Delivery failed for route {}: {}", addr, err)
            }
        }
    }
}

impl std::error::Error for RouteError {}

/// Route registry mapping RouteAddress to mailbox sinks
///
/// Uses DashMap for lock-free concurrent access with minimal contention.
/// Enforces route family isolation: routes in different families
/// never conflict even if they have the same path string.
struct RouteRegistry {
    sinks: DashMap<RouteAddress, Arc<dyn MailboxSink>>,
}

impl RouteRegistry {
    fn new() -> Self {
        Self {
            sinks: DashMap::new(),
        }
    }

    fn register(&self, address: RouteAddress, sink: Arc<dyn MailboxSink>) {
        self.sinks.insert(address, sink);
    }

    fn unregister(&self, address: &RouteAddress) {
        self.sinks.remove(address);
    }

    fn get(&self, address: &RouteAddress) -> Option<Arc<dyn MailboxSink>> {
        self.sinks.get(address).map(|r| r.clone())
    }

    fn len(&self) -> usize {
        self.sinks.len()
    }
}

/// Message router for in-process delivery
///
/// The router maintains a registry of route-to-mailbox mappings and delivers
/// envelopes to their destinations. It provides best-effort delivery
/// without guarantees or retries.
///
/// # Route Family Isolation
///
/// **CRITICAL**: The router enforces strict isolation between route families.
/// Routes with the same path in different families are completely independent.
///
/// # Thread Safety
///
/// Router is `Clone` and all clones share the same registry. This allows
/// multiple threads to route messages concurrently.
#[derive(Clone)]
pub struct Router {
    registry: Arc<RouteRegistry>,
}

impl Router {
    /// Create a new router with an empty registry
    pub fn new() -> Self {
        Self {
            registry: Arc::new(RouteRegistry::new()),
        }
    }

    /// Register a route's mailbox sink
    ///
    /// After registration, envelopes addressed to this route will be
    /// delivered to the provided sink.
    ///
    /// # Route Family Isolation
    ///
    /// Routes in different families are independent. Registering
    /// `(family=1, route="/user")` does not affect `(family=2, route="/user")`.
    ///
    /// # Note
    ///
    /// If a route is already registered, the old sink is replaced.
    pub fn register(&self, address: RouteAddress, sink: Arc<dyn MailboxSink>) {
        self.registry.register(address, sink);
    }

    /// Unregister a route
    ///
    /// After unregistration, envelopes addressed to this route will
    /// fail with `RouteNotFound` error.
    pub fn unregister(&self, address: &RouteAddress) {
        self.registry.unregister(address);
    }

    /// Route an envelope to its destination
    ///
    /// Extracts the destination from the envelope, looks up the registered
    /// sink, and attempts delivery.
    ///
    /// # Route Family Isolation
    ///
    /// **CRITICAL**: No cross-family routing. If an envelope is addressed to
    /// `(family=1, route="/user")`, the router will never attempt delivery to
    /// `(family=2, route="/user")`, even if family 1's route is not registered.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `RouteNotFound` if the destination is not registered
    /// - `DeliveryFailed` if the sink rejected the envelope
    ///
    /// # Invariants
    ///
    /// - Routing is synchronous and non-blocking (fails fast)
    /// - No retries or queuing on failure
    /// - Deadlines in envelope are not enforced (sink's responsibility)
    pub fn route(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();

        let sink = self
            .registry
            .get(&dest)
            .ok_or_else(|| RouteError::RouteNotFound(dest.clone()))?;

        sink.deliver(envelope)
            .map_err(|e| RouteError::DeliveryFailed(dest, e))
    }

    /// Route an envelope to the high-priority lane (runtime-internal use only)
    ///
    /// **CRITICAL**: This method is for runtime-internal use only and should
    /// never be exposed to user code. It's used by the runtime for control-plane
    /// operations (timers, supervision, leases) that must not be starved by
    /// data-plane saturation.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `RouteNotFound` if the destination is not registered
    /// - `DeliveryFailed(HighLaneFull)` if the high-priority lane is full
    ///
    /// # Invariants
    ///
    /// - High-priority lane has the SAME capacity as normal lane (no extra buffer)
    /// - Caller must handle HighLaneFull as a critical error (control plane saturated)
    /// - Scheduler guarantees high-priority messages process first (capped at 4/tick)
    #[allow(dead_code)]
    pub(crate) fn route_high_priority(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();

        let sink = self
            .registry
            .get(&dest)
            .ok_or_else(|| RouteError::RouteNotFound(dest.clone()))?;

        sink.deliver_high_priority(envelope)
            .map_err(|e| RouteError::DeliveryFailed(dest, e))
    }

    /// Check if a route is registered
    pub fn contains(&self, address: &RouteAddress) -> bool {
        self.registry.get(address).is_some()
    }

    /// Get the number of registered routes
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
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use parking_lot::Mutex;

    /// Helper to create test route addresses
    fn test_address(family: u64, route: &str) -> RouteAddress {
        RouteAddress::new(RouteFamily::new(family), Route::new(route))
    }

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
            self.delivered.lock().len()
        }
    }

    impl MailboxSink for MockSink {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            if self.should_fail {
                return Err(DeliveryError::MailboxFull {
                    capacity: 10,
                    current_len: 10,
                });
            }
            self.delivered.lock().push(envelope);
            Ok(())
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            // For tests, just use normal delivery
            self.deliver(envelope)
        }
    }

    #[test]
    fn should_register_route() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let sink = Arc::new(MockSink::new());

        // Act
        router.register(address.clone(), sink);

        // Assert
        assert!(router.contains(&address));
        assert_eq!(router.len(), 1);
    }

    #[test]
    fn should_unregister_route() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let sink = Arc::new(MockSink::new());
        router.register(address.clone(), sink);

        // Act
        router.unregister(&address);

        // Assert
        assert!(!router.contains(&address));
        assert!(router.is_empty());
    }

    #[test]
    fn should_route_envelope_to_registered_route() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let sink = Arc::new(MockSink::new());
        router.register(address.clone(), sink.clone());
        let envelope = Envelope::new(address, "test message");

        // Act
        let result = router.route(envelope);

        // Assert
        assert!(result.is_ok());
        assert_eq!(sink.count(), 1);
    }

    #[test]
    fn should_return_error_for_unregistered_route() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let envelope = Envelope::new(address.clone(), "test message");

        // Act
        let result = router.route(envelope);

        // Assert
        assert_eq!(result, Err(RouteError::RouteNotFound(address)));
    }

    #[test]
    fn should_return_error_for_failed_delivery() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let sink = Arc::new(MockSink::failing());
        router.register(address.clone(), sink);
        let envelope = Envelope::new(address.clone(), "test message");

        // Act
        let result = router.route(envelope);

        // Assert
        assert!(matches!(
            result,
            Err(RouteError::DeliveryFailed(
                _,
                DeliveryError::MailboxFull { .. }
            ))
        ));
    }

    #[test]
    fn should_support_multiple_routes() {
        // Arrange
        let router = Router::new();
        let addr1 = test_address(1, "/user/123");
        let addr2 = test_address(1, "/user/456");
        let sink1 = Arc::new(MockSink::new());
        let sink2 = Arc::new(MockSink::new());
        router.register(addr1.clone(), sink1.clone());
        router.register(addr2.clone(), sink2.clone());

        // Act
        router.route(Envelope::new(addr1, "msg1")).unwrap();
        router.route(Envelope::new(addr2, "msg2")).unwrap();

        // Assert
        assert_eq!(sink1.count(), 1);
        assert_eq!(sink2.count(), 1);
        assert_eq!(router.len(), 2);
    }

    #[test]
    fn should_clone_router() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let sink = Arc::new(MockSink::new());
        router.register(address.clone(), sink);

        // Act
        let cloned = router.clone();

        // Assert
        assert!(cloned.contains(&address));
        assert_eq!(cloned.len(), router.len());
    }

    #[test]
    fn should_handle_concurrent_routing() {
        // Arrange
        let router = Router::new();
        let address = test_address(1, "/user/123");
        let sink = Arc::new(MockSink::new());
        router.register(address.clone(), sink.clone());

        let router_clone = router.clone();
        let addr_clone = address.clone();
        let handle = std::thread::spawn(move || {
            for i in 0..10 {
                let envelope = Envelope::new(addr_clone.clone(), i);
                router_clone.route(envelope).unwrap();
            }
        });

        // Act - route from main thread concurrently
        for i in 10..20 {
            let envelope = Envelope::new(address.clone(), i);
            router.route(envelope).unwrap();
        }

        handle.join().unwrap();

        // Assert
        assert_eq!(sink.count(), 20);
    }

    #[test]
    fn should_isolate_same_route_in_different_families() {
        // Arrange
        let router = Router::new();
        let addr_family1 = test_address(1, "/user/123");
        let addr_family2 = test_address(2, "/user/123");
        let sink1 = Arc::new(MockSink::new());
        let sink2 = Arc::new(MockSink::new());
        router.register(addr_family1.clone(), sink1.clone());
        router.register(addr_family2.clone(), sink2.clone());

        // Act
        router.route(Envelope::new(addr_family1, "msg1")).unwrap();
        router.route(Envelope::new(addr_family2, "msg2")).unwrap();

        // Assert
        assert_eq!(sink1.count(), 1, "Family 1 should receive its message");
        assert_eq!(sink2.count(), 1, "Family 2 should receive its message");
        assert_eq!(
            router.len(),
            2,
            "Both routes should be registered independently"
        );
    }
}
