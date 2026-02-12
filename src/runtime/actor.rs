// LAYER: RUNTIME
//! Core actor abstractions and lifecycle management

use crate::runtime::context::TimerManager;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::{RouteError, Router};
use crate::runtime::routing::RouteAddress;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Metrics for actor message processing
#[derive(Debug, Default)]
pub struct ActorMetrics {
    /// Total messages processed successfully
    pub messages_processed: AtomicU64,
    /// Messages dropped due to expired deadline
    pub messages_expired: AtomicU64,
    /// Messages that caused panics
    pub messages_panicked: AtomicU64,
    /// Messages with type mismatch (wrong message type)
    pub messages_type_mismatch: AtomicU64,
    /// Total processing time in microseconds
    pub total_processing_time_us: AtomicU64,
}

impl ActorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successfully processed message
    pub fn record_processed(&self, processing_time_us: u64) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.total_processing_time_us
            .fetch_add(processing_time_us, Ordering::Relaxed);
    }

    /// Record an expired message
    pub fn record_expired(&self) {
        self.messages_expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a panic
    pub fn record_panic(&self) {
        self.messages_panicked.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a type mismatch error
    pub fn record_type_mismatch(&self) {
        self.messages_type_mismatch.fetch_add(1, Ordering::Relaxed);
    }

    /// Get average processing time in microseconds
    pub fn avg_processing_time_us(&self) -> u64 {
        let processed = self.messages_processed.load(Ordering::Relaxed);
        if processed == 0 {
            return 0;
        }
        self.total_processing_time_us.load(Ordering::Relaxed) / processed
    }

    /// Get snapshot of metrics
    pub fn snapshot(&self) -> ActorMetricsSnapshot {
        ActorMetricsSnapshot {
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            messages_expired: self.messages_expired.load(Ordering::Relaxed),
            messages_panicked: self.messages_panicked.load(Ordering::Relaxed),
            messages_type_mismatch: self.messages_type_mismatch.load(Ordering::Relaxed),
            avg_processing_time_us: self.avg_processing_time_us(),
        }
    }
}

/// Snapshot of actor metrics at a point in time
#[derive(Debug, Clone, Copy)]
pub struct ActorMetricsSnapshot {
    pub messages_processed: u64,
    pub messages_expired: u64,
    pub messages_panicked: u64,
    pub messages_type_mismatch: u64,
    pub avg_processing_time_us: u64,
}

/// The core Actor trait that all actors must implement.
///
/// Actors are single-threaded entities that process messages sequentially.
/// They maintain their own state and communicate only via message passing.
pub trait Actor: Send + 'static {
    /// The message type this actor can receive
    type Message: Send + 'static;

    /// Called when the actor receives a message
    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>);

    /// Called when the actor starts (before processing any messages)
    fn started(&mut self, _ctx: &mut Context<Self>) {}

    /// Called when the actor stops (after processing all messages)
    fn stopped(&mut self) {}

    /// Called when an error occurs during message processing
    fn on_error(&mut self, error: ActorError, _ctx: &mut Context<Self>) {
        eprintln!("Actor error: {}", error);
    }

    /// Called when a timer fires (timer scheduling via `Context::timer_manager()`)
    /// Default implementation is no-op. Actors may override to handle timers.
    fn on_timer(&mut self, _timer_id: crate::runtime::context::TimerId, _ctx: &mut Context<Self>) {}
}

/// Context provided to actors during message processing
///
/// The context provides access to:
/// - Actor's own route address
/// - Message sending capabilities (with automatic causation tracking)
/// - Lifecycle control (stopping the actor)
/// - Current envelope metadata (for causation chains)
/// - Metrics for observability
/// - Timer manager for scheduled messages
pub struct Context<A: Actor + ?Sized> {
    address: RouteAddress,
    state: ActorState,
    router: Arc<Router>,
    current_metadata: Option<crate::runtime::envelope::EnvelopeMetadata>,
    metrics: Arc<ActorMetrics>,
    timer_manager: TimerManager,
    _phantom: std::marker::PhantomData<*const A>,
}

impl<A: Actor + ?Sized> Context<A> {
    pub fn new(address: RouteAddress, router: Arc<Router>) -> Self {
        Self {
            address,
            state: ActorState::Running,
            router,
            current_metadata: None,
            timer_manager: TimerManager::new(),
            metrics: Arc::new(ActorMetrics::new()),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create context with shared metrics
    pub fn with_metrics(
        address: RouteAddress,
        router: Arc<Router>,
        metrics: Arc<ActorMetrics>,
    ) -> Self {
        Self {
            address,
            state: ActorState::Running,
            router,
            current_metadata: None,
            metrics,
            timer_manager: TimerManager::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get a reference to the actor metrics
    pub fn metrics(&self) -> &Arc<ActorMetrics> {
        &self.metrics
    }

    /// Set the current envelope metadata being processed (internal use by scheduler)
    pub(crate) fn set_current_metadata(
        &mut self,
        metadata: crate::runtime::envelope::EnvelopeMetadata,
    ) {
        self.current_metadata = Some(metadata);
    }

    /// Get the actor's route address
    pub fn address(&self) -> &RouteAddress {
        &self.address
    }

    /// Send a message to another actor
    ///
    /// This is the preferred way for actors to send messages. The context:
    /// - Sets the source to this actor's route address
    /// - Automatically tracks causation from the current message
    /// - Inherits deadline from the current message if present
    ///
    /// # Semantics
    ///
    /// **CRITICAL**: This is a **synchronous best-effort** send with **no retries**.
    /// If the destination mailbox is full, the send fails immediately with `MailboxFull`.
    /// Callers must implement exponential backoff or use message buffering.
    ///
    /// **WARNING**: Sending to self during `receive()` can deadlock if the mailbox is full.
    /// Consider using deferred sends or checking mailbox capacity first.
    pub fn send<M>(&self, dest: RouteAddress, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let mut envelope = Envelope::from_route(self.address.clone(), dest.clone(), msg);

        // Set causation from current envelope metadata
        if let Some(metadata) = &self.current_metadata {
            envelope = envelope.with_causation(metadata.id);

            // Inherit deadline if present
            if let Some(deadline) = metadata.deadline {
                envelope = envelope.with_deadline(deadline);
            }
        }

        self.router.route(envelope).map_err(|e| match e {
            RouteError::RouteNotFound(target) => SendError::RouteNotFound { target },
            RouteError::DeliveryFailed(target, delivery_err) => match delivery_err {
                crate::runtime::router::DeliveryError::MailboxFull {
                    capacity,
                    current_len,
                } => SendError::MailboxFull {
                    target,
                    occupancy: current_len as f64 / capacity as f64,
                },
                crate::runtime::router::DeliveryError::HighLaneFull {
                    capacity,
                    current_len,
                } => {
                    // High-priority lane should never be used by user code
                    // Treat as normal mailbox full for error reporting
                    SendError::MailboxFull {
                        target,
                        occupancy: current_len as f64 / capacity as f64,
                    }
                }
                crate::runtime::router::DeliveryError::ActorStopped => {
                    SendError::ActorStopped { target }
                }
            },
        })
    }

    /// Publish a domain event to the router.
    ///
    /// This is a convenience method for emitting `DomainPublishEvent`s.
    /// The event is routed based on its route field to the appropriate domain sink,
    /// which performs subscription matching and fanout internally.
    ///
    /// # Semantics
    ///
    /// Same as `send()`: synchronous best-effort with no retries.
    /// The route in the event determines which domain sink receives it.
    pub fn publish_event(
        &self,
        event: crate::runtime::domain_event::DomainPublishEvent,
    ) -> Result<(), SendError> {
        let addr = RouteAddress::new(event.family_id, event.route.clone());
        self.send(addr, event)
    }

    /// Reply to the sender of the current message
    ///
    /// This creates a reply envelope that:
    /// - Is addressed to the original sender
    /// - Has causation set to the current message ID
    /// - Inherits the deadline from the current message
    ///
    /// # Returns
    ///
    /// Returns `Err(SendError::ActorNotFound)` if:
    /// - There is no current envelope (called outside message processing)
    /// - The current envelope has no source (external message)
    pub fn reply<M>(&self, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let metadata = self
            .current_metadata
            .as_ref()
            .ok_or(SendError::RouteNotFound {
                target: self.address.clone(),
            })?;

        let source = metadata.source.as_ref().ok_or(SendError::RouteNotFound {
            target: self.address.clone(),
        })?;

        let mut reply_envelope = Envelope::from_route(self.address.clone(), source.clone(), msg)
            .with_causation(metadata.id);

        if let Some(deadline) = metadata.deadline {
            reply_envelope = reply_envelope.with_deadline(deadline);
        }

        self.router.route(reply_envelope).map_err(|e| match e {
            RouteError::RouteNotFound(target) => SendError::RouteNotFound { target },
            RouteError::DeliveryFailed(target, delivery_err) => match delivery_err {
                crate::runtime::router::DeliveryError::MailboxFull {
                    capacity,
                    current_len,
                } => SendError::MailboxFull {
                    target,
                    occupancy: current_len as f64 / capacity as f64,
                },
                crate::runtime::router::DeliveryError::HighLaneFull {
                    capacity,
                    current_len,
                } => {
                    // High-priority lane should never be used by user code
                    // Treat as normal mailbox full for error reporting
                    SendError::MailboxFull {
                        target,
                        occupancy: current_len as f64 / capacity as f64,
                    }
                }
                crate::runtime::router::DeliveryError::ActorStopped => {
                    SendError::ActorStopped { target }
                }
            },
        })
    }

    /// Stop this actor
    ///
    /// INVARIANT: Stopping an actor immediately:
    /// - Sets state to Stopping
    /// - Cancels ALL timers (no timer fires after stop)
    /// - Breaks message processing loop
    ///
    /// Timers are tied to actor lifecycle. On stop/restart, all timers are cleared.
    pub fn stop(&mut self) {
        self.state = ActorState::Stopping;
        // CRITICAL: Cancel all timers. No timer delivery after stop.
        self.timer_manager.clear();
    }

    /// Check if the actor should continue running
    pub fn is_running(&self) -> bool {
        matches!(self.state, ActorState::Running)
    }

    /// Get a mutable reference to the timer manager
    pub fn timer_manager(&mut self) -> &mut TimerManager {
        &mut self.timer_manager
    }
}

/// Unique identifier for an actor instance
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ActorId(u64);

impl ActorId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Actor({})", self.0)
    }
}

/// Reference to an actor for sending messages
///
/// ActorRef maintains type safety at the API level while using the
/// untyped router internally. Messages are wrapped in Envelopes before routing.
#[derive(Clone)]
pub struct ActorRef<M: Send + 'static> {
    address: RouteAddress,
    router: Arc<Router>,
    _phantom: std::marker::PhantomData<fn() -> M>,
}

impl<M: Send + 'static> ActorRef<M> {
    pub fn new(address: RouteAddress, router: Arc<Router>) -> Self {
        Self {
            address,
            router,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Send a message to this actor (non-blocking, may fail if mailbox is full)
    ///
    /// The message is wrapped in an Envelope and routed to the destination actor.
    /// The source is not set (external message).
    ///
    /// # Semantics
    ///
    /// This is a **synchronous best-effort** send with **no retries**.
    /// Returns detailed error information for adaptive backpressure.
    pub fn send(&self, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let envelope = Envelope::new(self.address.clone(), msg);
        self.router.route(envelope).map_err(|e| match e {
            RouteError::RouteNotFound(target) => SendError::RouteNotFound { target },
            RouteError::DeliveryFailed(target, delivery_err) => match delivery_err {
                crate::runtime::router::DeliveryError::MailboxFull {
                    capacity,
                    current_len,
                } => SendError::MailboxFull {
                    target,
                    occupancy: current_len as f64 / capacity as f64,
                },
                crate::runtime::router::DeliveryError::HighLaneFull {
                    capacity,
                    current_len,
                } => {
                    // High-priority lane should never be used by user code
                    // Treat as normal mailbox full for error reporting
                    SendError::MailboxFull {
                        target,
                        occupancy: current_len as f64 / capacity as f64,
                    }
                }
                crate::runtime::router::DeliveryError::ActorStopped => {
                    SendError::ActorStopped { target }
                }
            },
        })
    }

    /// Get the actor's route address
    pub fn address(&self) -> &RouteAddress {
        &self.address
    }
}

impl<M: Send + 'static> fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("address", &self.address)
            .finish()
    }
}

/// Actor lifecycle state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorState {
    Starting,
    Running,
    Stopping,
    Stopped,
}

/// Errors that can occur in the actor system
#[derive(Debug, Clone)]
pub enum ActorError {
    MailboxFull,
    ActorStopped,
    SendFailed(String),
    Panic(String),
    TypeMismatch { expected: String, envelope_id: u64 },
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorError::MailboxFull => write!(f, "Actor mailbox is full"),
            ActorError::ActorStopped => write!(f, "Actor has stopped"),
            ActorError::SendFailed(msg) => write!(f, "Failed to send message: {}", msg),
            ActorError::Panic(msg) => write!(f, "Actor panicked: {}", msg),
            ActorError::TypeMismatch {
                expected,
                envelope_id,
            } => {
                write!(
                    f,
                    "Type mismatch: expected {}, envelope ID {}",
                    expected, envelope_id
                )
            }
        }
    }
}

impl std::error::Error for ActorError {}

#[derive(Debug, Clone)]
pub enum SendError {
    /// Mailbox is full (backpressure) - includes occupancy for adaptive backoff
    MailboxFull {
        target: RouteAddress,
        occupancy: f64,
    },
    /// Actor has stopped
    ActorStopped { target: RouteAddress },
    /// Route not registered
    RouteNotFound { target: RouteAddress },
}

impl SendError {
    /// Get the target address for error context
    pub fn target(&self) -> &RouteAddress {
        match self {
            SendError::MailboxFull { target, .. } => target,
            SendError::ActorStopped { target } => target,
            SendError::RouteNotFound { target } => target,
        }
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::MailboxFull { target, occupancy } => {
                write!(
                    f,
                    "Mailbox is full for {} (occupancy: {:.1}%)",
                    target,
                    occupancy * 100.0
                )
            }
            SendError::ActorStopped { target } => {
                write!(f, "Actor {} has stopped", target)
            }
            SendError::RouteNotFound { target } => {
                write!(f, "Route {} not found", target)
            }
        }
    }
}

impl std::error::Error for SendError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mailbox::Mailbox;
    use crate::runtime::router::Router;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

    fn test_address(family: u64, route: &str) -> RouteAddress {
        RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
    }
    #[test]
    fn should_create_actor_id() {
        // Arrange
        let id = 42;

        // Act
        let actor_id = ActorId::new(id);

        // Assert
        assert_eq!(actor_id.as_u64(), id);
    }

    #[test]
    fn should_compare_equal_actor_ids() {
        // Arrange
        let id1 = ActorId::new(1);
        let id2 = ActorId::new(1);

        // Act
        let are_equal = id1 == id2;

        // Assert
        assert!(are_equal);
    }

    #[test]
    fn should_compare_unequal_actor_ids() {
        // Arrange
        let id1 = ActorId::new(1);
        let id2 = ActorId::new(2);

        // Act
        let are_equal = id1 == id2;

        // Assert
        assert!(!are_equal);
    }

    #[test]
    fn should_format_actor_id() {
        // Arrange
        let actor_id = ActorId::new(123);

        // Act
        let formatted = format!("{}", actor_id);

        // Assert
        assert_eq!(formatted, "Actor(123)");
    }

    #[test]
    fn should_create_context_with_running_state() {
        // Arrange
        let address = test_address(1, "/test/actor");
        let router = Arc::new(Router::new());

        // Act
        let ctx: Context<DummyActor> = Context::new(address.clone(), router);

        // Assert
        assert_eq!(ctx.address(), &address);
        assert!(ctx.is_running());
    }

    #[test]
    fn should_stop_context() {
        // Arrange
        let address = test_address(1, "/test/actor");
        let router = Arc::new(Router::new());
        let mut ctx: Context<DummyActor> = Context::new(address, router);

        // Act
        ctx.stop();

        // Assert
        assert!(!ctx.is_running());
    }

    #[test]
    fn should_send_message_via_actor_ref() {
        // Arrange
        let router = Arc::new(Router::new());
        let mailbox = Mailbox::new(10);
        let address = test_address(1, "/test/actor");
        router.register(address.clone(), Arc::new(mailbox.clone()));
        let actor_ref: ActorRef<i32> = ActorRef::new(address, router);

        // Act
        let result = actor_ref.send(42);

        // Assert
        assert!(result.is_ok());
        let envelope = mailbox.receiver().recv().unwrap();
        assert_eq!(envelope.into_payload::<i32>(), Some(42));
    }

    #[test]
    fn should_fail_send_when_mailbox_full() {
        // Arrange
        let router = Arc::new(Router::new());
        let mailbox = Mailbox::new(1);
        let address = test_address(1, "/test/actor");
        router.register(address.clone(), Arc::new(mailbox.clone()));
        let actor_ref: ActorRef<i32> = ActorRef::new(address, router);
        actor_ref.send(1).unwrap();

        // Act
        let result = actor_ref.send(2);

        // Assert
        assert!(matches!(result, Err(SendError::MailboxFull { .. })));
    }

    #[test]
    fn should_get_actor_id_from_ref() {
        // Arrange
        let router = Arc::new(Router::new());
        let address = test_address(1, "/test/actor");
        let actor_ref: ActorRef<i32> = ActorRef::new(address.clone(), router);

        // Act
        let addr = actor_ref.address();

        // Assert
        assert_eq!(addr, &address);
    }

    #[test]
    fn should_send_message_via_context() {
        // Arrange
        let router = Arc::new(Router::new());
        let sender_addr = test_address(1, "/test/sender");
        let receiver_addr = test_address(1, "/test/receiver");
        let receiver_mailbox = Mailbox::new(10);
        router.register(receiver_addr.clone(), Arc::new(receiver_mailbox.clone()));

        let ctx: Context<DummyActor> = Context::new(sender_addr.clone(), router);

        // Act
        let result = ctx.send(receiver_addr.clone(), 42_i32);

        // Assert
        assert!(result.is_ok());
        let envelope = receiver_mailbox.receiver().recv().unwrap();
        assert_eq!(envelope.source(), Some(&sender_addr));
        assert_eq!(envelope.destination(), &receiver_addr);
        assert_eq!(envelope.into_payload::<i32>(), Some(42));
    }

    #[test]
    fn should_inherit_causation_when_sending_from_context() {
        // Arrange
        let router = Arc::new(Router::new());
        let sender_addr = test_address(1, "/test/sender");
        let receiver_addr = test_address(1, "/test/receiver");
        let receiver_mailbox = Mailbox::new(10);
        router.register(receiver_addr.clone(), Arc::new(receiver_mailbox.clone()));

        let mut ctx: Context<DummyActor> = Context::new(sender_addr.clone(), router);

        // Simulate receiving a message with causation
        let parent_envelope =
            Envelope::from_route(test_address(1, "/test/parent"), sender_addr, ());
        let parent_id = parent_envelope.id();
        ctx.set_current_metadata(parent_envelope.metadata());

        // Act
        ctx.send(receiver_addr, 42_i32).unwrap();

        // Assert
        let envelope = receiver_mailbox.receiver().recv().unwrap();
        assert_eq!(envelope.causation(), Some(parent_id));
    }

    #[test]
    fn should_reply_to_sender_via_context() {
        // Arrange
        let router = Arc::new(Router::new());
        let sender_addr = test_address(1, "/test/sender");
        let receiver_addr = test_address(1, "/test/receiver");
        let sender_mailbox = Mailbox::new(10);
        router.register(sender_addr.clone(), Arc::new(sender_mailbox.clone()));

        let mut ctx: Context<DummyActor> = Context::new(receiver_addr.clone(), router);

        // Simulate receiving a message from sender
        let request_envelope =
            Envelope::from_route(sender_addr.clone(), receiver_addr.clone(), 10_i32);
        let request_id = request_envelope.id();
        ctx.set_current_metadata(request_envelope.metadata());

        // Act - reply to the sender
        let result = ctx.reply("response");

        // Assert
        assert!(result.is_ok());
        let reply_envelope = sender_mailbox.receiver().recv().unwrap();
        assert_eq!(reply_envelope.source(), Some(&receiver_addr));
        assert_eq!(reply_envelope.destination(), &sender_addr);
        assert_eq!(reply_envelope.causation(), Some(request_id));
        assert_eq!(reply_envelope.into_payload::<&str>(), Some("response"));
    }

    #[test]
    fn should_inherit_deadline_when_sending_from_context() {
        // Arrange
        let router = Arc::new(Router::new());
        let sender_addr = test_address(1, "/test/sender");
        let receiver_addr = test_address(1, "/test/receiver");
        let receiver_mailbox = Mailbox::new(10);
        router.register(receiver_addr.clone(), Arc::new(receiver_mailbox.clone()));

        let mut ctx: Context<DummyActor> = Context::new(sender_addr.clone(), router);

        // Simulate receiving a message with a deadline
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let parent_envelope =
            Envelope::from_route(test_address(1, "/test/parent"), sender_addr, ())
                .with_deadline(deadline);
        ctx.set_current_metadata(parent_envelope.metadata());

        // Act
        ctx.send(receiver_addr, 42_i32).unwrap();

        // Assert
        let envelope = receiver_mailbox.receiver().recv().unwrap();
        assert_eq!(envelope.deadline(), Some(deadline));
    }

    // Dummy actor for testing
    struct DummyActor;
    impl Actor for DummyActor {
        type Message = ();
        fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
    }
}
