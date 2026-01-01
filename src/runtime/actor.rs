//! Core actor abstractions and lifecycle management

use crate::transport::envelope::Envelope;
use crate::transport::router::{RouteError, Router};
use crate::transport::routing::RouteAddress;
use std::fmt;
use std::sync::Arc;

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
}

/// Context provided to actors during message processing
///
/// The context provides access to:
/// - Actor's own route address
/// - Message sending capabilities (with automatic causation tracking)
/// - Lifecycle control (stopping the actor)
/// - Current envelope metadata (for causation chains)
pub struct Context<A: Actor + ?Sized> {
    address: RouteAddress,
    state: ActorState,
    router: Arc<Router>,
    current_envelope: Option<Envelope>,
    _phantom: std::marker::PhantomData<*const A>,
}

impl<A: Actor + ?Sized> Context<A> {
    pub fn new(address: RouteAddress, router: Arc<Router>) -> Self {
        Self {
            address,
            state: ActorState::Running,
            router,
            current_envelope: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the current envelope being processed (internal use by scheduler)
    pub(crate) fn set_current_envelope(&mut self, envelope: Envelope) {
        self.current_envelope = Some(envelope);
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
    /// # Example
    ///
    /// ```ignore
    /// impl Actor for MyActor {
    ///     type Message = MyMessage;
    ///     
    ///     fn receive(&mut self, msg: MyMessage, ctx: &mut Context<Self>) {
    ///         // Send a message to another actor
    ///         let other_address = self.other_actor_ref.address();
    ///         ctx.send(other_address.clone(), ResponseMessage::Done).ok();
    ///     }
    /// }
    /// ```
    pub fn send<M>(&self, dest: RouteAddress, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let mut envelope = Envelope::from_route(self.address.clone(), dest, msg);

        // Set causation from current envelope
        if let Some(current) = &self.current_envelope {
            envelope = envelope.with_causation(current.id());

            // Inherit deadline if present
            if let Some(deadline) = current.deadline() {
                envelope = envelope.with_deadline(deadline);
            }
        }

        self.router
            .route(envelope)
            .map_err(|e| match e {
                RouteError::RouteNotFound(_) => SendError::ActorNotFound,
                RouteError::DeliveryFailed(_, _) => SendError::MailboxFull,
            })
    }

    /// Reply to the sender of the current message
    ///
    /// This creates a reply envelope that:
    /// - Is addressed to the original sender
    /// - Has causation set to the current message ID
    /// - Inherits the deadline from the current message
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - There is no current envelope (called outside message processing)
    /// - The current envelope has no source (external message)
    pub fn reply<M>(&self, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let current = self
            .current_envelope
            .as_ref()
            .expect("Cannot reply without a current envelope");

        let reply_envelope = current.reply_to(msg);

        self.router
            .route(reply_envelope)
            .map_err(|e| match e {
                RouteError::RouteNotFound(_) => SendError::ActorNotFound,
                RouteError::DeliveryFailed(_, _) => SendError::MailboxFull,
            })
    }

    /// Stop this actor
    pub fn stop(&mut self) {
        self.state = ActorState::Stopping;
    }

    /// Check if the actor should continue running
    pub fn is_running(&self) -> bool {
        matches!(self.state, ActorState::Running)
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
    pub fn send(&self, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let envelope = Envelope::new(self.address.clone(), msg);
        self.router
            .route(envelope)
            .map_err(|e| match e {
                RouteError::RouteNotFound(_) => SendError::ActorNotFound,
                RouteError::DeliveryFailed(_, _) => SendError::MailboxFull,
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
}

impl fmt::Display for ActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ActorError::MailboxFull => write!(f, "Actor mailbox is full"),
            ActorError::ActorStopped => write!(f, "Actor has stopped"),
            ActorError::SendFailed(msg) => write!(f, "Failed to send message: {}", msg),
            ActorError::Panic(msg) => write!(f, "Actor panicked: {}", msg),
        }
    }
}

impl std::error::Error for ActorError {}

#[derive(Debug, Clone)]
pub enum SendError {
    MailboxFull,
    ActorStopped,
    ActorNotFound,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::MailboxFull => write!(f, "Mailbox is full"),
            SendError::ActorStopped => write!(f, "Actor has stopped"),
            SendError::ActorNotFound => write!(f, "Actor not found"),
        }
    }
}

impl std::error::Error for SendError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::mailbox::Mailbox;
    use crate::transport::router::Router;    use crate::transport::routing::{Route, RouteFamily, RouteAddress};

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
        assert!(matches!(result, Err(SendError::MailboxFull)));
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
        let parent_envelope = Envelope::from_route(test_address(1, "/test/parent"), sender_addr, ());
        let parent_id = parent_envelope.id();
        ctx.set_current_envelope(parent_envelope);

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
        let request_envelope = Envelope::from_route(sender_addr.clone(), receiver_addr.clone(), 10_i32);
        let request_id = request_envelope.id();
        ctx.set_current_envelope(request_envelope);

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
        let parent_envelope = Envelope::from_route(test_address(1, "/test/parent"), sender_addr, ())
            .with_deadline(deadline);
        ctx.set_current_envelope(parent_envelope);

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
