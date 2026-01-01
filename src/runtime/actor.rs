//! Core actor abstractions and lifecycle management

use std::fmt;

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
pub struct Context<A: Actor + ?Sized> {
    actor_id: ActorId,
    state: ActorState,
    _phantom: std::marker::PhantomData<*const A>,
}

impl<A: Actor + ?Sized> Context<A> {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            state: ActorState::Running,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the actor's ID
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
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
#[derive(Clone)]
pub struct ActorRef<M: Send + 'static> {
    actor_id: ActorId,
    sender: crossbeam_channel::Sender<M>,
}

impl<M: Send + 'static> ActorRef<M> {
    pub fn new(actor_id: ActorId, sender: crossbeam_channel::Sender<M>) -> Self {
        Self { actor_id, sender }
    }

    /// Send a message to this actor (non-blocking, may fail if mailbox is full)
    pub fn send(&self, msg: M) -> Result<(), SendError> {
        self.sender
            .try_send(msg)
            .map_err(|_| SendError::MailboxFull)
    }

    /// Get the actor's ID
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

impl<M: Send + 'static> fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("actor_id", &self.actor_id)
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
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::MailboxFull => write!(f, "Mailbox is full"),
            SendError::ActorStopped => write!(f, "Actor has stopped"),
        }
    }
}

impl std::error::Error for SendError {}

#[cfg(test)]
mod tests {
    use super::*;

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
        let actor_id = ActorId::new(1);

        // Act
        let ctx: Context<DummyActor> = Context::new(actor_id);

        // Assert
        assert_eq!(ctx.actor_id(), actor_id);
        assert!(ctx.is_running());
    }

    #[test]
    fn should_stop_context() {
        // Arrange
        let actor_id = ActorId::new(1);
        let mut ctx: Context<DummyActor> = Context::new(actor_id);

        // Act
        ctx.stop();

        // Assert
        assert!(!ctx.is_running());
    }

    #[test]
    fn should_send_message_via_actor_ref() {
        // Arrange
        let (tx, rx) = crossbeam_channel::bounded(10);
        let actor_id = ActorId::new(1);
        let actor_ref = ActorRef::new(actor_id, tx);

        // Act
        let result = actor_ref.send(42);

        // Assert
        assert!(result.is_ok());
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[test]
    fn should_fail_send_when_mailbox_full() {
        // Arrange
        let (tx, _rx) = crossbeam_channel::bounded(1);
        let actor_id = ActorId::new(1);
        let actor_ref = ActorRef::new(actor_id, tx);
        actor_ref.send(1).unwrap();

        // Act
        let result = actor_ref.send(2);

        // Assert
        assert!(matches!(result, Err(SendError::MailboxFull)));
    }

    #[test]
    fn should_get_actor_id_from_ref() {
        // Arrange
        let (tx, _rx) = crossbeam_channel::bounded::<i32>(10);
        let actor_id = ActorId::new(42);
        let actor_ref = ActorRef::new(actor_id, tx);

        // Act
        let id = actor_ref.actor_id();

        // Assert
        assert_eq!(id, actor_id);
    }

    // Dummy actor for testing
    struct DummyActor;
    impl Actor for DummyActor {
        type Message = ();
        fn receive(&mut self, _msg: Self::Message, _ctx: &mut Context<Self>) {}
    }
}
