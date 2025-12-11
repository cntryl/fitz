//! Core actor runtime and supervision system.
//!
//! Provides the foundation for all actors in Fitz:
//! - `ActorRef<T>` for message passing
//! - `Mailbox<T>` for bounded message queues
//! - `Scheduler` for fair execution
//! - `System` for supervision and supervision

pub mod actor_ref;
pub mod mailbox;
pub mod scheduler;
pub mod system;
pub mod timers;
pub mod error;

pub use actor_ref::{ActorRef, WeakActorRef};
pub use mailbox::Mailbox;
pub use scheduler::Scheduler;
pub use system::ActorSystem;
pub use error::{ActorError, ActorResult};

/// Core actor trait. Every subsystem implements this.
pub trait Actor: Send + 'static {
    type Message: Send + 'static;

    /// Process a single message.
    fn on_message(&mut self, msg: Self::Message, ctx: &mut ActorContext<Self::Message>);

    /// Called when the actor is started.
    fn on_start(&mut self, _ctx: &mut ActorContext<Self::Message>) {}

    /// Called when the actor is stopped.
    fn on_stop(&mut self) {}
}

/// Context provided to actors during message processing.
pub struct ActorContext<M> {
    /// Reference to this actor (for passing to others).
    pub self_ref: ActorRef<M>,
    /// System-level operations.
    pub system: ActorSystem,
}

impl<M: Send + 'static> ActorContext<M> {
    pub fn new(self_ref: ActorRef<M>, system: ActorSystem) -> Self {
        Self { self_ref, system }
    }

    /// Schedule a timer message.
    pub fn schedule_once(&self, delay: std::time::Duration, msg: M) {
        // TODO: implement timer scheduling
        let _ = (delay, msg);
    }

    /// Stop this actor.
    pub fn stop(&self) {
        // TODO: implement graceful stop
    }
}

