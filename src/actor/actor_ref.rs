//! Strong and weak actor references for message passing.

use super::error::ActorResult;
use super::mailbox::Mailbox;
use std::sync::Arc;

/// Reference to an actor. Can be cloned and shared.
pub struct ActorRef<M> {
    mailbox: Arc<Mailbox<M>>,
    name: Arc<String>,
}

impl<M> ActorRef<M> {
    /// Create a new actor reference.
    pub fn new(mailbox: Arc<Mailbox<M>>, name: String) -> Self {
        Self {
            mailbox,
            name: Arc::new(name),
        }
    }

    /// Send a message (non-blocking).
    pub fn tell(&self, msg: M) -> ActorResult<()> {
        self.mailbox.try_send(msg)
    }

    /// Send a message (blocking).
    pub fn send(&self, msg: M) -> ActorResult<()> {
        self.mailbox.send(msg)
    }

    /// Get the actor name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if the mailbox is full.
    pub fn is_full(&self) -> bool {
        self.mailbox.len() >= self.mailbox.capacity()
    }
}

// Manual Clone impl that doesn't require M: Clone
impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            mailbox: self.mailbox.clone(),
            name: self.name.clone(),
        }
    }
}

impl<M> std::fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("name", &self.name)
            .finish()
    }
}

pub struct WeakActorRef<T> {
    // TODO: Implement weak reference
    _marker: std::marker::PhantomData<T>,
}

