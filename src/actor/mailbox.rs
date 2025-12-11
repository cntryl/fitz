//! Bounded mailbox with fair scheduling.

use super::error::{ActorError, ActorResult};
use crossbeam_channel::{bounded, Receiver, Sender};

/// Bounded mailbox for an actor.
pub struct Mailbox<M> {
    tx: Sender<M>,
    rx: Receiver<M>,
    capacity: usize,
}

impl<M> Mailbox<M> {
    /// Create a new mailbox with the given capacity.
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);
        Self { tx, rx, capacity }
    }

    /// Send a message (non-blocking, returns error if full).
    pub fn try_send(&self, msg: M) -> ActorResult<()> {
        self.tx.try_send(msg).map_err(|_| ActorError::MailboxFull)
    }

    /// Send a message (blocking).
    pub fn send(&self, msg: M) -> ActorResult<()> {
        self.tx.send(msg).map_err(|_| ActorError::ActorStopped)
    }

    /// Receive a message (non-blocking).
    pub fn try_recv(&self) -> Option<M> {
        self.rx.try_recv().ok()
    }

    /// Receive a message (blocking).
    pub fn recv(&self) -> Option<M> {
        self.rx.recv().ok()
    }

    /// Get the sender handle.
    pub fn sender(&self) -> Sender<M> {
        self.tx.clone()
    }

    /// Get the mailbox capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if the mailbox is empty.
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    /// Approximate length (not guaranteed to be exact).
    pub fn len(&self) -> usize {
        self.rx.len()
    }
}

impl<M> Clone for Mailbox<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
            capacity: self.capacity,
        }
    }
}

