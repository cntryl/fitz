// LAYER: RUNTIME
//! Actor mailbox implementation and message queuing

use crate::runtime::envelope::Envelope;
use crate::runtime::router::{DeliveryError, MailboxSink};
use crossbeam_channel::{bounded, Receiver, Sender};

/// Mailbox for actor message queuing with bounded capacity
///
/// Mailboxes store type-erased `Envelope` messages instead of typed messages.
/// This allows the runtime to handle any message type uniformly while maintaining
/// type safety at the actor boundary (messages are unwrapped to their concrete type
/// before being passed to actors).
pub struct Mailbox {
    sender: Sender<Envelope>,
    receiver: Receiver<Envelope>,
    capacity: usize,
}

impl Mailbox {
    /// Create a new mailbox with the specified capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self {
            sender,
            receiver,
            capacity,
        }
    }

    /// Get a sender for this mailbox
    pub fn sender(&self) -> Sender<Envelope> {
        self.sender.clone()
    }

    /// Get a receiver for this mailbox
    pub fn receiver(&self) -> &Receiver<Envelope> {
        &self.receiver
    }

    /// Get the mailbox capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if the mailbox is empty
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Get the current number of messages in the mailbox
    pub fn len(&self) -> usize {
        self.receiver.len()
    }
}

impl Clone for Mailbox {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            capacity: self.capacity,
        }
    }
}

/// Implement MailboxSink to bridge transport and runtime layers
///
/// This allows the router (in transport) to deliver envelopes to mailboxes
/// (in runtime) without creating a circular dependency.
impl MailboxSink for Mailbox {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.sender.try_send(envelope).map_err(|e| match e {
            crossbeam_channel::TrySendError::Full(_) => DeliveryError::MailboxFull,
            crossbeam_channel::TrySendError::Disconnected(_) => DeliveryError::ActorStopped,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

    fn test_address(family: u64, route: &str) -> RouteAddress {
        RouteAddress::new(RouteFamily::new(family), Route::new(route.to_string()))
    }

    #[test]
    fn should_create_mailbox_with_capacity() {
        // Arrange
        let capacity = 100;

        // Act
        let mailbox = Mailbox::new(capacity);

        // Assert
        assert_eq!(mailbox.capacity(), capacity);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn should_send_envelope_to_mailbox() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender = mailbox.sender();
        let envelope = Envelope::new(test_address(1, "/test/actor"), 42);

        // Act
        let result = sender.try_send(envelope);

        // Assert
        assert!(result.is_ok());
        assert!(!mailbox.is_empty());
        assert_eq!(mailbox.len(), 1);
    }

    #[test]
    fn should_receive_envelope_from_mailbox() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender = mailbox.sender();
        let envelope = Envelope::new(test_address(1, "/test/actor"), 42);
        sender.try_send(envelope).unwrap();

        // Act
        let received = mailbox.receiver().try_recv();

        // Assert
        assert!(received.is_ok());
        assert!(mailbox.is_empty());
    }

    #[test]
    fn should_respect_mailbox_capacity() {
        // Arrange
        let mailbox = Mailbox::new(2);
        let sender = mailbox.sender();
        let addr = test_address(1, "/test/actor");
        sender.try_send(Envelope::new(addr.clone(), 1)).unwrap();
        sender.try_send(Envelope::new(addr.clone(), 2)).unwrap();

        // Act
        let result = sender.try_send(Envelope::new(addr, 3));

        // Assert
        assert!(result.is_err());
        assert_eq!(mailbox.len(), 2);
    }

    #[test]
    fn should_clone_mailbox() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender = mailbox.sender();
        sender
            .try_send(Envelope::new(test_address(1, "/test/actor"), 42))
            .unwrap();

        // Act
        let cloned = mailbox.clone();

        // Assert
        assert_eq!(cloned.capacity(), mailbox.capacity());
        assert_eq!(cloned.len(), mailbox.len());
    }

    #[test]
    fn should_handle_multiple_senders() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender1 = mailbox.sender();
        let sender2 = mailbox.sender();
        let addr = test_address(1, "/test/actor");

        // Act
        sender1.try_send(Envelope::new(addr.clone(), 1)).unwrap();
        sender2.try_send(Envelope::new(addr, 2)).unwrap();

        // Assert
        assert_eq!(mailbox.len(), 2);
    }
}
