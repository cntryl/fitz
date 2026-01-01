//! Actor mailbox implementation and message queuing

use crossbeam_channel::{bounded, Receiver, Sender};

/// Mailbox for actor message queuing with bounded capacity
pub struct Mailbox<M: Send + 'static> {
    sender: Sender<M>,
    receiver: Receiver<M>,
    capacity: usize,
}

impl<M: Send + 'static> Mailbox<M> {
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
    pub fn sender(&self) -> Sender<M> {
        self.sender.clone()
    }

    /// Get a receiver for this mailbox
    pub fn receiver(&self) -> &Receiver<M> {
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

impl<M: Send + 'static> Clone for Mailbox<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            capacity: self.capacity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_mailbox_with_capacity() {
        // Arrange
        let capacity = 100;

        // Act
        let mailbox: Mailbox<i32> = Mailbox::new(capacity);

        // Assert
        assert_eq!(mailbox.capacity(), capacity);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn should_send_message_to_mailbox() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender = mailbox.sender();

        // Act
        let result = sender.try_send(42);

        // Assert
        assert!(result.is_ok());
        assert!(!mailbox.is_empty());
        assert_eq!(mailbox.len(), 1);
    }

    #[test]
    fn should_receive_message_from_mailbox() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender = mailbox.sender();
        sender.try_send(42).unwrap();

        // Act
        let msg = mailbox.receiver().try_recv();

        // Assert
        assert_eq!(msg.unwrap(), 42);
        assert!(mailbox.is_empty());
    }

    #[test]
    fn should_respect_mailbox_capacity() {
        // Arrange
        let mailbox = Mailbox::new(2);
        let sender = mailbox.sender();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();

        // Act
        let result = sender.try_send(3);

        // Assert
        assert!(result.is_err());
        assert_eq!(mailbox.len(), 2);
    }

    #[test]
    fn should_clone_mailbox() {
        // Arrange
        let mailbox = Mailbox::new(10);
        let sender = mailbox.sender();
        sender.try_send(42).unwrap();

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

        // Act
        sender1.try_send(1).unwrap();
        sender2.try_send(2).unwrap();

        // Assert
        assert_eq!(mailbox.len(), 2);
    }
}
