use crate::runtime::DeliveryError;

/// Map a reply-channel wait failure to the `DeliveryError` that actually
/// describes it, instead of collapsing every failure into `ActorStopped`.
///
/// The message was already accepted into the actor's mailbox by this point
/// (enqueue succeeded), so a wait failure here means one of two distinct
/// things:
/// - `Timeout`: the actor is still alive but did not reply before the
///   deadline (e.g. busy with other work) - retryable, not "dead".
/// - `Disconnected`: the reply sender was dropped without ever sending,
///   which only happens if the actor stopped (e.g. panicked) while holding
///   this message - genuinely stopped.
pub(super) fn map_reply_wait_error(error: crossbeam_channel::RecvTimeoutError) -> DeliveryError {
    match error {
        crossbeam_channel::RecvTimeoutError::Timeout => DeliveryError::Timeout,
        crossbeam_channel::RecvTimeoutError::Disconnected => DeliveryError::ActorStopped,
    }
}

#[cfg(test)]
mod reply_wait_error_tests {
    use super::{map_reply_wait_error, DeliveryError};

    #[test]
    fn should_map_reply_wait_timeout_to_timeout_not_actor_stopped() {
        // Arrange
        let error = crossbeam_channel::RecvTimeoutError::Timeout;

        // Act
        let delivery_error = map_reply_wait_error(error);

        // Assert
        assert!(matches!(delivery_error, DeliveryError::Timeout));
    }

    #[test]
    fn should_map_reply_wait_disconnect_to_actor_stopped() {
        // Arrange
        let error = crossbeam_channel::RecvTimeoutError::Disconnected;

        // Act
        let delivery_error = map_reply_wait_error(error);

        // Assert
        assert!(matches!(delivery_error, DeliveryError::ActorStopped));
    }
}
