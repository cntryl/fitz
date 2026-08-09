//! Thin adapter from the managed queue runtime to the queue core.

use super::{DeliveryError, Envelope, Instant, QueueDomainRuntime, QueueLiveCounts};

impl QueueDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        self.core.refresh_admin_snapshot_if_dirty();
    }

    pub(super) fn live_counts(&self) -> QueueLiveCounts {
        self.core.live_counts()
    }

    pub(super) fn cleanup_session(&self, session_id: u64) {
        self.core.cleanup_session(session_id);
    }

    pub(super) fn sweep_runtime_state_at(&self, now: Instant) {
        self.core.sweep_runtime_state_at(now);
    }

    pub(super) fn replay_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.core.replay_dead_letter(key, id)
    }

    pub(super) fn purge_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.core.purge_dead_letter(key, id)
    }
}
