//! Mailbox entry points: `MailboxSink`, the domain actor's `receive` loop, and
//! the thin runtime-to-core delegation used by both.

use super::model::{
    DeliveryError, Envelope, Instant, MailboxSink, Ordering, QueueDomainActor, QueueDomainCommand,
    QueueDomainRuntime, QueueDomainSink, QueueLiveCounts,
};
use crate::runtime::{Actor, Context};

pub(super) struct RuntimeSweepPendingReset<'a>(pub(super) &'a std::sync::atomic::AtomicBool);

impl Drop for RuntimeSweepPendingReset<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, true)
    }
}

impl Actor for QueueDomainActor {
    type Message = QueueDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            QueueDomainCommand::Deliver(envelope, reply, admission) => {
                let started_at = Instant::now();
                let outcome = runtime.deliver_envelope(&envelope);
                super::model::record_service_sample(&self.core.delivery_service_us, started_at);
                let _ = reply.send(outcome);
                // Explicit: the slot is released here, once the work is
                // actually done, and not when the caller gave up waiting.
                drop(admission);
            }
            QueueDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            QueueDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            QueueDomainCommand::CleanupSession(session_id, reply) => {
                runtime.cleanup_session(session_id);
                let _ = reply.send(());
            }
            QueueDomainCommand::SweepRuntimeStateAt(now, Some(reply)) => {
                runtime.sweep_runtime_state_at(now);
                let _ = reply.send(());
            }
            QueueDomainCommand::SweepRuntimeStateAt(now, None) => {
                let _pending_reset = RuntimeSweepPendingReset(&runtime.runtime_sweep_pending);
                runtime.sweep_runtime_state_at(now);
            }
            QueueDomainCommand::ReplayDeadLetter(key, id, reply) => {
                let _ = reply.send(runtime.replay_dead_letter(&key, id));
            }
            QueueDomainCommand::PurgeDeadLetter(key, id, reply) => {
                let _ = reply.send(runtime.purge_dead_letter(&key, id));
            }
            #[cfg(test)]
            QueueDomainCommand::PanicForTests => {
                panic!("test Queue domain actor panic");
            }
        }
    }
}

impl QueueDomainSink {
    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        // Admit BEFORE enqueueing so surplus load is refused as never-enqueued
        // (retryable) rather than accepted then timed out. Control-plane work
        // bypasses the window - cleanup arrives on the normal lane yet must
        // never be rationed by client load. See `admit_client_delivery`.
        let is_control_plane = high_priority
            || envelope
                .payload::<crate::runtime::SessionCleanup>()
                .is_some();
        let admission = if is_control_plane {
            None
        } else {
            Some(super::model::admit_client_delivery(
                &self.inflight_client_deliveries,
                &self.core.delivery_service_us,
                self.actor.is_running(),
            )?)
        };

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = QueueDomainCommand::Deliver(envelope, reply_tx, admission);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(super::model::QUEUE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
    }
}

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
