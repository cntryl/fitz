//! Mailbox entry points: `MailboxSink`, the domain actor's `receive` loop, and
//! the thin runtime-to-core delegation used by both.

use super::model::{QueueDomain, QueueDomainCommand, QueueFamilyRuntime};
use crate::domains::queue::actor::QUEUE_ACTOR_REPLY_TIMEOUT;
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
use std::sync::atomic::Ordering;
use std::time::Instant;

pub(super) struct RuntimeSweepPendingReset<'a>(pub(super) &'a std::sync::atomic::AtomicBool);

impl Drop for RuntimeSweepPendingReset<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl MailboxSink for QueueDomain {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, true)
    }
}

impl QueueFamilyRuntime {
    pub(super) fn receive_command(&mut self, msg: QueueDomainCommand) {
        match msg {
            QueueDomainCommand::Deliver(envelope, reply, admission) => {
                let started_at = Instant::now();
                let outcome = self.core.deliver_envelope(&envelope);
                super::model::record_service_sample(&self.core.delivery_service_us, started_at);
                let _ = reply.send(outcome);
                // Explicit: the slot is released here, once the work is
                // actually done, and not when the caller gave up waiting.
                drop(admission);
            }
            QueueDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                self.core.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            QueueDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(self.core.live_counts());
            }
            #[cfg(test)]
            QueueDomainCommand::CleanupSession(session_id, reply) => {
                self.core.cleanup_session(session_id);
                let _ = reply.send(());
            }
            QueueDomainCommand::SweepRuntimeStateAt(now, Some(reply)) => {
                self.core.sweep_runtime_state_at(now);
                let _ = reply.send(());
            }
            QueueDomainCommand::SweepRuntimeStateAt(now, None) => {
                let pending = self.core.runtime_sweep_pending.clone();
                let _pending_reset = RuntimeSweepPendingReset(&pending);
                self.core.sweep_runtime_state_at(now);
            }
            QueueDomainCommand::ReplayDeadLetter(key, id, reply) => {
                let _ = reply.send(self.core.replay_dead_letter(&key, id));
            }
            QueueDomainCommand::PurgeDeadLetter(key, id, reply) => {
                let _ = reply.send(self.core.purge_dead_letter(&key, id));
            }
            QueueDomainCommand::PanicForFailpoint => {
                panic!("injected Queue domain actor panic");
            }
            #[cfg(test)]
            QueueDomainCommand::InspectForTests(inspect, reply) => {
                inspect(&mut self.core);
                let _ = reply.send(());
            }
        }
    }
}

impl QueueDomain {
    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let family = *envelope.destination().family();
        let service = &self.config.delivery_service_us[&family.id()];
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
                service,
                self.family_runtime
                    .is_family_running(*envelope.destination().family()),
            )?)
        };

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        let command = QueueDomainCommand::Deliver(envelope, reply_tx, admission);
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(crate::runtime::family_actor_enqueue_error_to_delivery_error)?;

        reply_rx
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
    }
}
