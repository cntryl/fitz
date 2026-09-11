//! Mailbox-lane routing and the domain actor's message loop.

use super::model::{ScheduleDomain, ScheduleDomainCommand, ScheduleDomainRuntime};
use crate::runtime::{DeliveryError, Envelope, FamilyActorLane, MailboxSink};

impl MailboxSink for ScheduleDomain {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        let family = *envelope.destination().family();
        self.try_send(
            family,
            FamilyActorLane::Normal,
            ScheduleDomainCommand::Deliver(envelope),
        )
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        let family = *envelope.destination().family();
        self.try_send(
            family,
            FamilyActorLane::Control,
            ScheduleDomainCommand::Deliver(envelope),
        )
    }
}

impl ScheduleDomainRuntime<'_> {
    pub(super) fn receive(&mut self, msg: ScheduleDomainCommand) {
        match msg {
            ScheduleDomainCommand::Deliver(envelope) => {
                if let Err(error) = self.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "schedule", error = %error, "Schedule actor delivery failed");
                }
            }
            ScheduleDomainCommand::CleanupSession(session_id, reply) => {
                self.core.cleaned_up_sessions.mark(session_id);
                self.unsubscribe_all(session_id);
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(self.live_counts());
            }
            ScheduleDomainCommand::ReadPendingClaims(route_family, reply) => {
                let _ = reply.send(self.admin_pending_claims(route_family));
            }
            ScheduleDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                self.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ScanDueSchedules => {
                self.scan_due_schedules();
            }
            ScheduleDomainCommand::PreloadPersistedFamilies(reply) => {
                let _ = reply.send(self.preload_persisted_families());
            }
            ScheduleDomainCommand::BenchPublishEvent(event, reply) => {
                self.bench_publish_event(&event);
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ForceDueScanForTests(ready_count, reply) => {
                self.force_due_scan_for_tests(ready_count);
                let _ = reply.send(());
            }
            ScheduleDomainCommand::RunNow(route, deadline, reply) => {
                if std::time::Instant::now() >= deadline {
                    let _ = reply.send(Err(
                        "schedule run-now command expired before execution; no handoff was attempted"
                            .to_string(),
                    ));
                } else {
                    let _ = reply.send(Ok(self.run_now(&route)));
                }
            }
            ScheduleDomainCommand::PanicForFailpoint => {
                panic!("injected Schedule domain actor panic");
            }
            #[cfg(test)]
            ScheduleDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
            #[cfg(test)]
            ScheduleDomainCommand::InspectForTests(inspect, reply) => {
                inspect(self.core);
                let _ = reply.send(());
            }
        }
    }
}
