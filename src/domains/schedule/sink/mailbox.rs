//! Mailbox-lane routing and the domain actor's message loop.

use super::model::{
    DeliveryError, Envelope, MailboxSink, ScheduleDomainActor, ScheduleDomainCommand,
    ScheduleDomainSink,
};
use crate::runtime::{Actor, Context};

impl MailboxSink for ScheduleDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        self.actor
            .try_send(ScheduleDomainCommand::Deliver(envelope))
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        self.actor
            .try_send_high_priority(ScheduleDomainCommand::Deliver(envelope))
    }
}

impl Actor for ScheduleDomainActor {
    type Message = ScheduleDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.state.runtime();
        match msg {
            ScheduleDomainCommand::Deliver(envelope) => {
                if let Err(error) = runtime.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "schedule", error = %error, "Schedule actor delivery failed");
                }
            }
            ScheduleDomainCommand::CleanupSession(session_id, reply) => {
                runtime.core.cleaned_up_sessions.lock().mark(session_id);
                runtime.unsubscribe_all(session_id);
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            ScheduleDomainCommand::ReadPendingClaims(route_family, reply) => {
                let _ = reply.send(runtime.admin_pending_claims(route_family));
            }
            ScheduleDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ScanDueSchedules => {
                runtime.scan_due_schedules();
            }
            ScheduleDomainCommand::PreloadPersistedFamilies(reply) => {
                let _ = reply.send(runtime.preload_persisted_families());
            }
            ScheduleDomainCommand::BenchPublishEvent(event, reply) => {
                runtime.bench_publish_event(&event);
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ForceDueScanForTests(ready_count, reply) => {
                runtime.force_due_scan_for_tests(ready_count);
                let _ = reply.send(());
            }
            #[cfg(test)]
            ScheduleDomainCommand::PanicForTests => {
                panic!("test Schedule domain actor panic");
            }
            #[cfg(test)]
            ScheduleDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
    }
}
