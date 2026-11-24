//! Mailbox boundary for the managed KV domain actor.

use super::commands::KvDomainCommand;
use super::state::{KvDomain, KvFamilyRuntime};
use fitz_core::runtime::SessionScoped as _;
use fitz_core::runtime::{DeliveryError, Envelope, MailboxSink};

impl MailboxSink for KvDomain {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let envelope = self.config.router.retain_session_work(envelope);
        if let Some(session_id) = fitz_core::runtime::session_cleanup_id(&envelope) {
            return self.cleanup_session(session_id);
        }
        let family = *envelope.destination().family();
        self.try_send(
            family,
            fitz_core::runtime::FamilyActorLane::Normal,
            KvDomainCommand::Deliver(envelope),
        )
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let envelope = self.config.router.retain_session_work(envelope);
        if let Some(session_id) = fitz_core::runtime::session_cleanup_id(&envelope) {
            return self.cleanup_session(session_id);
        }
        let family = *envelope.destination().family();
        self.try_send(
            family,
            fitz_core::runtime::FamilyActorLane::Control,
            KvDomainCommand::Deliver(envelope),
        )
    }
}

impl KvFamilyRuntime<'_> {
    pub(super) fn receive(&mut self, msg: KvDomainCommand) {
        match msg {
            KvDomainCommand::Deliver(envelope) => {
                if let Err(error) = self.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "kv", error = %error, "KV actor delivery failed");
                }
            }
            KvDomainCommand::CleanupSession(session_id, reply) => {
                self.cleanup_session(session_id);
                let _ = reply.send(());
            }
            KvDomainCommand::CaptureSnapshot(selector, reply) => {
                let _ = reply.send(self.capture_kv_snapshot(&selector));
            }
            KvDomainCommand::RestoreSnapshot(artifact, reply) => {
                let _ = reply.send(self.restore_kv_snapshot(&artifact));
            }
            #[cfg(test)]
            KvDomainCommand::SyncAdminSnapshot(reply) => {
                self.sync_admin_snapshot();
                let _ = reply.send(());
            }
            #[cfg(test)]
            KvDomainCommand::ReadLatencySnapshots(resource_key, reply) => {
                let _ = reply.send(self.latency_snapshots(&resource_key));
            }
            #[cfg(test)]
            KvDomainCommand::ResolveCommitPersistence(persistence, reply) => {
                let _ = reply.send(self.resolve_commit_persistence(persistence));
            }
            KvDomainCommand::PanicForFailpoint => {
                panic!("injected KV domain actor panic");
            }
            #[cfg(test)]
            KvDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
            #[cfg(test)]
            KvDomainCommand::InspectForTests(inspect, reply) => {
                inspect(self.core);
                let _ = reply.send(());
            }
        }
    }
}
