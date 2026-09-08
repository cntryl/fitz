//! Mailbox boundary for the managed KV domain actor.

use super::commands::KvDomainCommand;
use super::state::{KvDomainRuntime, KvDomainSink};
use crate::runtime::{DeliveryError, Envelope, MailboxSink};

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        let family = *envelope.destination().family();
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Normal,
            KvDomainCommand::Deliver(envelope),
        )
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        let family = *envelope.destination().family();
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            KvDomainCommand::Deliver(envelope),
        )
    }
}

impl KvDomainRuntime<'_> {
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
            KvDomainCommand::ApplyWriteOptions(message, reply) => {
                let _ = reply.send(self.apply_write_options(message));
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
