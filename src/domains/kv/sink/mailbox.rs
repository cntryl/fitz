//! Mailbox boundary for the managed KV domain actor.

use super::commands::KvDomainCommand;
use super::state::{KvDomainMailboxActor, KvDomainSink};
use crate::runtime::{Actor, Context, DeliveryError, Envelope, MailboxSink};

impl MailboxSink for KvDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        self.actor.try_send(KvDomainCommand::Deliver(envelope))
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self.cleanup_session(cleanup.session_id);
        }
        self.actor
            .try_send_high_priority(KvDomainCommand::Deliver(envelope))
    }
}

impl Actor for KvDomainMailboxActor {
    type Message = KvDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        match msg {
            KvDomainCommand::Deliver(envelope) => {
                if let Err(error) = self.state.runtime().deliver_envelope(&envelope) {
                    tracing::warn!(domain = "kv", error = %error, "KV actor delivery failed");
                }
            }
            KvDomainCommand::CleanupSession(session_id, reply) => {
                self.state.runtime().cleanup_session(session_id);
                let _ = reply.send(());
            }
            KvDomainCommand::ReadActiveTransactionCount(reply) => {
                let _ = reply.send(self.state.runtime().active_transaction_count());
            }
            #[cfg(test)]
            KvDomainCommand::SyncAdminSnapshot(reply) => {
                self.state.runtime().sync_admin_snapshot();
                let _ = reply.send(());
            }
            #[cfg(test)]
            KvDomainCommand::ReadLatencySnapshots(resource_key, reply) => {
                let _ = reply.send(self.state.runtime().latency_snapshots(&resource_key));
            }
            #[cfg(test)]
            KvDomainCommand::ApplyWriteOptions(message, reply) => {
                let _ = reply.send(self.state.runtime().apply_write_options(message));
            }
            #[cfg(test)]
            KvDomainCommand::PanicForTests => {
                panic!("test KV domain actor panic");
            }
            #[cfg(test)]
            KvDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
    }
}
