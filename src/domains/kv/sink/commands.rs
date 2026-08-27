//! Managed-actor command protocol and synchronous public controls.

#[cfg(test)]
use super::locks::KvResourceLockKey;
use super::state::KvDomainSink;
use std::time::Duration;

pub(super) enum KvDomainCommand {
    Deliver(crate::runtime::Envelope),
    CleanupSession(u64, crossbeam_channel::Sender<()>),
    ReadActiveTransactionCount(crossbeam_channel::Sender<usize>),
    #[cfg(test)]
    SyncAdminSnapshot(crossbeam_channel::Sender<()>),
    #[cfg(test)]
    ReadLatencySnapshots(
        KvResourceLockKey,
        crossbeam_channel::Sender<(
            crate::control::admin::KvLatencySnapshot,
            crate::control::admin::KvLatencySnapshot,
        )>,
    ),
    #[cfg(test)]
    /// Ask the mailbox actor to apply its configured BEGIN write policy.
    ApplyWriteOptions(
        crate::domains::kv::KvMessage,
        crossbeam_channel::Sender<crate::domains::kv::KvMessage>,
    ),
    #[cfg(test)]
    PanicForTests,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

impl KvDomainSink {
    pub(super) fn request_actor<T>(
        &self,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<T>) -> KvDomainCommand,
    ) -> Option<T> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.actor.try_send_high_priority(build_command(reply_tx)) {
            tracing::warn!(domain = "kv", operation, error = %error, "KV actor command enqueue failed");
            return None;
        }

        match reply_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(reply) => Some(reply),
            Err(error) => {
                tracing::warn!(domain = "kv", operation, error = %error, "KV actor command reply failed");
                None
            }
        }
    }

    /// Remove all live state owned by a disconnected session.
    ///
    /// # Errors
    ///
    /// Returns the actor enqueue failure or a bounded reply-wait failure when
    /// cleanup execution cannot be confirmed.
    pub fn cleanup_session(&self, session_id: u64) -> Result<(), crate::runtime::DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.actor
            .try_send_high_priority(KvDomainCommand::CleanupSession(session_id, reply_tx))?;
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(crate::runtime::reply_wait::map_reply_wait_error)
    }

    #[cfg(test)]
    pub(super) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.actor
            .try_send_high_priority(KvDomainCommand::BlockForTests(entered, release))
            .expect("enqueue KV actor test block");
    }

    /// Return the number of live KV transactions, or zero if the actor does not reply.
    #[must_use]
    pub fn active_transaction_count(&self) -> usize {
        self.request_actor("active_transaction_count", |reply| {
            KvDomainCommand::ReadActiveTransactionCount(reply)
        })
        .unwrap_or_default()
    }
}
