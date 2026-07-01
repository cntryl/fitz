use super::model::{KvDomainCommand, KvDomainSink};
use std::time::Duration;

impl KvDomainSink {
    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    fn send_unit_actor_command(
        &self,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<()>) -> KvDomainCommand,
    ) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.actor.try_send_high_priority(build_command(reply_tx)) {
            tracing::warn!(domain = "kv", operation, error = %error, "KV actor command enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(Duration::from_secs(1)) {
            tracing::warn!(domain = "kv", operation, error = %error, "KV actor command reply failed");
        }
    }

    pub fn cleanup_session(&self, session_id: u64) {
        self.send_unit_actor_command("cleanup_session", |reply| {
            KvDomainCommand::CleanupSession(session_id, reply)
        });
    }

    pub fn active_transaction_count(&self) -> usize {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(KvDomainCommand::ReadActiveTransactionCount(reply_tx))
        {
            tracing::warn!(domain = "kv", error = %error, "KV active-transaction query enqueue failed");
            return 0;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }
}
