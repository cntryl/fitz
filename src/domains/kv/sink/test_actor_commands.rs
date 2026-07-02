use super::model::{KvDomainCommand, KvDomainSink, KvResourceLockKey};
use std::sync::atomic::Ordering;
use std::time::Duration;

impl KvDomainSink {
    pub(super) fn is_active_for_tests(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    pub(super) fn insert_actor_for_tests(
        &self,
        session_id: u64,
        actor: crate::domains::kv::KvActor,
    ) {
        self.state.core.actors.lock().insert(session_id, actor);
    }

    pub(super) fn watch_actors_are_empty_for_tests(&self) -> bool {
        self.state.core.watch_actors.lock().is_empty()
    }

    pub(super) fn sync_admin_snapshot(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(KvDomainCommand::SyncAdminSnapshot(reply_tx))
        {
            tracing::warn!(domain = "kv", error = %error, "KV admin snapshot enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(Duration::from_secs(1)) {
            tracing::warn!(domain = "kv", error = %error, "KV admin snapshot reply failed");
        }
    }

    pub(super) fn latency_snapshots(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> (
        crate::control::admin::KvLatencySnapshot,
        crate::control::admin::KvLatencySnapshot,
    ) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(KvDomainCommand::ReadLatencySnapshots(
                    resource_key.clone(),
                    reply_tx,
                ))
        {
            tracing::warn!(domain = "kv", error = %error, "KV latency snapshot enqueue failed");
            return Default::default();
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }

    pub(super) fn apply_sync_write_options(
        &self,
        message: crate::domains::kv::KvMessage,
    ) -> crate::domains::kv::KvMessage {
        let fallback = message.clone();
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(KvDomainCommand::ApplySyncWriteOptions(message, reply_tx))
        {
            tracing::warn!(domain = "kv", error = %error, "KV sync-write mapping enqueue failed");
            return fallback;
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(fallback)
    }
}
