//! Test-only controls for observing and driving the managed KV mailbox actor.

use super::commands::KvDomainCommand;
use super::locks::KvResourceLockKey;
use super::state::KvDomainSink;

impl KvDomainSink {
    /// Stop the mailbox actor without changing the sink's active flag.
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    /// Report whether the sink has not been stopped.
    pub(super) fn is_active_for_tests(&self) -> bool {
        use std::sync::atomic::Ordering;

        self.state.active.load(Ordering::Relaxed)
    }

    /// Seed one session actor for state-cleanup regressions.
    pub(super) fn insert_actor_for_tests(
        &self,
        session_id: u64,
        actor: crate::domains::kv::KvActor,
    ) {
        self.state.core.actors.lock().insert(
            session_id,
            std::sync::Arc::new(parking_lot::Mutex::new(actor)),
        );
    }

    /// Report whether all watch registries are empty.
    pub(super) fn watch_registries_are_empty_for_tests(&self) -> bool {
        self.state.core.watch_registries.lock().is_empty()
    }

    /// Report whether all session actors are absent.
    pub(super) fn actors_are_empty_for_tests(&self) -> bool {
        self.state.core.actors.lock().is_empty()
    }

    /// Report whether all write locks are absent.
    pub(super) fn resource_locks_are_empty_for_tests(&self) -> bool {
        self.state.core.resource_locks.lock().is_empty()
    }

    /// Rebuild the admin projection through the mailbox actor.
    pub(super) fn sync_admin_snapshot(&self) {
        let _ = self.request_actor("sync_admin_snapshot", KvDomainCommand::SyncAdminSnapshot);
    }

    /// Read the latency snapshots for one resource through the mailbox actor.
    pub(super) fn latency_snapshots(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> (
        crate::control::admin::KvLatencySnapshot,
        crate::control::admin::KvLatencySnapshot,
    ) {
        self.request_actor("latency_snapshots", |reply| {
            KvDomainCommand::ReadLatencySnapshots(resource_key.clone(), reply)
        })
        .unwrap_or_default()
    }

    /// Apply the configured BEGIN write policy through the mailbox actor.
    pub(super) fn apply_write_options(
        &self,
        message: crate::domains::kv::KvMessage,
    ) -> crate::domains::kv::KvMessage {
        let fallback = message.clone();
        self.request_actor("apply_write_options", |reply| {
            KvDomainCommand::ApplyWriteOptions(message, reply)
        })
        .unwrap_or(fallback)
    }
}
