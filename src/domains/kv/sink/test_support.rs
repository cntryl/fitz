//! Test-only controls for observing and driving the family-owned KV state.

use super::commands::KvDomainCommand;
use super::locks::KvResourceLockKey;
use super::state::KvDomain;

impl KvDomain {
    pub(super) fn run_on_family_for_tests<T: Send + 'static>(
        &self,
        family: crate::runtime::routing::RouteFamily,
        operation: impl FnOnce(&mut super::state::KvFamilyRuntime<'_>) -> T + Send + 'static,
    ) -> T {
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            KvDomainCommand::InspectForTests(
                Box::new(move |core| {
                    let mut runtime = super::state::KvFamilyRuntime { core };
                    let _ = result_tx.send(operation(&mut runtime));
                }),
                done_tx,
            ),
        )
        .expect("enqueue KV family test operation");
        done_rx.recv().expect("receive KV family test completion");
        result_rx.recv().expect("receive KV family test result")
    }

    /// Stop the mailbox actor without changing the sink's active flag.
    pub(super) fn stop_actor_for_tests(&self) {
        self.family_runtime.stop();
    }

    /// Report whether the sink has not been stopped.
    pub(super) fn is_active_for_tests(&self) -> bool {
        use std::sync::atomic::Ordering;

        self.active.load(Ordering::Relaxed)
    }

    /// Seed one session actor for state-cleanup regressions.
    pub(super) fn insert_actor_for_tests(
        &self,
        session_id: u64,
        actor: crate::domains::kv::KvActor,
    ) {
        self.inspect_for_tests(move |core| {
            core.actors.insert(session_id, actor);
        });
    }

    /// Report whether all watch registries are empty.
    pub(super) fn watch_registries_are_empty_for_tests(&self) -> bool {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.inspect_for_tests(move |core| {
            let _ = reply_tx.send(core.watch_registries.is_empty());
        });
        reply_rx.recv().expect("receive watch-registry inspection")
    }

    /// Report whether all session actors are absent.
    pub(super) fn actors_are_empty_for_tests(&self) -> bool {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.inspect_for_tests(move |core| {
            let _ = reply_tx.send(core.actors.is_empty());
        });
        reply_rx.recv().expect("receive actor inspection")
    }

    /// Report whether all write locks are absent.
    pub(super) fn resource_locks_are_empty_for_tests(&self) -> bool {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.inspect_for_tests(move |core| {
            let _ = reply_tx.send(core.resource_locks.is_empty());
        });
        reply_rx.recv().expect("receive lock inspection")
    }

    /// Rebuild the admin projection through the mailbox actor.
    pub(super) fn sync_admin_snapshot(&self) {
        let _ = self.request_actor(
            self.route_families[0],
            "sync_admin_snapshot",
            KvDomainCommand::SyncAdminSnapshot,
        );
    }

    /// Read the latency snapshots for one resource through the mailbox actor.
    pub(super) fn latency_snapshots(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> (
        crate::control::admin::KvLatencySnapshot,
        crate::control::admin::KvLatencySnapshot,
    ) {
        self.request_actor(self.route_families[0], "latency_snapshots", |reply| {
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
        self.request_actor(self.route_families[0], "apply_write_options", |reply| {
            KvDomainCommand::ApplyWriteOptions(message, reply)
        })
        .unwrap_or(fallback)
    }
}
