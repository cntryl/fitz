//! Sink construction, pre-registration configuration, and actor lifecycle.

use super::state::{
    KvDomainCore, KvDomainMailboxActor, KvDomainRuntime, KvDomainSink, KvDomainState,
};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{ManagedActor, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::commands::KvDomainCommand;

impl KvDomainState {
    #[must_use]
    fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            core: KvDomainCore {
                store,
                actors: Arc::new(Mutex::new(HashMap::new())),
                resource_locks: Mutex::new(HashMap::new()),
                watch_registries: Mutex::new(HashMap::new()),
                cleaned_up_sessions: Mutex::new(crate::runtime::CleanedUpSessions::new(
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                )),
                router,
                projection: crate::domains::kv::admin_projection::KvAdminProjection::new(
                    admin_read_model,
                ),
                metrics: None,
                sync_write_options: cntryl_midge::WriteOptions::sync(),
                buffered_write_options: cntryl_midge::WriteOptions::buffered(),
                idle_transaction_ttl: std::time::Duration::from_mins(5),
            },
            active: AtomicBool::new(true),
        }
    }

    pub(super) fn runtime(&self) -> KvDomainRuntime<'_> {
        KvDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl KvDomainMailboxActor {
    #[must_use]
    pub(super) fn new(state: Arc<KvDomainState>) -> Self {
        Self { state }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/kv"))
    }
}

impl KvDomainSink {
    #[must_use]
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let state = Arc::new(KvDomainState::new(store, router, admin_read_model));
        let actor = Self::spawn_actor(state.clone());
        Self { state, actor }
    }

    fn spawn_actor(state: Arc<KvDomainState>) -> ManagedActor<KvDomainCommand> {
        let router = state.core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            KvDomainMailboxActor::route_address(),
            move || KvDomainMailboxActor::new(state.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.state.clone());
    }

    fn state_for_builder(&mut self) -> &mut KvDomainState {
        Arc::get_mut(&mut self.state).expect("KV sink builders must run before sharing the sink")
    }

    #[must_use]
    /// Configure the sync policy before registering or sharing this sink.
    ///
    /// Like every consuming `with_*` method here, this updates private state
    /// and rebuilds the sink's managed actor before returning the new value.
    pub fn with_sync_write_options(self, write_options: cntryl_midge::WriteOptions) -> Self {
        let buffered_write_options =
            if write_options.is_cloud_async() || write_options.is_cloud_strict() {
                cntryl_midge::WriteOptions::cloud_async()
            } else {
                cntryl_midge::WriteOptions::buffered()
            };
        self.with_write_options(write_options, buffered_write_options)
    }

    #[must_use]
    /// Configure sync and buffered policies before registering or sharing this sink.
    ///
    /// This consuming method rebuilds the sink's private managed actor.
    pub fn with_write_options(
        mut self,
        sync_write_options: cntryl_midge::WriteOptions,
        buffered_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        self.actor.stop();
        let core = &mut self.state_for_builder().core;
        core.sync_write_options = sync_write_options;
        core.buffered_write_options = buffered_write_options;
        self.rebuild_actor();
        self
    }

    #[must_use]
    /// Configure idle transaction expiry before registering or sharing this sink.
    ///
    /// This consuming method rebuilds the sink's private managed actor.
    pub fn with_idle_transaction_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.actor.stop();
        self.state_for_builder().core.idle_transaction_ttl = ttl;
        self.rebuild_actor();
        self
    }

    #[must_use]
    /// Configure the KV metrics collector before registering or sharing this sink.
    ///
    /// This consuming method rebuilds the sink's private managed actor.
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        let state = self.state_for_builder();
        state.core.metrics = Some(crate::domains::kv::metrics::KvMetrics::new(collector));
        state.runtime().refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.state.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    #[cfg(test)]
    /// Mark the mailbox actor permanently failed without delivering a panic.
    pub(crate) fn mark_actor_permanently_failed_for_tests(&self) {
        self.actor.mark_permanently_failed_for_tests();
    }

    #[cfg(test)]
    /// Trigger the mailbox actor's fail-closed panic path.
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(KvDomainCommand::PanicForTests);
    }
}
