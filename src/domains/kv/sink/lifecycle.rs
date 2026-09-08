//! Sink construction, pre-registration configuration, and family lifecycle.

use super::commands::KvDomainCommand;
use super::state::{KvDomainConfig, KvDomainCore, KvDomainRuntime, KvDomainSink, KvDomainState};
use crate::runtime::routing::RouteFamily;
use crate::runtime::Router;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

impl KvDomainState {
    fn new(config: &KvDomainConfig) -> Self {
        Self {
            core: KvDomainCore {
                store: config.store.clone(),
                actors: HashMap::new(),
                resource_locks: HashMap::new(),
                watch_registries: HashMap::new(),
                cleaned_up_sessions: crate::runtime::CleanedUpSessions::new(
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                ),
                router: config.router.clone(),
                projection: config.projection.clone(),
                metrics: config.metrics.clone(),
                sync_write_options: config.sync_write_options,
                buffered_write_options: config.buffered_write_options,
                idle_transaction_ttl: config.idle_transaction_ttl,
            },
        }
    }

    pub(super) fn runtime(&mut self) -> KvDomainRuntime<'_> {
        KvDomainRuntime {
            core: &mut self.core,
        }
    }
}

impl KvDomainSink {
    pub(super) fn admin_core(&self) -> KvDomainCore {
        KvDomainState::new(&self.config).core
    }

    #[must_use]
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_families(
            store,
            router,
            admin_read_model,
            &[RouteFamily::new(1), RouteFamily::new(2)],
        )
    }

    pub(crate) fn new_with_families(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        route_families: &[RouteFamily],
    ) -> Self {
        assert!(
            !route_families.is_empty(),
            "KV route families must not be empty"
        );
        let config = KvDomainConfig {
            store,
            router,
            projection: Arc::new(
                crate::domains::kv::admin_projection::KvAdminProjection::new(admin_read_model),
            ),
            metrics: None,
            sync_write_options: cntryl_midge::WriteOptions::sync(),
            buffered_write_options: cntryl_midge::WriteOptions::buffered(),
            idle_transaction_ttl: std::time::Duration::from_mins(5),
        };
        let active = Arc::new(AtomicBool::new(true));
        let family_runtime =
            Self::spawn_family_runtime(config.clone(), active.clone(), route_families);
        Self {
            family_runtime,
            route_families: route_families.to_vec(),
            active,
            config,
        }
    }

    fn spawn_family_runtime(
        config: KvDomainConfig,
        active: Arc<AtomicBool>,
        route_families: &[RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<KvDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(route_families)
            .expect("validated KV family actor pool configuration");
        crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
            pool,
            active,
            move |_family| KvDomainState::new(&config),
            |state, _, _, command| state.runtime().receive(command),
            crate::domains::kv::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
        )
    }

    fn rebuild_family_runtime(&mut self) {
        self.family_runtime.stop();
        self.active = Arc::new(AtomicBool::new(true));
        self.family_runtime = Self::spawn_family_runtime(
            self.config.clone(),
            self.active.clone(),
            &self.route_families,
        );
    }

    pub(super) fn try_send(
        &self,
        family: RouteFamily,
        lane: crate::runtime::FamilyActorLane,
        command: KvDomainCommand,
    ) -> Result<(), crate::runtime::DeliveryError> {
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(crate::runtime::family_actor_enqueue_error_to_delivery_error)
    }

    #[must_use]
    pub fn with_sync_write_options(self, write_options: cntryl_midge::WriteOptions) -> Self {
        let buffered = if write_options.is_cloud_async() || write_options.is_cloud_strict() {
            cntryl_midge::WriteOptions::cloud_async()
        } else {
            cntryl_midge::WriteOptions::buffered()
        };
        self.with_write_options(write_options, buffered)
    }

    #[must_use]
    pub fn with_write_options(
        mut self,
        sync_write_options: cntryl_midge::WriteOptions,
        buffered_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        self.config.sync_write_options = sync_write_options;
        self.config.buffered_write_options = buffered_write_options;
        self.rebuild_family_runtime();
        self
    }

    #[must_use]
    pub fn with_idle_transaction_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.config.idle_transaction_ttl = ttl;
        self.rebuild_family_runtime();
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.config.metrics = Some(crate::domains::kv::metrics::KvMetrics::new(collector));
        self.rebuild_family_runtime();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ActorHealthSnapshot {
        self.family_runtime.actor_health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(crate) fn mark_actor_permanently_failed_for_tests(&self) {
        self.family_runtime.fail_closed();
    }

    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in &self.route_families {
            let _ = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                KvDomainCommand::PanicForFailpoint,
            );
        }
    }
}
