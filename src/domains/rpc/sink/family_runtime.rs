use super::state_model::{
    RpcDomain, RpcDomainCommand, RpcDomainConfig, RpcFamilyRuntime, RpcFamilyState, RpcState,
    RPC_DEFAULT_REQUEST_TIMEOUT, RPC_DEFAULT_ROUTE_PENDING_CAPACITY,
    RPC_MIN_TIMEOUT_SWEEP_INTERVAL,
};
use crate::runtime::routing::RouteFamily;
use crate::runtime::{DeliveryError, Router};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

impl RpcDomain {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_inner(router, admin_read_model, vec![RouteFamily::new(1)])
    }

    pub(crate) fn new_with_families(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        provisioned_families: &[RouteFamily],
    ) -> Self {
        Self::new_inner(router, admin_read_model, provisioned_families.to_vec())
    }

    fn new_inner(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        family_families: Vec<RouteFamily>,
    ) -> Self {
        let config = RpcDomainConfig {
            router,
            admin_read_model,
            request_timeout: RPC_DEFAULT_REQUEST_TIMEOUT,
            route_pending_capacity: RPC_DEFAULT_ROUTE_PENDING_CAPACITY,
            global_pending_count: Arc::new(AtomicUsize::new(0)),
            snapshot_epoch: Instant::now(),
            metrics: None,
        };
        let active = Arc::new(AtomicBool::new(true));
        let family_runtime = Self::spawn_family_runtime(&config, active.clone(), &family_families)
            .expect("validated RPC family actor pool configuration");
        Self {
            config,
            active,
            family_runtime,
            family_families,
        }
    }

    fn spawn_family_runtime(
        config: &RpcDomainConfig,
        active: Arc<AtomicBool>,
        families: &[RouteFamily],
    ) -> Result<crate::runtime::FamilyActorPoolRuntime<RpcDomainCommand>, String> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .map_err(|error| format!("create RPC family actor pool: {error}"))?;
        let family_config = config.clone();
        Ok(
            crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
                pool,
                active.clone(),
                move |family| Self::new_family_state(&family_config, family),
                move |state, family, _lane, command| {
                    let mut runtime = RpcFamilyRuntime {
                        core: state,
                        active: active.as_ref(),
                    };
                    match command {
                        RpcDomainCommand::Deliver(envelope, reply) => {
                            let result = if *envelope.destination().family() == family {
                                runtime.deliver_envelope(&envelope)
                            } else {
                                Err(DeliveryError::ActorStopped)
                            };
                            let _ = reply.send(result);
                        }
                        RpcDomainCommand::ExpireTimedOutRequestsAt(now, reply) => {
                            runtime.expire_timed_out_requests_at(now);
                            if let Some(reply) = reply {
                                let _ = reply.send(());
                            }
                        }
                        RpcDomainCommand::ReadLiveCounts(reply) => {
                            let _ = reply.send(runtime.live_counts());
                        }
                        #[cfg(test)]
                        RpcDomainCommand::SyncAdminSnapshot(reply) => {
                            runtime.sync_admin_snapshot();
                            if let Some(reply) = reply {
                                let _ = reply.send(());
                            }
                        }
                        RpcDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                            runtime.refresh_admin_snapshot_if_dirty();
                            if let Some(reply) = reply {
                                let _ = reply.send(());
                            }
                        }
                        #[cfg(test)]
                        RpcDomainCommand::ApplySessionCleanupForTests(session_id, reply) => {
                            let _ = reply.send(runtime.apply_session_cleanup(session_id));
                        }
                        #[cfg(test)]
                        RpcDomainCommand::ApplyWorkerUnsubscribeForTests(
                            worker_addr,
                            session_id,
                            reply,
                        ) => {
                            let _ = reply
                                .send(runtime.apply_worker_unsubscribe(&worker_addr, session_id));
                        }
                        RpcDomainCommand::PanicForFailpoint => {
                            panic!("injected RPC family actor panic");
                        }
                        #[cfg(test)]
                        RpcDomainCommand::BlockForTests(entered, release) => {
                            let _ = entered.send(());
                            let _ = release.recv();
                        }
                        #[cfg(test)]
                        RpcDomainCommand::InspectForTests(inspect, reply) => {
                            inspect(runtime.core);
                            let _ = reply.send(());
                        }
                        #[cfg(test)]
                        RpcDomainCommand::ForwardQueuedDispatchForTests(dispatch, reply) => {
                            runtime.forward_queued_dispatch(&dispatch);
                            let _ = reply.send(());
                        }
                    }
                },
                crate::domains::rpc::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
            ),
        )
    }

    fn new_family_state(config: &RpcDomainConfig, family: RouteFamily) -> RpcFamilyState {
        RpcFamilyState {
            family,
            state: RpcState::new(),
            cleaned_up_sessions: crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            ),
            router: config.router.clone(),
            admin_read_model: config.admin_read_model.clone(),
            request_timeout: config.request_timeout,
            route_pending_capacity: config.route_pending_capacity,
            global_pending_count: config.global_pending_count.clone(),
            snapshot_dirty: AtomicBool::new(false),
            snapshot_syncing: AtomicBool::new(false),
            last_snapshot_elapsed_us: AtomicU64::new(0),
            last_inline_timeout_elapsed_us: AtomicU64::new(0),
            snapshot_epoch: config.snapshot_epoch,
            metrics: config.metrics.clone(),
        }
    }

    fn rebuild_actor(&mut self) {
        self.family_runtime.stop();
        self.family_runtime =
            Self::spawn_family_runtime(&self.config, self.active.clone(), &self.family_families)
                .expect("validated RPC family actor pool configuration");
    }

    #[must_use]
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.family_runtime.stop();
        self.config.request_timeout = if request_timeout.is_zero() {
            RPC_MIN_TIMEOUT_SWEEP_INTERVAL
        } else {
            request_timeout
        };
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_route_pending_capacity(mut self, route_pending_capacity: usize) -> Self {
        self.family_runtime.stop();
        self.config.route_pending_capacity = route_pending_capacity.max(1);
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.family_runtime.stop();
        self.config.metrics = Some(crate::domains::rpc::RpcMetrics::new(metrics));
        self.rebuild_actor();
        self
    }
}
