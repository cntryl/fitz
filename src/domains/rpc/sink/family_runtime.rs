use super::state_model::{
    Arc, AtomicBool, AtomicU64, AtomicUsize, BTreeMap, DeliveryError, Duration, Instant, Mutex,
    RouteFamily, Router, RpcDomainActor, RpcDomainCommand, RpcDomainCore, RpcDomainRuntime,
    RpcDomainSink, RpcState, RPC_DEFAULT_REQUEST_TIMEOUT, RPC_DEFAULT_ROUTE_PENDING_CAPACITY,
    RPC_MIN_TIMEOUT_SWEEP_INTERVAL,
};

impl RpcDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_inner(router, admin_read_model, None)
    }

    pub(crate) fn new_with_families(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        provisioned_families: &[RouteFamily],
    ) -> Self {
        Self::new_inner(
            router,
            admin_read_model,
            Some(provisioned_families.to_vec()),
        )
    }

    fn new_inner(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        family_families: Option<Vec<RouteFamily>>,
    ) -> Self {
        let core = Arc::new(RpcDomainCore {
            state: Mutex::new(RpcState::new()),
            cleaned_up_sessions: Mutex::new(crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
            router,
            admin_read_model,
            request_timeout: RPC_DEFAULT_REQUEST_TIMEOUT,
            route_pending_capacity: RPC_DEFAULT_ROUTE_PENDING_CAPACITY,
            global_pending_count: Arc::new(AtomicUsize::new(0)),
            enforce_global_pending_count: false,
            snapshot_dirty: Arc::new(AtomicBool::new(false)),
            snapshot_syncing: Arc::new(AtomicBool::new(false)),
            last_snapshot_elapsed_us: Arc::new(AtomicU64::new(0)),
            last_inline_timeout_elapsed_us: Arc::new(AtomicU64::new(0)),
            snapshot_epoch: Instant::now(),
            metrics: None,
            family_cores: Arc::new(Mutex::new(BTreeMap::new())),
        });
        let active = Arc::new(AtomicBool::new(true));
        let actor = Self::spawn_actor(core.clone(), active.clone());
        let family_runtime = family_families
            .as_deref()
            .map(|families| Self::spawn_family_runtime(&core, active.clone(), families))
            .transpose()
            .expect("validated RPC family actor pool configuration");
        Self {
            core,
            active,
            actor,
            family_runtime,
            family_families,
        }
    }

    fn spawn_actor(
        core: Arc<RpcDomainCore>,
        active: Arc<AtomicBool>,
    ) -> crate::runtime::ManagedActor<RpcDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            RpcDomainActor::route_address(),
            move || RpcDomainActor::new(core.clone(), active.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn spawn_family_runtime(
        core: &Arc<RpcDomainCore>,
        active: Arc<AtomicBool>,
        families: &[RouteFamily],
    ) -> Result<crate::runtime::FamilyActorPoolRuntime<RpcDomainCommand>, String> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .map_err(|error| format!("create RPC family actor pool: {error}"))?;
        let core_for_factory = core.clone();
        Ok(
            crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
                pool,
                active.clone(),
                move |family| Self::family_core_for(&core_for_factory, family),
                move |core, family, _lane, command| {
                    let runtime = RpcDomainRuntime {
                        core,
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
                        #[cfg(test)]
                        RpcDomainCommand::PanicForTests => {
                            panic!("test RPC family actor panic");
                        }
                    }
                },
                crate::domains::rpc::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
            ),
        )
    }

    fn family_core_for(shared: &Arc<RpcDomainCore>, family: RouteFamily) -> Arc<RpcDomainCore> {
        let family_core = Arc::new(RpcDomainCore {
            state: Mutex::new(RpcState::new()),
            cleaned_up_sessions: Mutex::new(crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
            router: shared.router.clone(),
            admin_read_model: shared.admin_read_model.clone(),
            request_timeout: shared.request_timeout,
            route_pending_capacity: shared.route_pending_capacity,
            global_pending_count: shared.global_pending_count.clone(),
            enforce_global_pending_count: true,
            snapshot_dirty: shared.snapshot_dirty.clone(),
            snapshot_syncing: shared.snapshot_syncing.clone(),
            last_snapshot_elapsed_us: shared.last_snapshot_elapsed_us.clone(),
            last_inline_timeout_elapsed_us: shared.last_inline_timeout_elapsed_us.clone(),
            snapshot_epoch: shared.snapshot_epoch,
            metrics: shared.metrics.clone(),
            family_cores: shared.family_cores.clone(),
        });
        shared
            .family_cores
            .lock()
            .insert(family.id(), Arc::downgrade(&family_core));
        family_core
    }

    fn stop_family_runtime(&mut self) {
        if let Some(runtime) = self.family_runtime.take() {
            runtime.stop();
        }
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.stop_family_runtime();
        self.actor = Self::spawn_actor(self.core.clone(), self.active.clone());
        self.family_runtime = self
            .family_families
            .as_deref()
            .map(|families| Self::spawn_family_runtime(&self.core, self.active.clone(), families))
            .transpose()
            .expect("validated RPC family actor pool configuration");
    }

    fn core_for_builder(&mut self) -> &mut RpcDomainCore {
        self.stop_family_runtime();
        Arc::get_mut(&mut self.core).expect("RPC sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.actor.stop();
        self.core_for_builder().request_timeout = if request_timeout.is_zero() {
            RPC_MIN_TIMEOUT_SWEEP_INTERVAL
        } else {
            request_timeout
        };
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_route_pending_capacity(mut self, route_pending_capacity: usize) -> Self {
        self.actor.stop();
        self.core_for_builder().route_pending_capacity = route_pending_capacity.max(1);
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        metrics: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        self.core_for_builder().metrics = Some(crate::domains::rpc::RpcMetrics::new(metrics));
        self.runtime().refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }
}
