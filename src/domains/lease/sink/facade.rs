//! `LeaseDomainSink` family-runtime composition and synchronous facade.

#[cfg(any(test, feature = "benchkit"))]
use super::model::LeaseAcquireRequest;
use super::model::{
    Arc, AtomicBool, AtomicU64, BTreeMap, HashMap, LeaseDomainCommand, LeaseDomainCore,
    LeaseDomainRuntime, LeaseDomainSink, LeaseDomainState, LeaseListSnapshotCoordinator,
    LeaseLiveCounts, LeaseMetrics, Mutex, Ordering, LEASE_ACTOR_REPLY_TIMEOUT,
};
use crate::runtime::routing::RouteFamily;
#[cfg(any(test, feature = "benchkit"))]
use crate::runtime::routing::{Route, RouteAddress};
use crate::runtime::{DeliveryError, FamilyActorLane, Router};

impl LeaseDomainState {
    fn root(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Arc<Self> {
        Arc::new(Self {
            core: LeaseDomainCore {
                leases: Mutex::new(BTreeMap::new()),
                session_leases: Mutex::new(HashMap::new()),
                pending_acquires: Mutex::new(HashMap::new()),
                session_waiters: Mutex::new(HashMap::new()),
                cleaned_up_sessions: Mutex::new(crate::runtime::CleanedUpSessions::new(
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                )),
                next_token: Arc::new(AtomicU64::new(1)),
                router,
                families: Mutex::new(HashMap::new()),
                next_sub_id: Arc::new(AtomicU64::new(1)),
                holder_incarnation_hasher: Arc::new(std::collections::hash_map::RandomState::new()),
                list_snapshots: Arc::new(LeaseListSnapshotCoordinator::new()),
                next_list_snapshot_id: Arc::new(AtomicU64::new(Self::random_list_snapshot_seed())),
                admin_read_model,
                metrics: None,
                family_states: Arc::new(Mutex::new(BTreeMap::new())),
            },
            active: Arc::new(AtomicBool::new(true)),
        })
    }

    fn for_family(shared: &Arc<Self>, family: RouteFamily) -> Arc<Self> {
        let state = Arc::new(Self {
            core: LeaseDomainCore {
                leases: Mutex::new(BTreeMap::new()),
                session_leases: Mutex::new(HashMap::new()),
                pending_acquires: Mutex::new(HashMap::new()),
                session_waiters: Mutex::new(HashMap::new()),
                cleaned_up_sessions: Mutex::new(crate::runtime::CleanedUpSessions::new(
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                )),
                next_token: shared.core.next_token.clone(),
                router: shared.core.router.clone(),
                families: Mutex::new(HashMap::new()),
                next_sub_id: shared.core.next_sub_id.clone(),
                holder_incarnation_hasher: shared.core.holder_incarnation_hasher.clone(),
                list_snapshots: shared.core.list_snapshots.clone(),
                next_list_snapshot_id: shared.core.next_list_snapshot_id.clone(),
                admin_read_model: shared.core.admin_read_model.clone(),
                metrics: shared.core.metrics.clone(),
                family_states: shared.core.family_states.clone(),
            },
            active: shared.active.clone(),
        });
        shared
            .core
            .family_states
            .lock()
            .insert(family.id(), Arc::downgrade(&state));
        state
    }

    fn random_list_snapshot_seed() -> u64 {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes).expect("OS randomness required for Lease cursor identity");
        u64::from_ne_bytes(bytes) & (u64::MAX >> 1)
    }

    pub(in crate::domains::lease::sink) fn runtime(&self) -> LeaseDomainRuntime<'_> {
        LeaseDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl LeaseDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_families(
            router,
            admin_read_model,
            &[RouteFamily::new(1), RouteFamily::new(2)],
        )
    }

    pub(crate) fn new_with_families(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        route_families: &[RouteFamily],
    ) -> Self {
        let root = LeaseDomainState::root(router, admin_read_model);
        let families = route_families.to_vec();
        let family_runtime = Self::spawn_family_runtime(&root, &families);
        let state = Self::primary_state(&root, &families);
        Self {
            state,
            family_runtime,
            route_families: families,
        }
    }

    fn spawn_family_runtime(
        shared: &Arc<LeaseDomainState>,
        families: &[RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<LeaseDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .expect("validated Lease family actor pool configuration");
        let factory_state = shared.clone();
        crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
            pool,
            shared.active.clone(),
            move |family| LeaseDomainState::for_family(&factory_state, family),
            |state, _family, _lane, command| state.runtime().receive(command),
            crate::domains::lease::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
        )
    }

    fn primary_state(
        shared: &Arc<LeaseDomainState>,
        families: &[RouteFamily],
    ) -> Arc<LeaseDomainState> {
        let primary = families
            .first()
            .expect("Lease route families must not be empty");
        shared
            .core
            .family_states
            .lock()
            .get(&primary.id())
            .and_then(std::sync::Weak::upgrade)
            .expect("Lease primary family state was created")
    }

    fn rebuild_family_runtime(&mut self) {
        self.family_runtime.stop();
        self.family_runtime = Self::spawn_family_runtime(&self.state, &self.route_families);
        self.state = Self::primary_state(&self.state, &self.route_families);
    }

    fn state_for_builder(&mut self) -> &mut LeaseDomainState {
        self.family_runtime.stop();
        if let Some(primary) = self.route_families.first() {
            self.state.core.family_states.lock().remove(&primary.id());
        }
        Arc::get_mut(&mut self.state).expect("Lease sink builders must run before sharing the sink")
    }

    pub(super) fn enqueue(
        &self,
        family: RouteFamily,
        lane: FamilyActorLane,
        command: LeaseDomainCommand,
    ) -> Result<(), DeliveryError> {
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::enqueue_error)
    }

    fn enqueue_error(error: crate::runtime::FamilyActorEnqueueError) -> DeliveryError {
        match error {
            crate::runtime::FamilyActorEnqueueError::NormalLaneFull => DeliveryError::MailboxFull {
                capacity: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
                current_len: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
            },
            crate::runtime::FamilyActorEnqueueError::ControlLaneFull => {
                DeliveryError::HighLaneFull {
                    capacity: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                    current_len: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                }
            }
            crate::runtime::FamilyActorEnqueueError::UnknownFamily
            | crate::runtime::FamilyActorEnqueueError::ActorStopped => DeliveryError::ActorStopped,
        }
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.state_for_builder().core.metrics = Some(LeaseMetrics::new(collector));
        self.state.runtime().refresh_metrics_gauges();
        self.rebuild_family_runtime();
        self
    }

    pub fn stop(&self) {
        self.state.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn is_family_running(&self, family: RouteFamily) -> bool {
        self.family_runtime.is_family_running(family)
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ActorHealthSnapshot {
        self.family_runtime.actor_health_snapshot()
    }

    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in &self.route_families {
            let _ = self.enqueue(
                *family,
                FamilyActorLane::Control,
                LeaseDomainCommand::PanicForFailpoint,
            );
        }
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn panic_family_for_tests(&self, family: RouteFamily) {
        self.enqueue(
            family,
            FamilyActorLane::Control,
            LeaseDomainCommand::PanicForFailpoint,
        )
        .expect("enqueue Lease family panic");
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn stop_actor_for_tests(&self) {
        self.family_runtime.stop();
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.enqueue(
            self.route_families[0],
            FamilyActorLane::Control,
            LeaseDomainCommand::BlockForTests(entered, release),
        )
        .expect("enqueue Lease family test block");
    }

    /// Remove all ephemeral Lease state owned by `session_id` across families.
    ///
    /// # Errors
    ///
    /// Returns a bounded-lane delivery failure or a reply timeout when every
    /// provisioned family cannot confirm cleanup.
    pub fn cleanup_session(&self, session_id: u64) -> Result<(), DeliveryError> {
        let mut replies = Vec::with_capacity(self.route_families.len());
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            self.enqueue(
                *family,
                FamilyActorLane::Control,
                LeaseDomainCommand::CleanupSession(session_id, reply_tx),
            )?;
            replies.push(reply_rx);
        }
        for reply in replies {
            reply
                .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
                .map_err(crate::runtime::reply_wait::map_reply_wait_error)?;
        }
        Ok(())
    }

    pub(super) fn cleanup_family_session(
        &self,
        family: RouteFamily,
        session_id: u64,
    ) -> Result<(), DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.enqueue(
            family,
            FamilyActorLane::Control,
            LeaseDomainCommand::CleanupSession(session_id, reply_tx),
        )?;
        reply_rx
            .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
            .map_err(crate::runtime::reply_wait::map_reply_wait_error)
    }

    pub(crate) fn sweep_expired_state(&self) {
        let mut first_error = None;
        for family in &self.route_families {
            if let Err(error) = self.enqueue(
                *family,
                FamilyActorLane::Control,
                LeaseDomainCommand::SweepExpiredState,
            ) {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            self.record_sweep_enqueue_failure(&error);
        }
    }

    fn record_sweep_enqueue_failure(&self, error: &DeliveryError) {
        if let Some(metrics) = self.state.core.metrics.as_ref() {
            metrics
                .counter_inc(crate::domains::lease::metrics::METRIC_SWEEP_ENQUEUE_FAILURES_TOTAL);
        } else {
            crate::observability::counter_inc(
                crate::domains::lease::metrics::METRIC_SWEEP_ENQUEUE_FAILURES_TOTAL,
            );
        }
        tracing::warn!(domain = "lease", error = %error, "Lease sweep enqueue failed");
    }

    pub fn lease_count(&self) -> usize {
        self.live_counts().leases
    }

    pub fn subscription_count(&self) -> usize {
        self.live_counts().subscriptions
    }

    fn live_counts(&self) -> LeaseLiveCounts {
        let mut total = LeaseLiveCounts::default();
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if self
                .enqueue(
                    *family,
                    FamilyActorLane::Control,
                    LeaseDomainCommand::ReadLiveCounts(reply_tx),
                )
                .is_err()
            {
                return LeaseLiveCounts::default();
            }
            let counts = reply_rx
                .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
                .unwrap_or_default();
            total.leases = total.leases.saturating_add(counts.leases);
            total.subscriptions = total.subscriptions.saturating_add(counts.subscriptions);
        }
        total
    }

    pub fn admin_waiters(&self) -> Vec<crate::control::admin::LeaseWaiterInfo> {
        let mut waiters = Vec::new();
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if self
                .enqueue(
                    *family,
                    FamilyActorLane::Control,
                    LeaseDomainCommand::ReadWaiters(reply_tx),
                )
                .is_err()
            {
                return Vec::new();
            }
            waiters.extend(
                reply_rx
                    .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
                    .unwrap_or_default(),
            );
        }
        waiters
    }

    #[must_use]
    #[cfg(any(test, feature = "benchkit"))]
    pub(crate) fn acquire_for_bench(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_session_id: u64,
        owner_id: &str,
        ttl_secs: u64,
        route_family: RouteFamily,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        let request = LeaseAcquireRequest {
            key: key.clone(),
            owner_session_id,
            owner_id: owner_id.to_owned(),
            ttl_secs,
            wait_seconds: 0,
            reply_source: Self::internal_route_address(),
            reply_destination: None,
            channel: crate::runtime::ClientChannel::Lease,
            route_family,
        };
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                route_family,
                FamilyActorLane::Normal,
                LeaseDomainCommand::ApplyAcquireForBench(request, reply_tx),
            )
            .is_err()
        {
            return crate::domains::lease::protocol::LeaseResponse::Error(
                "Lease benchmark actor unavailable".to_string(),
            );
        }
        reply_rx
            .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|_| {
                crate::domains::lease::protocol::LeaseResponse::Error(
                    "Lease benchmark actor response timed out".to_string(),
                )
            })
    }

    #[must_use]
    #[cfg(any(test, feature = "benchkit"))]
    pub(crate) fn release_for_bench(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_id: &str,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .enqueue(
                key.family,
                FamilyActorLane::Normal,
                LeaseDomainCommand::ApplyReleaseForBench(
                    key.clone(),
                    owner_id.to_string(),
                    fencing_token,
                    reply_tx,
                ),
            )
            .is_err()
        {
            return crate::domains::lease::protocol::LeaseResponse::Error(
                "Lease benchmark actor unavailable".to_string(),
            );
        }
        reply_rx
            .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|_| {
                crate::domains::lease::protocol::LeaseResponse::Error(
                    "Lease benchmark actor response timed out".to_string(),
                )
            })
    }

    #[cfg(any(test, feature = "benchkit"))]
    fn internal_route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/lease"))
    }
}
