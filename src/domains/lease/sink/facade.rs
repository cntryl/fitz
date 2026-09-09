//! `LeaseDomain` family-runtime composition and synchronous facade.

#[cfg(any(test, feature = "benchkit"))]
use super::model::LeaseAcquireRequest;
use super::model::{
    LeaseDomain, LeaseDomainCommand, LeaseDomainConfig, LeaseFamilyRuntime, LeaseFamilyState,
    LeaseListSnapshotCoordinator, LeaseLiveCounts, LEASE_ACTOR_REPLY_TIMEOUT,
};
use crate::domains::lease::LeaseMetrics;
use crate::runtime::routing::RouteFamily;
#[cfg(any(test, feature = "benchkit"))]
use crate::runtime::routing::{Route, RouteAddress};
use crate::runtime::{DeliveryError, FamilyActorLane, Router};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

impl LeaseDomainConfig {
    fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            next_token: Arc::new(AtomicU64::new(1)),
            router,
            next_sub_id: Arc::new(AtomicU64::new(1)),
            holder_incarnation_hasher: Arc::new(std::collections::hash_map::RandomState::new()),
            list_snapshots: Arc::new(LeaseListSnapshotCoordinator::new()),
            next_list_snapshot_id: Arc::new(AtomicU64::new(Self::random_list_snapshot_seed())),
            admin_read_model,
            metrics: None,
        }
    }

    fn random_list_snapshot_seed() -> u64 {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes).expect("OS randomness required for Lease cursor identity");
        u64::from_ne_bytes(bytes) & (u64::MAX >> 1)
    }
}

impl LeaseFamilyState {
    fn new(config: &LeaseDomainConfig, route_family: RouteFamily) -> Self {
        Self {
            route_family,
            leases: BTreeMap::new(),
            session_leases: HashMap::new(),
            pending_acquires: HashMap::new(),
            session_waiters: HashMap::new(),
            cleaned_up_sessions: crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            ),
            next_token: config.next_token.clone(),
            router: config.router.clone(),
            families: HashMap::new(),
            next_sub_id: config.next_sub_id.clone(),
            holder_incarnation_hasher: config.holder_incarnation_hasher.clone(),
            list_snapshots: config.list_snapshots.clone(),
            next_list_snapshot_id: config.next_list_snapshot_id.clone(),
            admin_read_model: config.admin_read_model.clone(),
            metrics: config.metrics.clone(),
        }
    }
}

impl LeaseDomain {
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
        let config = LeaseDomainConfig::new(router, admin_read_model);
        let active = Arc::new(AtomicBool::new(true));
        let families = route_families.to_vec();
        let family_runtime = Self::spawn_family_runtime(&config, active.clone(), &families);
        Self {
            config,
            active,
            family_runtime,
            route_families: families,
        }
    }

    fn spawn_family_runtime(
        config: &LeaseDomainConfig,
        active: Arc<AtomicBool>,
        families: &[RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<LeaseDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .expect("validated Lease family actor pool configuration");
        let family_config = config.clone();
        crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
            pool,
            active.clone(),
            move |family| LeaseFamilyState::new(&family_config, family),
            move |state, _family, _lane, command| {
                LeaseFamilyRuntime {
                    core: state,
                    active: active.as_ref(),
                }
                .receive(command);
            },
            crate::domains::lease::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
        )
    }

    fn rebuild_family_runtime(&mut self) {
        self.family_runtime.stop();
        self.family_runtime =
            Self::spawn_family_runtime(&self.config, self.active.clone(), &self.route_families);
    }

    pub(super) fn enqueue(
        &self,
        family: RouteFamily,
        lane: FamilyActorLane,
        command: LeaseDomainCommand,
    ) -> Result<(), DeliveryError> {
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(crate::runtime::family_actor_enqueue_error_to_delivery_error)
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.family_runtime.stop();
        self.config.metrics = Some(LeaseMetrics::new(collector));
        self.rebuild_family_runtime();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn is_family_running(&self, family: RouteFamily) -> bool {
        self.family_runtime.is_family_running(family)
    }

    pub(crate) fn family_health_snapshot(
        &self,
    ) -> crate::runtime::family_actor_pool::FamilyActorPoolHealthSnapshot {
        self.family_runtime.health_snapshot()
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
    #[cfg(test)]
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
        if let Some(metrics) = self.config.metrics.as_ref() {
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

    #[cfg(test)]
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
