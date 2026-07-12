use super::model::{
    Arc, AtomicBool, AtomicU64, HashMap, Instant, LeaseAcquireRequest, LeaseDomainActor,
    LeaseDomainCommand, LeaseDomainCore, LeaseDomainRuntime, LeaseDomainSink, LeaseDomainState,
    LeaseLiveCounts, LeaseMetrics, Mutex, Ordering, SinkLeaseState, Utc, VecDeque,
};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::Router;

impl LeaseDomainState {
    fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            core: LeaseDomainCore {
                leases: Mutex::new(HashMap::new()),
                session_leases: Mutex::new(HashMap::new()),
                pending_acquires: Mutex::new(HashMap::new()),
                session_waiters: Mutex::new(HashMap::new()),
                next_token: AtomicU64::new(1),
                router,
                families: Mutex::new(HashMap::new()),
                next_sub_id: AtomicU64::new(1),
                admin_read_model,
                metrics: None,
            },
            active: AtomicBool::new(true),
        }
    }

    pub(super) fn runtime(&self) -> LeaseDomainRuntime<'_> {
        LeaseDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl LeaseDomainActor {
    pub(super) fn new(state: Arc<LeaseDomainState>) -> Self {
        Self { state }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/lease"))
    }
}

impl LeaseDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let state = Arc::new(LeaseDomainState::new(router, admin_read_model));
        let actor = Self::spawn_actor(state.clone());
        Self { state, actor }
    }

    fn spawn_actor(
        state: Arc<LeaseDomainState>,
    ) -> crate::runtime::ManagedActor<LeaseDomainCommand> {
        let router = state.core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            LeaseDomainActor::route_address(),
            move || LeaseDomainActor::new(state.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.state.clone());
    }

    fn state_for_builder(&mut self) -> &mut LeaseDomainState {
        Arc::get_mut(&mut self.state).expect("Lease sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        let state = self.state_for_builder();
        state.core.metrics = Some(LeaseMetrics::new(collector));
        state.runtime().refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.state.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(LeaseDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    pub fn cleanup_session(&self, session_id: u64) {
        if let Err(error) = self
            .actor
            .try_send_high_priority(LeaseDomainCommand::CleanupSession(session_id))
        {
            tracing::warn!(domain = "lease", error = %error, "Lease cleanup enqueue failed");
        }
    }

    pub(crate) fn sweep_expired_state(&self) {
        if let Err(error) = self
            .actor
            .try_send_high_priority(LeaseDomainCommand::SweepExpiredState)
        {
            tracing::warn!(domain = "lease", error = %error, "Lease sweep enqueue failed");
        }
    }

    pub fn lease_count(&self) -> usize {
        self.live_counts().leases
    }

    pub fn subscription_count(&self) -> usize {
        self.live_counts().subscriptions
    }

    fn live_counts(&self) -> LeaseLiveCounts {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(LeaseDomainCommand::ReadLiveCounts(reply_tx))
        {
            tracing::warn!(domain = "lease", error = %error, "Lease live-count query enqueue failed");
            return LeaseLiveCounts::default();
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_default()
    }

    pub fn admin_waiters(&self) -> Vec<crate::control::admin::LeaseWaiterInfo> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(LeaseDomainCommand::ReadWaiters(reply_tx))
        {
            tracing::warn!(domain = "lease", error = %error, "Lease waiter read enqueue failed");
            return Vec::new();
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_default()
    }

    #[must_use]
    pub(crate) fn acquire_direct_for_bench(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_session_id: u64,
        owner_id: &str,
        ttl_secs: u64,
        route_family: RouteFamily,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        self.state.runtime().handle_acquire(LeaseAcquireRequest {
            key: key.clone(),
            owner_session_id,
            owner_id: owner_id.to_owned(),
            ttl_secs,
            wait_seconds: 0,
            reply_source: LeaseDomainActor::route_address(),
            reply_destination: None,
            channel: crate::runtime::ClientChannel::Lease,
            route_family,
        })
    }

    #[must_use]
    pub(crate) fn release_direct_for_bench(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        owner_id: &str,
        fencing_token: u64,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        self.state
            .runtime()
            .handle_release(key, owner_id, fencing_token)
    }
}

impl LeaseDomainRuntime<'_> {
    #[cfg(test)]
    pub(super) fn session_inbox_address(
        route_family: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        let route = format!("inbox://session/{session_id}");
        crate::runtime::routing::RouteAddress::new(
            route_family,
            crate::runtime::routing::Route::new(route.as_str()),
        )
    }

    pub(super) fn lease_info_from_state(
        key: &crate::domains::lease::protocol::LeaseKey,
        state: &SinkLeaseState,
    ) -> crate::control::admin::LeaseInfo {
        let now = std::time::Instant::now();
        let expires_at = Utc::now()
            .checked_add_signed(chrono::TimeDelta::seconds(
                state
                    .expiry
                    .saturating_duration_since(now)
                    .as_secs()
                    .cast_signed(),
            ))
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        crate::control::admin::LeaseInfo::snapshot(
            key.family.as_u64(),
            &key.realm,
            &key.area,
            &key.resource,
            &state.owner_id,
            &state.acquired_at,
            expires_at,
            state.renewals,
            state.fencing_token,
        )
    }

    pub(super) fn upsert_admin_lease(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        state: &SinkLeaseState,
    ) {
        self.core
            .admin_read_model
            .upsert_lease(Self::lease_info_from_state(key, state));
    }

    pub(super) fn remove_admin_lease(&self, key: &crate::domains::lease::protocol::LeaseKey) {
        self.core.admin_read_model.remove_lease(
            key.family.as_u64(),
            &key.realm,
            &key.area,
            &key.resource,
        );
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        let lease_count = self.lease_count();
        let waiter_count = self.waiter_count();

        if let Some(metrics) = &self.core.metrics {
            metrics.set_active_leases(lease_count);
            metrics.set_waiter_depth(waiter_count);
        } else {
            crate::observability::gauge_set("fitz_lease_active_gauge", lease_count as u64);
            crate::observability::gauge_set("fitz_lease_waiter_depth", waiter_count as u64);
        }
    }

    pub(super) fn counter_inc(&self, name: &str) {
        if let Some(metrics) = &self.core.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(super) fn waiter_count(&self) -> usize {
        self.core
            .pending_acquires
            .lock()
            .values()
            .map(VecDeque::len)
            .sum()
    }

    pub(super) fn live_counts(&self) -> LeaseLiveCounts {
        LeaseLiveCounts {
            leases: self.lease_count(),
            subscriptions: self.subscription_count(),
        }
    }

    pub fn admin_waiters(&self) -> Vec<crate::control::admin::LeaseWaiterInfo> {
        let now = Instant::now();
        let mut waiters = Vec::new();

        for (key, queue) in self.core.pending_acquires.lock().iter() {
            for waiter in queue {
                let expires_at = Utc::now()
                    .checked_add_signed(chrono::TimeDelta::seconds(
                        waiter
                            .expires_at
                            .saturating_duration_since(now)
                            .as_secs()
                            .cast_signed(),
                    ))
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339();

                waiters.push(crate::control::admin::LeaseWaiterInfo {
                    route_family: key.family.as_u64(),
                    realm: key.realm.clone(),
                    area: key.area.clone(),
                    resource: key.resource.clone(),
                    owner_id: waiter.owner_id.clone(),
                    session_id: waiter.session_id.to_string(),
                    queued_token: waiter.queued_token,
                    expires_at,
                });
            }
        }

        waiters
    }

    pub(super) fn lease_response_is_failure(
        response: &crate::domains::lease::protocol::LeaseResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::lease::protocol::LeaseResponse::Timeout
                | crate::domains::lease::protocol::LeaseResponse::HeldByOther { .. }
                | crate::domains::lease::protocol::LeaseResponse::AlreadyQueued { .. }
                | crate::domains::lease::protocol::LeaseResponse::QueueFull { .. }
                | crate::domains::lease::protocol::LeaseResponse::NotHeld
                | crate::domains::lease::protocol::LeaseResponse::Expired
                | crate::domains::lease::protocol::LeaseResponse::Fenced { .. }
                | crate::domains::lease::protocol::LeaseResponse::NotFound
                | crate::domains::lease::protocol::LeaseResponse::Error(_)
        )
    }
}
