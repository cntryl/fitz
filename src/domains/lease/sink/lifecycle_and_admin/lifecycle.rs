#[cfg(any(test, feature = "benchkit"))]
use super::super::model::LeaseAcquireRequest;
use super::super::model::{
    Arc, AtomicBool, AtomicU64, HashMap, LeaseDomainActor, LeaseDomainCommand, LeaseDomainCore,
    LeaseDomainRuntime, LeaseDomainSink, LeaseDomainState, LeaseLiveCounts, LeaseMetrics, Mutex,
    Ordering, LEASE_ACTOR_REPLY_TIMEOUT,
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

    pub(in crate::domains::lease::sink) fn runtime(&self) -> LeaseDomainRuntime<'_> {
        LeaseDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl LeaseDomainActor {
    pub(in crate::domains::lease::sink) fn new(state: Arc<LeaseDomainState>) -> Self {
        Self { state }
    }

    pub(in crate::domains::lease::sink) fn route_address() -> RouteAddress {
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
    pub(in crate::domains::lease::sink) fn is_actor_running(&self) -> bool {
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
    pub(in crate::domains::lease::sink) fn stop_actor_for_tests(&self) {
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
            if let Some(metrics) = self.state.core.metrics.as_ref() {
                metrics.counter_inc(
                    crate::domains::lease::metrics::METRIC_SWEEP_ENQUEUE_FAILURES_TOTAL,
                );
            } else {
                crate::observability::counter_inc(
                    crate::domains::lease::metrics::METRIC_SWEEP_ENQUEUE_FAILURES_TOTAL,
                );
            }
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
            .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
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
            .recv_timeout(LEASE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_default()
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
            reply_source: LeaseDomainActor::route_address(),
            reply_destination: None,
            channel: crate::runtime::ClientChannel::Lease,
            route_family,
        };
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if self
            .actor
            .try_send(LeaseDomainCommand::ApplyAcquireForBench(request, reply_tx))
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
            .actor
            .try_send(LeaseDomainCommand::ApplyReleaseForBench(
                key.clone(),
                owner_id.to_string(),
                fencing_token,
                reply_tx,
            ))
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
}
