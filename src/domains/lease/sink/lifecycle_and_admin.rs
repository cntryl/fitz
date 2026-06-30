use super::model::{
    Arc, AtomicBool, AtomicU64, HashMap, Instant, LeaseDomainSink, LeaseMetrics, Mutex, Ordering,
    SinkLeaseState, Utc, VecDeque,
};
use crate::runtime::Router;

impl LeaseDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            leases: Mutex::new(HashMap::new()),
            session_leases: Mutex::new(HashMap::new()),
            pending_acquires: Mutex::new(HashMap::new()),
            session_waiters: Mutex::new(HashMap::new()),
            next_token: AtomicU64::new(1),
            router,
            active: AtomicBool::new(true),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            admin_read_model,
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(LeaseMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

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

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
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
        self.admin_read_model
            .upsert_lease(Self::lease_info_from_state(key, state));
        self.refresh_metrics_gauges();
    }

    pub(super) fn remove_admin_lease(&self, key: &crate::domains::lease::protocol::LeaseKey) {
        self.admin_read_model.remove_lease(
            key.family.as_u64(),
            &key.realm,
            &key.area,
            &key.resource,
        );
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        let lease_count = self.lease_count();
        let waiter_count = self.waiter_count();

        if let Some(metrics) = &self.metrics {
            metrics.set_active_leases(lease_count);
            metrics.set_waiter_depth(waiter_count);
        } else {
            crate::observability::gauge_set("fitz_lease_active_gauge", lease_count as u64);
            crate::observability::gauge_set("fitz_lease_waiter_depth", waiter_count as u64);
        }
    }

    pub(super) fn counter_inc(&self, name: &str) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(super) fn waiter_count(&self) -> usize {
        self.pending_acquires
            .lock()
            .values()
            .map(VecDeque::len)
            .sum()
    }

    pub fn admin_waiters(&self) -> Vec<crate::control::admin::LeaseWaiterInfo> {
        let now = Instant::now();
        let mut waiters = Vec::new();

        for (key, queue) in self.pending_acquires.lock().iter() {
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
        )
    }
}
