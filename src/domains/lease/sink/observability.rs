//! Admin read-model projection and metrics glue.

use super::model::{Instant, LeaseDomainRuntime, LeaseLiveCounts, SinkLeaseState, Utc};
use std::collections::VecDeque;

#[derive(Clone, Copy)]
pub(super) enum DeliveryDropKind {
    Response,
    Notification,
}

impl DeliveryDropKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Response => "response",
            Self::Notification => "notification",
        }
    }
}

impl LeaseDomainRuntime<'_> {
    pub(super) fn record_dropped_delivery(
        &self,
        kind: DeliveryDropKind,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        error: &impl std::fmt::Display,
    ) {
        match (self.core.metrics.as_ref(), kind) {
            (Some(metrics), DeliveryDropKind::Response) => metrics.record_response_drop(),
            (Some(metrics), DeliveryDropKind::Notification) => metrics.record_notify_drop(),
            (None, DeliveryDropKind::Response) => crate::observability::counter_inc(
                crate::domains::lease::metrics::METRIC_RESPONSE_DROPS_TOTAL,
            ),
            (None, DeliveryDropKind::Notification) => crate::observability::counter_inc(
                crate::domains::lease::metrics::METRIC_NOTIFY_DROPS_TOTAL,
            ),
        }
        tracing::warn!(
            domain = "lease",
            delivery_kind = kind.label(),
            session_id,
            route_family = route_family.as_u64(),
            error = %error,
            "Dropped best-effort Lease delivery"
        );
    }

    #[cfg(test)]
    pub(in crate::domains::lease::sink) fn session_inbox_address(
        route_family: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        let route = format!("inbox://session/{session_id}");
        crate::runtime::routing::RouteAddress::new(
            route_family,
            crate::runtime::routing::Route::new(route.as_str()),
        )
    }

    pub(in crate::domains::lease::sink) fn lease_info_from_state(
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

    pub(in crate::domains::lease::sink) fn upsert_admin_lease(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
        state: &SinkLeaseState,
    ) {
        self.core
            .admin_read_model
            .upsert_lease(Self::lease_info_from_state(key, state));
    }

    pub(in crate::domains::lease::sink) fn remove_admin_lease(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        self.core.admin_read_model.remove_lease(
            key.family.as_u64(),
            &key.realm,
            &key.area,
            &key.resource,
        );
    }

    pub(in crate::domains::lease::sink) fn refresh_metrics_gauges(&self) {
        let lease_count = self.lease_count();
        let waiter_count = self.waiter_count();

        if let Some(metrics) = &self.core.metrics {
            metrics.set_active_leases(lease_count);
            metrics.set_waiter_depth(waiter_count);
        } else {
            crate::observability::gauge_set(
                crate::domains::lease::metrics::METRIC_ACTIVE_GAUGE,
                lease_count as u64,
            );
            crate::observability::gauge_set(
                crate::domains::lease::metrics::METRIC_WAITERS_GAUGE,
                waiter_count as u64,
            );
        }
    }

    pub(in crate::domains::lease::sink) fn counter_inc(&self, name: &str) {
        if let Some(metrics) = &self.core.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(in crate::domains::lease::sink) fn waiter_count(&self) -> usize {
        self.core
            .pending_acquires
            .lock()
            .values()
            .map(VecDeque::len)
            .sum()
    }

    pub(in crate::domains::lease::sink) fn live_counts(&self) -> LeaseLiveCounts {
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
                    session_id: waiter.owner_session_id.to_string(),
                    queued_token: waiter.queued_token,
                    expires_at,
                });
            }
        }

        waiters
    }

    pub(in crate::domains::lease::sink) fn lease_response_is_failure(
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
                | crate::domains::lease::protocol::LeaseResponse::InvalidSubscriptionRoute(_)
                | crate::domains::lease::protocol::LeaseResponse::InvalidListPattern(_)
                | crate::domains::lease::protocol::LeaseResponse::InvalidListCursor
        )
    }

    pub(super) fn lease_count(&self) -> usize {
        self.core.leases.lock().len()
    }

    pub(super) fn subscription_count(&self) -> usize {
        let families = self.core.families.lock();
        families
            .values()
            .map(crate::domains::subscription_state::RoutedSubscriptionSet::subscription_count)
            .sum()
    }
}
