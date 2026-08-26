//! KV metrics, latency, and admin-projection updates.

use super::locks::KvResourceLockKey;
use super::state::{KvAdminTransactionUpdate, KvDomainRuntime};
#[cfg(test)]
use chrono::Utc;

impl KvDomainRuntime<'_> {
    pub(super) fn counter_inc(&self, name: &str) {
        if let Some(metrics) = &self.core.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(super) fn record_response_metrics(
        &self,
        response: &crate::domains::kv::KvResponse,
        started_at: std::time::Instant,
    ) {
        self.record_request_metrics(
            matches!(response, crate::domains::kv::KvResponse::Error { .. }),
            started_at,
        );
    }

    pub(super) fn record_request_metrics(&self, failed: bool, started_at: std::time::Instant) {
        if let Some(metrics) = &self.core.metrics {
            if failed {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
            return;
        }

        crate::observability::counter_inc(if failed {
            crate::domains::kv::metrics::METRIC_FAILURE_TOTAL
        } else {
            crate::domains::kv::metrics::METRIC_SUCCESS_TOTAL
        });
        let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        crate::observability::metrics()
            .histogram_observe_ms(crate::domains::kv::metrics::METRIC_LATENCY_MS, elapsed_ms);
    }

    pub(super) fn active_transaction_count(&self) -> usize {
        self.core.projection.active_transaction_count()
    }

    #[cfg(test)]
    pub(super) fn sync_admin_snapshot(&self) {
        let started_at = Utc::now().to_rfc3339();
        let actors: Vec<_> = self
            .core
            .actors
            .lock()
            .iter()
            .map(|(session_id, actor)| (*session_id, actor.clone()))
            .collect();
        let transactions = actors
            .iter()
            .flat_map(|(session_id, actor)| {
                actor
                    .lock()
                    .active_transaction_snapshots()
                    .into_iter()
                    .map(|snapshot| {
                        crate::control::admin::KvTransaction::snapshot(
                            snapshot.scope.route_family.as_u64(),
                            snapshot.tx_id,
                            *session_id,
                            &snapshot.scope.realm,
                            &snapshot.scope.area,
                            &snapshot.scope.resource,
                            &started_at,
                        )
                    })
            })
            .collect();
        self.core.projection.mark_dirty();
        self.core.projection.refresh_if_dirty(|| transactions);
        self.refresh_metrics_gauges();
    }

    pub(super) fn apply_admin_transaction_update(&self, update: KvAdminTransactionUpdate) {
        match update {
            KvAdminTransactionUpdate::None => return,
            KvAdminTransactionUpdate::Upsert(transaction) => {
                self.core.projection.upsert_transaction(transaction);
            }
            KvAdminTransactionUpdate::Remove { session_id, tx_id } => {
                self.core.projection.remove_transaction(session_id, tx_id);
            }
        }
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.core.metrics {
            metrics.set_active_transactions(self.active_transaction_count());
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    pub(super) fn subscription_count(&self) -> usize {
        self.core
            .watch_registries
            .lock()
            .values()
            .map(crate::domains::kv::watch_registry::KvWatchRegistry::subscription_count)
            .sum()
    }

    pub(super) fn active_transactions_for_resource(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> usize {
        self.core.projection.active_transactions_for_resource(
            resource_key.family_id,
            &resource_key.realm,
            &resource_key.area,
            &resource_key.resource,
        )
    }

    pub(super) fn latency_snapshots(
        &self,
        resource_key: &KvResourceLockKey,
    ) -> (
        crate::control::admin::KvLatencySnapshot,
        crate::control::admin::KvLatencySnapshot,
    ) {
        self.core.projection.latency_snapshots(resource_key)
    }

    pub(super) fn record_read_latency(
        &self,
        resource_key: &KvResourceLockKey,
        started_at: std::time::Instant,
    ) {
        self.core
            .projection
            .record_read_latency(resource_key, started_at.elapsed().as_secs_f64() * 1000.0);
    }

    pub(super) fn record_write_latency(
        &self,
        resource_key: &KvResourceLockKey,
        started_at: std::time::Instant,
    ) {
        self.core
            .projection
            .record_write_latency(resource_key, started_at.elapsed().as_secs_f64() * 1000.0);
    }

    #[cfg(test)]
    pub(super) fn session_inbox_address(
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            family_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        )
    }
}
