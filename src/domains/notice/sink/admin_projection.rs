//! Admin read-model projection: when and how the Notice subscription index
//! is mirrored into the admin snapshot.
//!
//! Projection failure must never affect domain correctness - it is a
//! dirty-flagged, best-effort reflection of live subscription state, not a
//! source of truth.

use super::NoticeDomainCore;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

impl NoticeDomainCore {
    /// Rebuild the admin read model from the current in-memory subscription
    /// state only.
    fn sync_admin_snapshot(&self) {
        let families = self.families.lock();
        let now = Instant::now();
        let created_at = Utc::now().to_rfc3339();
        let mut subscriptions = Vec::new();
        let mut routes: HashMap<super::NoticeRouteStatsKey, usize> = HashMap::new();
        for (route_family, state) in families.iter() {
            for subscription in state.values() {
                let pattern = subscription.pattern.route().to_string();
                if let Some(realm) = super::notice_route_realm(&pattern) {
                    subscriptions.push(crate::control::admin::NoticeSubscription::snapshot(
                        route_family.as_u64(),
                        subscription.subscription_id,
                        subscription.session_id,
                        realm,
                        pattern.clone(),
                        &created_at,
                    ));
                    let subscribers = routes
                        .entry((
                            *route_family,
                            std::sync::Arc::clone(&subscription.pattern_route),
                        ))
                        .or_insert(0);
                    *subscribers = subscribers.saturating_add(1);
                }
            }
        }
        drop(families);
        let mut route_stats = self.route_stats.lock();
        route_stats.retain(|route, stats| {
            let keep = routes.contains_key(route);
            if keep {
                stats.prune_recent_publishes(now);
            }
            keep
        });
        self.admin_read_model
            .replace_notice_subscriptions(subscriptions);
        self.admin_read_model.replace_notice_routes(
            routes
                .into_iter()
                .map(|((route_family, route), subscribers)| {
                    let (publishes_total, publishes_per_minute) = route_stats
                        .get_mut(&(route_family, std::sync::Arc::clone(&route)))
                        .map_or((0, 0.0), |stats| {
                            (stats.publishes_total(), stats.publishes_per_minute(now))
                        });
                    let mut entry = crate::control::admin::NoticeRouteInfo::snapshot(
                        route_family.as_u64(),
                        route.to_string(),
                        subscribers,
                    );
                    entry.publishes_total = publishes_total;
                    entry.publishes_per_minute = publishes_per_minute;
                    entry
                })
                .collect(),
        );
        if let Some(metrics) = &self.metrics {
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    pub(super) fn mark_admin_snapshot_dirty(&self) {
        self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }
}
