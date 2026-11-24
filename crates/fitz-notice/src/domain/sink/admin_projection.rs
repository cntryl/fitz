//! Immutable family observations projected into the admin read model.

use super::{notice_route_realm, NoticeDomainConfig, NoticeFamilyState};
use chrono::Utc;
use std::collections::HashMap;
use std::time::Instant;

impl NoticeFamilyState {
    pub(super) fn mark_admin_snapshot_dirty(&mut self) {
        self.publish_observation();
    }

    fn publish_observation(&mut self) {
        let now = Instant::now();
        let mut subscriptions = Vec::new();
        let mut subscriber_counts = HashMap::new();
        for (family, state) in &self.families {
            for subscription in state.values() {
                let pattern = subscription.pattern.route().to_string();
                if let Some(realm) = notice_route_realm(&pattern) {
                    subscriptions.push((
                        *family,
                        subscription.subscription_id,
                        subscription.session_id,
                        realm.to_owned(),
                        pattern.clone(),
                    ));
                    *subscriber_counts
                        .entry((*family, subscription.pattern_route.clone()))
                        .or_insert(0_usize) += 1;
                }
            }
        }
        self.route_stats
            .retain(|route, _| subscriber_counts.contains_key(route));
        let routes = subscriber_counts
            .into_iter()
            .map(|((family, route), subscribers)| {
                let (publishes_total, publishes_per_minute) = self
                    .route_stats
                    .get_mut(&(family, route.clone()))
                    .map_or((0, 0.0), |stats| {
                        stats.prune_recent_publishes(now);
                        (stats.publishes_total(), stats.publishes_per_minute(now))
                    });
                (
                    family,
                    route.to_string(),
                    subscribers,
                    publishes_total,
                    publishes_per_minute,
                )
            })
            .collect::<Vec<_>>();
        let created_at = Utc::now().to_rfc3339();
        let subscription_count = subscriptions.len();
        self.admin_read_model.replace_notice_family_subscriptions(
            self.family.as_u64(),
            subscriptions
                .into_iter()
                .map(|(family, subscription_id, session_id, realm, pattern)| {
                    crate::control::admin::NoticeSubscription::snapshot(
                        family.as_u64(),
                        subscription_id,
                        session_id,
                        &realm,
                        pattern,
                        &created_at,
                    )
                })
                .collect(),
        );
        self.admin_read_model.replace_notice_family_routes(
            self.family.as_u64(),
            routes
                .into_iter()
                .map(
                    |(family, route, subscribers, publishes_total, publishes_per_minute)| {
                        let mut entry = crate::control::admin::NoticeRouteInfo::snapshot(
                            family.as_u64(),
                            route,
                            subscribers,
                        );
                        entry.publishes_total = publishes_total;
                        entry.publishes_per_minute = publishes_per_minute;
                        entry
                    },
                )
                .collect(),
        );
        if let Some(metrics) = &self.metrics {
            let total = self.admin_read_model.notice_subscriptions(None, None).len();
            debug_assert!(total >= subscription_count);
            metrics.set_subscription_count(total);
        }
    }
}

impl NoticeDomainConfig {
    pub(super) fn refresh_admin_snapshot_if_dirty() {
        // Family workers publish immutable snapshots as part of each mutation.
    }
}
