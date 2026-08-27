//! Publish fan-out: matching subscribers to a published route and handing
//! delivery off to the per-route-family delivery workers.

use super::{
    notice_delivery_worker, NoticeDeliveryJob, NoticeDeliveryTarget, NoticeDeliveryTargets,
    NoticeDomainCore, NoticeMatchedRoutePatterns,
};
use std::sync::Arc;
use std::time::Instant;

impl NoticeDomainCore {
    fn fan_out_notice_event(
        &self,
        targets: &NoticeDeliveryTargets,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        for target in targets {
            self.route_notice_notify(target, route, payload);
        }
    }

    fn record_route_publishes(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        routes: &[Arc<str>],
    ) {
        if routes.is_empty() {
            return;
        }

        let now = Instant::now();
        let mut route_stats = self.route_stats.lock();
        for route in routes {
            route_stats
                .entry((route_family, Arc::clone(route)))
                .or_insert_with(super::NoticeRouteStats::new)
                .record_publish(now);
        }
    }

    fn route_notice_notify(
        &self,
        target: &NoticeDeliveryTarget,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        let family = *target.subscriber.family();
        let worker = notice_delivery_worker(&self.delivery_workers, &self.router, family);
        let Some(worker) = worker else {
            crate::observability::counter_inc(
                crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL,
            );
            return;
        };
        let job = NoticeDeliveryJob::new(target.clone(), route.clone(), payload.clone());
        if worker.try_send(job).is_err() {
            crate::observability::counter_inc(
                crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL,
            );
        }
    }

    fn collect_matching_targets_for_route(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &str,
    ) -> NoticeDeliveryTargets {
        let families = self.families.lock();
        let Some(state) = families.get(&family_id) else {
            return NoticeDeliveryTargets::new();
        };

        let mut targets = NoticeDeliveryTargets::with_capacity(state.matching_capacity_hint(route));
        let mut matching_routes = NoticeMatchedRoutePatterns::new();
        state.for_each_matching_route(family_id, route, |subscription| {
            targets.push(NoticeDeliveryTarget::from(subscription));
            let pattern_route = subscription.pattern_route.as_ref();
            if !matching_routes
                .iter()
                .any(|route| route.as_ref() == pattern_route)
            {
                matching_routes.push(Arc::clone(&subscription.pattern_route));
            }
        });
        self.record_route_publishes(family_id, &matching_routes);
        targets
    }

    pub(super) fn publish_route_payload(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        let targets = self.collect_matching_targets_for_route(family_id, route.as_str());
        if targets.is_empty() {
            return;
        }

        self.fan_out_notice_event(&targets, route, payload);
        self.mark_admin_snapshot_dirty();
    }

    fn publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_route_payload(event.family_id, &event.route, &event.payload);
    }

    pub(super) fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_event(event);
    }
}
