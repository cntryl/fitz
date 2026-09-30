//! Publish fan-out: matching subscribers to a published route and handing
//! delivery off to the per-route-family delivery workers.

use super::delivery_worker::{flush_notice_delivery_targets, push_notice_delivery_target};
use super::{
    notice_delivery_worker, NoticeDeliveryJob, NoticeDeliveryTarget, NoticeDeliveryTargets,
    NoticeFamilyState, NoticeMatchedRoutePatterns,
};
use std::sync::Arc;
use std::time::Instant;

impl NoticeFamilyState {
    fn record_route_publishes(
        &mut self,
        route_family: crate::runtime::routing::RouteFamily,
        routes: impl IntoIterator<Item = Arc<str>>,
    ) {
        let now = Instant::now();
        let route_stats = &mut self.route_stats;
        for route in routes {
            route_stats
                .entry((route_family, route))
                .or_insert_with(super::NoticeRouteStats::new)
                .record_publish(now);
        }
    }

    pub(super) fn publish_route_payload(
        &mut self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        let Some(state) = self.families.get(&family_id) else {
            return;
        };

        let mut targets = NoticeDeliveryTargets::new();
        let mut matching_routes =
            NoticeMatchedRoutePatterns::with_capacity_and_hasher(8, rustc_hash::FxBuildHasher);
        let mut delivery_worker: Option<crossbeam_channel::Sender<NoticeDeliveryJob>> = None;
        let mut delivery_worker_initialized = false;
        let mut enqueue_failed = false;
        let (delivery_workers, router, metrics) = (
            &mut self.delivery_workers,
            &self.router,
            self.metrics.as_ref(),
        );
        state.for_each_matching_route(family_id, route.as_str(), |subscription| {
            if !delivery_worker_initialized {
                delivery_worker_initialized = true;
                delivery_worker =
                    notice_delivery_worker(delivery_workers, router, family_id, metrics);
            }
            if let Some(batch) =
                push_notice_delivery_target(&mut targets, NoticeDeliveryTarget::from(subscription))
            {
                if let Some(worker) = delivery_worker.as_ref() {
                    let job = NoticeDeliveryJob::new(batch, route.clone(), payload.clone());
                    enqueue_failed |= worker.try_send(job).is_err();
                } else {
                    enqueue_failed = true;
                }
            }
            matching_routes.insert(Arc::clone(&subscription.pattern_route));
        });
        if let Some(batch) = flush_notice_delivery_targets(&mut targets) {
            if let Some(worker) = delivery_worker.as_ref() {
                let job = NoticeDeliveryJob::new(batch, route.clone(), payload.clone());
                enqueue_failed |= worker.try_send(job).is_err();
            } else {
                enqueue_failed = true;
            }
        }
        if enqueue_failed {
            super::delivery_worker::record_delivery_drop(metrics);
        }
        self.record_route_publishes(family_id, matching_routes);
        if delivery_worker_initialized {
            self.mark_admin_snapshot_dirty();
        }
    }

    fn publish_event(&mut self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_route_payload(event.family_id, &event.route, &event.payload);
    }

    pub(super) fn handle_domain_publish(&mut self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_event(event);
    }
}
