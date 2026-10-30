//! Publish fan-out: matching subscribers to a published route and handing
//! delivery off to the per-route-family delivery workers.

use super::delivery_worker::{flush_notice_delivery_targets, push_notice_delivery_target};
use super::{
    notice_delivery_worker, NoticeDeliveryJob, NoticeDeliveryTarget, NoticeDeliveryTargets,
    NoticeFamilyState, NoticeMatchedRoutePatterns,
};
use std::sync::Arc;
use std::time::Instant;

fn enqueue_notice_delivery_job(
    worker: &crossbeam_channel::Sender<NoticeDeliveryJob>,
    job: NoticeDeliveryJob,
) -> usize {
    match worker.try_send(job) {
        Ok(()) => 0,
        Err(
            crossbeam_channel::TrySendError::Full(job)
            | crossbeam_channel::TrySendError::Disconnected(job),
        ) => job.target_count(),
    }
}

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
        let mut dropped_deliveries = 0_usize;
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
                    dropped_deliveries =
                        dropped_deliveries.saturating_add(enqueue_notice_delivery_job(worker, job));
                } else {
                    dropped_deliveries = dropped_deliveries.saturating_add(batch.len());
                }
            }
            matching_routes.insert(Arc::clone(&subscription.pattern_route));
        });
        if let Some(batch) = flush_notice_delivery_targets(&mut targets) {
            if let Some(worker) = delivery_worker.as_ref() {
                let job = NoticeDeliveryJob::new(batch, route.clone(), payload.clone());
                dropped_deliveries =
                    dropped_deliveries.saturating_add(enqueue_notice_delivery_job(worker, job));
            } else {
                dropped_deliveries = dropped_deliveries.saturating_add(batch.len());
            }
        }
        if dropped_deliveries > 0 {
            super::delivery_worker::record_delivery_drops(metrics, dropped_deliveries);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::subscription_state::RoutedSubscriptionSet;
    use crate::runtime::routing::{RouteAddress, RouteFamily};

    #[test]
    fn should_count_each_dropped_notice_delivery_when_a_batch_is_rejected() {
        // Arrange
        let family = RouteFamily::new(1);
        let route = crate::runtime::routing::Route::new("notice://acme/app/events");
        let collector = crate::observability::metrics::MetricsCollector::new();
        let config = super::super::NoticeDomainConfig {
            next_sub_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            router: Arc::new(crate::runtime::Router::new()),
            admin_read_model: crate::control::admin::read_model::AdminReadModel::new(),
            metrics: Some(crate::domains::notice::metrics::NoticeMetrics::new(
                collector.clone(),
            )),
            active: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        let mut state = NoticeFamilyState::new(family, &config);
        let pattern = crate::runtime::DomainKind::Notice
            .descriptor()
            .compile_registration_pattern(route.as_str())
            .expect("valid exact Notice pattern");
        let mut subscriptions = RoutedSubscriptionSet::new();
        for session_id in 1..=130 {
            subscriptions.insert(
                family,
                super::super::NoticeSubscription {
                    pattern: pattern.clone(),
                    pattern_route: Arc::from(route.as_str()),
                    session_id,
                    subscription_id: session_id,
                    subscriber: RouteAddress::new(
                        family,
                        crate::runtime::routing::Route::new(format!(
                            "inbox://session/{session_id}"
                        )),
                    ),
                },
            );
        }
        state.families.insert(family, subscriptions);
        let (worker, receiver) = crossbeam_channel::bounded(2);
        let mut occupied_targets = NoticeDeliveryTargets::new();
        occupied_targets.push(NoticeDeliveryTarget {
            session_id: 99,
            subscription_id: 99,
            subscriber: RouteAddress::new(
                family,
                crate::runtime::routing::Route::new("inbox://session/99"),
            ),
        });
        worker
            .send(NoticeDeliveryJob::new(
                occupied_targets,
                route.clone(),
                bytes::Bytes::new(),
            ))
            .expect("occupy bounded worker queue");
        state.delivery_workers.insert(family, worker);

        // Act
        state.publish_route_payload(family, &route, &bytes::Bytes::from_static(b"event"));

        // Assert
        let _occupying_job = receiver.try_recv().expect("preloaded worker job");
        let accepted_job = receiver.try_recv().expect("first fanout batch accepted");
        assert_eq!(accepted_job.target_count(), 64);
        assert!(
            receiver.try_recv().is_err(),
            "later fanout batches were rejected"
        );
        assert_eq!(
            collector.counter_get(crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL),
            66,
            "the rejected batches contain 64 and 2 lost subscriber deliveries"
        );
    }
}
