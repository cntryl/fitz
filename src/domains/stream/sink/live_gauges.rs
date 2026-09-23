//! Shared publication of process-wide Stream live gauges.
//!
//! Family actors own their counts; this coordinator only aggregates their
//! latest observations. The publication lock prevents an older aggregate
//! from overwriting a newer one when distinct families update concurrently.

use super::model::StreamLiveCounts;
use crate::domains::stream::metrics::{
    METRIC_ACTIVE_GAUGE, METRIC_APPEND_SESSIONS_GAUGE, METRIC_SUBSCRIPTIONS_GAUGE,
};
use crate::domains::stream::StreamMetrics;
use crate::runtime::routing::RouteFamily;
use parking_lot::Mutex;
use std::collections::BTreeMap;

pub(super) struct StreamLiveGaugeCoordinator {
    families: Mutex<BTreeMap<u64, StreamLiveCounts>>,
    metrics: Option<StreamMetrics>,
}

impl StreamLiveGaugeCoordinator {
    pub(super) fn new(metrics: Option<StreamMetrics>) -> Self {
        Self {
            families: Mutex::new(BTreeMap::new()),
            metrics,
        }
    }

    pub(super) fn publish_family(&self, family: RouteFamily, counts: StreamLiveCounts) {
        let mut families = self.families.lock();
        families.insert(family.as_u64(), counts);
        self.publish_aggregate(&families);
    }

    pub(super) fn clear_family(&self, family: RouteFamily) {
        let mut families = self.families.lock();
        families.remove(&family.as_u64());
        self.publish_aggregate(&families);
    }

    fn publish_aggregate(&self, families: &BTreeMap<u64, StreamLiveCounts>) {
        let aggregate = families
            .values()
            .fold(StreamLiveCounts::default(), |mut total, counts| {
                total.streams = total.streams.saturating_add(counts.streams);
                total.append_sessions =
                    total.append_sessions.saturating_add(counts.append_sessions);
                total.subscriptions = total.subscriptions.saturating_add(counts.subscriptions);
                total
            });

        if let Some(metrics) = &self.metrics {
            metrics.set_stream_count(aggregate.streams);
            metrics.set_subscription_count(aggregate.subscriptions);
            metrics.set_append_session_count(aggregate.append_sessions);
        } else {
            crate::observability::gauge_set(METRIC_ACTIVE_GAUGE, aggregate.streams as u64);
            crate::observability::gauge_set(
                METRIC_SUBSCRIPTIONS_GAUGE,
                aggregate.subscriptions as u64,
            );
            crate::observability::gauge_set(
                METRIC_APPEND_SESSIONS_GAUGE,
                aggregate.append_sessions as u64,
            );
        }
    }
}
