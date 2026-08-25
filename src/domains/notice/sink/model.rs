use crate::domains::subscription_state::RoutedSubscription;
use smallvec::SmallVec;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) type NoticeDeliveryTargets = SmallVec<[NoticeDeliveryTarget; 8]>;
pub(super) type NoticeMatchedRoutePatterns = SmallVec<[Arc<str>; 8]>;
pub(super) type NoticeRouteStatsKey = (crate::runtime::routing::RouteFamily, Arc<str>);

/// Delivery-only projection of `NoticeSubscription`; matching state stays in the index.
#[derive(Clone)]
pub(super) struct NoticeDeliveryTarget {
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: crate::runtime::routing::RouteAddress,
}

pub(super) struct NoticeRouteStats {
    publishes_total: u64,
    recent_publishes: VecDeque<Instant>,
}

impl NoticeRouteStats {
    pub(super) fn new() -> Self {
        Self {
            publishes_total: 0,
            recent_publishes: VecDeque::new(),
        }
    }

    pub(super) fn record_publish(&mut self, now: Instant) {
        self.prune_recent_publishes(now);
        self.publishes_total = self.publishes_total.saturating_add(1);
        self.recent_publishes.push_back(now);
    }

    pub(super) fn publishes_total(&self) -> u64 {
        self.publishes_total
    }

    pub(super) fn publishes_per_minute(&mut self, now: Instant) -> f64 {
        self.prune_recent_publishes(now);
        usize_to_f64(self.recent_publishes.len())
    }

    pub(super) fn prune_recent_publishes(&mut self, now: Instant) {
        while let Some(oldest) = self.recent_publishes.front().copied() {
            if now.saturating_duration_since(oldest) <= Duration::from_mins(1) {
                break;
            }
            self.recent_publishes.pop_front();
        }
    }
}

pub(super) struct NoticeSubscription {
    pub(super) pattern: crate::runtime::matcher::Pattern,
    pub(super) pattern_route: Arc<str>,
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for NoticeSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

impl From<&NoticeSubscription> for NoticeDeliveryTarget {
    fn from(subscription: &NoticeSubscription) -> Self {
        Self {
            session_id: subscription.session_id,
            subscription_id: subscription.subscription_id,
            subscriber: subscription.subscriber.clone(),
        }
    }
}

pub(super) fn notice_route_realm(route: &str) -> Option<&str> {
    let path = route.split_once("://").map_or(route, |(_, path)| path);
    path.trim_start_matches('/')
        .split('/')
        .find(|segment| !segment.is_empty())
}

fn usize_to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Record the outcome of waiting for a delivery worker to confirm a handoff.
///
/// A timeout is not a drop: the worker may still complete the delivery. It is
/// counted separately so an uncertain outcome is visible rather than silently
/// discarded, which is how a saturated worker could look identical to a
/// healthy one.
pub(super) fn record_delivery_handoff_outcome(
    outcome: Result<(), crossbeam_channel::RecvTimeoutError>,
) {
    if outcome.is_err() {
        crate::observability::counter_inc(
            crate::domains::notice::metrics::METRIC_DELIVERY_HANDOFF_TIMEOUTS_TOTAL,
        );
    }
}

#[cfg(test)]
mod delivery_handoff_outcome_tests {
    use super::record_delivery_handoff_outcome;
    use crate::domains::notice::metrics::METRIC_DELIVERY_HANDOFF_TIMEOUTS_TOTAL;

    #[test]
    fn should_count_an_unconfirmed_delivery_handoff() {
        // Arrange
        let metrics = crate::observability::metrics();
        let before = metrics.counter_get(METRIC_DELIVERY_HANDOFF_TIMEOUTS_TOTAL);

        // Act
        record_delivery_handoff_outcome(Err(crossbeam_channel::RecvTimeoutError::Timeout));

        // Assert
        assert!(
            metrics.counter_get(METRIC_DELIVERY_HANDOFF_TIMEOUTS_TOTAL) > before,
            "an unconfirmed handoff must be observable"
        );
    }

    #[test]
    fn should_not_count_a_confirmed_delivery_handoff() {
        // Arrange
        let metrics = crate::observability::metrics();
        let before = metrics.counter_get(METRIC_DELIVERY_HANDOFF_TIMEOUTS_TOTAL);

        // Act
        record_delivery_handoff_outcome(Ok(()));

        // Assert
        assert_eq!(
            metrics.counter_get(METRIC_DELIVERY_HANDOFF_TIMEOUTS_TOTAL),
            before
        );
    }
}
