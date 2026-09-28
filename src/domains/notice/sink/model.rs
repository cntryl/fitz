use crate::domains::subscription_state::RoutedSubscription;
use smallvec::SmallVec;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long synchronous callers wait for a family actor's reply.
pub(super) const NOTICE_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(1);

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
    recent_publishes: VecDeque<(Instant, usize)>,
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
        if let Some((bucket_at, count)) = self.recent_publishes.back_mut() {
            if now.saturating_duration_since(*bucket_at) < Duration::from_secs(1) {
                *count = count.saturating_add(1);
                return;
            }
        }
        self.recent_publishes.push_back((now, 1));
    }

    pub(super) fn publishes_total(&self) -> u64 {
        self.publishes_total
    }

    pub(super) fn publishes_per_minute(&mut self, now: Instant) -> f64 {
        self.prune_recent_publishes(now);
        let recent_count = self
            .recent_publishes
            .iter()
            .fold(0_usize, |total, (_, count)| total.saturating_add(*count));
        usize_to_f64(recent_count)
    }

    pub(super) fn prune_recent_publishes(&mut self, now: Instant) {
        while let Some((oldest, _)) = self.recent_publishes.front().copied() {
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

#[cfg(test)]
mod tests {
    use super::NoticeRouteStats;
    use std::time::{Duration, Instant};

    #[test]
    fn should_aggregate_recent_publishes_into_second_buckets() {
        // Arrange
        let start = Instant::now();
        let mut stats = NoticeRouteStats::new();

        // Act
        stats.record_publish(start);
        stats.record_publish(start + Duration::from_millis(500));

        // Assert
        assert_eq!(stats.recent_publishes.len(), 1);
        assert!(
            (stats.publishes_per_minute(start + Duration::from_millis(500)) - 2.0).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn should_bound_recent_publish_buckets_to_the_rolling_window() {
        // Arrange
        let start = Instant::now();
        let mut stats = NoticeRouteStats::new();

        // Act
        for second in 0..=120 {
            stats.record_publish(start + Duration::from_secs(second));
        }

        // Assert
        assert!(stats.recent_publishes.len() <= 61);
        assert!(
            (stats.publishes_per_minute(start + Duration::from_secs(120)) - 61.0).abs()
                < f64::EPSILON
        );
    }
}
