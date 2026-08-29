//! Per-family ephemeral KV watch subscription state.

use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::runtime::matcher::Pattern;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct KvWatchRegistry {
    family_id: RouteFamily,
    subscriptions: RoutedSubscriptionSet<KvWatchSubscription>,
    next_sub_id: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct KvWatchTarget {
    pub session_id: u64,
    pub subscription_id: u64,
    pub subscriber: RouteAddress,
}

struct KvWatchSubscription {
    pattern: Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: RouteAddress,
}

impl RoutedSubscription for KvWatchSubscription {
    fn pattern(&self) -> &Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

impl KvWatchRegistry {
    #[must_use]
    pub(crate) fn new(family_id: RouteFamily) -> Self {
        Self {
            family_id,
            subscriptions: RoutedSubscriptionSet::new(),
            next_sub_id: AtomicU64::new(1),
        }
    }

    /// # Errors
    ///
    /// Returns `KvError::SubscriptionLimit` when a new wildcard registration
    /// would exceed the per-session wildcard quota.
    pub(crate) fn subscribe(
        &mut self,
        session_id: u64,
        pattern: Pattern,
        subscriber: RouteAddress,
    ) -> Result<u64, crate::domains::kv::KvError> {
        if let Some(existing_id) = self
            .subscriptions
            .find_existing_id(session_id, pattern.route())
        {
            return Ok(existing_id);
        }
        if self
            .subscriptions
            .wildcard_registration_limit_reached(session_id, &pattern)
        {
            return Err(crate::domains::kv::KvError::SubscriptionLimit);
        }

        let subscription_id = self
            .next_sub_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| crate::domains::kv::KvError::SubscriptionLimit)?;
        self.subscriptions.insert(
            self.family_id,
            KvWatchSubscription {
                pattern,
                session_id,
                subscription_id,
                subscriber,
            },
        );
        Ok(subscription_id)
    }

    pub(crate) fn unsubscribe(&mut self, session_id: u64, pattern: &str) -> usize {
        self.subscriptions
            .remove_session_pattern(self.family_id, session_id, pattern)
    }

    #[must_use]
    pub(crate) fn existing_subscription_id(&self, session_id: u64, pattern: &str) -> Option<u64> {
        self.subscriptions.find_existing_id(session_id, pattern)
    }

    pub(crate) fn remove_subscription_for_session(
        &mut self,
        session_id: u64,
        subscription_id: u64,
    ) -> bool {
        self.subscriptions.remove_subscription_for_session(
            self.family_id,
            session_id,
            subscription_id,
        )
    }

    pub(crate) fn remove_session(&mut self, session_id: u64) -> usize {
        self.subscriptions
            .remove_session(self.family_id, session_id)
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    #[must_use]
    pub(crate) fn subscription_count(&self) -> usize {
        self.subscriptions.subscription_count()
    }

    #[must_use]
    pub(crate) fn matching_targets(&self, route: &Route) -> Vec<KvWatchTarget> {
        let mut targets =
            Vec::with_capacity(self.subscriptions.matching_capacity_hint(route.as_str()));
        self.subscriptions.for_each_matching_route(
            self.family_id,
            route.as_str(),
            |subscription| {
                targets.push(KvWatchTarget {
                    session_id: subscription.session_id,
                    subscription_id: subscription.subscription_id,
                    subscriber: subscription.subscriber.clone(),
                });
            },
        );
        targets
    }
}

#[cfg(test)]
#[path = "tests/watch_registry.rs"]
mod tests;
