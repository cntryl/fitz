use crate::runtime::matcher::Pattern;
use crate::runtime::routing::{Route, RouteFamily};
use crate::runtime::{DomainPublishEvent, SubscriptionId, SubscriptionIndex};
use std::collections::HashMap;

pub(crate) trait RoutedSubscription {
    fn pattern(&self) -> &Pattern;
    fn session_id(&self) -> u64;
    fn subscription_id(&self) -> u64;
}

pub(crate) struct RoutedSubscriptionSet<T> {
    subscriptions: HashMap<u64, T>,
    index: SubscriptionIndex,
    exact_routes: HashMap<String, Vec<u64>>,
    wildcard_subscription_count: usize,
}

impl<T: RoutedSubscription> RoutedSubscriptionSet<T> {
    pub(crate) fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            index: SubscriptionIndex::new(),
            exact_routes: HashMap::new(),
            wildcard_subscription_count: 0,
        }
    }

    pub(crate) fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.subscriptions.values()
    }

    pub(crate) fn find_existing_id(&self, session_id: u64, pattern: &str) -> Option<u64> {
        self.subscriptions
            .values()
            .find(|subscription| {
                subscription.session_id() == session_id && subscription.pattern().route() == pattern
            })
            .map(RoutedSubscription::subscription_id)
    }

    pub(crate) fn insert(&mut self, family_id: RouteFamily, subscription: T) {
        let subscription_id = subscription.subscription_id();
        let pattern = subscription.pattern().route();

        if pattern.contains('*') {
            let route = Route::from_ref(pattern);
            self.index
                .insert(family_id, &route, SubscriptionId(subscription_id));
            self.wildcard_subscription_count += 1;
        } else {
            self.exact_routes
                .entry(pattern.to_string())
                .or_default()
                .push(subscription_id);
        }

        self.subscriptions.insert(subscription_id, subscription);
    }

    pub(crate) fn remove_session_pattern(
        &mut self,
        family_id: RouteFamily,
        session_id: u64,
        pattern: &str,
    ) -> usize {
        self.remove_matching(family_id, |subscription| {
            subscription.session_id() == session_id && subscription.pattern().route() == pattern
        })
    }

    pub(crate) fn remove_session(&mut self, family_id: RouteFamily, session_id: u64) -> usize {
        self.remove_matching(family_id, |subscription| {
            subscription.session_id() == session_id
        })
    }

    pub(crate) fn for_each_matching(
        &self,
        event: &DomainPublishEvent,
        mut visit: impl FnMut(&T),
    ) -> usize {
        let mut matched = 0;

        if let Some(exact_ids) = self.exact_routes.get(event.route.as_str()) {
            for subscription_id in exact_ids {
                if let Some(subscription) = self.subscriptions.get(subscription_id) {
                    matched += 1;
                    visit(subscription);
                }
            }
        }

        if self.wildcard_subscription_count > 0 {
            let wildcard_matches = self.index.match_all_with_capacity(
                event.family_id,
                &event.route,
                self.wildcard_subscription_count,
            );
            for subscription_id in wildcard_matches {
                if let Some(subscription) = self.subscriptions.get(&subscription_id.0) {
                    matched += 1;
                    visit(subscription);
                }
            }
        }

        matched
    }

    fn remove_matching(&mut self, family_id: RouteFamily, matches: impl Fn(&T) -> bool) -> usize {
        let removed_ids: Vec<u64> = self
            .subscriptions
            .iter()
            .filter_map(|(subscription_id, subscription)| {
                matches(subscription).then_some(*subscription_id)
            })
            .collect();

        for subscription_id in &removed_ids {
            self.remove_subscription(family_id, *subscription_id);
        }

        removed_ids.len()
    }

    fn remove_subscription(&mut self, family_id: RouteFamily, subscription_id: u64) {
        if let Some(subscription) = self.subscriptions.remove(&subscription_id) {
            let pattern = subscription.pattern().route();

            if pattern.contains('*') {
                let route = Route::from_ref(pattern);
                self.index
                    .remove(family_id, &route, SubscriptionId(subscription_id));
                self.wildcard_subscription_count =
                    self.wildcard_subscription_count.saturating_sub(1);
            } else {
                let is_empty = if let Some(route_ids) = self.exact_routes.get_mut(pattern) {
                    route_ids.retain(|id| *id != subscription_id);
                    route_ids.is_empty()
                } else {
                    false
                };

                if is_empty {
                    self.exact_routes.remove(pattern);
                }
            }
        }
    }
}
