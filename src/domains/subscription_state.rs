use crate::runtime::matcher::Pattern;
use crate::runtime::routing::{Route, RouteFamily};
use crate::runtime::{DomainPublishEvent, SubscriptionId, SubscriptionIndex};
use std::collections::{HashMap, HashSet};

/// Per-session wildcard registration cap shared by every wildcard-capable domain.
pub(crate) const MAX_WILDCARD_REGISTRATIONS_PER_SESSION: usize = 128;
/// Total Notice registration cap for one ephemeral session.
pub(crate) const MAX_NOTICE_REGISTRATIONS_PER_SESSION: usize = 1_024;

pub(crate) fn wildcard_registration_limit_reached(
    pattern: &Pattern,
    current_wildcard_count: usize,
) -> bool {
    pattern.is_wildcard() && current_wildcard_count >= MAX_WILDCARD_REGISTRATIONS_PER_SESSION
}

pub(crate) trait RoutedSubscription {
    fn pattern(&self) -> &Pattern;
    fn session_id(&self) -> u64;
    fn subscription_id(&self) -> u64;
}

pub(crate) struct RoutedSubscriptionSet<T> {
    subscriptions: HashMap<u64, T>,
    session_patterns: HashMap<u64, HashMap<String, u64>>,
    session_subscription_ids: HashMap<u64, HashSet<u64>>,
    wildcard_subscription_counts: HashMap<u64, usize>,
    index: SubscriptionIndex,
    exact_routes: HashMap<String, Vec<u64>>,
    wildcard_subscription_count: usize,
}

impl<T: RoutedSubscription> RoutedSubscriptionSet<T> {
    pub(crate) fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            session_patterns: HashMap::new(),
            session_subscription_ids: HashMap::new(),
            wildcard_subscription_counts: HashMap::new(),
            index: SubscriptionIndex::new(),
            exact_routes: HashMap::new(),
            wildcard_subscription_count: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    pub(crate) fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    pub(crate) fn subscription_count_for_session(&self, session_id: u64) -> usize {
        self.session_subscription_ids
            .get(&session_id)
            .map_or(0, HashSet::len)
    }

    pub(crate) fn wildcard_subscription_count_for_session(&self, session_id: u64) -> usize {
        self.wildcard_subscription_counts
            .get(&session_id)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn wildcard_registration_limit_reached(
        &self,
        session_id: u64,
        pattern: &Pattern,
    ) -> bool {
        wildcard_registration_limit_reached(
            pattern,
            self.wildcard_subscription_count_for_session(session_id),
        )
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.subscriptions.values()
    }

    pub(crate) fn get(&self, subscription_id: u64) -> Option<&T> {
        self.subscriptions.get(&subscription_id)
    }

    pub(crate) fn matching_ids(&self, family_id: RouteFamily, route: &str) -> Vec<u64> {
        let mut ids = self.exact_routes.get(route).cloned().unwrap_or_default();
        if self.wildcard_subscription_count > 0 {
            ids.extend(
                self.index
                    .match_all_route_str_with_capacity(
                        family_id,
                        route,
                        self.wildcard_subscription_count,
                    )
                    .into_iter()
                    .map(|id| id.0),
            );
        }
        ids
    }

    pub(crate) fn matching_capacity_hint(&self, route: &str) -> usize {
        self.exact_routes.get(route).map_or(0, Vec::len) + self.wildcard_subscription_count
    }

    pub(crate) fn find_existing_id(&self, session_id: u64, pattern: &str) -> Option<u64> {
        self.session_patterns
            .get(&session_id)
            .and_then(|patterns| patterns.get(pattern).copied())
    }

    pub(crate) fn insert(&mut self, family_id: RouteFamily, subscription: T) {
        let subscription_id = subscription.subscription_id();
        let session_id = subscription.session_id();
        let pattern = subscription.pattern().route();

        self.session_patterns
            .entry(session_id)
            .or_default()
            .insert(pattern.to_string(), subscription_id);
        self.session_subscription_ids
            .entry(session_id)
            .or_default()
            .insert(subscription_id);

        if subscription.pattern().is_wildcard() {
            let route = Route::from_ref(pattern);
            self.index
                .insert(family_id, &route, SubscriptionId(subscription_id));
            self.wildcard_subscription_count = self.wildcard_subscription_count.saturating_add(1);
            let session_wildcards = self
                .wildcard_subscription_counts
                .entry(session_id)
                .or_insert(0);
            *session_wildcards = session_wildcards.saturating_add(1);
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
        let Some(subscription_id) = self.find_existing_id(session_id, pattern) else {
            return 0;
        };

        self.remove_subscription(family_id, subscription_id);
        1
    }

    pub(crate) fn remove_session(&mut self, family_id: RouteFamily, session_id: u64) -> usize {
        let Some(subscription_ids) = self.session_subscription_ids.remove(&session_id) else {
            return 0;
        };

        let removed_ids: Vec<u64> = subscription_ids.into_iter().collect();
        for subscription_id in &removed_ids {
            self.remove_subscription(family_id, *subscription_id);
        }

        removed_ids.len()
    }

    pub(crate) fn remove_subscription_for_session(
        &mut self,
        family_id: RouteFamily,
        session_id: u64,
        subscription_id: u64,
    ) -> bool {
        let matches_session = self
            .subscriptions
            .get(&subscription_id)
            .is_some_and(|subscription| subscription.session_id() == session_id);

        if matches_session {
            self.remove_subscription(family_id, subscription_id);
        }

        matches_session
    }

    pub(crate) fn for_each_matching(
        &self,
        event: &DomainPublishEvent,
        visit: impl FnMut(&T),
    ) -> usize {
        self.for_each_matching_route(event.family_id, event.route.as_str(), visit)
    }

    pub(crate) fn for_each_matching_route(
        &self,
        family_id: RouteFamily,
        route: &str,
        mut visit: impl FnMut(&T),
    ) -> usize {
        let mut matched = 0_usize;

        if let Some(exact_ids) = self.exact_routes.get(route) {
            for subscription_id in exact_ids {
                if let Some(subscription) = self.subscriptions.get(subscription_id) {
                    matched = matched.saturating_add(1);
                    visit(subscription);
                }
            }
        }

        if self.wildcard_subscription_count > 0 {
            let wildcard_matches = self.index.match_all_route_str_with_capacity(
                family_id,
                route,
                self.wildcard_subscription_count,
            );
            for subscription_id in wildcard_matches {
                if let Some(subscription) = self.subscriptions.get(&subscription_id.0) {
                    matched = matched.saturating_add(1);
                    visit(subscription);
                }
            }
        }

        matched
    }

    fn remove_subscription(&mut self, family_id: RouteFamily, subscription_id: u64) {
        if let Some(subscription) = self.subscriptions.remove(&subscription_id) {
            let session_id = subscription.session_id();
            let pattern = subscription.pattern().route();

            let session_patterns_empty =
                if let Some(patterns) = self.session_patterns.get_mut(&session_id) {
                    patterns.remove(pattern);
                    patterns.is_empty()
                } else {
                    false
                };
            if session_patterns_empty {
                self.session_patterns.remove(&session_id);
            }

            let session_subscriptions_empty = if let Some(subscription_ids) =
                self.session_subscription_ids.get_mut(&session_id)
            {
                subscription_ids.remove(&subscription_id);
                subscription_ids.is_empty()
            } else {
                false
            };
            if session_subscriptions_empty {
                self.session_subscription_ids.remove(&session_id);
            }

            if subscription.pattern().is_wildcard() {
                let route = Route::from_ref(pattern);
                self.index
                    .remove(family_id, &route, SubscriptionId(subscription_id));
                self.wildcard_subscription_count =
                    self.wildcard_subscription_count.saturating_sub(1);

                let wildcard_count_empty =
                    if let Some(count) = self.wildcard_subscription_counts.get_mut(&session_id) {
                        *count = count.saturating_sub(1);
                        *count == 0
                    } else {
                        false
                    };
                if wildcard_count_empty {
                    self.wildcard_subscription_counts.remove(&session_id);
                }
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
