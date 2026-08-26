//! Subscribe/unsubscribe message handling: mutation of the live subscription
//! index in response to a client request.

use super::model::{Ordering, ScheduleDomainRuntime, ScheduleSubscription, ScheduleSubscriptionSet};

impl ScheduleDomainRuntime<'_> {
    pub(super) fn apply_subscribe_message(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        match crate::runtime::DomainKind::Schedule
            .descriptor()
            .compile_registration_pattern(route.as_str())
        {
            Ok(pattern) => {
                self.insert_schedule_subscription(family_id, route, session_id, subscriber, pattern)
            }
            Err(error) => ScheduleResponse::Error(ScheduleFailure::new(
                ScheduleFailureCategory::InvalidSubscriptionPattern,
                error,
            )),
        }
    }

    fn insert_schedule_subscription(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
        pattern: crate::runtime::matcher::Pattern,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        let fam_id = family_id.as_u64();
        let mut families = self.core.sub_families.lock();
        let state = families
            .entry(fam_id)
            .or_insert_with(ScheduleSubscriptionSet::new);

        let sub_id = if let Some(id) = state.find_existing_id(session_id, route.as_str()) {
            tracing::debug!(
                domain = "schedule",
                session = session_id,
                subscription_id = id,
                route = route.as_str(),
                "Schedule subscription already exists (idempotent)"
            );
            id
        } else {
            if state
                .subscriptions
                .wildcard_registration_limit_reached(session_id, &pattern)
            {
                return ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::SubscriptionLimit,
                    format!(
                        "wildcard subscription limit exceeded ({} per session)",
                        crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION
                    ),
                ));
            }
            let Ok(new_id) = self.core.next_sub_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| current.checked_add(1),
            ) else {
                let state_empty = state.is_empty();
                if state_empty {
                    families.remove(&fam_id);
                }
                return ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::SubscriptionLimit,
                    "subscription ID space exhausted",
                ));
            };
            state.insert(
                family_id,
                ScheduleSubscription {
                    pattern,
                    session_id,
                    subscription_id: new_id,
                    subscriber,
                },
            );

            tracing::debug!(
                domain = "schedule",
                session = session_id,
                subscription_id = new_id,
                route = route.as_str(),
                "Schedule subscription added"
            );
            new_id
        };

        ScheduleResponse::SubscribeOk {
            subscription_id: sub_id,
        }
    }

    pub(super) fn apply_unsubscribe_message(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        if let Err(error) = crate::runtime::DomainKind::Schedule
            .descriptor()
            .compile_registration_pattern(route.as_str())
        {
            return ScheduleResponse::Error(ScheduleFailure::new(
                ScheduleFailureCategory::InvalidSubscriptionPattern,
                error,
            ));
        }

        let fam_id = family_id.as_u64();
        let mut families = self.core.sub_families.lock();
        let remove_family = if let Some(state) = families.get_mut(&fam_id) {
            state.remove_session_route(family_id, session_id, route.as_str());
            state.is_empty()
        } else {
            false
        };
        if remove_family {
            families.remove(&fam_id);
        }
        ScheduleResponse::Ok
    }
}
