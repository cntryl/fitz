//! Subscribe/unsubscribe message handling: mutation of the live subscription
//! index in response to a client request.

use super::model::{ScheduleDomainRuntime, ScheduleSubscription};
use std::sync::atomic::Ordering;

impl ScheduleDomainRuntime<'_> {
    pub(super) fn rollback_undeliverable_schedule_subscribe(
        &mut self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
    ) {
        debug_assert_eq!(family_id, self.core.route_family);
        self.core
            .subscriptions
            .remove_session_route(family_id, session_id, route.as_str());
    }

    pub(super) fn apply_subscribe_message(
        &mut self,
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
        &mut self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
        pattern: crate::runtime::matcher::Pattern,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        debug_assert_eq!(family_id, self.core.route_family);
        let state = &mut self.core.subscriptions;

        let sub_id =
            if let Some(id) = state.find_existing_id(session_id, route.as_str()) {
                tracing::debug!(
                    domain = "schedule",
                    session = session_id,
                    subscription_id = id,
                    route = route.as_str(),
                    "Schedule subscription already exists (idempotent)"
                );
                id
            } else {
                if let Some(limit) = state
                    .subscriptions
                    .registration_limit_for_session(session_id, &pattern)
                {
                    return ScheduleResponse::Error(ScheduleFailure::new(
                        ScheduleFailureCategory::SubscriptionLimit,
                        registration_limit_message(limit),
                    ));
                }
                let Ok(new_id) = self.core.next_sub_id.try_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| current.checked_add(1),
                ) else {
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
        &mut self,
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

        debug_assert_eq!(family_id, self.core.route_family);
        self.core
            .subscriptions
            .remove_session_route(family_id, session_id, route.as_str());
        ScheduleResponse::Ok
    }
}

/// Schedule SUBSCRIBE limit errors are uncoded, so clients distinguish them only
/// by these exact messages. The wildcard text is the pre-#373 production wire text.
fn registration_limit_message(
    limit: crate::domains::subscription_state::RegistrationLimit,
) -> String {
    use crate::domains::subscription_state::{
        RegistrationLimit, MAX_TOTAL_REGISTRATIONS_PER_SESSION,
        MAX_WILDCARD_REGISTRATIONS_PER_SESSION,
    };

    match limit {
        RegistrationLimit::Wildcard => format!(
            "wildcard subscription limit exceeded ({MAX_WILDCARD_REGISTRATIONS_PER_SESSION} per session)"
        ),
        RegistrationLimit::Total => format!(
            "total subscription limit exceeded ({MAX_TOTAL_REGISTRATIONS_PER_SESSION} per session)"
        ),
    }
}
