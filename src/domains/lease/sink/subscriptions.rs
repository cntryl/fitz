//! Subscribe/unsubscribe message handling: mutation of the live subscription
//! index in response to a client request.

use super::model::{LeaseDomainRuntime, LeaseSubscription, Ordering, RoutedSubscriptionSet};
use crate::runtime::Envelope;

impl LeaseDomainRuntime<'_> {
    pub(super) fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        sub_msg: &crate::domains::lease::protocol::LeaseSubscriptionMessage,
    ) {
        use crate::domains::lease::protocol::LeaseSubscriptionMessage;

        let response = match sub_msg {
            LeaseSubscriptionMessage::Subscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => self.handle_lease_subscribe(
                envelope,
                meta,
                *family_id,
                route,
                *session_id,
                subscriber,
            ),
            LeaseSubscriptionMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => self.handle_lease_unsubscribe(
                envelope,
                meta,
                *family_id,
                route,
                *session_id,
                subscriber,
            ),
        };

        self.refresh_metrics_gauges();
        self.route_lease_response(envelope, meta, &response, request_started);
    }

    fn handle_lease_subscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            let compiled = match Self::compile_exact_lease_subscription_route(route) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let mut families = self.core.families.lock();
            let state = families
                .entry(family_id.as_u64())
                .or_insert_with(RoutedSubscriptionSet::new);
            if let Some(subscription_id) = state.find_existing_id(session_id, route.as_str()) {
                return LeaseResponse::SubscribeOk { subscription_id };
            }
            if let Ok(subscription_id) = self.core.next_sub_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| current.checked_add(1),
            ) {
                state.insert(
                    family_id,
                    LeaseSubscription {
                        route: compiled,
                        session_id,
                        route_address: subscriber.clone(),
                        subscription_id,
                    },
                );
                LeaseResponse::SubscribeOk { subscription_id }
            } else {
                if state.is_empty() {
                    families.remove(&family_id.as_u64());
                }
                LeaseResponse::Error("subscription ID space exhausted".to_string())
            }
        } else {
            LeaseResponse::Error("route family mismatch".to_string())
        }
    }

    fn handle_lease_unsubscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            if let Err(response) = Self::compile_exact_lease_subscription_route(route) {
                return response;
            }
            let mut families = self.core.families.lock();
            let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                state.remove_session_pattern(family_id, session_id, route.as_str());
                state.is_empty()
            } else {
                false
            };
            if remove_family {
                families.remove(&family_id.as_u64());
            }
            LeaseResponse::UnsubscribeOk
        } else {
            LeaseResponse::Error("route family mismatch".to_string())
        }
    }

    fn compile_exact_lease_subscription_route(
        route: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, crate::domains::lease::protocol::LeaseResponse>
    {
        use crate::domains::lease::protocol::LeaseResponse;

        // The exact-only rule lives on the Lease descriptor so ingress and
        // this sink reject the same patterns.
        crate::runtime::DomainKind::Lease
            .descriptor()
            .compile_registration_pattern(route.as_str())
            .map_err(LeaseResponse::InvalidSubscriptionRoute)
    }

    fn valid_subscription_request(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> bool {
        family_id == meta.route_family
            && *subscriber.family() == family_id
            && session_id == meta.session_id
            && envelope.source().is_none_or(|source| source == subscriber)
    }
}
