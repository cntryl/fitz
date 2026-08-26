//! Subscribe and unsubscribe frames. The sink owns stream subscription state
//! outright; these never reach `StreamActor`.

use super::{
    Envelope, Ordering, RoutedSubscriptionSet, StreamClientResponseBody, StreamDomainCore,
    StreamSubscription,
};

impl StreamDomainCore {
    pub(super) fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        sub_msg: crate::domains::stream::protocol::StreamSubscriptionMessage,
    ) {
        use crate::domains::stream::protocol::StreamSubscriptionMessage;

        let response = match sub_msg {
            StreamSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_stream_subscribe(
                envelope, meta, family_id, &pattern, session_id, subscriber,
            ),
            StreamSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_stream_unsubscribe(
                envelope,
                meta,
                family_id,
                &pattern,
                session_id,
                &subscriber,
            ),
        };

        self.refresh_metrics_gauges();
        self.route_stream_response(envelope, meta, &response, request_started);
    }

    fn handle_stream_subscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> StreamClientResponseBody {
        if Self::valid_stream_subscription_request(
            envelope,
            meta,
            family_id,
            session_id,
            &subscriber,
        ) {
            let compiled = match Self::compile_stream_subscription_pattern(pattern) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let mut families = self.subscriptions.families.lock();
            let state = families
                .entry(family_id.as_u64())
                .or_insert_with(RoutedSubscriptionSet::new);
            if let Some(subscription_id) = state.find_existing_id(session_id, pattern.as_str()) {
                return StreamClientResponseBody::Ok {
                    session_id: Some(subscription_id),
                    data: vec![],
                };
            }
            if state.wildcard_registration_limit_reached(session_id, &compiled) {
                return StreamClientResponseBody::SubscriptionError(
                    crate::domains::stream::StreamSubscriptionFailure::Limit,
                );
            }
            if let Ok(subscription_id) = self.subscriptions.next_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| current.checked_add(1),
            ) {
                state.insert(
                    family_id,
                    StreamSubscription {
                        pattern: compiled,
                        session_id,
                        subscription_id,
                        subscriber,
                    },
                );
                StreamClientResponseBody::Ok {
                    session_id: Some(subscription_id),
                    data: vec![],
                }
            } else {
                if state.is_empty() {
                    families.remove(&family_id.as_u64());
                }
                Self::stream_error_response("subscription ID space exhausted")
            }
        } else {
            Self::stream_error_response("route family mismatch")
        }
    }

    fn handle_stream_unsubscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> StreamClientResponseBody {
        if Self::valid_stream_subscription_request(
            envelope, meta, family_id, session_id, subscriber,
        ) {
            if let Err(response) = Self::compile_stream_subscription_pattern(pattern) {
                return response;
            }
            let mut families = self.subscriptions.families.lock();
            let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                state.remove_session_pattern(family_id, session_id, pattern.as_str());
                state.is_empty()
            } else {
                false
            };
            if remove_family {
                families.remove(&family_id.as_u64());
            }
            drop(families);
            self.remove_pending_notifications_for_pattern(session_id, pattern.as_str());
            StreamClientResponseBody::Ok {
                session_id: None,
                data: vec![],
            }
        } else {
            Self::stream_error_response("route family mismatch")
        }
    }

    fn compile_stream_subscription_pattern(
        pattern: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, StreamClientResponseBody> {
        let invalid_pattern = |error: String| {
            StreamClientResponseBody::SubscriptionError(
                crate::domains::stream::StreamSubscriptionFailure::InvalidPattern(error),
            )
        };
        let compiled = crate::runtime::DomainKind::Stream
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
            .map_err(invalid_pattern)?;
        crate::domains::stream::route_grammar::classify_stream_route_shape(pattern.as_str())
            .map_err(invalid_pattern)?;
        Ok(compiled)
    }

    fn valid_stream_subscription_request(
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
