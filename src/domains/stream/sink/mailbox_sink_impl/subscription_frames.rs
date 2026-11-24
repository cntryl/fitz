//! Subscribe and unsubscribe are owned by the Stream family actor.

use super::super::subscriptions::{validate_selector, RegistrationError};
use super::{Envelope, StreamClientResponseBody, StreamFamilyState};
use crate::domains::stream::protocol::StreamSubscriptionMessage;
use crate::domains::stream::StreamSubscriptionFailure;
use crate::runtime::routing::{RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;

impl StreamFamilyState {
    pub(super) fn handle_subscription_frame(
        &mut self,
        envelope: &Envelope,
        meta: ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        message: StreamSubscriptionMessage,
    ) {
        let (subscribe, family, pattern, session, subscriber) = match message {
            StreamSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => (true, family_id, pattern, session_id, subscriber),
            StreamSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => (false, family_id, pattern, session_id, subscriber),
        };
        let (response, added, removed) =
            if valid_request(envelope, meta, family, session, &subscriber) {
                match validate_selector(pattern.as_str()) {
                    Err(error) => (
                        StreamClientResponseBody::SubscriptionError(
                            StreamSubscriptionFailure::InvalidPattern(error),
                        ),
                        None,
                        false,
                    ),
                    Ok(shape) if subscribe => {
                        match self.subscriptions.subscribe(
                            session,
                            pattern.as_str(),
                            shape,
                            subscriber,
                        ) {
                            Ok((id, added)) => {
                                (subscription_ack(Some(id)), added.then_some(id), false)
                            }
                            Err(RegistrationError::Limit) => (
                                StreamClientResponseBody::SubscriptionError(
                                    StreamSubscriptionFailure::Limit,
                                ),
                                None,
                                false,
                            ),
                            Err(RegistrationError::IdExhausted) => (
                                Self::stream_error_response("subscription ID space exhausted"),
                                None,
                                false,
                            ),
                        }
                    }
                    Ok(_) => {
                        let removed = self
                            .subscriptions
                            .find(session, pattern.as_str())
                            .is_some_and(|id| self.remove_subscription(id));
                        (subscription_ack(None), None, removed)
                    }
                }
            } else {
                (
                    Self::stream_error_response("route family mismatch"),
                    None,
                    false,
                )
            };
        let delivered = self.route_stream_response(envelope, meta, &response, request_started);
        if let Some(id) = added.filter(|_| !delivered) {
            self.remove_subscription(id);
        }
        if removed || (added.is_some() && delivered) {
            self.mark_admin_snapshot_dirty();
        } else {
            self.refresh_metrics_gauges();
        }
    }

    fn remove_subscription(&mut self, id: u64) -> bool {
        let Some(entry) = self.subscriptions.remove(id) else {
            return false;
        };
        self.delivery.retire(&entry.canonical, id);
        true
    }
}

fn subscription_ack(id: Option<u64>) -> StreamClientResponseBody {
    StreamClientResponseBody::Ok {
        session_id: id,
        data: vec![],
    }
}

fn valid_request(
    envelope: &Envelope,
    meta: ClientFrameMeta,
    family: RouteFamily,
    session: u64,
    subscriber: &RouteAddress,
) -> bool {
    family == meta.route_family
        && *subscriber.family() == family
        && session == meta.session_id
        && envelope.source().is_none_or(|source| source == subscriber)
}
