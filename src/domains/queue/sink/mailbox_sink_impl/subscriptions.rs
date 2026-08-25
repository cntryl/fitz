//! Watch/unwatch subscription handling for queue domain frames.
//!
//! Split out of `mailbox_sink_impl.rs` to keep that file under the
//! repo's per-file line budget - this block is a cohesive unit (one
//! dispatcher plus the two operations it dispatches to, plus their shared
//! validation helper) with no dependency on the rest of the file besides
//! `QueueDomainCore` itself and `QueueOpKind::InflightExpired`.

use super::{
    Envelope, Instant, QueueDomainCore, QueueOpKind, QueueSubscription, QueueSubscriptionMessage,
    RoutedSubscriptionSet,
};
use crate::runtime::routing::RouteFamily;
use std::sync::atomic::Ordering;

type SubscriptionOutcome = (
    crate::domains::queue::QueueResponse,
    Option<(
        RouteFamily,
        crate::runtime::matcher::Pattern,
        u64,
        u64,
        crate::runtime::routing::RouteAddress,
    )>,
    bool,
);

impl QueueDomainCore {
    pub(super) fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        sub_msg: QueueSubscriptionMessage,
    ) {
        let (response, initial_watch_snapshot, state_changed) = match sub_msg {
            QueueSubscriptionMessage::Watch {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_watch_subscription(
                envelope, meta, family_id, &pattern, session_id, subscriber,
            ),
            QueueSubscriptionMessage::Unwatch {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_unwatch_subscription(
                envelope,
                meta,
                family_id,
                &pattern,
                session_id,
                &subscriber,
            ),
        };

        self.route_queue_response(envelope, meta, &response);
        if state_changed {
            self.mark_admin_snapshot_dirty();
        }
        if let Some((family_id, pattern, session_id, subscription_id, subscriber)) =
            initial_watch_snapshot
        {
            self.emit_current_ready_notifications_for_watch(
                family_id,
                &pattern,
                session_id,
                subscription_id,
                &subscriber,
            );
        }

        self.record_operation_metrics(request_started, &response, QueueOpKind::InflightExpired);
    }

    fn handle_watch_subscription(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> SubscriptionOutcome {
        if !Self::valid_subscription_request(envelope, meta, family_id, session_id, &subscriber) {
            return (
                crate::domains::queue::QueueResponse::BadRequest {
                    reason: "route family mismatch".to_string(),
                },
                None,
                false,
            );
        }
        let pattern_str = pattern.as_str();
        let parsed_pattern = match crate::runtime::DomainKind::Queue
            .descriptor()
            .compile_registration_pattern(pattern_str)
        {
            Ok(pattern) => pattern,
            Err(reason) => {
                return (
                    crate::domains::queue::QueueResponse::InvalidSubscriptionPattern { reason },
                    None,
                    false,
                );
            }
        };
        let (subscription_id, state_changed) = {
            let mut families = self.families.lock();
            let state = families
                .entry(family_id.as_u64())
                .or_insert_with(RoutedSubscriptionSet::new);

            if let Some(id) = state.find_existing_id(session_id, pattern_str) {
                (id, false)
            } else {
                if state.wildcard_registration_limit_reached(session_id, &parsed_pattern) {
                    return (
                        crate::domains::queue::QueueResponse::SubscriptionLimit,
                        None,
                        false,
                    );
                }
                let Ok(id) = self.next_sub_id.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| current.checked_add(1),
                ) else {
                    let state_empty = state.is_empty();
                    if state_empty {
                        families.remove(&family_id.as_u64());
                    }
                    return (
                        crate::domains::queue::QueueResponse::BadRequest {
                            reason: "subscription ID space exhausted".to_string(),
                        },
                        None,
                        false,
                    );
                };
                state.insert(
                    family_id,
                    QueueSubscription {
                        pattern: parsed_pattern.clone(),
                        session_id,
                        subscription_id: id,
                        subscriber: subscriber.clone(),
                    },
                );
                (id, true)
            }
        };

        (
            crate::domains::queue::QueueResponse::WatchOk { subscription_id },
            Some((
                family_id,
                parsed_pattern,
                session_id,
                subscription_id,
                subscriber,
            )),
            state_changed,
        )
    }

    fn handle_unwatch_subscription(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> SubscriptionOutcome {
        if !Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            return (
                crate::domains::queue::QueueResponse::BadRequest {
                    reason: "route family mismatch".to_string(),
                },
                None,
                false,
            );
        }
        if let Err(reason) = crate::runtime::DomainKind::Queue
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
        {
            return (
                crate::domains::queue::QueueResponse::InvalidSubscriptionPattern { reason },
                None,
                false,
            );
        }

        let mut families = self.families.lock();
        let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
            state.remove_session_pattern(family_id, session_id, pattern.as_str());
            state.is_empty()
        } else {
            false
        };
        if remove_family {
            families.remove(&family_id.as_u64());
        }
        (crate::domains::queue::QueueResponse::UnwatchOk, None, true)
    }

    fn valid_subscription_request(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: RouteFamily,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> bool {
        family_id == meta.route_family
            && *subscriber.family() == family_id
            && session_id == meta.session_id
            && envelope.source().is_none_or(|source| source == subscriber)
    }
}
