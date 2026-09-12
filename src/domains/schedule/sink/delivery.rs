//! Due-schedule fan-out: claiming due fires, delivering them to live
//! subscribers or the durable pending-fire path, and acknowledging delivery.

use super::delivery_strategy::DeliveryStrategy;
use super::model::{
    PendingFireKey, PendingFireState, PendingFireStates, ScheduleDomainRuntime,
    ScheduleRunNowOutcome, ScheduleRunNowResult, EXECUTIONS_WINDOW_MS,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;
use std::collections::{HashMap, HashSet};

type PendingAckRetryMap = HashMap<crate::runtime::routing::RouteFamily, Vec<PendingFireKey>>;

type LivePublishCandidate = (
    crate::runtime::routing::RouteFamily,
    u64,
    String,
    crate::domains::schedule::ScheduleDeliveryMode,
    bytes::Bytes,
);

struct DueScanPlan {
    live_publish_candidates: Vec<LivePublishCandidate>,
    ack_retry_candidates: PendingAckRetryMap,
    snapshot_dirty: bool,
}

impl ScheduleDomainRuntime<'_> {
    pub(super) fn scan_due_schedules(&mut self) {
        let DueScanPlan {
            live_publish_candidates,
            mut ack_retry_candidates,
            snapshot_dirty,
        } = self.claim_due();
        let had_live_handoffs =
            self.deliver_claims(live_publish_candidates, &mut ack_retry_candidates);
        let acknowledged_handoffs = self.acknowledge_delivered(ack_retry_candidates);

        if snapshot_dirty || had_live_handoffs || acknowledged_handoffs {
            self.schedule_admin_snapshot(false);
        }

        self.refresh_metrics_gauges();
    }

    pub(super) fn force_due_scan_for_tests(&mut self, ready_count: usize) {
        {
            let actor = &mut self.core.actor;
            if let Some(actor) = actor.as_mut() {
                actor.bench_prepare_scan(ready_count);
            }
        }

        self.scan_due_schedules();
        self.schedule_admin_snapshot(true);
    }

    fn claim_due(&mut self) -> DueScanPlan {
        let mut live_publish_candidates = Vec::new();
        let mut ack_retry_candidates = PendingAckRetryMap::new();
        let mut snapshot_dirty = false;
        let actor = &mut self.core.actor;
        let pending_ack_retries = &mut self.core.pending_ack_retries;

        if let Some(actor) = actor.as_mut() {
            if !actor.claim_due_fires().is_empty() {
                snapshot_dirty = true;
            }

            Self::collect_family_pending_fires(
                self.core.route_family,
                actor,
                pending_ack_retries,
                &mut live_publish_candidates,
                &mut ack_retry_candidates,
            );
        }

        DueScanPlan {
            live_publish_candidates,
            ack_retry_candidates,
            snapshot_dirty,
        }
    }

    fn collect_family_pending_fires(
        family: crate::runtime::routing::RouteFamily,
        actor: &crate::domains::schedule::ScheduleActor,
        pending_ack_retries: &mut HashMap<u64, PendingFireStates>,
        live_publish_candidates: &mut Vec<LivePublishCandidate>,
        ack_retry_candidates: &mut PendingAckRetryMap,
    ) {
        let family_id = family.as_u64();
        let pending_fires = actor.pending_claimed_occurrences_for_publish();
        let mut pending_keys = HashSet::with_capacity(pending_fires.len());
        let remove_retry_entry = {
            let tracked_retries = pending_ack_retries.entry(family_id).or_default();
            for pending_fire in pending_fires {
                let pending_key = (pending_fire.fire_ms, pending_fire.route.clone());
                pending_keys.insert(pending_key.clone());

                match tracked_retries
                    .entry(pending_key.clone())
                    .or_insert(PendingFireState::Claimed)
                {
                    PendingFireState::HandedOff => {
                        ack_retry_candidates
                            .entry(family)
                            .or_default()
                            .push(pending_key);
                        continue;
                    }
                    PendingFireState::Acknowledged => continue,
                    PendingFireState::Claimed => {}
                }

                live_publish_candidates.push((
                    family,
                    pending_fire.fire_ms,
                    pending_fire.route,
                    pending_fire.delivery_mode,
                    pending_fire.payload,
                ));
            }

            tracked_retries.retain(|pending_key, _| pending_keys.contains(pending_key));
            tracked_retries.is_empty()
        };

        if remove_retry_entry {
            pending_ack_retries.remove(&family_id);
        }
    }

    fn deliver_claims(
        &mut self,
        live_publish_candidates: Vec<LivePublishCandidate>,
        ack_retry_candidates: &mut PendingAckRetryMap,
    ) -> bool {
        let mut had_live_handoffs = false;

        for (family, fire_ms, route, delivery_mode, payload) in live_publish_candidates {
            let handoffs =
                self.handle_schedule_publish_with_counts(family, &route, delivery_mode, &payload);
            had_live_handoffs |= handoffs.accepted_handoffs > 0;
            // Only handoffs the router rejected are failures. A fire with no
            // matching live registration attempts none and is a normal outcome.
            let rejected = handoffs
                .attempted_handoffs
                .saturating_sub(handoffs.accepted_handoffs);
            self.core.live_publish_failures = self
                .core
                .live_publish_failures
                .saturating_add(u64::try_from(rejected).unwrap_or(u64::MAX));
            ack_retry_candidates
                .entry(family)
                .or_default()
                .push((fire_ms, route));
        }

        had_live_handoffs
    }

    fn acknowledge_delivered(&mut self, ack_retry_candidates: PendingAckRetryMap) -> bool {
        let mut acknowledged_handoffs = false;

        if ack_retry_candidates.is_empty() {
            return false;
        }

        let mut actor = self.core.actor.take();
        let mut pending_ack_retries = std::mem::take(&mut self.core.pending_ack_retries);
        for (family, ack_candidates) in ack_retry_candidates {
            if family == self.core.route_family {
                let Some(actor) = actor.as_mut() else {
                    continue;
                };
                acknowledged_handoffs |= self.acknowledge_family_pending_fire_claims(
                    family,
                    actor,
                    &ack_candidates,
                    &mut pending_ack_retries,
                );
            }
        }
        self.core.actor = actor;
        self.core.pending_ack_retries = pending_ack_retries;

        acknowledged_handoffs
    }

    fn acknowledge_family_pending_fire_claims(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        actor: &mut crate::domains::schedule::ScheduleActor,
        ack_candidates: &[PendingFireKey],
        pending_ack_retries: &mut HashMap<u64, PendingFireStates>,
    ) -> bool {
        let family_id = family.as_u64();
        let tracked = pending_ack_retries.entry(family_id).or_default();
        for pending_key in ack_candidates {
            tracked.insert(pending_key.clone(), PendingFireState::HandedOff);
        }
        match actor.ack_pending_fire_claims(ack_candidates) {
            Ok((acked, acknowledged_at_ms)) if acked > 0 => {
                for pending_key in ack_candidates {
                    tracked.insert(pending_key.clone(), PendingFireState::Acknowledged);
                }
                Self::clear_ack_retry_candidates(family_id, ack_candidates, pending_ack_retries);
                self.record_recent_acknowledgements(acked, acknowledged_at_ms);
                true
            }
            Ok(_) => {
                Self::clear_ack_retry_candidates(family_id, ack_candidates, pending_ack_retries);
                false
            }
            Err(error) => {
                self.core.ack_failures = self.core.ack_failures.saturating_add(1);
                tracing::warn!(
                    route_family = family.as_u64(),
                    error = %error,
                    "Failed to acknowledge pending schedule fires"
                );
                false
            }
        }
    }

    fn clear_ack_retry_candidates(
        family_id: u64,
        ack_candidates: &[PendingFireKey],
        pending_ack_retries: &mut HashMap<u64, PendingFireStates>,
    ) {
        let remove_retry_entry =
            if let Some(tracked_retries) = pending_ack_retries.get_mut(&family_id) {
                for pending_key in ack_candidates {
                    tracked_retries.remove(pending_key);
                }
                tracked_retries.is_empty()
            } else {
                false
            };
        if remove_retry_entry {
            pending_ack_retries.remove(&family_id);
        }
    }

    fn record_recent_acknowledgements(&mut self, acked: usize, acknowledged_at_ms: u64) {
        let deque = &mut self.core.recent_acknowledgement_ms;
        let cutoff = acknowledged_at_ms.saturating_sub(EXECUTIONS_WINDOW_MS);
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        for _ in 0..acked {
            deque.push_back(acknowledged_at_ms);
        }
    }

    pub(super) fn route_live_notify(
        router: &crate::runtime::Router,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &str,
        payload: &bytes::Bytes,
    ) -> bool {
        #[cfg(test)]
        let notify_payload = crate::dispatch::protocol::schedule_codec::encode_notify(
            subscription_id,
            route,
            payload.as_ref(),
        );

        #[cfg(test)]
        let notify_ctx = FrameContext::new(
            session_id,
            crate::dispatch::protocol::frame::ChannelId::Sub,
            crate::dispatch::protocol::tlv::MessageType::new(705),
            bytes::Bytes::from(notify_payload),
            *subscriber.family(),
        );

        #[cfg(test)]
        let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);

        #[cfg(not(test))]
        let notify_envelope = Envelope::new(
            subscriber.clone(),
            crate::domains::schedule::ScheduleClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.to_string(),
                payload.clone(),
            ),
        );

        // Subscriber notify routing is best-effort and must not redefine the
        // schedule domain's durable acknowledgement boundary.
        router.route(notify_envelope).is_ok()
    }

    pub(super) fn handle_schedule_publish(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        route: &str,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: &bytes::Bytes,
    ) -> bool {
        self.handle_schedule_publish_with_counts(family, route, delivery_mode, payload)
            .accepted_handoffs
            > 0
    }

    fn handle_schedule_publish_with_counts(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        route: &str,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: &bytes::Bytes,
    ) -> ScheduleRunNowResult {
        debug_assert_eq!(family, self.core.route_family);
        let router = self.core.router.clone();
        let state = &mut self.core.subscriptions;
        if state.is_empty() {
            return ScheduleRunNowResult {
                delivery_mode,
                matched_subscriptions: 0,
                attempted_handoffs: 0,
                accepted_handoffs: 0,
                outcome: ScheduleRunNowOutcome::NoLiveSubscriptions,
            };
        }
        let mut subscription_ids = state.matching_ids(family, route);
        subscription_ids.sort_unstable();
        if subscription_ids.is_empty() {
            return ScheduleRunNowResult {
                delivery_mode,
                matched_subscriptions: 0,
                attempted_handoffs: 0,
                accepted_handoffs: 0,
                outcome: ScheduleRunNowOutcome::NoLiveSubscriptions,
            };
        }

        let cursor = state
            .round_robin_cursors
            .get(route)
            .copied()
            .unwrap_or_else(|| super::delivery_strategy::initial_round_robin_cursor(route));
        let strategy =
            DeliveryStrategy::select_recipients(delivery_mode, &subscription_ids, cursor);
        let mut accepted_handoffs = 0;
        let mut attempted_handoffs = 0;
        for subscription_id in strategy.recipients() {
            let Some(subscription) = state.subscriptions.get(*subscription_id) else {
                continue;
            };
            attempted_handoffs += 1;
            let accepted = Self::route_live_notify(
                &router,
                subscription.session_id,
                subscription.subscription_id,
                &subscription.subscriber,
                route,
                payload,
            );
            accepted_handoffs += usize::from(accepted);
            if accepted && strategy.stops_after_success() {
                let index = subscription_ids
                    .iter()
                    .position(|candidate| candidate == subscription_id)
                    .unwrap_or(cursor);
                state
                    .round_robin_cursors
                    .insert(route.to_string(), (index + 1) % subscription_ids.len());
                return ScheduleRunNowResult {
                    delivery_mode,
                    matched_subscriptions: subscription_ids.len(),
                    attempted_handoffs,
                    accepted_handoffs,
                    outcome: ScheduleRunNowOutcome::HandoffAccepted,
                };
            }
        }
        if strategy.stops_after_success() {
            state
                .round_robin_cursors
                .insert(route.to_string(), (cursor + 1) % subscription_ids.len());
        }
        ScheduleRunNowResult {
            delivery_mode,
            matched_subscriptions: subscription_ids.len(),
            attempted_handoffs,
            accepted_handoffs,
            outcome: if accepted_handoffs > 0 {
                ScheduleRunNowOutcome::HandoffAccepted
            } else {
                ScheduleRunNowOutcome::NoHandoffAccepted
            },
        }
    }

    pub(super) fn handle_domain_publish(&mut self, event: &crate::runtime::DomainPublishEvent) {
        self.handle_schedule_publish(
            event.family_id,
            event.route.as_str(),
            crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            &event.payload,
        );
    }

    #[allow(clippy::question_mark)]
    pub(super) fn run_now(&mut self, route: &str) -> Option<ScheduleRunNowResult> {
        let Some((delivery_mode, payload)) = self
            .core
            .actor
            .as_ref()
            .and_then(|actor| actor.run_now_definition(route))
        else {
            return None;
        };
        let result = self.handle_schedule_publish_with_counts(
            self.core.route_family,
            route,
            delivery_mode,
            &payload,
        );
        crate::observability::counter_inc(
            crate::domains::schedule::metrics::METRIC_RUN_NOW_PROCESSED_TOTAL,
        );
        if result.accepted_handoffs > 0 {
            crate::observability::counter_add(
                crate::domains::schedule::metrics::METRIC_RUN_NOW_ACCEPTED_HANDOFFS_TOTAL,
                u64::try_from(result.accepted_handoffs).unwrap_or(u64::MAX),
            );
        } else {
            crate::observability::counter_inc(
                crate::domains::schedule::metrics::METRIC_RUN_NOW_ZERO_ACCEPT_TOTAL,
            );
        }
        Some(result)
    }
}
