//! Due-schedule fan-out: claiming due fires, delivering them to live
//! subscribers or the durable pending-fire path, and acknowledging delivery.

use super::delivery_strategy::DeliveryStrategy;
use super::model::{
    Envelope, HashMap, HashSet, Ordering, PendingFireKey, PendingFireState, PendingFireStates,
    ScheduleDomainRuntime, EXECUTIONS_WINDOW_MS,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;

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
    pub(super) fn scan_due_schedules(&self) {
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

    pub(super) fn force_due_scan_for_tests(&self, ready_count: usize) {
        {
            let mut actors = self.core.actors.lock();
            for actor in actors.values_mut() {
                actor.bench_prepare_scan(ready_count);
            }
        }

        self.scan_due_schedules();
        self.schedule_admin_snapshot(true);
    }

    fn claim_due(&self) -> DueScanPlan {
        let mut live_publish_candidates = Vec::new();
        let mut ack_retry_candidates = PendingAckRetryMap::new();
        let mut snapshot_dirty = false;
        let mut actors = self.core.actors.lock();
        let mut pending_ack_retries = self.core.pending_ack_retries.lock();

        for (family, actor) in actors.iter_mut() {
            if !actor.claim_due_fires().is_empty() {
                snapshot_dirty = true;
            }

            Self::collect_family_pending_fires(
                *family,
                actor,
                &mut pending_ack_retries,
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
        &self,
        live_publish_candidates: Vec<LivePublishCandidate>,
        ack_retry_candidates: &mut PendingAckRetryMap,
    ) -> bool {
        let mut had_live_handoffs = false;

        for (family, fire_ms, route, delivery_mode, payload) in live_publish_candidates {
            let accepted = self.handle_schedule_publish(family, &route, delivery_mode, &payload);
            had_live_handoffs |= accepted;
            if !accepted {
                self.core
                    .live_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            ack_retry_candidates
                .entry(family)
                .or_default()
                .push((fire_ms, route));
        }

        had_live_handoffs
    }

    fn acknowledge_delivered(&self, ack_retry_candidates: PendingAckRetryMap) -> bool {
        let mut acknowledged_handoffs = false;

        if ack_retry_candidates.is_empty() {
            return false;
        }

        let mut actors = self.core.actors.lock();
        let mut pending_ack_retries = self.core.pending_ack_retries.lock();
        for (family, ack_candidates) in ack_retry_candidates {
            if let Some(actor) = actors.get_mut(&family) {
                acknowledged_handoffs |= self.acknowledge_family_pending_fire_claims(
                    family,
                    actor,
                    &ack_candidates,
                    &mut pending_ack_retries,
                );
            }
        }

        acknowledged_handoffs
    }

    fn acknowledge_family_pending_fire_claims(
        &self,
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
                self.core.ack_failures.fetch_add(1, Ordering::Relaxed);
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

    fn record_recent_acknowledgements(&self, acked: usize, acknowledged_at_ms: u64) {
        let mut deque = self.core.recent_acknowledgement_ms.lock();
        let cutoff = acknowledged_at_ms.saturating_sub(EXECUTIONS_WINDOW_MS);
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        for _ in 0..acked {
            deque.push_back(acknowledged_at_ms);
        }
    }

    pub(super) fn route_live_notify(
        &self,
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
        self.core.router.route(notify_envelope).is_ok()
    }

    pub(super) fn handle_schedule_publish(
        &self,
        family: crate::runtime::routing::RouteFamily,
        route: &str,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: &bytes::Bytes,
    ) -> bool {
        let mut families = self.core.sub_families.lock();
        let Some(state) = families.get_mut(&family) else {
            return false;
        };
        let mut subscription_ids = state.matching_ids(family, route);
        subscription_ids.sort_unstable();
        if subscription_ids.is_empty() {
            return false;
        }

        let cursor = state
            .round_robin_cursors
            .get(route)
            .copied()
            .unwrap_or_else(|| super::delivery_strategy::initial_round_robin_cursor(route));
        let strategy =
            DeliveryStrategy::select_recipients(delivery_mode, &subscription_ids, cursor);
        let mut any_accepted = false;
        for subscription_id in strategy.recipients() {
            let Some(subscription) = state.subscriptions.get(*subscription_id) else {
                continue;
            };
            let accepted = self.route_live_notify(
                subscription.session_id,
                subscription.subscription_id,
                &subscription.subscriber,
                route,
                payload,
            );
            any_accepted |= accepted;
            if accepted && strategy.stops_after_success() {
                let index = subscription_ids
                    .iter()
                    .position(|candidate| candidate == subscription_id)
                    .unwrap_or(cursor);
                state
                    .round_robin_cursors
                    .insert(route.to_string(), (index + 1) % subscription_ids.len());
                return true;
            }
        }
        if strategy.stops_after_success() {
            state
                .round_robin_cursors
                .insert(route.to_string(), (cursor + 1) % subscription_ids.len());
        }
        any_accepted
    }

    pub(super) fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        self.handle_schedule_publish(
            event.family_id,
            event.route.as_str(),
            crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            &event.payload,
        );
    }
}
