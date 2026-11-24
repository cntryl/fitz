//! Bounded, best-effort live delivery for committed Stream records.

use super::model::StreamFamilyState;
use fitz_core::runtime::Envelope;
use std::ops::Bound::{Excluded, Included};

mod notification_gating;
mod queue;
mod watermark_coordination;
pub(super) use queue::{DeliveryQueue, SharedDeliveryBudgets};

const MAX_DISPATCH_JOBS_PER_TURN: usize = 64;
const MAX_DISPATCH_TARGETS_PER_TURN: usize = 64;

impl StreamFamilyState {
    pub(super) fn dispatch_ready_notifications(&mut self) {
        if !self.delivery.begin_dispatch() {
            return;
        }
        let mut targets_left = MAX_DISPATCH_TARGETS_PER_TURN;
        for _ in 0..MAX_DISPATCH_JOBS_PER_TURN {
            if targets_left == 0 {
                break;
            }
            let Some((id, deferred, job)) = self.delivery.next_ready() else {
                break;
            };
            let key = &job.event.resource;
            let current = !deferred
                || self
                    .stream_store
                    .resource_drop_cutoff(key.family.as_u64(), &key.realm, &key.area, &key.resource)
                    .is_ok_and(|cutoff| job.event.offsets.first_global >= cutoff);
            if !current {
                record_drops(job.remaining());
                self.delivery.remove(id);
                continue;
            }

            let ids = self
                .subscriptions
                .ids(&job.selector)
                .expect("live targets have a registration bucket");
            for subscription_id in ids
                .range((Excluded(job.cursor), Included(job.cutoff)))
                .copied()
                .take(targets_left)
            {
                let subscriber = self.subscriptions.get(subscription_id);
                let result =
                    route_notification(&self.router, subscriber, subscription_id, &job.event.event);
                #[cfg(feature = "benchkit")]
                crate::stream_dispatch::record(job.event.dispatch_started);
                if !result {
                    record_drops(1);
                }
                job.finish_target(subscription_id);
                targets_left -= 1;
            }
            if job.remaining() == 0 {
                self.delivery.remove(id);
            }
        }
    }
}

fn record_drops(count: usize) {
    if count > 0 {
        fitz_core::observability::counter_add(
            crate::domain::metrics::METRIC_NOTIFY_DROPS_TOTAL,
            super::model::usize_to_u64_saturating(count),
        );
    }
}

fn route_notification(
    router: &fitz_core::runtime::Router,
    subscriber: &super::subscriptions::Subscription,
    id: u64,
    event: &fitz_core::runtime::DomainPublishEvent,
) -> bool {
    #[cfg(test)]
    let notify = {
        let mut encoder = fitz_core::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        let payload = crate::wire::stream_codec::encode_notify_into(
            &mut encoder,
            id,
            &event.route,
            &event.payload,
        );
        fitz_core::protocol::FrameContext::new(
            subscriber.session_id,
            fitz_core::protocol::frame::ChannelId::Sub,
            fitz_core::protocol::message::MessageType::new(609),
            bytes::Bytes::from(payload),
            event.family_id,
        )
    };
    #[cfg(not(test))]
    let notify = crate::domain::StreamClientNotification::new(
        subscriber.session_id,
        event.family_id,
        id,
        event.route.clone(),
        event.payload.clone(),
    );
    router
        .route(Envelope::new(subscriber.subscriber.clone(), notify))
        .is_ok()
}
