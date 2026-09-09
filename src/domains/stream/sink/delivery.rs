//! Publish/notification fan-out: matching subscribers to a committed event,
//! delivering to live subscribers, and notifying watermark coordinators.

use super::model::StreamFamilyState;
#[cfg(test)]
use crate::dispatch::protocol::payload_codec::PayloadEncoder;
use crate::runtime::Envelope;

mod notification_gating;
mod watermark_coordination;

impl StreamFamilyState {
    pub(in crate::domains::stream::sink) fn handle_domain_publish(
        &mut self,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        let ready = self.collect_ready_notifications(event);
        self.route_ready_notifications(ready);
    }

    pub(in crate::domains::stream::sink) fn handle_visibility_advance(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
    ) {
        let ready = self.collect_visible_pending_notifications(family.as_u64());
        self.route_ready_notifications(ready);
    }

    fn route_ready_notifications(&mut self, ready: Vec<super::model::ReadyStreamNotification>) {
        #[cfg(test)]
        let mut payload_encoder = PayloadEncoder::with_capacity(256);
        for notification in ready {
            let target = notification.target;
            let event = notification.event;
            if *target.subscriber.family() != event.family_id {
                crate::observability::counter_inc(
                    crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                );
                continue;
            }
            #[cfg(test)]
            self.route_commit_notify(
                target.session_id,
                target.subscription_id,
                &target.subscriber,
                &event,
                &mut payload_encoder,
            );
            #[cfg(not(test))]
            self.route_commit_notify(
                target.session_id,
                target.subscription_id,
                &target.subscriber,
                &event,
            );
        }
    }

    #[cfg(test)]
    pub(in crate::domains::stream::sink) fn route_commit_notify(
        &mut self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        event: &crate::runtime::DomainPublishEvent,
        payload_encoder: &mut PayloadEncoder,
    ) {
        let notify_payload = crate::dispatch::protocol::stream_codec::encode_notify_into(
            payload_encoder,
            subscription_id,
            &event.route,
            &event.payload,
        );
        let notify_ctx = crate::dispatch::protocol::FrameContext::new(
            session_id,
            crate::dispatch::protocol::frame::ChannelId::Sub,
            crate::dispatch::protocol::tlv::MessageType::new(609),
            bytes::Bytes::from(notify_payload),
            event.family_id,
        );
        let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc(
                crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL,
            );
        }
    }

    #[cfg(not(test))]
    pub(in crate::domains::stream::sink) fn route_commit_notify(
        &mut self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        let notify = crate::domains::stream::StreamClientNotification::new(
            session_id,
            event.family_id,
            subscription_id,
            event.route.clone(),
            event.payload.clone(),
        );
        let notify_envelope = Envelope::new(subscriber.clone(), notify);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc(
                crate::domains::stream::metrics::METRIC_NOTIFY_DROPS_TOTAL,
            );
        }
    }
}
