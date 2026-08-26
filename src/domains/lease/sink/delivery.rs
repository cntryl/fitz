//! Publish fan-out: notifying subscribers of a lease state change.

#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;

use super::model::LeaseDomainRuntime;

impl LeaseDomainRuntime<'_> {
    pub(in crate::domains::lease::sink) fn notify_lease_change(
        &self,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        if !self.core.families.lock().contains_key(&key.family.as_u64()) {
            return;
        }

        let event = crate::runtime::DomainPublishEvent::new(
            key.family,
            key.to_route(),
            bytes::Bytes::new(),
        );
        self.handle_domain_publish(&event);
    }

    /// Removes both the per-session references and their matching per-key queue entries.
    pub(in crate::domains::lease::sink) fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        let family_id = event.family_id.as_u64();
        let targets = {
            let families = self.core.families.lock();
            let mut targets = Vec::new();
            if let Some(family_state) = families.get(&family_id) {
                family_state.for_each_matching(event, |sub| {
                    targets.push((
                        sub.session_id,
                        sub.subscription_id,
                        sub.route_address.clone(),
                    ));
                });
            }
            targets
        };

        #[cfg(test)]
        let mut payload_encoder =
            crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        for (session_id, subscription_id, route_address) in targets {
            #[cfg(test)]
            {
                let notify_payload = crate::dispatch::protocol::lease_codec::encode_notify_into(
                    &mut payload_encoder,
                    subscription_id,
                    event.route.as_str(),
                    &event.payload,
                );
                let notify_ctx = FrameContext::new(
                    session_id,
                    crate::dispatch::protocol::frame::ChannelId::Sub,
                    crate::dispatch::protocol::tlv::MessageType::new(
                        crate::dispatch::protocol::lease_codec::msg_type::NOTIFY,
                    ),
                    bytes::Bytes::from(notify_payload),
                    event.family_id,
                );

                let notify_envelope = Envelope::new(route_address, notify_ctx);
                if let Err(error) = self.core.router.route(notify_envelope) {
                    self.record_dropped_delivery(
                        super::observability::DeliveryDropKind::Notification,
                        session_id,
                        event.family_id,
                        &error,
                    );
                }
            }

            #[cfg(not(test))]
            {
                let notification = crate::domains::lease::LeaseClientNotification::new(
                    session_id,
                    event.family_id,
                    subscription_id,
                    event.route.clone(),
                    event.payload.clone(),
                );
                let notify_envelope = Envelope::new(route_address, notification);
                if let Err(error) = self.core.router.route(notify_envelope) {
                    self.record_dropped_delivery(
                        super::observability::DeliveryDropKind::Notification,
                        session_id,
                        event.family_id,
                        &error,
                    );
                }
            }
        }
    }
}
