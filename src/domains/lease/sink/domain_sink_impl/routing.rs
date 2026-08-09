use super::super::model::{LeaseDomainRuntime, PendingAcquire};
#[cfg(test)]
use super::test_protocol_channel_from_client;
use super::DeliveryDropKind;
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;

impl LeaseDomainRuntime<'_> {
    fn record_dropped_delivery(
        &self,
        kind: DeliveryDropKind,
        session_id: u64,
        route_family: crate::runtime::routing::RouteFamily,
        error: &impl std::fmt::Display,
    ) {
        match (self.core.metrics.as_ref(), kind) {
            (Some(metrics), DeliveryDropKind::Response) => metrics.record_response_drop(),
            (Some(metrics), DeliveryDropKind::Notification) => metrics.record_notify_drop(),
            (None, DeliveryDropKind::Response) => crate::observability::counter_inc(
                crate::domains::lease::metrics::METRIC_RESPONSE_DROPS_TOTAL,
            ),
            (None, DeliveryDropKind::Notification) => crate::observability::counter_inc(
                crate::domains::lease::metrics::METRIC_NOTIFY_DROPS_TOTAL,
            ),
        }
        tracing::warn!(
            domain = "lease",
            delivery_kind = kind.label(),
            session_id,
            route_family = route_family.as_u64(),
            error = %error,
            "Dropped best-effort Lease delivery"
        );
    }

    pub(in crate::domains::lease::sink) fn send_waiter_response(
        &self,
        waiter: &PendingAcquire,
        response: &crate::domains::lease::protocol::LeaseResponse,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(128);
            let response_bytes =
                crate::dispatch::protocol::lease_codec::encode_domain_response_into(
                    &mut payload_encoder,
                    response,
                );
            FrameContext::new(
                waiter.owner_session_id,
                test_protocol_channel_from_client(waiter.channel),
                crate::dispatch::protocol::tlv::MessageType::new(
                    crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
                ),
                bytes::Bytes::from(response_bytes),
                waiter.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::lease::LeaseClientResponse::new(
            crate::runtime::ClientFrameMeta::new(
                waiter.owner_session_id,
                waiter.channel,
                crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
                waiter.route_family,
            ),
            response.clone(),
        );

        let response_envelope = Envelope::from_route(
            waiter.reply_source.clone(),
            waiter.reply_destination.clone(),
            response_ctx,
        );
        if let Err(error) = self.core.router.route(response_envelope) {
            self.record_dropped_delivery(
                DeliveryDropKind::Response,
                waiter.owner_session_id,
                waiter.route_family,
                &error,
            );
        }
    }

    pub(in crate::domains::lease::sink) fn route_lease_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::lease::protocol::LeaseResponse,
        request_started: Option<std::time::Instant>,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes =
                crate::dispatch::protocol::lease_codec::encode_domain_response(response);
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::lease::LeaseClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let response_sink = self
                .core
                .router
                .resolve_sink(response_envelope.destination());
            if let Some(sink) = response_sink {
                if let Err(error) = sink.deliver(response_envelope) {
                    self.record_dropped_delivery(
                        DeliveryDropKind::Response,
                        meta.session_id,
                        meta.route_family,
                        &error,
                    );
                }
            } else if let Err(error) = self.core.router.route(response_envelope) {
                self.record_dropped_delivery(
                    DeliveryDropKind::Response,
                    meta.session_id,
                    meta.route_family,
                    &error,
                );
            }
        }

        if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started) {
            if Self::lease_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
    }

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
                        DeliveryDropKind::Notification,
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
                        DeliveryDropKind::Notification,
                        session_id,
                        event.family_id,
                        &error,
                    );
                }
            }
        }
    }
}
