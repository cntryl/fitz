//! Response encoding and best-effort routing back to the requester or to a
//! queued waiter.

use super::model::LeaseDomainRuntime;
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;

impl LeaseDomainRuntime<'_> {
    pub(in crate::domains::lease::sink) fn send_waiter_response(
        &self,
        waiter: &super::model::PendingAcquire,
        response: &crate::domains::lease::protocol::LeaseResponse,
    ) -> bool {
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
                crate::protocol::test_support::channel_id_from_client(waiter.channel),
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
                super::observability::DeliveryDropKind::Response,
                waiter.owner_session_id,
                waiter.route_family,
                &error,
            );
            return false;
        }
        true
    }

    pub(in crate::domains::lease::sink) fn route_lease_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::lease::protocol::LeaseResponse,
        request_started: Option<std::time::Instant>,
    ) -> bool {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes =
                crate::dispatch::protocol::lease_codec::encode_domain_response(response);
            FrameContext::new(
                meta.session_id,
                crate::protocol::test_support::channel_id_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = crate::domains::lease::LeaseClientResponse::new(meta, response.clone());

        let mut delivered = false;
        if let Some(response_envelope) = envelope.try_reply_to(response_ctx.clone()) {
            let response_sink = self
                .core
                .router
                .resolve_sink(response_envelope.destination());
            if let Some(sink) = response_sink {
                if let Err(error) = self
                    .core
                    .router
                    .route_to_resolved_sink(response_envelope, &sink)
                {
                    if matches!(
                        error,
                        crate::runtime::RouteError::DeliveryFailed(
                            _,
                            crate::runtime::DeliveryError::ActorStopped
                        )
                    ) {
                        if let Some(retry) = envelope.try_reply_to(response_ctx) {
                            match self.core.router.route(retry) {
                                Ok(()) => delivered = true,
                                Err(retry_error) => self.record_dropped_delivery(
                                    super::observability::DeliveryDropKind::Response,
                                    meta.session_id,
                                    meta.route_family,
                                    &retry_error,
                                ),
                            }
                        }
                    } else {
                        self.record_dropped_delivery(
                            super::observability::DeliveryDropKind::Response,
                            meta.session_id,
                            meta.route_family,
                            &error,
                        );
                    }
                } else {
                    delivered = true;
                }
            } else {
                match self.core.router.route(response_envelope) {
                    Ok(()) => delivered = true,
                    Err(error) => self.record_dropped_delivery(
                        super::observability::DeliveryDropKind::Response,
                        meta.session_id,
                        meta.route_family,
                        &error,
                    ),
                }
            }
        }

        if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started) {
            if Self::lease_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
        delivered
    }
}
