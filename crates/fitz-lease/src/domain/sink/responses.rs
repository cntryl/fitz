//! Response encoding and best-effort routing back to the requester or to a
//! queued waiter.

use super::model::LeaseFamilyRuntime;
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::Envelope;

impl LeaseFamilyRuntime<'_> {
    pub(in crate::domain::sink) fn send_waiter_response(
        &mut self,
        waiter: &super::model::PendingAcquire,
        response: &crate::domain::protocol::LeaseResponse,
    ) -> bool {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::wire::payload_codec::PayloadEncoder::with_capacity(128);
            let response_bytes = crate::wire::lease_codec::encode_domain_response_into(
                &mut payload_encoder,
                response,
            );
            FrameContext::new(
                waiter.owner_session_id,
                crate::wire::test_support::channel_id_from_client(waiter.channel),
                crate::wire::message::MessageType::new(crate::wire::lease_codec::msg_type::ACQUIRE),
                bytes::Bytes::from(response_bytes),
                waiter.route_family,
            )
            .with_correlation(waiter.correlation)
        };

        // A granted waiter is answering the ACQUIRE that queued it, however much
        // later it arrives, so it carries that request's correlation exactly as
        // an uncontended grant would.
        #[cfg(not(test))]
        let response_ctx = crate::domain::LeaseClientResponse::new(
            fitz_core::runtime::ClientFrameMeta::new(
                waiter.owner_session_id,
                waiter.channel,
                crate::wire::lease_codec::msg_type::ACQUIRE,
                waiter.route_family,
            )
            .with_correlation(waiter.correlation),
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

    pub(in crate::domain::sink) fn route_lease_response(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        response: &crate::domain::protocol::LeaseResponse,
        request_started: Option<std::time::Instant>,
    ) -> bool {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes = crate::wire::lease_codec::encode_domain_response(response);
            FrameContext::new(
                meta.session_id,
                crate::wire::test_support::channel_id_from_client(meta.channel),
                crate::wire::message::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
            .with_correlation(meta.correlation)
            .with_response_final(!matches!(
                response,
                crate::domain::protocol::LeaseResponse::Queued { .. }
            ))
        };

        #[cfg(not(test))]
        let response_ctx = crate::domain::LeaseClientResponse::new(
            meta.with_response_final(!matches!(
                response,
                crate::domain::protocol::LeaseResponse::Queued { .. }
            )),
            response.clone(),
        );

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
                        fitz_core::runtime::RouteError::DeliveryFailed(
                            _,
                            fitz_core::runtime::DeliveryError::ActorStopped
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
