//! Envelope ingress: validate an inbound envelope, parse it into an RPC
//! message, and dispatch to the registration/delivery/response layers.

use super::state_model::{
    DeliveryError, Envelope, Instant, RpcClientRequest, RpcClientResponseBody,
    RpcDeliveryOutcome as DeliveryOutcome, RpcDomainRuntime, RPC_MSG_TYPE_REQUEST,
};
use crate::domains::rpc::protocol::RpcMessage;

impl RpcDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;
        Self::log_delivery(envelope);

        let request = Self::extract_request(envelope)?;
        let meta = request.meta;
        let request_started = self.record_request_start();

        if !Self::valid_request_envelope(envelope, meta) {
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_rpc_client_response(
                envelope,
                response_meta,
                &RpcClientResponseBody::Error("route family mismatch".to_string()),
            );
            return Ok(());
        }

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority lane) and
        // jumped ahead of it. Reject rather than silently recreating a worker
        // registration or pending request for a session that is already gone
        // and will never be cleaned up again.
        if self.is_cleaned_up_session(meta.session_id) {
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_rpc_client_response(
                envelope,
                response_meta,
                &RpcClientResponseBody::Error("session already closed".to_string()),
            );
            return Ok(());
        }

        Self::log_parse_start(meta);

        let Some(rpc_msg) = self.parse_request_message(
            envelope,
            meta,
            request.message,
            &request.raw_payload,
            request_started,
        ) else {
            return Ok(());
        };

        if !Self::valid_rpc_message(meta, &rpc_msg) {
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_rpc_client_response(
                envelope,
                response_meta,
                &RpcClientResponseBody::Error("route family mismatch".to_string()),
            );
            return Ok(());
        }

        let (response, snapshot_policy, request_failed) =
            self.handle_rpc_message(envelope, &meta, rpc_msg);

        self.complete_request(
            envelope,
            meta,
            response,
            snapshot_policy,
            request_failed,
            request_started,
        );

        Ok(())
    }

    fn handle_rpc_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        rpc_msg: RpcMessage,
    ) -> DeliveryOutcome {
        match rpc_msg {
            RpcMessage::RegisterWorker {
                worker_addr,
                max_concurrent,
            } => self.handle_register_worker_message(envelope, meta, worker_addr, max_concurrent),
            RpcMessage::UnregisterWorker { worker_addr } => {
                self.handle_unregister_worker_message(meta, worker_addr)
            }
            RpcMessage::Request(req) => self.handle_request_message(envelope, meta, req),
            RpcMessage::Response(resp) => self.handle_response_message(envelope, meta, &resp),
        }
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        crate::runtime::ingress_support::ensure_actor_active(self.active)
    }

    fn log_delivery(envelope: &Envelope) {
        crate::runtime::ingress_support::log_envelope_received(
            "rpc",
            "RPC domain sink: received envelope",
            envelope,
        );
    }

    fn extract_request(envelope: &Envelope) -> Result<RpcClientRequest, DeliveryError> {
        Self::request_from_envelope(envelope).ok_or_else(|| {
            tracing::warn!(domain = "rpc", "Envelope payload was not RpcClientRequest");
            DeliveryError::ActorStopped
        })
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(crate::domains::rpc::RpcMetrics::record_request_start)
    }

    fn log_parse_start(meta: crate::runtime::ClientFrameMeta) {
        tracing::debug!(
            domain = "rpc",
            session = meta.session_id,
            msg_type = meta.message_type,
            "RPC: parsing request"
        );
    }

    fn parse_request_message(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: Result<
            crate::domains::rpc::protocol::RpcMessage,
            crate::domains::rpc::protocol::RpcDecodeError,
        >,
        raw_payload: &[u8],
        request_started: Option<Instant>,
    ) -> Option<crate::domains::rpc::protocol::RpcMessage> {
        match message {
            Ok(msg) => Some(msg),
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "rpc", error = %e, "Failed to parse RPC message");
                let (error_code, error_message) = match &e {
                    crate::domains::rpc::protocol::RpcDecodeError::InvalidCallRoute(_) => (
                        crate::dispatch::protocol::error_codes::rpc::ERR_INVALID_ROUTE,
                        "Invalid RPC call route",
                    ),
                    crate::domains::rpc::protocol::RpcDecodeError::InvalidRegistrationPattern(
                        _,
                    ) => (
                        crate::dispatch::protocol::error_codes::rpc::ERR_INVALID_SUBSCRIPTION_PATTERN,
                        "Invalid RPC registration pattern",
                    ),
                    crate::domains::rpc::protocol::RpcDecodeError::StructurallyUndecodable(_) => (
                        crate::dispatch::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                        "RPC message parse failed",
                    ),
                };
                if meta.message_type == RPC_MSG_TYPE_REQUEST {
                    if let Ok(correlation_id) =
                        crate::dispatch::protocol::rpc_codec::extract_request_correlation_id(
                            raw_payload,
                        )
                    {
                        self.route_rpc_terminal_error_response(
                            envelope,
                            Self::response_meta_for_source(envelope, meta),
                            correlation_id,
                            error_code,
                            error_message,
                        );
                        return None;
                    }
                }
                self.route_rpc_client_response(
                    envelope,
                    Self::response_meta_for_source(envelope, meta),
                    &RpcClientResponseBody::CodeError {
                        code: error_code,
                        message: error_message.to_string(),
                    },
                );
                None
            }
        }
    }

    fn valid_request_envelope(envelope: &Envelope, meta: crate::runtime::ClientFrameMeta) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    pub(super) fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    fn valid_rpc_message(
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::rpc::protocol::RpcMessage,
    ) -> bool {
        use crate::domains::rpc::protocol::RpcMessage;

        match message {
            RpcMessage::RegisterWorker { worker_addr, .. }
            | RpcMessage::UnregisterWorker { worker_addr } => {
                *worker_addr.family() == meta.route_family
            }
            RpcMessage::Request(request) => request.family_id == meta.route_family,
            RpcMessage::Response(_) => true,
        }
    }

    fn complete_request(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: Option<RpcClientResponseBody>,
        snapshot_policy: Option<bool>,
        request_failed: bool,
        request_started: Option<Instant>,
    ) {
        if let Some(force_snapshot) = snapshot_policy {
            self.schedule_admin_snapshot(force_snapshot);
        }

        if let Some(response) = response {
            self.route_rpc_client_response(envelope, meta, &response);
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if request_failed {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
    }
}
