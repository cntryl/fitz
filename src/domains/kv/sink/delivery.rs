//! Request delivery, lifecycle rejection, parsing, and dispatch selection.

use super::state::KvDomainRuntime;
use crate::domains::kv::{KvClientFrame, KvClientRequest};
use crate::runtime::{DeliveryError, Envelope};
use std::sync::atomic::Ordering;

impl KvDomainRuntime<'_> {
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
            let response = Self::error_response("route family mismatch");
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_kv_response(envelope, response_meta, &response, request_started)?;
            return Ok(());
        }

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority lane) and
        // jumped ahead of it. Reject rather than silently recreating
        // per-session state -- an actor and, for a write BEGIN, a resource
        // lock -- for a session that is already gone and will never be
        // cleaned up again.
        if self.is_cleaned_up_session(meta.session_id) {
            let response = Self::error_response("session already closed");
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_kv_response(envelope, response_meta, &response, request_started)?;
            return Ok(());
        }

        let operation_started = Self::record_operation_start();
        let Some(parsed_frame) =
            self.parse_request_frame(envelope, meta, request.frame, request_started)
        else {
            return Ok(());
        };

        match parsed_frame {
            KvClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg)
            }
            KvClientFrame::Op(kv_message) => self.handle_actor_operation_frame(
                envelope,
                meta,
                request_started,
                operation_started,
                kv_message,
            ),
        }
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        Ok(())
    }

    fn log_delivery(envelope: &Envelope) {
        tracing::debug!(
            domain = "kv",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "KV domain sink: received envelope"
        );
    }

    fn extract_request(envelope: &Envelope) -> Result<KvClientRequest, DeliveryError> {
        Self::request_from_envelope(envelope).ok_or_else(|| {
            tracing::warn!(
                domain = "kv",
                destination = ?envelope.destination(),
                "Envelope payload was not KvClientRequest"
            );
            DeliveryError::ActorStopped
        })
    }

    fn record_operation_start() -> std::time::Instant {
        std::time::Instant::now()
    }

    fn record_request_start(&self) -> std::time::Instant {
        if let Some(metrics) = self.core.metrics.as_ref() {
            metrics.record_request_start()
        } else {
            crate::observability::counter_inc(crate::domains::kv::metrics::METRIC_REQUESTS_TOTAL);
            std::time::Instant::now()
        }
    }

    fn parse_request_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: Result<KvClientFrame, String>,
        request_started: std::time::Instant,
    ) -> Option<KvClientFrame> {
        let parsed_frame = match frame {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    error = %e,
                    "Failed to parse KV message"
                );
                let response = Self::error_response(&e);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                let _ = self.route_kv_response(envelope, response_meta, &response, request_started);
                return None;
            }
        };

        tracing::debug!(
            domain = "kv",
            session = meta.session_id,
            channel = ?meta.channel,
            msg_type = meta.message_type,
            "Parsed KV message successfully"
        );

        Some(parsed_frame)
    }
}
