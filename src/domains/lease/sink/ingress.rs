//! Envelope ingress: validate an inbound envelope, parse it into a Lease
//! request, and dispatch to the subscriptions/acquire/response layers.

use super::model::{DeliveryError, LeaseAcquireRequest, LeaseDomainRuntime};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::Envelope;

pub(super) enum LeaseRequestView<'a> {
    Borrowed(&'a crate::domains::lease::LeaseClientRequest),
    #[cfg(test)]
    Owned(crate::domains::lease::LeaseClientRequest),
}

impl LeaseRequestView<'_> {
    fn meta(&self) -> crate::runtime::ClientFrameMeta {
        match self {
            Self::Borrowed(request) => request.meta,
            #[cfg(test)]
            Self::Owned(request) => request.meta,
        }
    }

    fn frame(&self) -> &Result<crate::domains::lease::protocol::LeaseClientFrame, String> {
        match self {
            Self::Borrowed(request) => &request.frame,
            #[cfg(test)]
            Self::Owned(request) => &request.frame,
        }
    }
}

impl LeaseDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        Self::log_delivery(envelope);

        if let Some(request) =
            envelope.payload::<crate::domains::lease::protocol::PreparedLeaseClientRequest>()
        {
            if !Self::valid_request_envelope(envelope, request.meta) {
                let response = Self::error_response("route family mismatch");
                let response_meta = Self::response_meta_for_source(envelope, request.meta);
                self.route_lease_response(envelope, response_meta, &response, None);
                return Ok(());
            }
            self.handle_prepared_request(envelope, request);
            return Ok(());
        }

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta();
        let request_started = self.record_request_start();

        if !Self::valid_request_envelope(envelope, meta) {
            let response = Self::error_response("route family mismatch");
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_lease_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority lane) and
        // jumped ahead of it. Reject rather than silently recreating a
        // lease, waiter, or subscription for a session that is already gone
        // and will never be cleaned up again.
        if self.is_cleaned_up_session(meta.session_id) {
            let response = Self::error_response("session already closed");
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_lease_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let Some(parsed_frame) =
            self.parse_request_frame(envelope, meta, request.frame(), request_started)
        else {
            return Ok(());
        };

        match parsed_frame {
            crate::domains::lease::protocol::LeaseClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg);
                Ok(())
            }
            crate::domains::lease::protocol::LeaseClientFrame::Op(lease_msg) => {
                self.handle_actor_operation_frame(envelope, meta, request_started, lease_msg);
                Ok(())
            }
        }
    }

    fn handle_prepared_request(
        &self,
        envelope: &Envelope,
        request: &crate::domains::lease::protocol::PreparedLeaseClientRequest,
    ) {
        let meta = request.meta;
        let request_started = self.record_request_start();
        if self.is_cleaned_up_session(meta.session_id) {
            let response = Self::error_response("session already closed");
            self.route_lease_response(envelope, meta, &response, request_started);
            return;
        }
        let Some(operation) =
            self.parse_prepared_request_frame(envelope, meta, &request.frame, request_started)
        else {
            return;
        };
        if Self::prepared_operation_family(operation) != meta.route_family {
            let response = Self::error_response("route family mismatch");
            self.route_lease_response(envelope, meta, &response, request_started);
            return;
        }
        self.handle_prepared_operation_frame(envelope, meta, request_started, operation);
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        crate::runtime::ingress_support::ensure_actor_active(self.active)
    }

    fn handle_domain_publish_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                crate::observability::counter_inc("fitz_lease_publish_family_mismatch_total");
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        crate::runtime::ingress_support::log_envelope_received(
            "lease",
            "Lease domain sink: received envelope",
            envelope,
        );
    }

    fn extract_request(envelope: &Envelope) -> Result<Option<LeaseRequestView<'_>>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            Ok(Some(request))
        } else {
            tracing::warn!(
                domain = "lease",
                "Envelope payload was not LeaseClientRequest"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn record_request_start(&self) -> Option<std::time::Instant> {
        self.core
            .metrics
            .as_ref()
            .map(crate::domains::lease::LeaseMetrics::record_request_start)
    }

    fn parse_request_frame<'a>(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: &'a Result<crate::domains::lease::protocol::LeaseClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<&'a crate::domains::lease::protocol::LeaseClientFrame> {
        match frame {
            Ok(msg) => {
                tracing::debug!(
                    domain = "lease",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    "Lease: parsed message successfully"
                );
                Some(msg)
            }
            Err(error) => {
                tracing::warn!(domain = "lease", error = %error, "Failed to parse lease message");
                let response = Self::error_response(error);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                self.route_lease_response(envelope, response_meta, &response, request_started);
                None
            }
        }
    }

    fn parse_prepared_request_frame<'a>(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: &'a Result<crate::domains::lease::protocol::PreparedLeaseOperation, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<&'a crate::domains::lease::protocol::PreparedLeaseOperation> {
        match frame {
            Ok(operation) => {
                tracing::debug!(
                    domain = "lease",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    "Lease: prepared message successfully"
                );
                Some(operation)
            }
            Err(error) => {
                tracing::warn!(domain = "lease", error = %error, "Failed to prepare lease message");
                let response = Self::error_response(error);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                self.route_lease_response(envelope, response_meta, &response, request_started);
                None
            }
        }
    }

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        lease_msg: &crate::domains::lease::protocol::LeaseMessage,
    ) {
        if !Self::valid_lease_message(envelope, meta, lease_msg) {
            let response = Self::error_response("route family mismatch");
            self.route_lease_response(envelope, meta, &response, request_started);
            return;
        }

        if matches!(
            lease_msg,
            crate::domains::lease::protocol::LeaseMessage::Tick
        ) {
            self.sweep_expired_state();
            if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started)
            {
                metrics.record_success(started_at);
            }
            return;
        }

        let scoped_owner_id = Self::scope_operation_owner(meta.session_id, lease_msg);
        let acquire_key = match lease_msg {
            crate::domains::lease::protocol::LeaseMessage::Acquire {
                family_id, route, ..
            } => crate::domains::lease::protocol::LeaseKey::from_route(*family_id, route),
            _ => None,
        };
        let domain_response =
            self.dispatch_actor_operation(envelope, meta, lease_msg, scoped_owner_id.as_deref());
        if !self.route_lease_response(envelope, meta, &domain_response, request_started) {
            if let Some(key) = acquire_key.as_ref() {
                self.rollback_undeliverable_acquire(key, meta.session_id, &domain_response);
            }
        }
    }

    fn scope_operation_owner(
        session_id: u64,
        lease_msg: &crate::domains::lease::protocol::LeaseMessage,
    ) -> Option<String> {
        use crate::domains::lease::protocol::LeaseMessage;

        match lease_msg {
            LeaseMessage::Acquire { owner_id, .. }
            | LeaseMessage::Extend { owner_id, .. }
            | LeaseMessage::Release { owner_id, .. } => Some(
                crate::domains::lease::protocol::session_scoped_owner_id(session_id, owner_id),
            ),
            LeaseMessage::Query { .. } | LeaseMessage::List { .. } | LeaseMessage::Tick => None,
        }
    }

    fn dispatch_actor_operation(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        lease_msg: &crate::domains::lease::protocol::LeaseMessage,
        scoped_owner_id: Option<&str>,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::{LeaseKey, LeaseMessage, LeaseResponse};

        match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => {
                if owner_id.len() > crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES {
                    return LeaseResponse::Error(format!(
                        "owner_id exceeds maximum length of {} bytes",
                        crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES
                    ));
                }
                match LeaseKey::from_route(*family_id, route) {
                    Some(key) => self.handle_acquire(LeaseAcquireRequest {
                        key,
                        owner_session_id: meta.session_id,
                        owner_id: scoped_owner_id
                            .expect("acquire owner must be scoped before dispatch")
                            .to_string(),
                        ttl_secs: *ttl_secs,
                        wait_seconds: *wait_seconds,
                        reply_source: envelope.destination().clone(),
                        reply_destination: envelope.source().cloned(),
                        channel: meta.channel,
                        route_family: meta.route_family,
                    }),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Extend {
                family_id,
                route,
                fencing_token,
                ttl_secs,
                ..
            } => match LeaseKey::from_route(*family_id, route) {
                Some(key) => self.handle_extend(
                    &key,
                    scoped_owner_id.expect("extend owner must be scoped before dispatch"),
                    *fencing_token,
                    *ttl_secs,
                ),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Release {
                family_id,
                route,
                fencing_token,
                ..
            } => match LeaseKey::from_route(*family_id, route) {
                Some(key) => self.handle_release(
                    &key,
                    scoped_owner_id.expect("release owner must be scoped before dispatch"),
                    *fencing_token,
                ),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(*family_id, route) {
                    Some(key) => self.handle_query(&key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::List {
                family_id,
                pattern,
                cursor,
                limit,
            } => self.handle_list(*family_id, pattern, *cursor, *limit, meta.session_id),
            LeaseMessage::Tick => unreachable!("tick is handled before operation dispatch"),
        }
    }

    fn handle_prepared_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        operation: &crate::domains::lease::protocol::PreparedLeaseOperation,
    ) {
        use crate::domains::lease::protocol::{LeaseResponse, PreparedLeaseOperation};

        if Self::prepared_operation_family(operation) != meta.route_family {
            let response = Self::error_response("route family mismatch");
            self.route_lease_response(envelope, meta, &response, request_started);
            return;
        }

        let domain_response = match operation {
            PreparedLeaseOperation::Acquire {
                key,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => {
                // `owner_id` here is already session-scoped (prefixed
                // `session:{id}:`); bounding the scoped length is a close
                // enough proxy for bounding the client-supplied part, since
                // the prefix overhead is a handful of bytes.
                let scoped_limit = crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES + 32;
                if owner_id.len() > scoped_limit {
                    LeaseResponse::Error(format!(
                        "owner_id exceeds maximum length of {} bytes",
                        crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES
                    ))
                } else {
                    self.handle_acquire(LeaseAcquireRequest {
                        key: key.clone(),
                        owner_session_id: meta.session_id,
                        owner_id: owner_id.clone(),
                        ttl_secs: *ttl_secs,
                        wait_seconds: *wait_seconds,
                        reply_source: envelope.destination().clone(),
                        reply_destination: envelope.source().cloned(),
                        channel: meta.channel,
                        route_family: meta.route_family,
                    })
                }
            }
            PreparedLeaseOperation::Extend {
                key,
                owner_id,
                fencing_token,
                ttl_secs,
            } => self.handle_extend(key, owner_id, *fencing_token, *ttl_secs),
            PreparedLeaseOperation::Release {
                key,
                owner_id,
                fencing_token,
            } => self.handle_release(key, owner_id, *fencing_token),
            PreparedLeaseOperation::Query { key } => self.handle_query(key),
        };

        if matches!(domain_response, LeaseResponse::NotFound) {
            tracing::debug!(
                domain = "lease",
                "Lease prepared operation returned not found"
            );
        }
        let delivered =
            self.route_lease_response(envelope, meta, &domain_response, request_started);
        if !delivered {
            if let PreparedLeaseOperation::Acquire { key, .. } = operation {
                self.rollback_undeliverable_acquire(key, meta.session_id, &domain_response);
            }
        }
    }

    fn request_from_envelope(envelope: &Envelope) -> Option<LeaseRequestView<'_>> {
        if let Some(request) = envelope.payload::<crate::domains::lease::LeaseClientRequest>() {
            return Some(LeaseRequestView::Borrowed(request));
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::lease_codec::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::dispatch::protocol::lease_codec::ParsedLeaseFrame::Op(message) => {
                    crate::domains::lease::LeaseClientFrame::Op(message)
                }
                crate::dispatch::protocol::lease_codec::ParsedLeaseFrame::Sub(message) => {
                    crate::domains::lease::LeaseClientFrame::Sub(message)
                }
            });
            Some(LeaseRequestView::Owned(
                crate::domains::lease::LeaseClientRequest::new(meta, parsed),
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn valid_request_envelope(envelope: &Envelope, meta: crate::runtime::ClientFrameMeta) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    fn error_response(reason: &str) -> crate::domains::lease::protocol::LeaseResponse {
        crate::domains::lease::protocol::LeaseResponse::Error(reason.to_string())
    }

    fn valid_lease_message(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::lease::protocol::LeaseMessage,
    ) -> bool {
        use crate::domains::lease::protocol::LeaseMessage;
        match message {
            LeaseMessage::Acquire { family_id, .. }
            | LeaseMessage::Extend { family_id, .. }
            | LeaseMessage::Release { family_id, .. }
            | LeaseMessage::Query { family_id, .. }
            | LeaseMessage::List { family_id, .. } => *family_id == meta.route_family,
            LeaseMessage::Tick => {
                meta.channel == crate::runtime::ClientChannel::Internal
                    && envelope.source().is_none()
            }
        }
    }

    fn prepared_operation_family(
        operation: &crate::domains::lease::protocol::PreparedLeaseOperation,
    ) -> crate::runtime::routing::RouteFamily {
        use crate::domains::lease::protocol::PreparedLeaseOperation;
        match operation {
            PreparedLeaseOperation::Acquire { key, .. }
            | PreparedLeaseOperation::Extend { key, .. }
            | PreparedLeaseOperation::Release { key, .. }
            | PreparedLeaseOperation::Query { key } => key.family,
        }
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::dispatch::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::dispatch::protocol::frame::ChannelId::Control => {
            crate::runtime::ClientChannel::Control
        }
        crate::dispatch::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::dispatch::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::dispatch::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::dispatch::protocol::frame::ChannelId::Internal => {
            crate::runtime::ClientChannel::Internal
        }
    }
}
