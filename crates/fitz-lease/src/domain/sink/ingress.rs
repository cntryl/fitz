//! Envelope ingress: validate an inbound envelope, parse it into a Lease
//! request, and dispatch to the subscriptions/acquire/response layers.

use super::model::{LeaseAcquireRequest, LeaseFamilyRuntime};
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::SessionScoped as _;
use fitz_core::runtime::{DeliveryError, Envelope};

pub(super) enum LeaseRequestView<'a> {
    Borrowed(&'a crate::domain::LeaseClientRequest),
    #[cfg(test)]
    Owned(crate::domain::LeaseClientRequest),
}

impl LeaseRequestView<'_> {
    fn meta(&self) -> fitz_core::runtime::ClientFrameMeta {
        match self {
            Self::Borrowed(request) => request.meta,
            #[cfg(test)]
            Self::Owned(request) => request.meta,
        }
    }

    fn frame(&self) -> &Result<crate::domain::protocol::LeaseClientFrame, String> {
        match self {
            Self::Borrowed(request) => &request.frame,
            #[cfg(test)]
            Self::Owned(request) => &request.frame,
        }
    }
}

impl LeaseFamilyRuntime<'_> {
    pub(super) fn deliver_envelope(&mut self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        Self::log_delivery(envelope);

        if let Some(request) =
            envelope.payload::<crate::domain::protocol::PreparedLeaseClientRequest>()
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
        if self.is_cleaned_up_request(meta.session_id, envelope) {
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
            crate::domain::protocol::LeaseClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg);
                Ok(())
            }
            crate::domain::protocol::LeaseClientFrame::Op(lease_msg) => {
                self.handle_actor_operation_frame(envelope, meta, request_started, lease_msg);
                Ok(())
            }
        }
    }

    fn handle_prepared_request(
        &mut self,
        envelope: &Envelope,
        request: &crate::domain::protocol::PreparedLeaseClientRequest,
    ) {
        let meta = request.meta;
        let request_started = self.record_request_start();
        if self.is_cleaned_up_request(meta.session_id, envelope) {
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

    fn ensure_active(&mut self) -> Result<(), DeliveryError> {
        fitz_core::runtime::ingress_support::ensure_actor_active(self.active)
    }

    fn handle_domain_publish_envelope(&mut self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<fitz_core::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                fitz_core::observability::counter_inc("fitz_lease_publish_family_mismatch_total");
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        fitz_core::runtime::ingress_support::log_envelope_received(
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

    fn record_request_start(&mut self) -> Option<std::time::Instant> {
        self.core
            .metrics
            .as_ref()
            .map(crate::domain::LeaseMetrics::record_request_start)
    }

    fn parse_request_frame<'a>(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        frame: &'a Result<crate::domain::protocol::LeaseClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<&'a crate::domain::protocol::LeaseClientFrame> {
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
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        frame: &'a Result<crate::domain::protocol::PreparedLeaseOperation, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<&'a crate::domain::protocol::PreparedLeaseOperation> {
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
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        lease_msg: &crate::domain::protocol::LeaseMessage,
    ) {
        if !Self::valid_lease_message(envelope, meta, lease_msg) {
            let response = Self::error_response("route family mismatch");
            self.route_lease_response(envelope, meta, &response, request_started);
            return;
        }

        if matches!(lease_msg, crate::domain::protocol::LeaseMessage::Tick) {
            self.sweep_expired_state();
            if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started)
            {
                metrics.record_success(started_at);
            }
            return;
        }

        let scoped_owner_id = Self::scope_operation_owner(meta.session_id, lease_msg);
        let acquire_key = match lease_msg {
            crate::domain::protocol::LeaseMessage::Acquire {
                family_id, route, ..
            } => crate::domain::protocol::LeaseKey::from_route(*family_id, route),
            _ => None,
        };
        let domain_response =
            self.dispatch_actor_operation(envelope, meta, lease_msg, scoped_owner_id.as_deref());
        let delivered =
            self.route_lease_response(envelope, meta, &domain_response, request_started);
        if let crate::domain::protocol::LeaseMessage::List {
            family_id,
            pattern,
            cursor,
            ..
        } = lease_msg
        {
            self.finish_list_response_delivery(
                *family_id,
                pattern.as_str(),
                *cursor,
                meta.session_id,
                &domain_response,
                delivered,
            );
        }
        if !delivered {
            if let Some(key) = acquire_key.as_ref() {
                self.rollback_undeliverable_acquire(key, meta.session_id, &domain_response);
            }
        }
    }

    fn scope_operation_owner(
        session_id: u64,
        lease_msg: &crate::domain::protocol::LeaseMessage,
    ) -> Option<String> {
        use crate::domain::protocol::LeaseMessage;

        match lease_msg {
            LeaseMessage::Acquire { owner_id, .. }
            | LeaseMessage::Extend { owner_id, .. }
            | LeaseMessage::Release { owner_id, .. } => Some(
                crate::domain::protocol::session_scoped_owner_id(session_id, owner_id),
            ),
            LeaseMessage::Query { .. } | LeaseMessage::List { .. } | LeaseMessage::Tick => None,
        }
    }

    fn dispatch_actor_operation(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        lease_msg: &crate::domain::protocol::LeaseMessage,
        scoped_owner_id: Option<&str>,
    ) -> crate::domain::protocol::LeaseResponse {
        use crate::domain::protocol::{LeaseKey, LeaseMessage, LeaseResponse};

        match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => {
                if owner_id.len() > crate::domain::protocol::LEASE_MAX_OWNER_ID_BYTES {
                    return LeaseResponse::Error(format!(
                        "owner_id exceeds maximum length of {} bytes",
                        crate::domain::protocol::LEASE_MAX_OWNER_ID_BYTES
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
                        correlation: meta.correlation,
                    }),
                    None => Self::error_response("invalid lease route"),
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
                None => Self::error_response("invalid lease route"),
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
                None => Self::error_response("invalid lease route"),
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(*family_id, route) {
                    Some(key) => self.handle_query(&key),
                    None => Self::error_response("invalid lease route"),
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
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        operation: &crate::domain::protocol::PreparedLeaseOperation,
    ) {
        use crate::domain::protocol::{LeaseResponse, PreparedLeaseOperation};

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
                let scoped_limit = crate::domain::protocol::LEASE_MAX_OWNER_ID_BYTES + 32;
                if owner_id.len() > scoped_limit {
                    LeaseResponse::Error(format!(
                        "owner_id exceeds maximum length of {} bytes",
                        crate::domain::protocol::LEASE_MAX_OWNER_ID_BYTES
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
                        correlation: meta.correlation,
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
        if let Some(request) = envelope.payload::<crate::domain::LeaseClientRequest>() {
            return Some(LeaseRequestView::Borrowed(request));
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
            });
            let meta = fitz_core::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            )
            .with_correlation(frame_ctx.correlation);
            let parsed = crate::wire::lease_codec::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::wire::lease_codec::ParsedLeaseFrame::Op(message) => {
                    crate::domain::LeaseClientFrame::Op(message)
                }
                crate::wire::lease_codec::ParsedLeaseFrame::Sub(message) => {
                    crate::domain::LeaseClientFrame::Sub(message)
                }
            });
            Some(LeaseRequestView::Owned(
                crate::domain::LeaseClientRequest::new(meta, parsed),
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn valid_request_envelope(
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
    ) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    fn response_meta_for_source(
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
    ) -> fitz_core::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    fn error_response(reason: &str) -> crate::domain::protocol::LeaseResponse {
        crate::domain::protocol::LeaseResponse::Error(reason.to_string())
    }

    fn valid_lease_message(
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        message: &crate::domain::protocol::LeaseMessage,
    ) -> bool {
        use crate::domain::protocol::LeaseMessage;
        match message {
            LeaseMessage::Acquire { family_id, .. }
            | LeaseMessage::Extend { family_id, .. }
            | LeaseMessage::Release { family_id, .. }
            | LeaseMessage::Query { family_id, .. }
            | LeaseMessage::List { family_id, .. } => *family_id == meta.route_family,
            LeaseMessage::Tick => {
                meta.channel == fitz_core::runtime::ClientChannel::Internal
                    && envelope.source().is_none()
            }
        }
    }

    fn prepared_operation_family(
        operation: &crate::domain::protocol::PreparedLeaseOperation,
    ) -> fitz_core::runtime::routing::RouteFamily {
        use crate::domain::protocol::PreparedLeaseOperation;
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
    channel: crate::wire::frame::ChannelId,
) -> fitz_core::runtime::ClientChannel {
    match channel {
        crate::wire::frame::ChannelId::Control => fitz_core::runtime::ClientChannel::Control,
        crate::wire::frame::ChannelId::Pub => fitz_core::runtime::ClientChannel::Pub,
        crate::wire::frame::ChannelId::Sub => fitz_core::runtime::ClientChannel::Sub,
        crate::wire::frame::ChannelId::Rpc => fitz_core::runtime::ClientChannel::Rpc,
        crate::wire::frame::ChannelId::Lease => fitz_core::runtime::ClientChannel::Lease,
        crate::wire::frame::ChannelId::Internal => fitz_core::runtime::ClientChannel::Internal,
    }
}
