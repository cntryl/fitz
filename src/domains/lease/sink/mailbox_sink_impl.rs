use super::model::{
    DeliveryError, Envelope, LeaseAcquireRequest, LeaseDomainActor, LeaseDomainCommand,
    LeaseDomainRuntime, LeaseDomainSink, LeaseSubscription, MailboxSink, Ordering,
    RoutedSubscriptionSet,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::{Actor, Context};

enum LeaseRequestView<'a> {
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

impl MailboxSink for LeaseDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor.try_send(LeaseDomainCommand::Deliver(envelope))
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor
            .try_send_high_priority(LeaseDomainCommand::Deliver(envelope))
    }
}

impl Actor for LeaseDomainActor {
    type Message = LeaseDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.state.runtime();
        match msg {
            LeaseDomainCommand::Deliver(envelope) => {
                if let Err(error) = runtime.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "lease", error = %error, "Lease actor delivery failed");
                }
            }
            LeaseDomainCommand::CleanupSession(session_id) => {
                runtime.cleanup_session(session_id);
            }
            LeaseDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            LeaseDomainCommand::ReadWaiters(reply) => {
                let _ = reply.send(runtime.admin_waiters());
            }
            LeaseDomainCommand::SweepExpiredState => {
                runtime.sweep_expired_state();
            }
            #[cfg(any(test, feature = "benchkit"))]
            LeaseDomainCommand::ApplyAcquireForBench(request, reply) => {
                let _ = reply.send(runtime.handle_acquire(request));
            }
            #[cfg(any(test, feature = "benchkit"))]
            LeaseDomainCommand::ApplyReleaseForBench(key, owner_id, fencing_token, reply) => {
                let _ = reply.send(runtime.handle_release(&key, owner_id.as_str(), fencing_token));
            }
            #[cfg(test)]
            LeaseDomainCommand::ApplyAcquireForTests(request, reply) => {
                let _ = reply.send(runtime.handle_acquire(request));
            }
            #[cfg(test)]
            LeaseDomainCommand::ApplyExtendForTests(
                key,
                owner_id,
                fencing_token,
                ttl_secs,
                reply,
            ) => {
                let _ = reply.send(runtime.handle_extend(
                    &key,
                    owner_id.as_str(),
                    fencing_token,
                    ttl_secs,
                ));
            }
            #[cfg(test)]
            LeaseDomainCommand::ExpireLeaseForTests(key, reply) => {
                let expired = if let Some(lease) = runtime.core.leases.lock().get_mut(&key) {
                    lease.expiry = std::time::Instant::now()
                        .checked_sub(std::time::Duration::from_millis(1))
                        .expect("past instant");
                    true
                } else {
                    false
                };
                let _ = reply.send(expired);
            }
            #[cfg(test)]
            LeaseDomainCommand::ReadPendingWaiterCountForTests(key, reply) => {
                let _ = reply.send(runtime.pending_waiter_count(&key));
            }
            #[cfg(test)]
            LeaseDomainCommand::PanicForTests => {
                panic!("test Lease domain actor panic");
            }
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
        let Some(operation) =
            self.parse_prepared_request_frame(envelope, meta, &request.frame, request_started)
        else {
            return;
        };
        if Self::prepared_operation_family(operation)
            .is_some_and(|family_id| family_id != meta.route_family)
        {
            let response = Self::error_response("route family mismatch");
            self.route_lease_response(envelope, meta, &response, request_started);
            return;
        }
        self.handle_prepared_operation_frame(envelope, meta, request_started, operation);
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return true;
        }

        false
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        Ok(())
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
        tracing::debug!(
            domain = "lease",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Lease domain sink: received envelope"
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

    fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        sub_msg: &crate::domains::lease::protocol::LeaseSubscriptionMessage,
    ) {
        use crate::domains::lease::protocol::LeaseSubscriptionMessage;

        let response = match sub_msg {
            LeaseSubscriptionMessage::Subscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => self.handle_lease_subscribe(
                envelope,
                meta,
                *family_id,
                route,
                *session_id,
                subscriber,
            ),
            LeaseSubscriptionMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => self.handle_lease_unsubscribe(
                envelope,
                meta,
                *family_id,
                route,
                *session_id,
                subscriber,
            ),
        };

        self.refresh_metrics_gauges();
        self.route_lease_response(envelope, meta, &response, request_started);
    }

    fn handle_lease_subscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            let compiled = match Self::compile_exact_lease_subscription_route(route) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let mut families = self.core.families.lock();
            let state = families
                .entry(family_id.as_u64())
                .or_insert_with(RoutedSubscriptionSet::new);
            if let Some(subscription_id) = state.find_existing_id(session_id, route.as_str()) {
                return LeaseResponse::SubscribeOk { subscription_id };
            }
            if let Ok(subscription_id) = self.core.next_sub_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| current.checked_add(1),
            ) {
                state.insert(
                    family_id,
                    LeaseSubscription {
                        route: compiled,
                        session_id,
                        route_address: subscriber.clone(),
                        subscription_id,
                    },
                );
                LeaseResponse::SubscribeOk { subscription_id }
            } else {
                if state.is_empty() {
                    families.remove(&family_id.as_u64());
                }
                LeaseResponse::Error("subscription ID space exhausted".to_string())
            }
        } else {
            LeaseResponse::Error("route family mismatch".to_string())
        }
    }

    fn handle_lease_unsubscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> crate::domains::lease::protocol::LeaseResponse {
        use crate::domains::lease::protocol::LeaseResponse;

        if Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            if let Err(response) = Self::compile_exact_lease_subscription_route(route) {
                return response;
            }
            let mut families = self.core.families.lock();
            let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                state.remove_session_pattern(family_id, session_id, route.as_str());
                state.is_empty()
            } else {
                false
            };
            if remove_family {
                families.remove(&family_id.as_u64());
            }
            LeaseResponse::UnsubscribeOk
        } else {
            LeaseResponse::Error("route family mismatch".to_string())
        }
    }

    fn compile_exact_lease_subscription_route(
        route: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, crate::domains::lease::protocol::LeaseResponse>
    {
        use crate::domains::lease::protocol::LeaseResponse;

        // The exact-only rule lives on the Lease descriptor so ingress and
        // this sink reject the same patterns.
        crate::runtime::DomainKind::Lease
            .descriptor()
            .compile_registration_pattern(route.as_str())
            .map_err(LeaseResponse::InvalidSubscriptionRoute)
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
        let domain_response =
            self.dispatch_actor_operation(envelope, meta, lease_msg, scoped_owner_id.as_deref());
        self.route_lease_response(envelope, meta, &domain_response, request_started);
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
            LeaseMessage::Query { .. } | LeaseMessage::Tick => None,
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
                ttl_secs,
                wait_seconds,
                ..
            } => match LeaseKey::from_route(*family_id, route) {
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
            },
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

        if Self::prepared_operation_family(operation)
            .is_some_and(|family_id| family_id != meta.route_family)
        {
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
            } => self.handle_acquire(LeaseAcquireRequest {
                key: key.clone(),
                owner_session_id: meta.session_id,
                owner_id: owner_id.clone(),
                ttl_secs: *ttl_secs,
                wait_seconds: *wait_seconds,
                reply_source: envelope.destination().clone(),
                reply_destination: envelope.source().cloned(),
                channel: meta.channel,
                route_family: meta.route_family,
            }),
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
            PreparedLeaseOperation::NotFound => LeaseResponse::NotFound,
        };

        if matches!(domain_response, LeaseResponse::NotFound) {
            tracing::debug!(
                domain = "lease",
                "Lease prepared operation returned not found"
            );
        }
        self.route_lease_response(envelope, meta, &domain_response, request_started);
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

    fn valid_subscription_request(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> bool {
        family_id == meta.route_family
            && *subscriber.family() == family_id
            && session_id == meta.session_id
            && envelope.source().is_none_or(|source| source == subscriber)
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
            | LeaseMessage::Query { family_id, .. } => *family_id == meta.route_family,
            LeaseMessage::Tick => {
                meta.channel == crate::runtime::ClientChannel::Internal
                    && envelope.source().is_none()
            }
        }
    }

    fn prepared_operation_family(
        operation: &crate::domains::lease::protocol::PreparedLeaseOperation,
    ) -> Option<crate::runtime::routing::RouteFamily> {
        use crate::domains::lease::protocol::PreparedLeaseOperation;
        match operation {
            PreparedLeaseOperation::Acquire { key, .. }
            | PreparedLeaseOperation::Extend { key, .. }
            | PreparedLeaseOperation::Release { key, .. }
            | PreparedLeaseOperation::Query { key } => Some(key.family),
            PreparedLeaseOperation::NotFound => None,
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
