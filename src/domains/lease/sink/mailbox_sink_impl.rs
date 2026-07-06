use super::model::{
    DeliveryError, Envelope, LeaseAcquireRequest, LeaseDomainActor, LeaseDomainCommand,
    LeaseDomainRuntime, LeaseDomainSink, LeaseSubscription, MailboxSink, Ordering,
    RoutedSubscriptionSet,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
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
            self.handle_prepared_request(envelope, request)?;
            return Ok(());
        }

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta();
        let request_started = self.record_request_start();

        let parsed_frame = self.parse_request_frame(meta, request.frame(), request_started)?;

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
    ) -> Result<(), DeliveryError> {
        let meta = request.meta;
        let request_started = self.record_request_start();
        let operation = self.parse_prepared_request_frame(meta, &request.frame, request_started)?;
        self.handle_prepared_operation_frame(envelope, meta, request_started, operation);
        Ok(())
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
        meta: crate::runtime::ClientFrameMeta,
        frame: &'a Result<crate::domains::lease::protocol::LeaseClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Result<&'a crate::domains::lease::protocol::LeaseClientFrame, DeliveryError> {
        match frame {
            Ok(msg) => {
                tracing::debug!(
                    domain = "lease",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    "Lease: parsed message successfully"
                );
                Ok(msg)
            }
            Err(error) => {
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "lease", error = %error, "Failed to parse lease message");
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn parse_prepared_request_frame<'a>(
        &self,
        meta: crate::runtime::ClientFrameMeta,
        frame: &'a Result<crate::domains::lease::protocol::PreparedLeaseOperation, String>,
        request_started: Option<std::time::Instant>,
    ) -> Result<&'a crate::domains::lease::protocol::PreparedLeaseOperation, DeliveryError> {
        match frame {
            Ok(operation) => {
                tracing::debug!(
                    domain = "lease",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    "Lease: prepared message successfully"
                );
                Ok(operation)
            }
            Err(error) => {
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "lease", error = %error, "Failed to prepare lease message");
                Err(DeliveryError::ActorStopped)
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
        use crate::domains::lease::protocol::{LeaseResponse, LeaseSubscriptionMessage};

        let response = match sub_msg {
            LeaseSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => {
                let pattern_str = pattern.as_str().to_string();
                let subscription_id = {
                    let mut families = self.core.families.lock();
                    let state = families
                        .entry(family_id.as_u64())
                        .or_insert_with(RoutedSubscriptionSet::new);

                    if let Some(existing_id) =
                        state.find_existing_id(*session_id, pattern_str.as_str())
                    {
                        existing_id
                    } else {
                        let new_id = self.core.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        state.insert(
                            *family_id,
                            LeaseSubscription {
                                pattern: crate::runtime::matcher::Pattern::new(
                                    pattern_str.as_str(),
                                ),
                                session_id: *session_id,
                                route_address: subscriber.clone(),
                                subscription_id: new_id,
                            },
                        );
                        new_id
                    }
                };
                LeaseResponse::SubscribeOk { subscription_id }
            }
            LeaseSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                ..
            } => {
                let mut families = self.core.families.lock();
                let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                    state.remove_session_pattern(*family_id, *session_id, pattern.as_str());
                    state.is_empty()
                } else {
                    false
                };
                if remove_family {
                    families.remove(&family_id.as_u64());
                }
                LeaseResponse::UnsubscribeOk
            }
        };

        self.refresh_metrics_gauges();
        self.route_lease_response(envelope, meta, &response, request_started);
    }

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        lease_msg: &crate::domains::lease::protocol::LeaseMessage,
    ) {
        use crate::domains::lease::protocol::{LeaseKey, LeaseMessage, LeaseResponse};

        let session_prefix = meta.session_id.to_string();
        let effective_owner = |owner_id: &str| {
            if owner_id.is_empty() {
                let mut scoped = String::with_capacity("session:".len() + session_prefix.len());
                scoped.push_str("session:");
                scoped.push_str(&session_prefix);
                scoped
            } else {
                let mut scoped = String::with_capacity(
                    "session::".len() + session_prefix.len() + owner_id.len(),
                );
                scoped.push_str("session:");
                scoped.push_str(&session_prefix);
                scoped.push(':');
                scoped.push_str(owner_id);
                scoped
            }
        };

        let domain_response = match lease_msg {
            LeaseMessage::Acquire {
                family_id,
                route,
                owner_id,
                ttl_secs,
                wait_seconds,
            } => match LeaseKey::from_route(*family_id, route) {
                Some(key) => self.handle_acquire(LeaseAcquireRequest {
                    key,
                    owner_session_id: meta.session_id,
                    owner_id: effective_owner(owner_id),
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
                owner_id,
                fencing_token,
                ttl_secs,
            } => match LeaseKey::from_route(*family_id, route) {
                Some(key) => {
                    self.handle_extend(&key, &effective_owner(owner_id), *fencing_token, *ttl_secs)
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Release {
                family_id,
                route,
                owner_id,
                fencing_token,
            } => match LeaseKey::from_route(*family_id, route) {
                Some(key) => self.handle_release(&key, &effective_owner(owner_id), *fencing_token),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(*family_id, route) {
                    Some(key) => self.handle_query(&key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Tick => {
                self.sweep_expired_state();
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    metrics.record_success(started_at);
                }
                return;
            }
        };

        self.route_lease_response(envelope, meta, &domain_response, request_started);
    }

    fn handle_prepared_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        operation: &crate::domains::lease::protocol::PreparedLeaseOperation,
    ) {
        use crate::domains::lease::protocol::{LeaseResponse, PreparedLeaseOperation};

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
            let parsed = crate::protocol::lease_codec::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::protocol::lease_codec::ParsedLeaseFrame::Op(message) => {
                    crate::domains::lease::LeaseClientFrame::Op(message)
                }
                crate::protocol::lease_codec::ParsedLeaseFrame::Sub(message) => {
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
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::protocol::frame::ChannelId::Control => crate::runtime::ClientChannel::Control,
        crate::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::protocol::frame::ChannelId::Internal => crate::runtime::ClientChannel::Internal,
    }
}
