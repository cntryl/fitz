use super::model::*;

impl MailboxSink for LeaseDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        tracing::debug!(
            domain = "lease",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Lease domain sink: received envelope"
        );

        let request = match Self::request_from_envelope(&envelope) {
            Some(request) => request,
            None => {
                tracing::warn!(
                    domain = "lease",
                    "Envelope payload was not LeaseClientRequest"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };
        let meta = request.meta;
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());

        let parsed_frame = match request.frame {
            Ok(msg) => {
                tracing::debug!(
                    domain = "lease",
                    session = meta.session_id,
                    msg_type = meta.message_type,
                    "Lease: parsed message successfully"
                );
                msg
            }
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "lease", error = %e, "Failed to parse lease message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::lease::protocol::{
            LeaseClientFrame, LeaseKey, LeaseMessage, LeaseResponse, LeaseSubscriptionMessage,
        };

        if let LeaseClientFrame::Sub(sub_msg) = parsed_frame {
            let response = match sub_msg {
                LeaseSubscriptionMessage::Subscribe {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let pattern_str = pattern.as_str().to_string();
                    let subscription_id = {
                        let mut families = self.families.lock();
                        let state = families
                            .entry(family_id.as_u64())
                            .or_insert_with(RoutedSubscriptionSet::new);

                        if let Some(existing_id) =
                            state.find_existing_id(session_id, pattern_str.as_str())
                        {
                            existing_id
                        } else {
                            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(
                                family_id,
                                LeaseSubscription {
                                    pattern: crate::runtime::matcher::Pattern::new(
                                        pattern_str.as_str(),
                                    ),
                                    session_id,
                                    route_address: subscriber,
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
                    let mut families = self.families.lock();
                    let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
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
            return self.route_lease_response(&envelope, meta, &response, request_started);
        }

        let lease_msg = match parsed_frame {
            LeaseClientFrame::Op(msg) => msg,
            LeaseClientFrame::Sub(_) => unreachable!(),
        };

        let session_prefix = meta.session_id.to_string();
        let effective_owner = |owner_id: String| {
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
                scoped.push_str(&owner_id);
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
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_acquire(LeaseAcquireRequest {
                    key,
                    owner_session_id: meta.session_id,
                    owner_id: effective_owner(owner_id),
                    ttl_secs,
                    wait_seconds,
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
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => {
                    self.handle_extend(key, effective_owner(owner_id), fencing_token, ttl_secs)
                }
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Release {
                family_id,
                route,
                owner_id,
                fencing_token,
            } => match LeaseKey::from_route(family_id, &route) {
                Some(key) => self.handle_release(key, effective_owner(owner_id), fencing_token),
                None => LeaseResponse::NotFound,
            },
            LeaseMessage::Query { family_id, route } => {
                match LeaseKey::from_route(family_id, &route) {
                    Some(key) => self.handle_query(key),
                    None => LeaseResponse::NotFound,
                }
            }
            LeaseMessage::Tick => {
                self.sweep_expired_state();
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_success(started_at);
                }
                return Ok(());
            }
        };

        self.route_lease_response(&envelope, meta, &domain_response, request_started)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl LeaseDomainSink {
    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Option<crate::domains::lease::LeaseClientRequest> {
        if let Some(request) = envelope.payload::<crate::domains::lease::LeaseClientRequest>() {
            return Some(request.clone());
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
            Some(crate::domains::lease::LeaseClientRequest::new(meta, parsed))
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
