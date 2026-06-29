use super::model::*;

impl MailboxSink for ScheduleDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        tracing::debug!(
            domain = "schedule",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Schedule domain sink: received envelope"
        );

        let request = match Self::request_from_envelope(&envelope) {
            Some(request) => request,
            None => {
                tracing::warn!(
                    domain = "schedule",
                    "Envelope payload was not ScheduleClientRequest"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };
        let meta = request.meta;
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());

        let schedule_msg = match request.message {
            Ok(msg) => msg,
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "schedule",
                    error = %e,
                    "Failed to parse schedule message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};
        let mut schedule_snapshot_dirty = false;

        let response = {
            let mut actors = self.actors.lock();
            let actor = match self.get_or_create_actor(&mut actors, route_family) {
                Ok(actor) => actor,
                Err(error) => {
                    let response = ScheduleResponse::Error(error);
                    self.route_schedule_response(&envelope, meta, &response, request_started);
                    return Ok(());
                }
            };

            match schedule_msg {
                ScheduleMessage::Create {
                    route,
                    cron,
                    payload,
                } => match actor.create_schedule(route, cron, payload) {
                    Ok(changed) => {
                        if changed {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::CreateBatch { entries } => match actor.create_schedules(entries) {
                    Ok(changed) => {
                        if changed > 0 {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::Cancel { route } => match actor.delete_schedule(route) {
                    Ok(removed) => {
                        if removed {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::List { offset, limit } => {
                    let (entries, total_count) = actor.list_entries(offset, limit);

                    ScheduleResponse::ListDefs {
                        entries,
                        total_count,
                    }
                }
                ScheduleMessage::Subscribe {
                    family_id,
                    route,
                    session_id,
                    subscriber,
                } => {
                    if let Err(error) =
                        crate::domains::schedule::protocol::validate_concrete_schedule_route(
                            route.as_str(),
                        )
                    {
                        ScheduleResponse::Error(error)
                    } else {
                        let fam_id = family_id.as_u64();

                        let mut families = self.sub_families.lock();
                        let state = families
                            .entry(fam_id)
                            .or_insert_with(ScheduleSubscriptionSet::new);

                        let existing_sub_id = state.find_existing_id(session_id, route.as_str());

                        let sub_id = if let Some(id) = existing_sub_id {
                            tracing::debug!(
                                domain = "schedule",
                                session = session_id,
                                subscription_id = id,
                                route = route.as_str(),
                                "Schedule subscription already exists (idempotent)"
                            );
                            id
                        } else {
                            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(ScheduleSubscription {
                                route: route.as_str().to_string(),
                                session_id,
                                subscription_id: new_id,
                                subscriber,
                            });

                            tracing::debug!(
                                domain = "schedule",
                                session = session_id,
                                subscription_id = new_id,
                                route = route.as_str(),
                                "Schedule subscription added"
                            );
                            new_id
                        };

                        ScheduleResponse::SubscribeOk {
                            subscription_id: sub_id,
                        }
                    }
                }
                ScheduleMessage::Unsubscribe {
                    family_id,
                    route,
                    session_id,
                    ..
                } => {
                    if let Err(error) =
                        crate::domains::schedule::protocol::validate_concrete_schedule_route(
                            route.as_str(),
                        )
                    {
                        ScheduleResponse::Error(error)
                    } else {
                        let fam_id = family_id.as_u64();
                        let mut families = self.sub_families.lock();
                        let remove_family = if let Some(state) = families.get_mut(&fam_id) {
                            state.remove_session_route(session_id, route.as_str());
                            state.is_empty()
                        } else {
                            false
                        };
                        if remove_family {
                            families.remove(&fam_id);
                        }
                        ScheduleResponse::Ok
                    }
                }
                ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                    self.unsubscribe_all(session_id);
                    ScheduleResponse::Ok
                }
            }
        };

        if schedule_snapshot_dirty {
            self.schedule_admin_snapshot(false);
        }

        self.route_schedule_response(&envelope, meta, &response, request_started);

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl ScheduleDomainSink {
    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Option<crate::domains::schedule::ScheduleClientRequest> {
        if let Some(request) = envelope.payload::<crate::domains::schedule::ScheduleClientRequest>()
        {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::protocol::schedule_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(crate::domains::schedule::ScheduleClientRequest::new(
                meta, parsed,
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn route_schedule_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::schedule::ScheduleResponse,
        request_started: Option<std::time::Instant>,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::protocol::schedule_codec::encode_response_into(
                &mut payload_encoder,
                response,
            );
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx =
            crate::domains::schedule::ScheduleClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::schedule_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
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

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}
