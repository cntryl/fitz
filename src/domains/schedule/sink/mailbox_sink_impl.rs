use super::model::{
    DeliveryError, Envelope, MailboxSink, Ordering, ScheduleDomainActor, ScheduleDomainCommand,
    ScheduleDomainRuntime, ScheduleDomainSink, ScheduleSubscription, ScheduleSubscriptionSet,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{Actor, Context};

impl MailboxSink for ScheduleDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor
            .try_send(ScheduleDomainCommand::Deliver(envelope))
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor
            .try_send_high_priority(ScheduleDomainCommand::Deliver(envelope))
    }
}

impl Actor for ScheduleDomainActor {
    type Message = ScheduleDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.state.runtime();
        match msg {
            ScheduleDomainCommand::Deliver(envelope) => {
                if let Err(error) = runtime.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "schedule", error = %error, "Schedule actor delivery failed");
                }
            }
            ScheduleDomainCommand::CleanupSession(session_id) => {
                runtime.unsubscribe_all(session_id);
            }
            ScheduleDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            ScheduleDomainCommand::ReadPendingClaims(route_family, reply) => {
                let _ = reply.send(runtime.admin_pending_claims(route_family));
            }
            ScheduleDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ScanDueSchedules => {
                runtime.scan_due_schedules();
            }
            ScheduleDomainCommand::PreloadPersistedFamilies(reply) => {
                let _ = reply.send(runtime.preload_persisted_families());
            }
            ScheduleDomainCommand::ForceDueScanForTests(ready_count, reply) => {
                runtime.force_due_scan_for_tests(ready_count);
                let _ = reply.send(());
            }
        }
    }
}

impl ScheduleDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        Self::log_delivery(envelope);

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        let schedule_msg = self.parse_request_message(request.message, request_started)?;

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        let Some((response, schedule_snapshot_dirty)) = self.dispatch_schedule_message(
            envelope,
            meta,
            request_started,
            route_family,
            schedule_msg,
        ) else {
            return Ok(());
        };

        if schedule_snapshot_dirty {
            self.schedule_admin_snapshot(false);
        }

        self.route_schedule_response(envelope, meta, &response, request_started);

        Ok(())
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
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
            domain = "schedule",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Schedule domain sink: received envelope"
        );
    }

    fn extract_request(
        envelope: &Envelope,
    ) -> Result<Option<crate::domains::schedule::ScheduleClientRequest>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            Ok(Some(request))
        } else {
            tracing::warn!(
                domain = "schedule",
                "Envelope payload was not ScheduleClientRequest"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn record_request_start(&self) -> Option<std::time::Instant> {
        self.core
            .metrics
            .as_ref()
            .map(crate::domains::schedule::ScheduleMetrics::record_request_start)
    }

    fn parse_request_message(
        &self,
        message: Result<crate::domains::schedule::ScheduleMessage, String>,
        request_started: Option<std::time::Instant>,
    ) -> Result<crate::domains::schedule::ScheduleMessage, DeliveryError> {
        match message {
            Ok(message) => Ok(message),
            Err(error) => {
                if let (Some(metrics), Some(started_at)) =
                    (self.core.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "schedule",
                    error = %error,
                    "Failed to parse schedule message"
                );
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn dispatch_schedule_message(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        route_family: crate::runtime::routing::RouteFamily,
        schedule_msg: crate::domains::schedule::ScheduleMessage,
    ) -> Option<(crate::domains::schedule::ScheduleResponse, bool)> {
        use crate::domains::schedule::ScheduleResponse;

        let mut actors = self.core.actors.lock();
        let actor = match self.get_or_create_actor(&mut actors, route_family) {
            Ok(actor) => actor,
            Err(error) => {
                let response = ScheduleResponse::Error(error);
                self.route_schedule_response(envelope, meta, &response, request_started);
                return None;
            }
        };

        Some(self.apply_schedule_message(actor, schedule_msg))
    }

    fn apply_schedule_message(
        &self,
        actor: &mut crate::domains::schedule::ScheduleActor,
        schedule_msg: crate::domains::schedule::ScheduleMessage,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};

        let mut schedule_snapshot_dirty = false;
        let response = match schedule_msg {
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
            ScheduleMessage::Cancel { route } => match actor.delete_schedule(&route) {
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
            } => self.apply_subscribe_message(family_id, &route, session_id, subscriber),
            ScheduleMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                ..
            } => self.apply_unsubscribe_message(family_id, &route, session_id),
            ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(session_id);
                ScheduleResponse::Ok
            }
        };

        (response, schedule_snapshot_dirty)
    }

    fn apply_subscribe_message(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::ScheduleResponse;

        if let Err(error) =
            crate::domains::schedule::protocol::validate_concrete_schedule_route(route.as_str())
        {
            return ScheduleResponse::Error(error);
        }

        let fam_id = family_id.as_u64();
        let mut families = self.core.sub_families.lock();
        let state = families
            .entry(fam_id)
            .or_insert_with(ScheduleSubscriptionSet::new);

        let sub_id = if let Some(id) = state.find_existing_id(session_id, route.as_str()) {
            tracing::debug!(
                domain = "schedule",
                session = session_id,
                subscription_id = id,
                route = route.as_str(),
                "Schedule subscription already exists (idempotent)"
            );
            id
        } else {
            let new_id = self.core.next_sub_id.fetch_add(1, Ordering::Relaxed);
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

    fn apply_unsubscribe_message(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::ScheduleResponse;

        if let Err(error) =
            crate::domains::schedule::protocol::validate_concrete_schedule_route(route.as_str())
        {
            return ScheduleResponse::Error(error);
        }

        let fam_id = family_id.as_u64();
        let mut families = self.core.sub_families.lock();
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
            let _ = self.core.router.route(response_envelope);
        }

        if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started) {
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
