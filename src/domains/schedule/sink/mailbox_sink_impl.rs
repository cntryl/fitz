use super::model::{
    DeliveryError, Envelope, MailboxSink, Ordering, ScheduleDomainActor, ScheduleDomainCommand,
    ScheduleDomainRuntime, ScheduleDomainSink, ScheduleSubscription, ScheduleSubscriptionSet,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
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
            ScheduleDomainCommand::BenchPublishEvent(event, reply) => {
                runtime.bench_publish_event(&event);
                let _ = reply.send(());
            }
            ScheduleDomainCommand::ForceDueScanForTests(ready_count, reply) => {
                runtime.force_due_scan_for_tests(ready_count);
                let _ = reply.send(());
            }
            #[cfg(test)]
            ScheduleDomainCommand::PanicForTests => {
                panic!("test Schedule domain actor panic");
            }
            #[cfg(test)]
            ScheduleDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
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

        if !Self::valid_request_envelope(envelope, meta) {
            let response = crate::domains::schedule::ScheduleResponse::Error(
                crate::domains::schedule::ScheduleFailure::new(
                    crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                    "route family mismatch",
                ),
            );
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let Some(schedule_msg) =
            self.parse_request_message(envelope, meta, request.message, request_started)
        else {
            return Ok(());
        };

        if !Self::valid_schedule_message(envelope, meta, &schedule_msg) {
            let response = crate::domains::schedule::ScheduleResponse::Error(
                crate::domains::schedule::ScheduleFailure::new(
                    crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                    "route family mismatch",
                ),
            );
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

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
            if *envelope.destination().family() != event.family_id {
                self.core
                    .live_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                return true;
            }
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
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: Result<
            crate::domains::schedule::ScheduleMessage,
            crate::domains::schedule::ScheduleFailure,
        >,
        request_started: Option<std::time::Instant>,
    ) -> Option<crate::domains::schedule::ScheduleMessage> {
        match message {
            Ok(message) => Some(message),
            Err(error) => {
                tracing::warn!(
                    domain = "schedule",
                    error = %error,
                    "Failed to parse schedule message"
                );
                let response = crate::domains::schedule::ScheduleResponse::Error(error);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                self.route_schedule_response(envelope, response_meta, &response, request_started);
                None
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

        match &schedule_msg {
            crate::domains::schedule::ScheduleMessage::Subscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => {
                return Some((
                    self.apply_subscribe_message(
                        *family_id,
                        route,
                        *session_id,
                        subscriber.clone(),
                    ),
                    false,
                ));
            }
            crate::domains::schedule::ScheduleMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                ..
            } => {
                return Some((
                    self.apply_unsubscribe_message(*family_id, route, *session_id),
                    false,
                ));
            }
            crate::domains::schedule::ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(*session_id);
                return Some((ScheduleResponse::Ok, false));
            }
            _ => {}
        }

        let mut actors = self.core.actors.lock();
        let actor = match self.get_or_create_actor(&mut actors, route_family) {
            Ok(actor) => actor,
            Err(error) => {
                let response = ScheduleResponse::Error(
                    crate::domains::schedule::ScheduleFailure::parse(error),
                );
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

        match schedule_msg {
            ScheduleMessage::Create {
                route,
                cron,
                delivery_mode,
                payload,
            } => Self::apply_create_message(actor, route, cron, delivery_mode, payload),
            ScheduleMessage::CreateBatch { entries } => {
                Self::apply_create_batch_message(actor, entries)
            }
            ScheduleMessage::Cancel { route } => Self::apply_cancel_message(actor, &route),
            ScheduleMessage::List { offset, limit } => {
                let (entries, total_count) = actor.list_entries(offset, limit);

                (
                    ScheduleResponse::ListDefs {
                        entries,
                        total_count,
                    },
                    false,
                )
            }
            ScheduleMessage::ListV2 { cursor, limit } => {
                let (entries, has_more, continuation) =
                    actor.list_entries_v2(cursor.as_deref(), limit);
                (
                    ScheduleResponse::ListPage {
                        entries,
                        has_more,
                        continuation,
                    },
                    false,
                )
            }
            ScheduleMessage::Subscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => (
                self.apply_subscribe_message(family_id, &route, session_id, subscriber),
                false,
            ),
            ScheduleMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                ..
            } => (
                self.apply_unsubscribe_message(family_id, &route, session_id),
                false,
            ),
            ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(session_id);
                (ScheduleResponse::Ok, false)
            }
        }
    }

    fn apply_create_message(
        actor: &mut crate::domains::schedule::ScheduleActor,
        route: String,
        cron: String,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: bytes::Bytes,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{ScheduleFailure, ScheduleResponse};

        if let Some(failure) =
            crate::domains::schedule::definition_validation::schedule_definition_failure(
                &route, &cron,
            )
        {
            return (ScheduleResponse::Error(failure), false);
        }

        match actor.create_schedule_with_mode(route, cron, delivery_mode, payload) {
            Ok(changed) => (ScheduleResponse::Ok, changed),
            Err(error) => (
                ScheduleResponse::Error(ScheduleFailure::parse(error)),
                false,
            ),
        }
    }

    fn apply_create_batch_message(
        actor: &mut crate::domains::schedule::ScheduleActor,
        entries: Vec<crate::domains::schedule::ScheduleCreateEntry>,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{ScheduleFailure, ScheduleResponse};

        if let Some(failure) = entries.iter().find_map(|entry| {
            crate::domains::schedule::definition_validation::schedule_definition_failure(
                &entry.route,
                &entry.cron,
            )
        }) {
            return (ScheduleResponse::Error(failure), false);
        }

        match actor.create_schedules(entries) {
            Ok(changed) => (ScheduleResponse::Ok, changed > 0),
            Err(error) => (
                ScheduleResponse::Error(ScheduleFailure::parse(error)),
                false,
            ),
        }
    }

    fn apply_cancel_message(
        actor: &mut crate::domains::schedule::ScheduleActor,
        route: &str,
    ) -> (crate::domains::schedule::ScheduleResponse, bool) {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        if let Err(error) =
            crate::domains::schedule::protocol::validate_concrete_schedule_route(route)
        {
            return (
                ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::InvalidTarget,
                    error,
                )),
                false,
            );
        }

        match actor.delete_schedule(route) {
            Ok(removed) => (ScheduleResponse::Ok, removed),
            Err(error) => (
                ScheduleResponse::Error(ScheduleFailure::parse(error)),
                false,
            ),
        }
    }

    fn apply_subscribe_message(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        match crate::runtime::DomainKind::Schedule
            .descriptor()
            .compile_registration_pattern(route.as_str())
        {
            Ok(pattern) => {
                self.insert_schedule_subscription(family_id, route, session_id, subscriber, pattern)
            }
            Err(error) => ScheduleResponse::Error(ScheduleFailure::new(
                ScheduleFailureCategory::InvalidSubscriptionPattern,
                error,
            )),
        }
    }

    fn insert_schedule_subscription(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
        pattern: crate::runtime::matcher::Pattern,
    ) -> crate::domains::schedule::ScheduleResponse {
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

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
            if state
                .subscriptions
                .wildcard_registration_limit_reached(session_id, &pattern)
            {
                return ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::SubscriptionLimit,
                    format!(
                        "wildcard subscription limit exceeded ({} per session)",
                        crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION
                    ),
                ));
            }
            let Ok(new_id) = self.core.next_sub_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| current.checked_add(1),
            ) else {
                let state_empty = state.is_empty();
                if state_empty {
                    families.remove(&fam_id);
                }
                return ScheduleResponse::Error(ScheduleFailure::new(
                    ScheduleFailureCategory::SubscriptionLimit,
                    "subscription ID space exhausted",
                ));
            };
            state.insert(
                family_id,
                ScheduleSubscription {
                    pattern,
                    session_id,
                    subscription_id: new_id,
                    subscriber,
                },
            );

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
        use crate::domains::schedule::{
            ScheduleFailure, ScheduleFailureCategory, ScheduleResponse,
        };

        if let Err(error) = crate::runtime::DomainKind::Schedule
            .descriptor()
            .compile_registration_pattern(route.as_str())
        {
            return ScheduleResponse::Error(ScheduleFailure::new(
                ScheduleFailureCategory::InvalidSubscriptionPattern,
                error,
            ));
        }

        let fam_id = family_id.as_u64();
        let mut families = self.core.sub_families.lock();
        let remove_family = if let Some(state) = families.get_mut(&fam_id) {
            state.remove_session_route(family_id, session_id, route.as_str());
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
            let parsed = crate::dispatch::protocol::schedule_codec::parse_request(
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
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::schedule_codec::encode_response_into(
                &mut payload_encoder,
                meta.message_type,
                response,
            );
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx =
            crate::domains::schedule::ScheduleClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.core.router.route(response_envelope) {
                if let Some(metrics) = self.core.metrics.as_ref() {
                    metrics.record_response_drop();
                } else {
                    crate::observability::counter_inc(
                        crate::domains::schedule::metrics::METRIC_RESPONSE_DROPS_TOTAL,
                    );
                }
                tracing::warn!(
                    domain = "schedule",
                    session_id = meta.session_id,
                    route_family = meta.route_family.as_u64(),
                    error = %error,
                    "Dropped best-effort Schedule response"
                );
            }
        }

        if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started) {
            if Self::schedule_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
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

    fn valid_schedule_message(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::schedule::ScheduleMessage,
    ) -> bool {
        use crate::domains::schedule::ScheduleMessage;

        match message {
            ScheduleMessage::Subscribe {
                family_id,
                session_id,
                subscriber,
                ..
            }
            | ScheduleMessage::Unsubscribe {
                family_id,
                session_id,
                subscriber,
                ..
            } => {
                *family_id == meta.route_family
                    && *session_id == meta.session_id
                    && *subscriber.family() == *family_id
                    && envelope.source().is_none_or(|source| source == subscriber)
            }
            ScheduleMessage::UnsubscribeAll {
                session_id,
                subscriber,
            } => {
                *session_id == meta.session_id
                    && *subscriber.family() == meta.route_family
                    && envelope.source().is_none_or(|source| source == subscriber)
            }
            ScheduleMessage::Create { .. }
            | ScheduleMessage::CreateBatch { .. }
            | ScheduleMessage::Cancel { .. }
            | ScheduleMessage::List { .. }
            | ScheduleMessage::ListV2 { .. } => true,
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

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::dispatch::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => {
            crate::dispatch::protocol::frame::ChannelId::Control
        }
        crate::runtime::ClientChannel::Pub => crate::dispatch::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::dispatch::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::dispatch::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => {
            crate::dispatch::protocol::frame::ChannelId::Internal
        }
    }
}
