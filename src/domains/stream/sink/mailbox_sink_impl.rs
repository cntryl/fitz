use super::model::{
    Arc, DeliveryError, Envelope, MailboxSink, Mutex, Ordering, PayloadEncoder, Route, RouteFamily,
    RoutedSubscriptionSet, StreamActor, StreamClientFrame, StreamClientRequest,
    StreamClientResponseBody, StreamDomainActor, StreamDomainCommand, StreamDomainCore,
    StreamDomainRuntime, StreamDomainSink, StreamReadExecution, StreamSessionOwner,
    StreamSubscription, STREAM_OPERATIONS_TOTAL,
};
#[cfg(test)]
use crate::dispatch::protocol::FrameContext;
use crate::domains::stream::protocol::{IngestMetadata, StreamDiscriminator};
use crate::domains::stream::store::StreamStoreError;
#[cfg(test)]
use crate::runtime::routing::RouteAddress;
use crate::runtime::{Actor, Context};

impl MailboxSink for StreamDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.actor.is_running()
            || self
                .family_runtime
                .as_ref()
                .is_some_and(|runtime| !runtime.is_running())
        {
            return Err(DeliveryError::ActorStopped);
        }

        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, false)
        } else {
            self.deliver_to_actor(envelope, false)
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, true)
        } else {
            self.deliver_to_actor(envelope, true)
        }
    }
}

impl Actor for StreamDomainActor {
    type Message = StreamDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            StreamDomainCommand::Deliver(envelope, reply) => {
                let _ = reply.send(runtime.deliver_envelope(&envelope));
            }
            StreamDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            StreamDomainCommand::ReadResourceRecords(command) => {
                let request = command.request.as_borrowed();
                let _ = command
                    .reply
                    .send(runtime.admin_read_resource_records(request));
            }
            StreamDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            StreamDomainCommand::RunMaintenance { family, reply } => {
                runtime.run_maintenance_slice(family);
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            #[cfg(test)]
            StreamDomainCommand::SyncAdminSnapshot(reply) => {
                runtime.sync_admin_snapshot();
                let _ = reply.send(());
            }
            #[cfg(test)]
            StreamDomainCommand::PanicForTests => {
                panic!("test Stream domain actor panic");
            }
        }
    }
}

impl StreamDomainSink {
    fn deliver_to_family(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let Some(runtime) = self.family_runtime.as_ref() else {
            return Err(DeliveryError::ActorStopped);
        };
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let family = *envelope.destination().family();
        let command = StreamDomainCommand::Deliver(envelope, reply_tx);
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::family_enqueue_error)?;

        // Family delivery is called synchronously by the async transport edge.
        // The actor routes client responses through the router, so waiting for
        // the handler result here would block a Tokio worker. The receiver is
        // deliberately dropped after the bounded enqueue succeeds; the actor
        // still owns the command and treats the reply as best effort.
        drop(reply_rx);
        Ok(())
    }

    fn family_enqueue_error(error: crate::runtime::FamilyActorEnqueueError) -> DeliveryError {
        match error {
            crate::runtime::FamilyActorEnqueueError::NormalLaneFull => DeliveryError::MailboxFull {
                capacity: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
                current_len: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
            },
            crate::runtime::FamilyActorEnqueueError::ControlLaneFull => {
                DeliveryError::HighLaneFull {
                    capacity: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                    current_len: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                }
            }
            crate::runtime::FamilyActorEnqueueError::UnknownFamily
            | crate::runtime::FamilyActorEnqueueError::ActorStopped => DeliveryError::ActorStopped,
        }
    }

    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = StreamDomainCommand::Deliver(envelope, reply_tx);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or(Err(DeliveryError::ActorStopped))
    }
}

impl StreamDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }
}

impl StreamDomainCore {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        if meta.route_family != *envelope.destination().family()
            || envelope
                .source()
                .is_some_and(|source| *source.family() != meta.route_family)
        {
            let response = Self::stream_error_response("route family mismatch");
            let response_meta = envelope.source().map_or(meta, |source| {
                let mut response_meta = meta;
                response_meta.route_family = *source.family();
                response_meta
            });
            self.route_stream_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let Some(parsed_frame) =
            self.parse_request_frame(envelope, meta, request.frame, request_started)
        else {
            return Ok(());
        };

        self.record_operation();

        match parsed_frame {
            StreamClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg);
                Ok(())
            }
            StreamClientFrame::Op(stream_msg) => {
                self.handle_actor_operation_frame(envelope, meta, request_started, stream_msg);
                Ok(())
            }
        }
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
                crate::observability::counter_inc("fitz_stream_publish_family_mismatch_total");
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn extract_request(envelope: &Envelope) -> Result<Option<StreamClientRequest>, DeliveryError> {
        Ok(Some(
            Self::request_from_envelope(envelope).ok_or(DeliveryError::ActorStopped)?,
        ))
    }

    fn record_request_start(&self) -> Option<std::time::Instant> {
        self.metrics
            .as_ref()
            .map(crate::domains::stream::StreamMetrics::record_request_start)
    }

    fn parse_request_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: Result<StreamClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<StreamClientFrame> {
        match frame {
            Ok(frame) => Some(frame),
            Err(error) => {
                let response = Self::stream_error_response(error);
                self.route_stream_response(envelope, meta, &response, request_started);
                None
            }
        }
    }

    fn record_operation(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(STREAM_OPERATIONS_TOTAL);
        } else {
            crate::observability::counter_inc(STREAM_OPERATIONS_TOTAL);
        }
    }

    fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        sub_msg: crate::domains::stream::protocol::StreamSubscriptionMessage,
    ) {
        use crate::domains::stream::protocol::StreamSubscriptionMessage;

        let response = match sub_msg {
            StreamSubscriptionMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_stream_subscribe(
                envelope, meta, family_id, &pattern, session_id, subscriber,
            ),
            StreamSubscriptionMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_stream_unsubscribe(
                envelope,
                meta,
                family_id,
                &pattern,
                session_id,
                &subscriber,
            ),
        };

        self.refresh_metrics_gauges();
        self.route_stream_response(envelope, meta, &response, request_started);
    }

    fn handle_stream_subscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> StreamClientResponseBody {
        if Self::valid_stream_subscription_request(
            envelope,
            meta,
            family_id,
            session_id,
            &subscriber,
        ) {
            let compiled = match Self::compile_stream_subscription_pattern(pattern) {
                Ok(compiled) => compiled,
                Err(response) => return response,
            };
            let mut families = self.subscriptions.families.lock();
            let state = families
                .entry(family_id.as_u64())
                .or_insert_with(RoutedSubscriptionSet::new);
            if let Some(subscription_id) = state.find_existing_id(session_id, pattern.as_str()) {
                return StreamClientResponseBody::Ok {
                    session_id: Some(subscription_id),
                    data: vec![],
                };
            }
            if state.wildcard_registration_limit_reached(session_id, &compiled) {
                return StreamClientResponseBody::SubscriptionError(
                    crate::domains::stream::StreamSubscriptionFailure::Limit,
                );
            }
            if let Ok(subscription_id) = self.subscriptions.next_id.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| current.checked_add(1),
            ) {
                state.insert(
                    family_id,
                    StreamSubscription {
                        pattern: compiled,
                        session_id,
                        subscription_id,
                        subscriber,
                    },
                );
                StreamClientResponseBody::Ok {
                    session_id: Some(subscription_id),
                    data: vec![],
                }
            } else {
                if state.is_empty() {
                    families.remove(&family_id.as_u64());
                }
                Self::stream_error_response("subscription ID space exhausted")
            }
        } else {
            Self::stream_error_response("route family mismatch")
        }
    }

    fn handle_stream_unsubscribe(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> StreamClientResponseBody {
        if Self::valid_stream_subscription_request(
            envelope, meta, family_id, session_id, subscriber,
        ) {
            if let Err(response) = Self::compile_stream_subscription_pattern(pattern) {
                return response;
            }
            let mut families = self.subscriptions.families.lock();
            let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                state.remove_session_pattern(family_id, session_id, pattern.as_str());
                state.is_empty()
            } else {
                false
            };
            if remove_family {
                families.remove(&family_id.as_u64());
            }
            drop(families);
            self.remove_pending_notifications_for_pattern(session_id, pattern.as_str());
            StreamClientResponseBody::Ok {
                session_id: None,
                data: vec![],
            }
        } else {
            Self::stream_error_response("route family mismatch")
        }
    }

    fn compile_stream_subscription_pattern(
        pattern: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, StreamClientResponseBody> {
        let invalid_pattern = |error: String| {
            StreamClientResponseBody::SubscriptionError(
                crate::domains::stream::StreamSubscriptionFailure::InvalidPattern(error),
            )
        };
        let compiled = crate::runtime::DomainKind::Stream
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
            .map_err(invalid_pattern)?;
        crate::domains::stream::route_grammar::classify_stream_route_shape(pattern.as_str())
            .map_err(invalid_pattern)?;
        Ok(compiled)
    }

    fn valid_stream_subscription_request(
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

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        stream_msg: crate::domains::stream::protocol::StreamMessage,
    ) {
        use crate::domains::stream::protocol::StreamMessage;

        let message_family = match &stream_msg {
            StreamMessage::Begin { family_id, .. }
            | StreamMessage::Read { family_id, .. }
            | StreamMessage::Last { family_id, .. }
            | StreamMessage::GetMetadata { family_id, .. } => Some(*family_id),
            StreamMessage::Append { .. }
            | StreamMessage::Commit { .. }
            | StreamMessage::Rollback { .. } => None,
        };
        if message_family.is_some_and(|family_id| family_id != meta.route_family) {
            let response = Self::stream_error_response("route family mismatch");
            self.route_stream_response(envelope, meta, &response, request_started);
            return;
        }

        let (response, commit_notify, should_refresh_admin_snapshot) = match stream_msg {
            StreamMessage::Begin {
                family_id,
                route,
                ingest_metadata,
            } => self.handle_begin_operation(meta, family_id, &route, ingest_metadata),
            StreamMessage::Append {
                session_id,
                expected_offset,
                body,
                metadata,
                discriminator,
            } => self.handle_append_operation(
                meta,
                session_id,
                expected_offset,
                body,
                metadata,
                discriminator,
            ),
            StreamMessage::Commit { session_id, mode } => {
                self.handle_commit_operation(meta, session_id, mode)
            }
            StreamMessage::Rollback { session_id } => {
                self.handle_rollback_operation(meta, session_id)
            }
            StreamMessage::Read {
                family_id,
                route,
                from_offset,
                limit,
                max_bytes,
                filter,
                cursor_fingerprint,
                captured_watermark,
            } => self.handle_read_operation(StreamReadExecution {
                family_id,
                route: &route,
                from_offset,
                limit,
                max_bytes,
                filter: filter.as_ref(),
                cursor_fingerprint,
                captured_watermark,
            }),
            StreamMessage::Last { family_id, route } => {
                self.handle_last_operation(family_id, &route)
            }
            StreamMessage::GetMetadata { family_id, route } => {
                self.handle_metadata_operation(family_id, &route)
            }
        };

        if should_refresh_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some((family_id, route, payload)) = commit_notify {
            let event = crate::runtime::DomainPublishEvent::new(family_id, route, payload);
            self.handle_domain_publish(&event);
        }

        self.route_stream_response(envelope, meta, &response, request_started);
    }

    fn handle_begin_operation(
        &self,
        meta: crate::runtime::ClientFrameMeta,
        family_id: crate::runtime::routing::RouteFamily,
        route: &Route,
        ingest_metadata: Option<IngestMetadata>,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        if family_id != meta.route_family {
            return (
                Self::stream_error_response("route family mismatch"),
                None,
                false,
            );
        }

        match Self::actor_key_for_route(family_id, route) {
            Ok(key) => {
                let Ok(stream_session_id) = self.next_session_id.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| current.checked_add(1),
                ) else {
                    return (
                        Self::stream_error_response("stream session ID space exhausted"),
                        None,
                        false,
                    );
                };

                match self.get_or_create_actor(&key) {
                    Ok(actor) => {
                        match actor.lock().begin_append_session(
                            meta.session_id,
                            stream_session_id,
                            ingest_metadata,
                        ) {
                            Ok(session_id) => {
                                self.session_owners.lock().insert(
                                    session_id,
                                    StreamSessionOwner {
                                        key,
                                        owner_session_id: meta.session_id,
                                        actor: actor.clone(),
                                    },
                                );
                                self.counter_inc("fitz_stream_append_sessions_started_total");
                                (
                                    StreamClientResponseBody::Ok {
                                        session_id: Some(session_id),
                                        data: vec![],
                                    },
                                    None,
                                    true,
                                )
                            }
                            Err(error) => {
                                crate::observability::counter_inc(
                                    "fitz_stream_append_conflicts_total",
                                );
                                (Self::stream_error_response(error), None, false)
                            }
                        }
                    }
                    Err(error) => (Self::stream_error_response(error), None, false),
                }
            }
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn session_owner_for(
        &self,
        owner_session_id: u64,
        family_id: RouteFamily,
        stream_session_id: u64,
    ) -> Option<StreamSessionOwner> {
        self.session_owners
            .lock()
            .get(&stream_session_id)
            .filter(|owner| {
                owner.owner_session_id == owner_session_id
                    && owner.key.family_id == family_id.as_u64()
            })
            .cloned()
    }

    fn session_actor_for(
        &self,
        owner_session_id: u64,
        family_id: RouteFamily,
        stream_session_id: u64,
    ) -> Option<Arc<Mutex<StreamActor>>> {
        self.session_owners
            .lock()
            .get(&stream_session_id)
            .filter(|owner| {
                owner.owner_session_id == owner_session_id
                    && owner.key.family_id == family_id.as_u64()
            })
            .map(|owner| owner.actor.clone())
    }

    fn handle_append_operation(
        &self,
        meta: crate::runtime::ClientFrameMeta,
        session_id: u64,
        expected_offset: u64,
        body: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        discriminator: Option<StreamDiscriminator>,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        let Some(actor) = self.session_actor_for(meta.session_id, meta.route_family, session_id)
        else {
            return (
                Self::stream_error_response(StreamStoreError::SessionNotFound.client_message()),
                None,
                false,
            );
        };
        let append_result = {
            let mut actor = actor.lock();
            actor.append_to_session_with_discriminator_for_owner(
                meta.session_id,
                session_id,
                expected_offset,
                body,
                metadata,
                discriminator,
            )
        };
        match append_result {
            Ok(assigned_offset) => {
                let mut encoder = PayloadEncoder::new();
                encoder.put_u64(assigned_offset);
                (
                    StreamClientResponseBody::Ok {
                        session_id: None,
                        data: encoder.finish(),
                    },
                    None,
                    false,
                )
            }
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn handle_commit_operation(
        &self,
        meta: crate::runtime::ClientFrameMeta,
        session_id: u64,
        mode: crate::domains::stream::protocol::StreamWriteMode,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        let mode = if mode == crate::domains::stream::protocol::StreamWriteMode::Sync {
            self.sync_write_mode
        } else {
            mode
        };
        let Some(owner) = self.session_owner_for(meta.session_id, meta.route_family, session_id)
        else {
            return (
                Self::stream_error_response(StreamStoreError::SessionNotFound.client_message()),
                None,
                false,
            );
        };
        let commit_result = {
            let mut actor = owner.actor.lock();
            actor.commit_session_for_owner(meta.session_id, session_id, mode)
        };
        match commit_result {
            Ok(commit) => {
                self.session_owners.lock().remove(&session_id);
                self.counter_inc("fitz_stream_append_sessions_ended_total");
                self.notify_area_batch_committed(
                    RouteFamily::try_from(owner.key.family_id)
                        .expect("stream family IDs originate from RouteFamily"),
                    &owner.key.realm,
                    &owner.key.area,
                    &crate::domains::stream::protocol::BatchCommitted {
                        first_area_offset: commit.first_area_offset,
                        last_area_offset: commit.last_area_offset,
                        first_realm_offset: commit.first_realm_offset,
                        last_realm_offset: commit.last_realm_offset,
                        first_global_offset: commit.first_global_offset,
                        last_global_offset: commit.last_global_offset,
                    },
                );
                let payload = Self::encode_stream_commit_notify_payload(&commit);
                (
                    StreamClientResponseBody::Ok {
                        session_id: None,
                        data: vec![],
                    },
                    Some((
                        RouteFamily::try_from(owner.key.family_id)
                            .expect("stream family IDs originate from RouteFamily"),
                        owner.key.resource_route(),
                        payload,
                    )),
                    true,
                )
            }
            Err(error) => {
                self.handle_visibility_advance(meta.route_family);
                (Self::stream_error_response(error), None, false)
            }
        }
    }

    fn handle_rollback_operation(
        &self,
        meta: crate::runtime::ClientFrameMeta,
        session_id: u64,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        let Some(owner) = self.session_owner_for(meta.session_id, meta.route_family, session_id)
        else {
            return (
                Self::stream_error_response(StreamStoreError::SessionNotFound.client_message()),
                None,
                false,
            );
        };
        let rollback_result = {
            let mut actor = owner.actor.lock();
            actor.rollback_session_for_owner(meta.session_id, session_id)
        };
        match rollback_result {
            Ok(()) => {
                self.session_owners.lock().remove(&session_id);
                self.counter_inc("fitz_stream_append_sessions_ended_total");
                self.handle_visibility_advance(meta.route_family);
                (
                    StreamClientResponseBody::Ok {
                        session_id: None,
                        data: vec![],
                    },
                    None,
                    true,
                )
            }
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn encode_operation_result(
        result: Result<Vec<u8>, String>,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        match result {
            Ok(data) => (
                StreamClientResponseBody::Ok {
                    session_id: None,
                    data,
                },
                None,
                false,
            ),
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn handle_read_operation(
        &self,
        request: StreamReadExecution<'_>,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        Self::encode_operation_result(self.encode_read_response_data(request))
    }

    fn handle_last_operation(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &Route,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        Self::encode_operation_result(self.encode_last_response_data(family_id, route))
    }

    fn handle_metadata_operation(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &Route,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        Self::encode_operation_result(self.encode_metadata_response_data(family_id, route))
    }

    fn request_from_envelope(envelope: &Envelope) -> Option<StreamClientRequest> {
        if let Some(request) = envelope.payload::<StreamClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                RouteAddress::new(
                    *envelope.destination().family(),
                    Route::new(format!("inbox://session/{}", frame_ctx.session_id)),
                )
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::stream_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(StreamClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn route_stream_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &StreamClientResponseBody,
        request_started: Option<std::time::Instant>,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::stream_codec::encode_response_into(
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
            crate::domains::stream::StreamClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.record_response_drop();
                } else {
                    crate::observability::counter_inc(
                        crate::domains::stream::metrics::METRIC_RESPONSE_DROPS_TOTAL,
                    );
                }
                tracing::warn!(
                    domain = "stream",
                    session_id = meta.session_id,
                    route_family = meta.route_family.as_u64(),
                    error = %error,
                    "Dropped best-effort Stream response"
                );
            }
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::stream_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
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
