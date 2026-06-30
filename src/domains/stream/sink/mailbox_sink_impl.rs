use super::model::{
    DeliveryError, Envelope, MailboxSink, Ordering, PayloadEncoder, Route, RoutedSubscriptionSet,
    StreamClientFrame, StreamClientRequest, StreamClientResponseBody, StreamDomainSink,
    StreamSessionOwner, StreamSubscription, STREAM_OPERATIONS_TOTAL,
};
use crate::domains::stream::protocol::{IngestMetadata, StreamDiscriminator};
#[cfg(test)]
use crate::protocol::FrameContext;
#[cfg(test)]
use crate::runtime::routing::RouteAddress;

impl MailboxSink for StreamDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(&envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(&envelope) {
            return Ok(());
        }

        let Some(request) = Self::extract_request(&envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        let Some(parsed_frame) =
            self.parse_request_frame(&envelope, meta, request.frame, request_started)
        else {
            return Ok(());
        };

        self.record_operation();

        match parsed_frame {
            StreamClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(&envelope, meta, request_started, sub_msg);
                Ok(())
            }
            StreamClientFrame::Op(stream_msg) => {
                self.handle_actor_operation_frame(&envelope, meta, request_started, stream_msg);
                Ok(())
            }
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl StreamDomainSink {
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
            } => {
                let mut families = self.families.lock();
                let state = families
                    .entry(family_id.as_u64())
                    .or_insert_with(RoutedSubscriptionSet::new);

                let subscription_id =
                    if let Some(id) = state.find_existing_id(session_id, pattern.as_str()) {
                        id
                    } else {
                        let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                        state.insert(
                            family_id,
                            StreamSubscription {
                                pattern: crate::runtime::matcher::Pattern::new(pattern.as_str()),
                                session_id,
                                subscription_id: new_id,
                                subscriber,
                            },
                        );
                        new_id
                    };

                StreamClientResponseBody::Ok {
                    session_id: Some(subscription_id),
                    data: vec![],
                }
            }
            StreamSubscriptionMessage::Unsubscribe {
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
                StreamClientResponseBody::Ok {
                    session_id: None,
                    data: vec![],
                }
            }
        };

        self.refresh_metrics_gauges();
        self.route_stream_response(envelope, meta, &response, request_started);
    }

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        stream_msg: crate::domains::stream::protocol::StreamMessage,
    ) {
        use crate::domains::stream::protocol::StreamMessage;

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
            } => self.handle_read_operation(
                family_id,
                &route,
                from_offset,
                limit,
                max_bytes,
                filter.as_ref(),
            ),
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

        if let Some((route, payload)) = commit_notify {
            let event = crate::runtime::DomainPublishEvent::new(meta.route_family, route, payload);
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
        Option<(Route, bytes::Bytes)>,
        bool,
    ) {
        match Self::actor_key_for_route(family_id, route) {
            Ok(key) => match self.get_or_create_actor(&key) {
                Ok(actor) => {
                    let stream_session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
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
                            crate::observability::counter_inc("fitz_stream_append_conflicts_total");
                            (Self::stream_error_response(error), None, false)
                        }
                    }
                }
                Err(error) => (Self::stream_error_response(error), None, false),
            },
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn session_owner_for(
        &self,
        owner_session_id: u64,
        stream_session_id: u64,
    ) -> Option<StreamSessionOwner> {
        self.session_owners
            .lock()
            .get(&stream_session_id)
            .filter(|owner| owner.owner_session_id == owner_session_id)
            .cloned()
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
        Option<(Route, bytes::Bytes)>,
        bool,
    ) {
        let Some(owner) = self.session_owner_for(meta.session_id, session_id) else {
            return (
                Self::stream_error_response("session not found"),
                None,
                false,
            );
        };
        match self.get_or_create_actor(&owner.key) {
            Ok(actor) => match actor.lock().append_to_session_with_discriminator_for_owner(
                meta.session_id,
                session_id,
                expected_offset,
                body,
                metadata,
                discriminator,
            ) {
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
            },
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
        Option<(Route, bytes::Bytes)>,
        bool,
    ) {
        let mode = if mode == crate::domains::stream::protocol::StreamWriteMode::Sync {
            self.sync_write_mode
        } else {
            mode
        };
        let Some(owner) = self.session_owner_for(meta.session_id, session_id) else {
            return (
                Self::stream_error_response("session not found"),
                None,
                false,
            );
        };
        match self.get_or_create_actor(&owner.key) {
            Ok(actor) => {
                match actor
                    .lock()
                    .commit_session_for_owner(meta.session_id, session_id, mode)
                {
                    Ok(commit) => {
                        self.session_owners.lock().remove(&session_id);
                        self.counter_inc("fitz_stream_append_sessions_ended_total");
                        let payload = Self::encode_stream_commit_notify_payload(
                            commit.first_resource_offset,
                            commit.last_resource_offset,
                            commit.first_area_offset,
                            commit.last_area_offset,
                            commit.first_realm_offset,
                            commit.last_realm_offset,
                            commit.batch_size,
                        );
                        (
                            StreamClientResponseBody::Ok {
                                session_id: None,
                                data: vec![],
                            },
                            Some((owner.key.resource_route(), payload)),
                            true,
                        )
                    }
                    Err(error) => (Self::stream_error_response(error), None, false),
                }
            }
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn handle_rollback_operation(
        &self,
        meta: crate::runtime::ClientFrameMeta,
        session_id: u64,
    ) -> (
        StreamClientResponseBody,
        Option<(Route, bytes::Bytes)>,
        bool,
    ) {
        let Some(owner) = self.session_owner_for(meta.session_id, session_id) else {
            return (
                Self::stream_error_response("session not found"),
                None,
                false,
            );
        };
        match self.get_or_create_actor(&owner.key) {
            Ok(actor) => match actor
                .lock()
                .rollback_session_for_owner(meta.session_id, session_id)
            {
                Ok(()) => {
                    self.session_owners.lock().remove(&session_id);
                    self.counter_inc("fitz_stream_append_sessions_ended_total");
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
            },
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn encode_operation_result(
        result: Result<Vec<u8>, String>,
    ) -> (
        StreamClientResponseBody,
        Option<(Route, bytes::Bytes)>,
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
        family_id: crate::runtime::routing::RouteFamily,
        route: &Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&crate::domains::stream::protocol::StreamFilterSet>,
    ) -> (
        StreamClientResponseBody,
        Option<(Route, bytes::Bytes)>,
        bool,
    ) {
        Self::encode_operation_result(self.encode_read_response_data(
            family_id,
            route,
            from_offset,
            limit,
            max_bytes,
            filter,
        ))
    }

    fn handle_last_operation(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &Route,
    ) -> (
        StreamClientResponseBody,
        Option<(Route, bytes::Bytes)>,
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
        Option<(Route, bytes::Bytes)>,
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
            let parsed = crate::protocol::stream_codec::parse_request(
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
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes =
                crate::protocol::stream_codec::encode_response_into(&mut payload_encoder, response);
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
            crate::domains::stream::StreamClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
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
