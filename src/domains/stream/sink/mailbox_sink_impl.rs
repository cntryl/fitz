use super::model::*;

impl MailboxSink for StreamDomainSink {
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

        let request = match Self::request_from_envelope(&envelope) {
            Some(request) => request,
            None => return Err(DeliveryError::ActorStopped),
        };
        let meta = request.meta;
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());

        let parsed_frame = match request.frame {
            Ok(frame) => frame,
            Err(error) => {
                let response = Self::stream_error_response(error);
                self.route_stream_response(&envelope, meta, &response, request_started);
                return Ok(());
            }
        };

        use crate::domains::stream::protocol::{StreamMessage, StreamSubscriptionMessage};

        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(STREAM_OPERATIONS_TOTAL);
        } else {
            crate::observability::counter_inc(STREAM_OPERATIONS_TOTAL);
        }

        // Subscription messages are handled entirely by the sink without touching StreamActor.
        if let StreamClientFrame::Sub(sub_msg) = parsed_frame {
            let (response, _commit_notify, _should_refresh_admin_snapshot): (
                StreamClientResponseBody,
                Option<(Route, bytes::Bytes)>,
                bool,
            ) = match sub_msg {
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

                    let subscription_id = if let Some(id) =
                        state.find_existing_id(session_id, pattern.as_str())
                    {
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

                    (
                        StreamClientResponseBody::Ok {
                            session_id: Some(subscription_id),
                            data: vec![],
                        },
                        None,
                        false,
                    )
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
                    (
                        StreamClientResponseBody::Ok {
                            session_id: None,
                            data: vec![],
                        },
                        None,
                        false,
                    )
                }
            };

            self.refresh_metrics_gauges();
            self.route_stream_response(&envelope, meta, &response, request_started);
            return Ok(());
        }

        let stream_msg = match parsed_frame {
            StreamClientFrame::Op(msg) => msg,
            StreamClientFrame::Sub(_) => unreachable!(),
        };

        let (response, commit_notify, should_refresh_admin_snapshot) = match stream_msg {
            StreamMessage::Begin {
                family_id,
                route,
                ingest_metadata,
            } => match Self::actor_key_for_route(family_id, &route) {
                Ok(key) => match self.get_or_create_actor(&key) {
                    Ok(actor) => {
                        let stream_session_id =
                            self.next_session_id.fetch_add(1, Ordering::Relaxed);
                        let outcome = actor.lock().begin_append_session(
                            meta.session_id,
                            stream_session_id,
                            ingest_metadata,
                        );
                        match outcome {
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
                                crate::observability::counter_inc(
                                    "fitz_stream_append_conflicts_total",
                                );
                                (Self::stream_error_response(error), None, false)
                            }
                        }
                    }
                    Err(error) => (Self::stream_error_response(error), None, false),
                },
                Err(error) => (Self::stream_error_response(error), None, false),
            },
            StreamMessage::Append {
                session_id,
                expected_offset,
                body,
                metadata,
                discriminator,
            } => {
                let owner = self.session_owners.lock().get(&session_id).cloned();
                match owner.filter(|owner| owner.owner_session_id == meta.session_id) {
                    Some(owner) => match self.get_or_create_actor(&owner.key) {
                        Ok(actor) => {
                            let outcome =
                                actor.lock().append_to_session_with_discriminator_for_owner(
                                    meta.session_id,
                                    session_id,
                                    expected_offset,
                                    body,
                                    metadata,
                                    discriminator,
                                );
                            match outcome {
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
                        Err(error) => (Self::stream_error_response(error), None, false),
                    },
                    None => (
                        Self::stream_error_response("session not found"),
                        None,
                        false,
                    ),
                }
            }
            StreamMessage::Commit { session_id, mode } => {
                let mode = if mode == crate::domains::stream::protocol::StreamWriteMode::Sync {
                    self.sync_write_mode
                } else {
                    mode
                };
                let owner = self.session_owners.lock().get(&session_id).cloned();
                match owner.filter(|owner| owner.owner_session_id == meta.session_id) {
                    Some(owner) => match self.get_or_create_actor(&owner.key) {
                        Ok(actor) => {
                            let outcome = actor.lock().commit_session_for_owner(
                                meta.session_id,
                                session_id,
                                mode,
                            );
                            match outcome {
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
                    },
                    None => (
                        Self::stream_error_response("session not found"),
                        None,
                        false,
                    ),
                }
            }
            StreamMessage::Rollback { session_id } => {
                let owner = self.session_owners.lock().get(&session_id).cloned();
                match owner.filter(|owner| owner.owner_session_id == meta.session_id) {
                    Some(owner) => match self.get_or_create_actor(&owner.key) {
                        Ok(actor) => {
                            let outcome = actor
                                .lock()
                                .rollback_session_for_owner(meta.session_id, session_id);
                            match outcome {
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
                            }
                        }
                        Err(error) => (Self::stream_error_response(error), None, false),
                    },
                    None => (
                        Self::stream_error_response("session not found"),
                        None,
                        false,
                    ),
                }
            }
            StreamMessage::Read {
                family_id,
                route,
                from_offset,
                limit,
                max_bytes,
                filter,
            } => match self.encode_read_response_data(
                family_id,
                &route,
                from_offset,
                limit,
                max_bytes,
                filter.as_ref(),
            ) {
                Ok(data) => (
                    StreamClientResponseBody::Ok {
                        session_id: None,
                        data,
                    },
                    None,
                    false,
                ),
                Err(error) => (Self::stream_error_response(error), None, false),
            },
            StreamMessage::Last { family_id, route } => {
                match self.encode_last_response_data(family_id, &route) {
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
            StreamMessage::GetMetadata { family_id, route } => {
                match self.encode_metadata_response_data(family_id, &route) {
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
        };

        if should_refresh_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some((route, payload)) = commit_notify {
            let event = crate::runtime::DomainPublishEvent::new(meta.route_family, route, payload);
            let _ = self.handle_domain_publish(&event);
        }

        self.route_stream_response(&envelope, meta, &response, request_started);

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl StreamDomainSink {
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
