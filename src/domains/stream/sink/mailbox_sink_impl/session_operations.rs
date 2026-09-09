//! Append-session and read frames: the operations that reach `StreamActor`.

use super::{
    Envelope, IngestMetadata, Ordering, PayloadEncoder, Route, RouteFamily,
    StreamClientResponseBody, StreamDiscriminator, StreamFamilyState, StreamReadExecution,
    StreamSessionOwner, StreamStoreError,
};
use crate::domains::stream::sink::model::{OperationOutcome, StreamCommitOutcome, WatermarkCommit};
use crate::domains::stream::StreamMessage;

impl super::super::model::StreamFamilyRuntime {
    pub(super) fn handle_actor_operation_frame(
        &mut self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        stream_msg: crate::domains::stream::protocol::StreamMessage,
    ) {
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
            let response = StreamFamilyState::stream_error_response("route family mismatch");
            self.core
                .route_stream_response(envelope, meta, &response, request_started);
            return;
        }

        let is_begin = matches!(&stream_msg, StreamMessage::Begin { .. });
        let outcome: OperationOutcome = (match stream_msg {
            StreamMessage::Begin {
                family_id,
                route,
                ingest_metadata,
            } => self
                .core
                .handle_begin_operation(meta, family_id, &route, ingest_metadata),
            StreamMessage::Append {
                session_id,
                expected_offset,
                body,
                metadata,
                discriminator,
            } => self.core.handle_append_operation(
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
                self.core.handle_rollback_operation(meta, session_id)
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
            } => self.core.handle_read_operation(StreamReadExecution {
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
                self.core.handle_last_operation(family_id, &route)
            }
            StreamMessage::GetMetadata { family_id, route } => {
                self.core.handle_metadata_operation(family_id, &route)
            }
        })
        .into();

        if outcome.admin_dirty {
            self.core.mark_admin_snapshot_dirty();
        }

        if let Some(notification) = outcome.notification.as_ref() {
            let event = crate::runtime::DomainPublishEvent::new(
                notification.family,
                notification.route.clone(),
                notification.payload.clone(),
            );
            self.core.handle_domain_publish(&event);
        }

        self.core
            .route_operation_response(envelope, meta, request_started, is_begin, &outcome);
    }

    fn handle_commit_operation(
        &mut self,
        meta: crate::runtime::ClientFrameMeta,
        session_id: u64,
        mode: crate::domains::stream::protocol::StreamWriteMode,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        let outcome = self.core.handle_commit_operation(meta, session_id, mode);
        if let Some(commit) = outcome.watermark {
            self.enqueue_watermark_commit(commit);
        }
        (outcome.response, outcome.notification, outcome.admin_dirty)
    }
}

impl StreamFamilyState {
    fn route_operation_response(
        &mut self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        is_begin: bool,
        outcome: &OperationOutcome,
    ) {
        let begin_session = match (&outcome.response, is_begin) {
            (
                StreamClientResponseBody::Ok {
                    session_id: Some(session_id),
                    ..
                },
                true,
            ) => Some(*session_id),
            _ => None,
        };
        if !self.route_stream_response(envelope, meta, &outcome.response, request_started) {
            if let Some(session_id) = begin_session {
                let (_, _, admin_dirty) = self.handle_rollback_operation(meta, session_id);
                if admin_dirty {
                    self.mark_admin_snapshot_dirty();
                }
            }
        }
    }

    fn handle_begin_operation(
        &mut self,
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
                        let begin_result = actor.begin_append_session(
                            meta.session_id,
                            stream_session_id,
                            ingest_metadata,
                        );
                        match begin_result {
                            Ok(session_id) => {
                                self.session_owners.insert(
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
                }
            }
            Err(error) => (Self::stream_error_response(error), None, false),
        }
    }

    fn session_owner_for(
        &mut self,
        owner_session_id: u64,
        family_id: RouteFamily,
        stream_session_id: u64,
    ) -> Option<StreamSessionOwner> {
        self.session_owners
            .get(&stream_session_id)
            .filter(|owner| {
                owner.owner_session_id == owner_session_id && owner.key.family == family_id
            })
            .cloned()
    }

    fn handle_append_operation(
        &mut self,
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
        let Some(owner) = self.session_owner_for(meta.session_id, meta.route_family, session_id)
        else {
            return (
                Self::stream_error_response(StreamStoreError::SessionNotFound.client_message()),
                None,
                false,
            );
        };
        let Some(actor) = self.actors.get_mut(&owner.key) else {
            return (
                Self::stream_error_response(StreamStoreError::SessionNotFound.client_message()),
                None,
                false,
            );
        };
        let append_result = actor.append_to_session_with_discriminator_for_owner(
            meta.session_id,
            session_id,
            expected_offset,
            body,
            metadata,
            discriminator,
        );
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
        &mut self,
        meta: crate::runtime::ClientFrameMeta,
        session_id: u64,
        mode: crate::domains::stream::protocol::StreamWriteMode,
    ) -> StreamCommitOutcome {
        let mode = if mode == crate::domains::stream::protocol::StreamWriteMode::Sync {
            self.sync_write_mode
        } else {
            mode
        };
        let Some(owner) = self.session_owner_for(meta.session_id, meta.route_family, session_id)
        else {
            return StreamCommitOutcome {
                response: Self::stream_error_response(
                    StreamStoreError::SessionNotFound.client_message(),
                ),
                notification: None,
                admin_dirty: false,
                watermark: None,
            };
        };
        let commit_result = {
            self.actors
                .get_mut(&owner.key)
                .expect("session owner references a live Stream actor")
                .commit_session_for_owner(meta.session_id, session_id, mode)
        };
        match commit_result {
            Ok(commit) => {
                self.session_owners.remove(&session_id);
                self.counter_inc("fitz_stream_append_sessions_ended_total");
                self.durable_metrics.record_events(commit.batch_size);
                let watermark_commit = WatermarkCommit {
                    family: owner.key.family,
                    realm: owner.key.realm.clone(),
                    area: owner.key.area.clone(),
                    batch: crate::domains::stream::protocol::BatchCommitted {
                        first_area_offset: commit.first_area_offset,
                        last_area_offset: commit.last_area_offset,
                        first_realm_offset: commit.first_realm_offset,
                        last_realm_offset: commit.last_realm_offset,
                        first_global_offset: commit.first_global_offset,
                        last_global_offset: commit.last_global_offset,
                    },
                };
                let payload = Self::encode_stream_commit_notify_payload(&commit);
                StreamCommitOutcome {
                    response: StreamClientResponseBody::Ok {
                        session_id: None,
                        data: vec![],
                    },
                    notification: Some((owner.key.family, owner.key.resource_route(), payload)),
                    admin_dirty: true,
                    watermark: Some(watermark_commit),
                }
            }
            Err(error) => {
                self.handle_visibility_advance(meta.route_family);
                StreamCommitOutcome {
                    response: Self::stream_error_response(error),
                    notification: None,
                    admin_dirty: false,
                    watermark: None,
                }
            }
        }
    }

    fn handle_rollback_operation(
        &mut self,
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
            self.actors
                .get_mut(&owner.key)
                .expect("session owner references a live Stream actor")
                .rollback_session_for_owner(meta.session_id, session_id)
        };
        match rollback_result {
            Ok(()) => {
                self.session_owners.remove(&session_id);
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
        &mut self,
        request: StreamReadExecution<'_>,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        Self::encode_operation_result(self.encode_read_response_data(request))
    }

    fn handle_last_operation(
        &mut self,
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
        &mut self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &Route,
    ) -> (
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    ) {
        Self::encode_operation_result(self.encode_metadata_response_data(family_id, route))
    }
}
