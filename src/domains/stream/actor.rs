//! Stream actor: live append-session owner for a single resource stream.

use bytes::Bytes;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;

use super::protocol::{
    AppendResponse, BeginSessionResponse, CommitSessionResponse, GetMetadataResponse,
    IngestMetadata, PeekResponse, ReadResponse, StreamError, StreamMessage, StreamResponse,
    StreamWriteMode, MAX_EVENT_SIZE,
};
use super::store::{CommitRecordsParams, EventPayload, StreamStore};

#[derive(Debug, Clone)]
struct ActiveAppendSession {
    stream_session_id: u64,
    owner_session_id: u64,
    expected_offset: u64,
    staged_events: Vec<EventPayload>,
    total_bytes: usize,
    ingest_metadata: Option<IngestMetadata>,
}

impl ActiveAppendSession {
    fn next_assigned_offset(&self) -> u64 {
        self.expected_offset + self.staged_events.len() as u64
    }
}

/// Warm in-memory append-session state for a single resource stream.
///
/// The actor holds only live session state for the current broker process.
/// Committed events, offsets, and metadata stay in [`StreamStore`].
pub struct StreamActor {
    #[allow(dead_code)]
    family_id: RouteFamily,
    realm: String,
    area: String,
    resource: String,
    store: Arc<StreamStore>,
    next_resource_offset: u64,
    active_session: Option<ActiveAppendSession>,
    next_local_session_id: u64,
}

impl StreamActor {
    pub fn new(
        family_id: RouteFamily,
        realm: String,
        area: String,
        resource: String,
        store: Arc<StreamStore>,
    ) -> Self {
        let next_resource_offset = store
            .get_next_resource_offset(family_id.as_u64(), &realm, &area, &resource)
            .unwrap_or(0);

        Self {
            family_id,
            realm,
            area,
            resource,
            store,
            next_resource_offset,
            active_session: None,
            next_local_session_id: 1,
        }
    }

    fn map_error(error: &str) -> StreamError {
        match error {
            "session already active" => StreamError::SessionAlreadyActive,
            "session not found" => StreamError::SessionNotFound,
            "concurrency conflict" | "ERR_CONCURRENCY_CONFLICT" => StreamError::ConcurrencyConflict,
            "event too large" => StreamError::EventTooLarge,
            "batch too large" => StreamError::BatchTooLarge,
            _ => StreamError::InvalidReadBound,
        }
    }

    pub fn resource_identity(&self) -> (&str, &str, &str) {
        (&self.realm, &self.area, &self.resource)
    }

    pub fn has_active_session(&self) -> bool {
        self.active_session.is_some()
    }

    pub fn active_session_owner(&self) -> Option<u64> {
        self.active_session
            .as_ref()
            .map(|session| session.owner_session_id)
    }

    pub fn begin_append_session(
        &mut self,
        owner_session_id: u64,
        stream_session_id: u64,
        expected_offset: u64,
        ingest_metadata: Option<IngestMetadata>,
    ) -> Result<u64, String> {
        if self.active_session.is_some() {
            return Err("session already active".to_string());
        }
        if expected_offset != self.next_resource_offset {
            return Err("concurrency conflict".to_string());
        }

        self.active_session = Some(ActiveAppendSession {
            stream_session_id,
            owner_session_id,
            expected_offset,
            staged_events: Vec::new(),
            total_bytes: 0,
            ingest_metadata,
        });
        Ok(stream_session_id)
    }

    pub fn append_to_session(
        &mut self,
        stream_session_id: u64,
        body: Bytes,
        metadata: Option<Bytes>,
    ) -> Result<u64, String> {
        let session = self
            .active_session
            .as_mut()
            .filter(|session| session.stream_session_id == stream_session_id)
            .ok_or_else(|| "session not found".to_string())?;

        let event_size = body.len() + metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        if event_size > MAX_EVENT_SIZE {
            return Err("event too large".to_string());
        }

        let limits = self.store.batch_limits();
        if session.staged_events.len() + 1 > limits.max_batch_events
            || session.total_bytes + event_size > limits.max_batch_bytes
        {
            return Err("batch too large".to_string());
        }

        let assigned_offset = session.next_assigned_offset();
        session.total_bytes += event_size;
        session.staged_events.push(EventPayload { body, metadata });
        Ok(assigned_offset)
    }

    pub fn commit_session(
        &mut self,
        stream_session_id: u64,
        mode: StreamWriteMode,
    ) -> Result<CommitSessionResponse, String> {
        let session = self
            .active_session
            .take()
            .ok_or_else(|| "session not found".to_string())?;

        if session.stream_session_id != stream_session_id {
            self.active_session = Some(session);
            return Err("session not found".to_string());
        }

        if session.staged_events.is_empty() {
            self.active_session = Some(session);
            return Err("empty batch".to_string());
        }

        let response = match self.store.commit_records(CommitRecordsParams {
            family: self.family_id.as_u64(),
            realm: &self.realm,
            area: &self.area,
            resource: &self.resource,
            expected_resource_next_offset: session.expected_offset,
            events: &session.staged_events,
            ingest_metadata: session.ingest_metadata.clone(),
            mode,
        }) {
            Ok(response) => response,
            Err(error) => {
                self.active_session = Some(session);
                return Err(match error.as_str() {
                    "ERR_CONCURRENCY_CONFLICT" => "concurrency conflict".to_string(),
                    _ => error,
                });
            }
        };

        self.next_resource_offset = response.last_resource_offset.saturating_add(1);
        Ok(CommitSessionResponse {
            first_resource_offset: response.first_resource_offset,
            last_resource_offset: response.last_resource_offset,
            first_area_offset: response.first_area_offset,
            last_area_offset: response.last_area_offset,
            first_realm_offset: response.first_realm_offset,
            last_realm_offset: response.last_realm_offset,
            batch_size: response.batch_size,
            ingest_metadata: response.ingest_metadata,
        })
    }

    pub fn rollback_session(&mut self, stream_session_id: u64) -> Result<(), String> {
        let session = self
            .active_session
            .take()
            .ok_or_else(|| "session not found".to_string())?;

        if session.stream_session_id != stream_session_id {
            self.active_session = Some(session);
            return Err("session not found".to_string());
        }

        Ok(())
    }

    pub fn cleanup_session(&mut self, owner_session_id: u64) -> Option<u64> {
        match self.active_session.as_ref() {
            Some(session) if session.owner_session_id == owner_session_id => self
                .active_session
                .take()
                .map(|session| session.stream_session_id),
            _ => None,
        }
    }

    pub fn read(
        &self,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<ReadResponse, String> {
        if limit == 0 || from_offset >= self.next_resource_offset {
            return Ok(ReadResponse {
                records: Vec::new(),
                cursor: super::protocol::ReadCursor {
                    last_resource_offset: from_offset,
                    last_area_offset: None,
                    last_realm_offset: None,
                    has_more: false,
                },
            });
        }

        let params = crate::domains::stream::store::ReadResourceParams {
            family: self.family_id.as_u64(),
            realm: &self.realm,
            area: &self.area,
            resource: &self.resource,
            from_offset,
            limit,
            max_bytes,
        };
        let (records, cursor) = self.store.read_resource(&params)?;
        Ok(ReadResponse { records, cursor })
    }

    pub fn last(&self) -> Result<PeekResponse, String> {
        if self.next_resource_offset == 0 {
            return Ok(PeekResponse { record: None });
        }

        let record = self.store.peek_resource(
            self.family_id.as_u64(),
            &self.realm,
            &self.area,
            &self.resource,
        )?;
        Ok(PeekResponse { record })
    }

    pub fn metadata(&self) -> Result<GetMetadataResponse, String> {
        let metadata = self.store.get_metadata(
            self.family_id.as_u64(),
            &self.realm,
            &self.area,
            &self.resource,
        )?;
        Ok(GetMetadataResponse { metadata })
    }
}

impl Actor for StreamActor {
    type Message = StreamMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let response = match msg {
            StreamMessage::Begin {
                expected_offset,
                ingest_metadata,
                ..
            } => {
                let stream_session_id = self.next_local_session_id;
                self.next_local_session_id = self.next_local_session_id.saturating_add(1);
                match self.begin_append_session(
                    0,
                    stream_session_id,
                    expected_offset,
                    ingest_metadata,
                ) {
                    Ok(session_id) => StreamResponse::BeginOk(BeginSessionResponse { session_id }),
                    Err(error) => StreamResponse::Error(Self::map_error(&error)),
                }
            }
            StreamMessage::Append {
                session_id,
                body,
                metadata,
            } => match self.append_to_session(session_id, body, metadata) {
                Ok(_) => StreamResponse::AppendOk(AppendResponse { success: true }),
                Err(error) => StreamResponse::Error(Self::map_error(&error)),
            },
            StreamMessage::Commit { session_id, mode } => {
                match self.commit_session(session_id, mode) {
                    Ok(response) => StreamResponse::CommitOk(response),
                    Err(error) => StreamResponse::Error(Self::map_error(&error)),
                }
            }
            StreamMessage::Rollback { session_id } => match self.rollback_session(session_id) {
                Ok(()) => StreamResponse::RollbackOk,
                Err(error) => StreamResponse::Error(Self::map_error(&error)),
            },
            StreamMessage::Read {
                from_offset,
                limit,
                max_bytes,
                ..
            } => match self.read(from_offset, limit, max_bytes) {
                Ok(response) => StreamResponse::ReadOk(response),
                Err(error) => StreamResponse::Error(Self::map_error(&error)),
            },
            StreamMessage::Last { .. } => match self.last() {
                Ok(response) => StreamResponse::LastOk(response),
                Err(error) => StreamResponse::Error(Self::map_error(&error)),
            },
            StreamMessage::GetMetadata { .. } => match self.metadata() {
                Ok(response) => StreamResponse::MetadataOk(response),
                Err(error) => StreamResponse::Error(Self::map_error(&error)),
            },
        };

        let _ = ctx.reply(response).ok();
    }
}
