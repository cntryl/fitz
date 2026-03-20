//! Stream actor: manages append-only event log for a single resource

use bytes::Bytes;
use std::sync::Arc;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

use super::protocol::{
    AppendResponse, BatchCommitted, BeginSessionResponse, CommitSessionResponse, LeaseGrant,
    OffsetLease, PeekResponse, ReadResponse, StreamError, StreamMessage, StreamResponse,
    StreamWriteMode, DEFAULT_LEASE_SIZE, MAX_EVENT_SIZE,
};
use super::store::{EventPayload, SessionId, StreamStore};
use std::collections::VecDeque;

/// StreamActor manages a single resource stream
///
/// **CRITICAL: Does NOT hold events in memory.**
/// All events are persisted to Midge via StreamStore. Actor only tracks:
/// - next_resource_offset (sequencing)
/// - area_lease (pre-allocated area offsets)
/// - realm_lease (pre-allocated realm offsets)
/// - realm/area/resource identity for this stream
pub struct StreamActor {
    #[allow(dead_code)]
    family_id: RouteFamily,

    /// Stream identity
    realm: String,
    area: String,
    resource: String,

    /// Storage layer (shared, durable)
    store: Arc<StreamStore>,

    /// Next expected resource offset (sequence validation)
    next_resource_offset: u64,

    /// Pre-allocated area offset lease
    area_lease: OffsetLease,

    /// Pre-allocated realm offset lease
    realm_lease: OffsetLease,

    /// Active session (at most one per resource)
    active_session: Option<SessionId>,

    /// Pending commits awaiting lease grants
    pending_commits: VecDeque<(SessionId, StreamWriteMode)>,

    /// Cached routes (hot path optimization)
    area_actor_route: Route,
    commit_notification_route: Route,

    /// Debounce timer id for commit notification
    commit_timer: Option<crate::runtime::context::TimerId>,

    /// Pending commit publish event (debounced)
    pending_publish: Option<DomainPublishEvent>,
}

impl StreamActor {
    const NOTICE_DEBOUNCE_MS: u64 = 25;

    pub fn new(
        family_id: RouteFamily,
        realm: String,
        area: String,
        resource: String,
        store: Arc<StreamStore>,
    ) -> Self {
        // **CRITICAL: Recover next_resource_offset from metadata counter**
        // This prevents offset reuse after TTL expiry and process restart
        let next_resource_offset =
            match store.get_next_resource_offset(family_id.as_u64(), &realm, &area, &resource) {
                Ok(offset) => offset,
                Err(e) => {
                    eprintln!(
                        "FATAL: Failed to recover resource offset for {}/{}/{}: {}",
                        realm, area, resource, e
                    );
                    0 // Fallback to 0, may cause conflict if stream has data
                }
            };

        let area_actor_route = Route::new(format!("stream://{}/{}/__area__", realm, area));
        let commit_notification_route =
            Route::new(format!("stream://{}/{}/{}", realm, area, resource));

        Self {
            family_id,
            realm,
            area,
            resource,
            store,
            next_resource_offset,
            area_lease: OffsetLease::new(),
            realm_lease: OffsetLease::new(),
            active_session: None,
            pending_commits: VecDeque::new(),
            area_actor_route,
            commit_notification_route,
            commit_timer: None,
            pending_publish: None,
        }
    }

    fn handle_begin_session(
        &mut self,
        expected_offset: u64,
        ingest_metadata: Option<super::protocol::IngestMetadata>,
        _ctx: &mut Context<Self>,
    ) -> Result<BeginSessionResponse, StreamError> {
        // Enforce single active session per resource
        if self.active_session.is_some() {
            return Err(StreamError::SessionAlreadyActive);
        }

        // Validate expected offset
        if expected_offset != self.next_resource_offset {
            return Err(StreamError::ConcurrencyConflict);
        }

        // Begin session in store (no offsets allocated yet)
        let session_id = self
            .store
            .begin_session(
                self.family_id.as_u64(),
                &self.realm,
                &self.area,
                &self.resource,
                ingest_metadata,
            )
            .map_err(|_| StreamError::SessionAlreadyActive)?;

        self.active_session = Some(session_id);

        Ok(BeginSessionResponse { session_id })
    }

    fn handle_append_to_session(
        &self,
        session_id: &SessionId,
        body: Bytes,
        metadata: Option<Bytes>,
    ) -> Result<AppendResponse, StreamError> {
        // Validate event size
        if body.len() + metadata.as_ref().map(|m| m.len()).unwrap_or(0) > MAX_EVENT_SIZE {
            return Err(StreamError::EventTooLarge);
        }

        let event = EventPayload { body, metadata };

        self.store
            .append_to_session(self.family_id.as_u64(), session_id, event)
            .map_err(|_| StreamError::EventTooLarge)?;

        Ok(AppendResponse { success: true })
    }

    fn handle_commit_session(
        &mut self,
        session_id: &SessionId,
        mode: StreamWriteMode,
        ctx: &mut Context<Self>,
    ) -> Result<CommitSessionResponse, StreamError> {
        // Verify this is the active session
        if self.active_session.as_ref() != Some(session_id) {
            return Err(StreamError::SessionNotFound);
        }

        // Get event count (for offset allocation)
        let event_count = self
            .store
            .session_event_count(session_id)
            .ok_or(StreamError::SessionNotFound)?;

        if event_count == 0 {
            self.active_session = None;
            return Err(StreamError::ConcurrencyConflict);
        }

        let count = event_count as u64;

        // Ensure sufficient leases BEFORE allocating
        if self.area_lease.remaining() < count || self.realm_lease.remaining() < count {
            // Request leases from AreaActor
            let lease_size = DEFAULT_LEASE_SIZE.max(count);
            let lease_req = StreamMessage::RequestLease {
                realm: self.realm.clone(),
                area: self.area.clone(),
                count: lease_size,
                reply_to: format!(
                    "stream_actor_{}_{}_{}",
                    self.realm, self.area, self.resource
                ),
            };
            let area_addr = RouteAddress::new(self.family_id, self.area_actor_route.clone());
            let _ = ctx.send(area_addr, lease_req);

            // Queue this commit (and its mode) to be processed when lease arrives
            self.pending_commits.push_back((*session_id, mode));
            return Err(StreamError::LeaseRequested);
        }

        // Allocate offsets contiguously
        let first_resource_offset = self.next_resource_offset;
        let first_area_offset = self.area_lease.next;
        let first_realm_offset = self.realm_lease.next;

        // Advance lease cursors
        self.area_lease.next += count;
        self.realm_lease.next += count;

        // Commit to storage with pre-assigned first offsets (atomic write)
        let response = self
            .store
            .commit_session(
                self.family_id.as_u64(),
                session_id,
                first_resource_offset,
                first_area_offset,
                first_realm_offset,
                mode,
            )
            .map_err(|_| StreamError::ConcurrencyConflict)?;

        // Update local offset tracking ONLY on success
        self.next_resource_offset += count;

        // Clear active session
        self.active_session = None;

        // Send BatchCommitted notification to AreaActor for watermark tracking
        let notification = StreamMessage::BatchCommitted(BatchCommitted {
            first_area_offset: response.first_area_offset,
            last_area_offset: response.last_area_offset,
            first_realm_offset: response.first_realm_offset,
            last_realm_offset: response.last_realm_offset,
        });
        let area_addr = RouteAddress::new(self.family_id, self.area_actor_route.clone());
        let _ = ctx.send(area_addr, notification);

        // Publish notification: stream://realm/area/resource/committed
        let route = self.commit_notification_route.clone();

        let payload_json = serde_json::json!({
            "first_resource_offset": response.first_resource_offset,
            "last_resource_offset": response.last_resource_offset,
            "first_area_offset": response.first_area_offset,
            "last_area_offset": response.last_area_offset,
            "first_realm_offset": response.first_realm_offset,
            "last_realm_offset": response.last_realm_offset,
            "batch_size": response.batch_size,
        });
        let payload = Bytes::from(payload_json.to_string());

        let publish_event = DomainPublishEvent::new(self.family_id, route, payload);

        // Debounce commit notifications (do not send immediately)
        self.pending_publish = Some(publish_event);
        if self.commit_timer.is_none() {
            let timer_id = ctx
                .timer_manager()
                .schedule_once(std::time::Duration::from_millis(Self::NOTICE_DEBOUNCE_MS));
            self.commit_timer = Some(timer_id);
        }

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

    fn handle_abort_session(&mut self, session_id: &SessionId) -> Result<(), StreamError> {
        if self.active_session.as_ref() != Some(session_id) {
            return Err(StreamError::SessionNotFound);
        }

        self.store
            .abort_session(session_id)
            .map_err(|_| StreamError::SessionNotFound)?;

        self.active_session = None;
        Ok(())
    }

    fn handle_read(
        &self,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<ReadResponse, StreamError> {
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

        // Read from Midge storage (NOT from memory!) with cursor
        let params = crate::domains::stream::store::ReadResourceParams {
            family: self.family_id.as_u64(),
            realm: &self.realm,
            area: &self.area,
            resource: &self.resource,
            from_offset,
            limit,
            max_bytes,
        };
        let (records, cursor) = self
            .store
            .read_resource(&params)
            .map_err(|_| StreamError::InvalidReadBound)?;

        Ok(ReadResponse { records, cursor })
    }

    fn handle_last(&self) -> Result<PeekResponse, StreamError> {
        if self.next_resource_offset == 0 {
            return Ok(PeekResponse { record: None });
        }

        // Get the last visible entry in the stream (tail operation)
        let record = self
            .store
            .peek_resource(
                self.family_id.as_u64(),
                &self.realm,
                &self.area,
                &self.resource,
            )
            .map_err(|_| StreamError::InvalidReadBound)?;

        Ok(PeekResponse { record })
    }

    fn handle_get_metadata(&self) -> Result<super::protocol::GetMetadataResponse, StreamError> {
        // Get stream metadata (limits, TTL, offsets, watermarks)
        let metadata = self
            .store
            .get_metadata(
                self.family_id.as_u64(),
                &self.realm,
                &self.area,
                &self.resource,
            )
            .map_err(|_| StreamError::InvalidReadBound)?;

        Ok(super::protocol::GetMetadataResponse { metadata })
    }

    /// Update area lease from grant
    pub fn update_area_lease(&mut self, grant: LeaseGrant) {
        self.area_lease.update_from_area_lease(&grant);
    }

    /// Update realm lease from grant
    pub fn update_realm_lease(&mut self, grant: LeaseGrant) {
        self.realm_lease.update_from_realm_lease(&grant);
    }

    /// Process pending commits after lease grant arrives
    fn process_pending_commits(&mut self, ctx: &mut Context<Self>) {
        // Process all pending commits that now have sufficient leases
        while let Some((session_id, mode)) = self.pending_commits.pop_front() {
            // Try to commit (will re-queue if still insufficient)
            if self.handle_commit_session(&session_id, mode, ctx).is_err() {
                // If still insufficient, push back and stop
                self.pending_commits.push_front((session_id, mode));
                break;
            }
        }
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
            } => match self.handle_begin_session(expected_offset, ingest_metadata, ctx) {
                Ok(resp) => StreamResponse::BeginOk(resp),
                Err(err) => StreamResponse::Error(err),
            },
            StreamMessage::Append {
                session_id,
                body,
                metadata,
            } => match self.handle_append_to_session(&session_id, body, metadata) {
                Ok(resp) => StreamResponse::AppendOk(resp),
                Err(err) => StreamResponse::Error(err),
            },
            StreamMessage::Commit { session_id, mode } => {
                match self.handle_commit_session(&session_id, mode, ctx) {
                    Ok(resp) => StreamResponse::CommitOk(resp),
                    Err(err) => StreamResponse::Error(err),
                }
            }
            StreamMessage::Rollback { session_id } => {
                match self.handle_abort_session(&session_id) {
                    Ok(()) => StreamResponse::CommitOk(CommitSessionResponse {
                        first_resource_offset: 0,
                        last_resource_offset: 0,
                        first_area_offset: 0,
                        last_area_offset: 0,
                        first_realm_offset: 0,
                        last_realm_offset: 0,
                        batch_size: 0,
                        ingest_metadata: None,
                    }),
                    Err(err) => StreamResponse::Error(err),
                }
            }
            StreamMessage::Read {
                from_offset,
                limit,
                max_bytes,
                ..
            } => match self.handle_read(from_offset, limit, max_bytes) {
                Ok(resp) => StreamResponse::ReadOk(resp),
                Err(err) => StreamResponse::Error(err),
            },
            StreamMessage::Last { .. } => match self.handle_last() {
                Ok(resp) => StreamResponse::LastOk(resp),
                Err(err) => StreamResponse::Error(err),
            },
            StreamMessage::GetMetadata { .. } => match self.handle_get_metadata() {
                Ok(resp) => StreamResponse::MetadataOk(resp),
                Err(err) => StreamResponse::Error(err),
            },
            StreamMessage::LeaseGranted { grant } => {
                // Update leases from AreaActor grant
                self.update_area_lease(grant.clone());
                self.update_realm_lease(grant);

                // Process any pending commits now that we have leases
                self.process_pending_commits(ctx);

                // Internal message - no response needed
                return;
            }
            // Subscribe/Unsubscribe messages are handled at session level, not in actor
            StreamMessage::Subscribe { .. }
            | StreamMessage::Unsubscribe { .. }
            | StreamMessage::UnsubscribeAll { .. } => {
                return; // No response for pub/sub control messages
            }
            // Batch/watermark notifications from other actors - no response
            StreamMessage::BatchCommitted(_) | StreamMessage::AreaWatermarkAdvanced(_) => {
                return;
            }
            // Internal lease request - no response
            StreamMessage::RequestLease { .. } => {
                return;
            }
            // Internal realm lease request - no response
            StreamMessage::RequestRealmLease { .. } => {
                return;
            }
        };

        let _ = ctx.reply(response).ok();
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
        // If commit debounce timer fired, send pending publish
        if self.commit_timer.is_some() && Some(timer_id) == self.commit_timer {
            if let Some(event) = self.pending_publish.take() {
                let _ = ctx.publish_event(event);
            }
            self.commit_timer = None;
        }
    }
}
