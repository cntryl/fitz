//! Stream actor: manages append-only event log for a single resource

use bytes::Bytes;
use std::sync::Arc;

use crate::domains::notification::protocol::{NotificationMessage, PublishMessage};
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

use super::protocol::{
    AppendResponse, BeginSessionResponse, CommitSessionResponse, LeaseGrant, OffsetLease,
    PeekResponse, ReadResponse, StreamError, StreamMessage, DEFAULT_LEASE_SIZE, MAX_EVENT_SIZE,
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
    pending_commits: VecDeque<SessionId>,

    /// Cached routes (hot path optimization)
    area_actor_route: Route,
    commit_notification_route: Route,

    /// Debounce timer id for commit notification
    commit_timer: Option<crate::runtime::context::TimerId>,

    /// Pending commit publish message (debounced)
    pending_publish: Option<PublishMessage>,
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
        let next_resource_offset = match store.get_next_resource_offset(&realm, &area, &resource) {
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
        let commit_notification_route = Route::new(format!(
            "notice://{}/{}/{}/committed",
            realm, area, resource
        ));

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
            .begin_session(&self.realm, &self.area, &self.resource, ingest_metadata)
            .map_err(|_| StreamError::SessionAlreadyActive)?;

        self.active_session = Some(session_id.clone());

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
            .append_to_session(session_id, event)
            .map_err(|_| StreamError::EventTooLarge)?;

        Ok(AppendResponse { success: true })
    }

    fn handle_commit_session(
        &mut self,
        session_id: &SessionId,
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

            // Queue this commit to be processed when lease arrives
            self.pending_commits.push_back(session_id.clone());
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
                session_id,
                first_resource_offset,
                first_area_offset,
                first_realm_offset,
            )
            .map_err(|_| StreamError::ConcurrencyConflict)?;

        // Update local offset tracking ONLY on success
        self.next_resource_offset += count;

        // Clear active session
        self.active_session = None;

        // Send BatchCommitted notification to AreaActor for watermark tracking
        let notification = StreamMessage::BatchCommitted {
            first_area_offset: response.first_area_offset,
            last_area_offset: response.last_area_offset,
            first_realm_offset: response.first_realm_offset,
            last_realm_offset: response.last_realm_offset,
        };
        let area_addr = RouteAddress::new(self.family_id, self.area_actor_route.clone());
        let _ = ctx.send(area_addr, notification);

        // Publish notification: notice://realm/area/resource/committed
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

        let publish_msg = PublishMessage::new(self.family_id, route.clone(), payload);

        // Debounce commit notifications (do not send immediately)
        self.pending_publish = Some(publish_msg);
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
        // Read from Midge storage (NOT from memory!) with cursor
        let (records, cursor) = self
            .store
            .read_resource(
                &self.realm,
                &self.area,
                &self.resource,
                from_offset,
                limit,
                max_bytes,
            )
            .map_err(|_| StreamError::InvalidReadBound)?;

        Ok(ReadResponse { records, cursor })
    }

    fn handle_peek(&self) -> Result<PeekResponse, StreamError> {
        // Peek at last committed record (tail operation)
        let record = self
            .store
            .peek_resource(&self.realm, &self.area, &self.resource)
            .map_err(|_| StreamError::InvalidReadBound)?;

        Ok(PeekResponse { record })
    }

    fn handle_get_metadata(&self) -> Result<super::protocol::GetMetadataResponse, StreamError> {
        // Get stream metadata (limits, TTL, offsets, watermarks)
        let metadata = self
            .store
            .get_metadata(&self.realm, &self.area, &self.resource)
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
        while let Some(session_id) = self.pending_commits.pop_front() {
            // Try to commit (will re-queue if still insufficient)
            if self.handle_commit_session(&session_id, ctx).is_err() {
                // If still insufficient, push back and stop
                self.pending_commits.push_front(session_id);
                break;
            }
        }
    }
}

impl Actor for StreamActor {
    type Message = StreamMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::BeginSession {
                expected_offset,
                ingest_metadata,
                ..
            } => {
                let _ = self.handle_begin_session(expected_offset, ingest_metadata, ctx);
            }
            StreamMessage::AppendToSession {
                session_id,
                body,
                metadata,
            } => {
                let _ = self.handle_append_to_session(&session_id, body, metadata);
            }
            StreamMessage::CommitSession { session_id } => {
                let _ = self.handle_commit_session(&session_id, ctx);
            }
            StreamMessage::AbortSession { session_id } => {
                let _ = self.handle_abort_session(&session_id);
            }
            StreamMessage::Read {
                from_offset,
                limit,
                max_bytes,
                ..
            } => {
                let _ = self.handle_read(from_offset, limit, max_bytes);
            }
            StreamMessage::Peek { .. } => {
                let _ = self.handle_peek();
            }
            StreamMessage::GetMetadata { .. } => {
                let _ = self.handle_get_metadata();
            }
            StreamMessage::LeaseGranted { grant } => {
                // Update leases from AreaActor grant
                self.update_area_lease(grant.clone());
                self.update_realm_lease(grant);

                // Process any pending commits now that we have leases
                self.process_pending_commits(ctx);
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
        // If commit debounce timer fired, send pending publish
        if self.commit_timer.is_some() && Some(timer_id) == self.commit_timer {
            if let Some(publish_msg) = self.pending_publish.take() {
                let route = publish_msg.route.clone();
                let notice_addr = RouteAddress::new(self.family_id, route);
                let _ = ctx.send(notice_addr, NotificationMessage::Publish(publish_msg));
            }
            self.commit_timer = None;
        }
    }
}
