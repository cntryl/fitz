//! Stream actor: manages append-only event log for a single resource

use std::sync::Arc;
use bytes::Bytes;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::{RouteFamily, Route, RouteAddress};
use crate::domains::notification::protocol::{PublishMessage, NotificationMessage};

use super::protocol::{
    BeginSessionResponse, AppendResponse, CommitSessionResponse, ReadResponse, PeekResponse,
    StreamError, StreamMessage, LeaseGrant
};
use super::store::{StreamStore, EventPayload, SessionId};

/// Maximum event size (1 MB)
const MAX_EVENT_SIZE: usize = 1_048_576;

/// Default lease size for area/realm offsets (1000 offsets per lease)
const DEFAULT_LEASE_SIZE: u64 = 1000;

/// Lease for area or realm offsets
/// 
/// **Semantics**: `end` is EXCLUSIVE (not inclusive)
/// Valid range: [next, end)
#[derive(Debug, Clone)]
struct OffsetLease {
    next: u64,
    end: u64,  // exclusive
}

impl OffsetLease {
    fn new() -> Self {
        Self { next: 0, end: 0 }
    }
    
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.next >= self.end
    }
    
    fn remaining(&self) -> u64 {
        if self.end > self.next {
            self.end - self.next
        } else {
            0
        }
    }
    
    fn update_from_area_lease(&mut self, grant: &LeaseGrant) {
        self.next = grant.area_start;
        self.end = grant.area_end_exclusive;  // Already exclusive
    }
    
    fn update_from_realm_lease(&mut self, grant: &LeaseGrant) {
        self.next = grant.realm_start;
        self.end = grant.realm_end_exclusive;  // Already exclusive
    }
}

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
}

impl StreamActor {
    pub fn new(
        family_id: RouteFamily, 
        realm: String,
        area: String,
        resource: String,
        store: Arc<StreamStore>
    ) -> Self {
        // **CRITICAL: Recover next_resource_offset from metadata counter**
        // This prevents offset reuse after TTL expiry and process restart
        let next_resource_offset = match store.get_next_resource_offset(&realm, &area, &resource) {
            Ok(offset) => offset,
            Err(e) => {
                eprintln!("FATAL: Failed to recover resource offset for {}/{}/{}: {}", realm, area, resource, e);
                0  // Fallback to 0, may cause conflict if stream has data
            }
        };
        
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
        let session_id = self.store.begin_session(
            &self.realm,
            &self.area,
            &self.resource,
            ingest_metadata,
        ).map_err(|_| StreamError::SessionAlreadyActive)?;
        
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
        
        self.store.append_to_session(session_id, event)
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
        let event_count = self.store.session_event_count(session_id)
            .ok_or(StreamError::SessionNotFound)?;
        
        if event_count == 0 {
            self.active_session = None;
            return Err(StreamError::ConcurrencyConflict);
        }
        
        let count = event_count as u64;
        
        // Ensure sufficient leases BEFORE allocating
        // If short, request leases (in real impl, would await response)
        if self.area_lease.remaining() < count || self.realm_lease.remaining() < count {
            // Request leases from AreaActor
            let lease_size = DEFAULT_LEASE_SIZE.max(count);
            let lease_req = StreamMessage::RequestLease {
                realm: self.realm.clone(),
                area: self.area.clone(),
                count: lease_size,
                reply_to: format!("stream_actor_{}_{}_{}", self.realm, self.area, self.resource),
            };
            let area_addr = self.area_actor_address();
            let _ = ctx.send(area_addr, lease_req);
            
            // TODO: In full impl, would await LeaseGranted before proceeding
            // For now, return error to indicate async operation needed
            return Err(StreamError::SessionFull);
        }
        
        // Allocate offsets contiguously
        let first_resource_offset = self.next_resource_offset;
        let first_area_offset = self.area_lease.next;
        let first_realm_offset = self.realm_lease.next;
        
        // Advance lease cursors
        self.area_lease.next += count;
        self.realm_lease.next += count;
        
        // Commit to storage with pre-assigned first offsets (atomic write)
        let response = self.store.commit_session(
            session_id,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
        ).map_err(|_| StreamError::ConcurrencyConflict)?;
        
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
        let area_addr = self.area_actor_address();
        let _ = ctx.send(area_addr, notification);
        
        // Publish notification: notice://realm/area/resource/committed
        let route_str = format!("notice://{}/{}/{}/committed", self.realm, self.area, self.resource);
        let route = Route::new(route_str);
        
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
        let notice_addr = RouteAddress::new(self.family_id, route);
        let _ = ctx.send(notice_addr, NotificationMessage::Publish(publish_msg));
        
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
    
    fn handle_abort_session(
        &mut self,
        session_id: &SessionId,
    ) -> Result<(), StreamError> {
        if self.active_session.as_ref() != Some(session_id) {
            return Err(StreamError::SessionNotFound);
        }
        
        self.store.abort_session(session_id)
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
        let (records, cursor) = self.store.read_resource(
            &self.realm,
            &self.area,
            &self.resource,
            from_offset,
            limit,
            max_bytes,
        ).map_err(|_| StreamError::InvalidReadBound)?;
        
        Ok(ReadResponse { records, cursor })
    }
    
    fn handle_peek(&self) -> Result<PeekResponse, StreamError> {
        // Peek at last committed record (tail operation)
        let record = self.store.peek_resource(
            &self.realm,
            &self.area,
            &self.resource,
        ).map_err(|_| StreamError::InvalidReadBound)?;
        
        Ok(PeekResponse { record })
    }
    
    fn handle_get_metadata(&self) -> Result<super::protocol::GetMetadataResponse, StreamError> {
        // Get stream metadata (limits, TTL, offsets, watermarks)
        let metadata = self.store.get_metadata(
            &self.realm,
            &self.area,
            &self.resource,
        ).map_err(|_| StreamError::InvalidReadBound)?;
        
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
    
    /// Get RouteAddress for AreaActor coordination
    fn area_actor_address(&self) -> RouteAddress {
        let route = Route::new(format!("stream://{}/{}/__area__", self.realm, self.area));
        RouteAddress::new(self.family_id, route)
    }
}

impl Actor for StreamActor {
    type Message = StreamMessage;
    
    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::BeginSession { expected_offset, ingest_metadata, .. } => {
                let _ = self.handle_begin_session(expected_offset, ingest_metadata, ctx);
            }
            StreamMessage::AppendToSession { session_id, body, metadata } => {
                let _ = self.handle_append_to_session(&session_id, body, metadata);
            }
            StreamMessage::CommitSession { session_id } => {
                let _ = self.handle_commit_session(&session_id, ctx);
            }
            StreamMessage::AbortSession { session_id } => {
                let _ = self.handle_abort_session(&session_id);
            }
            StreamMessage::Read { from_offset, limit, max_bytes, .. } => {
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
            }
            _ => {}
        }
    }
}
