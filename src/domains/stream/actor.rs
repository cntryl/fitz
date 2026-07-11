//! Stream actor: live append-session owner for a single resource stream.

use bytes::Bytes;
use std::sync::Arc;

use crate::runtime::routing::RouteFamily;

use super::protocol::{
    CommitSessionResponse, GetMetadataResponse, IngestMetadata, PeekResponse, ReadResponse,
    StreamDiscriminator, StreamFilterSet, StreamWriteMode, MAX_EVENT_SIZE,
};
use super::store::{CommitRecordsParams, EventPayload, StreamStore};

#[derive(Debug, Clone)]
struct ActiveAppendSession {
    stream_session_id: u64,
    owner_session_id: u64,
    expected_offset: Option<u64>,
    staged_events: Vec<EventPayload>,
    total_bytes: usize,
    ingest_metadata: Option<IngestMetadata>,
}

impl ActiveAppendSession {}

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
}

impl StreamActor {
    /// # Errors
    ///
    /// Returns an error when loading the next durable resource offset from the
    /// backing `StreamStore` fails.
    pub fn new(
        family_id: RouteFamily,
        realm: String,
        area: String,
        resource: String,
        store: Arc<StreamStore>,
    ) -> Result<Self, String> {
        let next_resource_offset =
            store.get_next_resource_offset(family_id.as_u64(), &realm, &area, &resource)?;

        Ok(Self {
            family_id,
            realm,
            area,
            resource,
            store,
            next_resource_offset,
            active_session: None,
        })
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

    /// # Errors
    ///
    /// Returns an error when a live append session is already active for this
    /// resource stream.
    pub fn begin_append_session(
        &mut self,
        owner_session_id: u64,
        stream_session_id: u64,
        ingest_metadata: Option<IngestMetadata>,
    ) -> Result<u64, String> {
        if self.active_session.is_some() {
            return Err("session already active".to_string());
        }

        self.active_session = Some(ActiveAppendSession {
            stream_session_id,
            owner_session_id,
            expected_offset: None,
            staged_events: Vec::new(),
            total_bytes: 0,
            ingest_metadata,
        });
        Ok(stream_session_id)
    }

    /// # Errors
    ///
    /// Returns an error when the session is missing, the expected offset does
    /// not match the live staged state, the event exceeds size limits, or the
    /// staged batch exceeds configured limits.
    pub fn append_to_session_with_discriminator_for_owner(
        &mut self,
        owner_session_id: u64,
        stream_session_id: u64,
        expected_offset: u64,
        body: Bytes,
        metadata: Option<Bytes>,
        discriminator: Option<StreamDiscriminator>,
    ) -> Result<u64, String> {
        let session = self
            .active_session
            .as_mut()
            .filter(|session| {
                session.stream_session_id == stream_session_id
                    && session.owner_session_id == owner_session_id
            })
            .ok_or_else(|| "session not found".to_string())?;

        let assigned_offset = if let Some(base_expected_offset) = session.expected_offset {
            let staged_event_count = u64::try_from(session.staged_events.len())
                .map_err(|_| "concurrency conflict".to_string())?;
            let next_expected_offset = base_expected_offset
                .checked_add(staged_event_count)
                .ok_or_else(|| "concurrency conflict".to_string())?;
            if expected_offset != next_expected_offset {
                return Err("concurrency conflict".to_string());
            }
            next_expected_offset
        } else {
            if expected_offset != self.next_resource_offset {
                return Err("concurrency conflict".to_string());
            }
            session.expected_offset = Some(expected_offset);
            expected_offset
        };

        let event_size = body
            .len()
            .checked_add(metadata.as_ref().map_or(0, Bytes::len))
            .and_then(|size| {
                size.checked_add(
                    discriminator
                        .as_ref()
                        .map_or(0, |value| value.as_str().len()),
                )
            })
            .ok_or_else(|| "event too large".to_string())?;
        if event_size > MAX_EVENT_SIZE {
            return Err("event too large".to_string());
        }

        let limits = self.store.batch_limits();
        let next_event_count = session
            .staged_events
            .len()
            .checked_add(1)
            .ok_or_else(|| "batch too large".to_string())?;
        let next_total_bytes = session
            .total_bytes
            .checked_add(event_size)
            .ok_or_else(|| "batch too large".to_string())?;
        if next_event_count > limits.max_batch_events || next_total_bytes > limits.max_batch_bytes {
            return Err("batch too large".to_string());
        }

        session.total_bytes = next_total_bytes;
        session.staged_events.push(EventPayload {
            body,
            metadata,
            discriminator,
        });
        Ok(assigned_offset)
    }

    /// # Errors
    ///
    /// Returns an error when the session is missing, empty, uninitialized, or
    /// when the durable commit fails.
    pub fn commit_session_for_owner(
        &mut self,
        owner_session_id: u64,
        stream_session_id: u64,
        mode: StreamWriteMode,
    ) -> Result<CommitSessionResponse, String> {
        let session = self
            .active_session
            .take()
            .ok_or_else(|| "session not found".to_string())?;

        if session.stream_session_id != stream_session_id
            || session.owner_session_id != owner_session_id
        {
            self.active_session = Some(session);
            return Err("session not found".to_string());
        }

        if session.staged_events.is_empty() {
            self.active_session = Some(session);
            return Err("empty batch".to_string());
        }

        let Some(expected_offset) = session.expected_offset else {
            self.active_session = Some(session);
            return Err("session not initialized".to_string());
        };

        let response = match self.store.commit_records(CommitRecordsParams {
            family: self.family_id.as_u64(),
            realm: &self.realm,
            area: &self.area,
            resource: &self.resource,
            expected_resource_next_offset: expected_offset,
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

    /// # Errors
    ///
    /// Returns an error when the session is missing or owned by a different
    /// caller.
    pub fn rollback_session_for_owner(
        &mut self,
        owner_session_id: u64,
        stream_session_id: u64,
    ) -> Result<(), String> {
        let session = self
            .active_session
            .take()
            .ok_or_else(|| "session not found".to_string())?;

        if session.stream_session_id != stream_session_id
            || session.owner_session_id != owner_session_id
        {
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

    /// # Errors
    ///
    /// Returns an error when the underlying durable read fails.
    pub fn read(
        &self,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<ReadResponse, String> {
        self.read_with_filter(from_offset, limit, max_bytes, None)
    }

    /// # Errors
    ///
    /// Returns an error when the underlying durable read fails.
    pub fn read_with_filter(
        &self,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
        filter: Option<&StreamFilterSet>,
    ) -> Result<ReadResponse, String> {
        if limit == 0 || from_offset >= self.next_resource_offset {
            return Ok(ReadResponse {
                items: Vec::new(),
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
        let (items, cursor) = self.store.read_resource_with_filter(&params, filter)?;
        Ok(ReadResponse { items, cursor })
    }

    /// # Errors
    ///
    /// Returns an error when the underlying durable peek fails.
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

    /// # Errors
    ///
    /// Returns an error when loading durable stream metadata fails.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::stream::storage::{
        encode_stream_layout_marker_key, StreamLayoutMarkerValue,
    };
    use crate::domains::stream::store::StreamStorageLayout;
    use crate::testkit::create_test_engine_with_cfs;

    #[test]
    fn should_return_error_given_legacy_layout_marker_during_actor_recovery() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let mut txn = db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        txn.put(
            encode_stream_layout_marker_key(),
            StreamLayoutMarkerValue::new(StreamStorageLayout::LegacyCovering).encode(),
            None,
        )
        .expect("write legacy layout marker");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit legacy layout marker");
        let store = Arc::new(StreamStore::new(db));

        // Act
        let result = StreamActor::new(
            RouteFamily::new(1),
            "test".to_string(),
            "events".to_string(),
            "orders".to_string(),
            store,
        );

        // Assert
        let Err(error) = result else {
            panic!("actor recovery should surface layout mismatch");
        };
        assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_MISMATCH"));
    }

    #[test]
    fn should_preserve_active_session_given_store_commit_failure() {
        // Arrange
        let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
        let mut actor = StreamActor::new(
            RouteFamily::new(1),
            "test".to_string(),
            "events".to_string(),
            "orders".to_string(),
            store.clone(),
        )
        .expect("create actor");
        actor
            .begin_append_session(10, 100, None)
            .expect("begin append session");
        actor
            .append_to_session_with_discriminator_for_owner(
                10,
                100,
                0,
                Bytes::from_static(b"retry"),
                None,
                None,
            )
            .expect("append staged event");
        store.fail_next_promotion_frontier_commit_for_tests();

        // Act
        let failed = actor.commit_session_for_owner(10, 100, StreamWriteMode::Buffered);
        let retry = actor
            .commit_session_for_owner(10, 100, StreamWriteMode::Buffered)
            .expect("retry commit should preserve staged event");
        let (records, cursor) = store
            .read_resource(&crate::domains::stream::store::ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: 0,
                limit: 10,
                max_bytes: None,
            })
            .expect("read committed resource records");

        // Assert
        assert_eq!(
            failed.expect_err("injected store failure should fail"),
            "Injected stream commit failure"
        );
        assert_eq!(retry.first_resource_offset, 0);
        assert_eq!(retry.first_area_offset, 0);
        assert_eq!(retry.first_realm_offset, 0);
        assert!(!actor.has_active_session());
        assert_eq!(records.len(), 1);
        assert_eq!(cursor.last_resource_offset, 0);
    }

    #[test]
    fn should_preserve_active_session_given_uninitialized_staged_state() {
        // Arrange
        let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
        let mut actor = StreamActor::new(
            RouteFamily::new(1),
            "test".to_string(),
            "events".to_string(),
            "orders".to_string(),
            store,
        )
        .expect("create actor");
        actor
            .begin_append_session(10, 100, None)
            .expect("begin append session");
        let session = actor.active_session.as_mut().expect("active session");
        session.staged_events.push(EventPayload {
            body: Bytes::from_static(b"staged"),
            metadata: None,
            discriminator: None,
        });
        session.total_bytes = 6;

        // Act
        let result = actor.commit_session_for_owner(10, 100, StreamWriteMode::Buffered);

        // Assert
        assert_eq!(
            result.expect_err("uninitialized state should fail"),
            "session not initialized"
        );
        assert!(actor.has_active_session());
    }
}
