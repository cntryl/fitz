//! Stream actor: live append-session owner for a single resource stream.

use bytes::Bytes;
use std::sync::Arc;

use crate::runtime::routing::RouteFamily;

use super::protocol::{
    CommitSessionResponse, GetMetadataResponse, IngestMetadata, PeekResponse, ReadResponse,
    StreamDiscriminator, StreamFilterSet, StreamWriteMode, MAX_EVENT_SIZE,
};
use super::store::{CommitRecordsParams, EventPayload, StreamStore, StreamStoreError};

#[derive(Debug, Clone)]
struct ActiveAppendSession {
    stream_session_id: u64,
    owner_session_id: u64,
    expected_offset: Option<u64>,
    staged_events: Vec<EventPayload>,
    total_bytes: usize,
    ingest_metadata: Option<IngestMetadata>,
}

/// Warm in-memory append-session state for a single resource stream.
///
/// The actor holds only live session state for the current broker process.
/// Committed events, offsets, and metadata stay in [`StreamStore`].
pub struct StreamActor {
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
            .ok_or_else(|| {
                StreamStoreError::SessionNotFound
                    .client_message()
                    .to_string()
            })?;

        let assigned_offset = if let Some(base_expected_offset) = session.expected_offset {
            let staged_event_count = u64::try_from(session.staged_events.len()).map_err(|_| {
                StreamStoreError::ConcurrencyConflict
                    .client_message()
                    .to_string()
            })?;
            let next_expected_offset = base_expected_offset
                .checked_add(staged_event_count)
                .ok_or_else(|| {
                    StreamStoreError::ConcurrencyConflict
                        .client_message()
                        .to_string()
                })?;
            if expected_offset != next_expected_offset {
                return Err(StreamStoreError::ConcurrencyConflict
                    .client_message()
                    .to_string());
            }
            next_expected_offset
        } else {
            if expected_offset != self.next_resource_offset {
                return Err(StreamStoreError::ConcurrencyConflict
                    .client_message()
                    .to_string());
            }
            session.expected_offset = Some(expected_offset);
            expected_offset
        };

        let payload_bytes = body
            .len()
            .checked_add(metadata.as_ref().map_or(0, Bytes::len))
            .ok_or_else(|| "event too large".to_string())?;
        if payload_bytes > MAX_EVENT_SIZE {
            return Err("event too large".to_string());
        }

        let event_size = payload_bytes
            .checked_add(
                discriminator
                    .as_ref()
                    .map_or(0, |value| value.as_str().len()),
            )
            .ok_or_else(|| "event too large".to_string())?;

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
        let session = self.active_session.take().ok_or_else(|| {
            StreamStoreError::SessionNotFound
                .client_message()
                .to_string()
        })?;

        if session.stream_session_id != stream_session_id
            || session.owner_session_id != owner_session_id
        {
            self.active_session = Some(session);
            return Err(StreamStoreError::SessionNotFound
                .client_message()
                .to_string());
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
                return Err(StreamStoreError::from_store_message(&error)
                    .map_or(error, |error| error.client_message().to_string()));
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
            first_global_offset: response.first_global_offset,
            last_global_offset: response.last_global_offset,
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
        let session = self.active_session.take().ok_or_else(|| {
            StreamStoreError::SessionNotFound
                .client_message()
                .to_string()
        })?;

        if session.stream_session_id != stream_session_id
            || session.owner_session_id != owner_session_id
        {
            self.active_session = Some(session);
            return Err(StreamStoreError::SessionNotFound
                .client_message()
                .to_string());
        }

        if let Err(error) = self.store.abandon_pending_global_reservation(
            self.family_id.as_u64(),
            &self.realm,
            &self.area,
            &self.resource,
        ) {
            self.active_session = Some(session);
            return Err(error);
        }

        Ok(())
    }

    pub fn cleanup_session(&mut self, owner_session_id: u64) -> Option<u64> {
        let matches_owner = self
            .active_session
            .as_ref()
            .is_some_and(|session| session.owner_session_id == owner_session_id);
        if !matches_owner {
            return None;
        }
        let session = self.active_session.take()?;
        if let Err(error) = self.store.abandon_pending_global_reservation(
            self.family_id.as_u64(),
            &self.realm,
            &self.area,
            &self.resource,
        ) {
            tracing::warn!(
                domain = "stream",
                route_family = self.family_id.id(),
                realm = self.realm.as_str(),
                area = self.area.as_str(),
                resource = self.resource.as_str(),
                error = %error,
                "Stream cleanup could not resolve an abandoned global reservation"
            );
        }
        Some(session.stream_session_id)
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
                    last_global_offset: None,
                    has_more: false,
                    cursor_fingerprint: None,
                    captured_watermark: None,
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
    use crate::testkit::create_test_engine_with_cfs;

    mod state_model;

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
    fn should_resolve_retry_reservation_when_session_is_rolled_back() {
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
                Bytes::from_static(b"abandoned"),
                None,
                None,
            )
            .expect("append staged event");
        store.fail_next_promotion_frontier_commit_for_tests();
        let _failed = actor.commit_session_for_owner(10, 100, StreamWriteMode::Buffered);
        let later_events = [EventPayload {
            body: Bytes::from_static(b"visible"),
            metadata: None,
            discriminator: None,
        }];
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audit",
                expected_resource_next_offset: 0,
                events: &later_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("commit later global range");
        let held_watermark = store.get_global_watermark(1).expect("held watermark");

        // Act
        actor
            .rollback_session_for_owner(10, 100)
            .expect("roll back failed commit session");

        // Assert
        assert_eq!(held_watermark, 0);
        assert_eq!(
            store.get_global_watermark(1).expect("advanced watermark"),
            2
        );
        assert!(!actor.has_active_session());
    }

    #[test]
    fn should_reject_an_append_larger_than_a_read_response_can_carry() {
        // Arrange
        // Every read plane frames one response as a single TLV value with a
        // `u16` length, so a record whose wire cost exceeds that ceiling can
        // never be replayed - and because the read accumulator refuses to
        // build an unencodable response, it permanently blocks pagination at
        // its own offset on the resource, area, realm, and global planes
        // alike. Stream guarantees exact replay of committed history, so such
        // an append must be refused rather than committed and then wedged.
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

        // Act
        let rejected = actor.append_to_session_with_discriminator_for_owner(
            10,
            100,
            0,
            Bytes::from(vec![b'a'; 200_000]),
            None,
            None,
        );

        // Assert
        let error = rejected.expect_err("an unreadable append must be refused");
        assert!(
            error.contains("event too large"),
            "unexpected error: {error}"
        );
        assert!(
            actor
                .commit_session_for_owner(10, 100, StreamWriteMode::Buffered)
                .is_err(),
            "the refused event must not have been staged for commit"
        );
    }

    #[test]
    fn should_accept_an_append_at_the_published_event_size_limit() {
        // Arrange
        // The public limit reserves enough space for the largest valid route,
        // so an event at that boundary must remain readable here.
        let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
        let realm = "r".repeat(
            crate::utils::route_shape::MAX_ROUTE_BYTES - "stream://".len() - "/events/orders".len(),
        );
        let mut actor = StreamActor::new(
            RouteFamily::new(1),
            realm,
            "events".to_string(),
            "orders".to_string(),
            store.clone(),
        )
        .expect("create actor");
        actor
            .begin_append_session(10, 100, None)
            .expect("begin append session");
        let largest_body = MAX_EVENT_SIZE;

        // Act
        let accepted = actor.append_to_session_with_discriminator_for_owner(
            10,
            100,
            0,
            Bytes::from(vec![b'a'; largest_body]),
            None,
            None,
        );
        actor
            .commit_session_for_owner(10, 100, StreamWriteMode::Buffered)
            .expect("commit the largest readable event");
        let read = actor.read(0, 10, None).expect("read it back");

        // Assert
        assert_eq!(accepted, Ok(0));
        assert_eq!(
            read.items.len(),
            1,
            "the largest accepted event must read back"
        );
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
