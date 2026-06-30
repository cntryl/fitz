use super::{
    encode_compact_resource_page_key, AppendSession, CommitPromotionFrontierBatchParams,
    CommitRecordsParams, CommitResponse, CompactResourcePageValue, EventPayload, IngestMetadata,
    ResourceMetaValue, SessionId, StreamAdminRecord, StreamRecord, StreamStore, StreamWriteMode,
    ERR_SESSION_ROUTE_FAMILY_MISMATCH,
};

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl StreamStore {
    /// # Errors
    ///
    /// Returns an error if layout activation fails, the batch is empty, the
    /// expected frontier no longer matches persisted state, or the commit write
    /// path fails.
    pub fn commit_records(
        &self,
        params: CommitRecordsParams<'_>,
    ) -> Result<CommitResponse, String> {
        self.ensure_layout_activation_for_family(params.family)?;

        self.commit_records_promotion_frontier(params)
    }

    pub(super) fn commit_records_promotion_frontier(
        &self,
        params: CommitRecordsParams<'_>,
    ) -> Result<CommitResponse, String> {
        let CommitRecordsParams {
            family,
            realm,
            area,
            resource,
            expected_resource_next_offset,
            events,
            ingest_metadata,
            mode,
        } = params;

        if events.is_empty() {
            return Err("ERR_EMPTY_BATCH".to_string());
        }

        let sequencing_guard = self.resource_sequence_guard(family, realm, area, resource);
        let _sequencing_lock = sequencing_guard.lock();

        let resource_meta_state = self.resource_meta_state(family, realm, area, resource);
        let mut resource_meta_state = resource_meta_state.lock();
        let (resource_meta_before, _) =
            self.load_resource_meta_snapshot(family, realm, area, resource)?;
        resource_meta_state.snapshot = Some(resource_meta_before.clone());
        if resource_meta_before.next_offset != expected_resource_next_offset {
            return Err("ERR_CONCURRENCY_CONFLICT".to_string());
        }

        let realm_sequence_state = self.realm_sequence_state(family, realm);
        let mut realm_sequence_state = realm_sequence_state.lock();
        let (area_next_offset, _) = self.load_area_next_offset_snapshot(family, realm, area)?;
        realm_sequence_state
            .next_area_offsets
            .insert(area.to_string(), area_next_offset);
        let (realm_next_offset, _) = self.load_realm_next_offset_snapshot(family, realm)?;
        realm_sequence_state.next_realm_offset = Some(realm_next_offset);

        let (response, resource_meta_after) =
            self.commit_promotion_frontier_batch(CommitPromotionFrontierBatchParams {
                family,
                realm,
                area,
                resource,
                first_resource_offset: resource_meta_before.next_offset,
                first_area_offset: area_next_offset,
                first_realm_offset: realm_next_offset,
                events,
                committed_size_before: resource_meta_before.committed_size_bytes,
                ingest_metadata,
                mode,
            })?;

        resource_meta_state.snapshot = Some(resource_meta_after);
        realm_sequence_state.next_area_offsets.insert(
            area.to_string(),
            response.last_area_offset.saturating_add(1),
        );
        realm_sequence_state.next_realm_offset = Some(response.last_realm_offset.saturating_add(1));

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error if layout activation fails or metadata scanning cannot
    /// be completed from storage.
    pub fn list_resource_metadata(&self, family: u64) -> Result<Vec<StreamAdminRecord>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.list_resource_metadata_promotion_frontier(family)
    }

    pub(super) fn list_resource_metadata_promotion_frontier(
        &self,
        family: u64,
    ) -> Result<Vec<StreamAdminRecord>, String> {
        let txn = self
            .db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        let resource_meta_query = cntryl_midge::Query::new();
        let mut resource_meta_iter = txn
            .scan(&resource_meta_query)
            .map_err(|e| format!("scan error: {e:?}"))?;

        let mut values = Vec::new();
        for (key, value) in resource_meta_iter.collect_all() {
            let Ok((realm, area, resource)) = Self::resource_identity_from_key(
                crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8,
                &key,
            ) else {
                continue;
            };
            let meta = ResourceMetaValue::decode(&value)?;
            if meta.next_offset == 0 {
                continue;
            }
            values.push(StreamAdminRecord {
                realm,
                area,
                resource,
                next_offset: meta.next_offset,
                committed_size_bytes: meta.committed_size_bytes,
            });
        }

        values.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
            ))
        });
        Ok(values)
    }

    /// # Errors
    ///
    /// Returns an error only if future session allocation or initialization
    /// fails.
    pub fn begin_session(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        ingest_metadata: Option<IngestMetadata>,
    ) -> Result<SessionId, String> {
        let session_id = self
            .next_session_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let initial_capacity = self.limits.max_batch_events.min(128);

        let session = AppendSession {
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            staged_events: Vec::with_capacity(initial_capacity),
            event_count: 0,
            total_bytes: 0,
            ingest_metadata,
        };

        self.sessions.lock().insert(session_id, session);
        Ok(session_id)
    }

    /// # Errors
    ///
    /// Returns an error if the session is missing, the route family does not
    /// match, or the staged batch would exceed configured size limits.
    pub fn append_to_session(
        &self,
        family: u64,
        session_id: SessionId,
        event: EventPayload,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(&session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;

        if session.family != family {
            return Err(ERR_SESSION_ROUTE_FAMILY_MISMATCH.to_string());
        }

        if session.event_count + 1 > self.limits.max_batch_events {
            return Err(format!(
                "ERR_BATCH_TOO_LARGE: event count {} exceeds max_batch_events {}",
                session.event_count + 1,
                self.limits.max_batch_events
            ));
        }

        let event_bytes = event.body.len() + event.metadata.as_ref().map_or(0, bytes::Bytes::len);
        if session.total_bytes + event_bytes > self.limits.max_batch_bytes {
            return Err(format!(
                "ERR_BATCH_TOO_LARGE: total {} + event {} exceeds max_batch_bytes {}",
                session.total_bytes, event_bytes, self.limits.max_batch_bytes
            ));
        }

        session.staged_events.push(event);
        session.total_bytes += event_bytes;
        session.event_count += 1;

        Ok(())
    }

    /// Commit session with StreamActor-provided first offsets.
    ///
    /// **STORAGE ONLY - VALIDATED FRONTIER**
    /// - Accepts first offsets from `StreamActor` after durable frontier validation
    /// - Computes subsequent offsets by index: first + i
    /// - Does NOT validate `expected_offset` (`StreamActor`'s job)
    ///
    /// # Errors
    ///
    /// Returns an error if the session is missing, the family or provided
    /// frontier does not match persisted state, layout activation fails, or the
    /// commit write path fails.
    pub fn commit_session(
        &self,
        family: u64,
        session_id: SessionId,
        first_resource_offset: u64,
        first_area_offset: u64,
        first_realm_offset: u64,
        mode: StreamWriteMode,
    ) -> Result<CommitResponse, String> {
        self.commit_session_promotion_frontier(
            family,
            session_id,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            mode,
        )
    }

    pub(super) fn commit_session_promotion_frontier(
        &self,
        family: u64,
        session_id: SessionId,
        first_resource_offset: u64,
        first_area_offset: u64,
        first_realm_offset: u64,
        mode: StreamWriteMode,
    ) -> Result<CommitResponse, String> {
        let session = {
            let mut sessions = self.sessions.lock();
            sessions
                .remove(&session_id)
                .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?
        };

        if session.family != family {
            self.sessions.lock().insert(session_id, session);
            return Err(ERR_SESSION_ROUTE_FAMILY_MISMATCH.to_string());
        }

        if session.event_count == 0 {
            self.sessions.lock().insert(session_id, session);
            return Err("ERR_EMPTY_BATCH".to_string());
        }

        if let Err(error) = self.ensure_layout_activation_for_family(family) {
            self.sessions.lock().insert(session_id, session);
            return Err(error);
        }

        let sequencing_guard =
            self.resource_sequence_guard(family, &session.realm, &session.area, &session.resource);
        let _sequencing_lock = sequencing_guard.lock();

        let resource_meta_state =
            self.resource_meta_state(family, &session.realm, &session.area, &session.resource);
        let mut resource_meta_state = resource_meta_state.lock();
        let realm_sequence_state = self.realm_sequence_state(family, &session.realm);
        let mut realm_sequence_state = realm_sequence_state.lock();

        let (resource_meta_before, _) = match self.load_resource_meta_snapshot(
            family,
            &session.realm,
            &session.area,
            &session.resource,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.sessions.lock().insert(session_id, session);
                return Err(error);
            }
        };
        resource_meta_state.snapshot = Some(resource_meta_before.clone());

        if resource_meta_before.next_offset != first_resource_offset {
            self.sessions.lock().insert(session_id, session);
            return Err("ERR_CONCURRENCY_CONFLICT".to_string());
        }

        let (area_next_offset, _) =
            match self.load_area_next_offset_snapshot(family, &session.realm, &session.area) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.sessions.lock().insert(session_id, session);
                    return Err(error);
                }
            };
        if area_next_offset != first_area_offset {
            self.sessions.lock().insert(session_id, session);
            return Err("ERR_CONCURRENCY_CONFLICT".to_string());
        }
        realm_sequence_state
            .next_area_offsets
            .insert(session.area.clone(), area_next_offset);

        let (realm_next_offset, _) =
            match self.load_realm_next_offset_snapshot(family, &session.realm) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    self.sessions.lock().insert(session_id, session);
                    return Err(error);
                }
            };
        if realm_next_offset != first_realm_offset {
            self.sessions.lock().insert(session_id, session);
            return Err("ERR_CONCURRENCY_CONFLICT".to_string());
        }
        realm_sequence_state.next_realm_offset = Some(realm_next_offset);

        let result = self.commit_promotion_frontier_batch(CommitPromotionFrontierBatchParams {
            family,
            realm: &session.realm,
            area: &session.area,
            resource: &session.resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events: &session.staged_events,
            committed_size_before: resource_meta_before.committed_size_bytes,
            ingest_metadata: session.ingest_metadata.clone(),
            mode,
        });

        let (response, resource_meta_after) = match result {
            Ok(result) => result,
            Err(error) => {
                self.sessions.lock().insert(session_id, session);
                return Err(error);
            }
        };

        resource_meta_state.snapshot = Some(resource_meta_after);
        realm_sequence_state.next_area_offsets.insert(
            session.area.clone(),
            response.last_area_offset.saturating_add(1),
        );
        realm_sequence_state.next_realm_offset = Some(response.last_realm_offset.saturating_add(1));

        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error if the session does not exist.
    pub fn abort_session(&self, session_id: SessionId) -> Result<(), String> {
        self.sessions
            .lock()
            .remove(&session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;
        Ok(())
    }

    pub fn session_event_count(&self, session_id: SessionId) -> Option<usize> {
        self.sessions.lock().get(&session_id).map(|s| s.event_count)
    }

    /// Peek at the last committed record in a resource stream (tail operation)
    ///
    /// **NO WATERMARK GATING**: Resource reads are strictly ordered by `StreamActor`.
    /// Watermark is for area/realm dimensions only.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation fails or the backing store read
    /// path cannot be completed.
    pub fn peek_resource(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<StreamRecord>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.peek_resource_promotion_frontier(family, realm, area, resource)
    }

    pub(super) fn peek_resource_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<StreamRecord>, String> {
        match self.get_last_resource_offset_promotion_frontier(family, realm, area, resource)? {
            Some(last_offset) => {
                self.load_compact_resource_record(family, realm, area, resource, last_offset)
            }
            None => Ok(None),
        }
    }

    pub(super) fn load_compact_resource_record(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        resource_offset: u64,
    ) -> Result<Option<StreamRecord>, String> {
        let page_start = Self::page_start_offset(resource_offset);
        let page_key = encode_compact_resource_page_key(realm, area, resource, page_start);
        let txn = self
            .db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        match txn
            .get(&page_key)
            .map_err(|e| format!("get error: {e:?}"))?
        {
            Some(value_bytes) => {
                let page = CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                    Self::invalid_compact_resource_page_error(
                        realm, area, resource, page_start, &error,
                    )
                })?;
                let slot = u64_to_usize_saturating(resource_offset - page_start);
                Ok(page.records.get(slot).map(|record| StreamRecord {
                    resource_offset,
                    area_offset: Some(record.area_offset),
                    realm_offset: Some(record.realm_offset),
                    body: record.body.clone(),
                    metadata: record.metadata.clone(),
                    created_at: record.created_at,
                }))
            }
            None => Ok(None),
        }
    }
}
