use super::{
    encode_family_writer_epoch_key, encode_global_counter_key, family_to_storage_partition,
    AppendSession, CommitPromotionFrontierBatchParams, CommitRecordsParams, CommitResponse,
    EventPayload, GlobalReservation, IngestMetadata, PendingGlobalReservation,
    PromotionCommitFailure, ReadResourceParams, RealmCounterValue, ResourceMetaValue,
    SequenceGuardKey, SessionId, StreamAdminRecord, StreamReadItem, StreamRecord, StreamStore,
    StreamStoreError, StreamWriteMode, ERR_SESSION_ROUTE_FAMILY_MISMATCH,
};

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

const MAX_SCOPE_CONFLICT_RETRIES: usize = 8;

struct PreparedSessionReservation {
    key: (u64, String, String, String),
    reservation: GlobalReservation,
}

impl StreamStore {
    fn preserve_retryable_global_reservation(
        &self,
        key: SequenceGuardKey,
        pending: PendingGlobalReservation,
        failure: &PromotionCommitFailure,
    ) -> bool {
        if !failure.is_retryable() {
            return false;
        }
        self.pending_global_reservations.lock().insert(key, pending);
        true
    }

    fn resolve_pending_global_reservation(
        &self,
        key: &SequenceGuardKey,
        pending: PendingGlobalReservation,
    ) -> Result<(), String> {
        let count = u64::try_from(pending.event_count).unwrap_or(u64::MAX);
        if let Err(error) = self.resolve_global_range(
            key.0,
            pending.reservation.first_offset,
            pending.reservation.first_offset.saturating_add(count),
        ) {
            self.pending_global_reservations
                .lock()
                .insert(key.clone(), pending);
            return Err(error);
        }
        Ok(())
    }

    fn commit_with_scope_conflict_retries(
        &self,
        params: &CommitPromotionFrontierBatchParams<'_>,
    ) -> Result<(CommitResponse, ResourceMetaValue), PromotionCommitFailure> {
        let mut first_area_offset = params.first_area_offset;
        let mut first_realm_offset = params.first_realm_offset;
        let mut scope_conflict_retries = 0;
        loop {
            let result = self.commit_promotion_frontier_batch(CommitPromotionFrontierBatchParams {
                family: params.family,
                realm: params.realm,
                area: params.area,
                resource: params.resource,
                first_resource_offset: params.first_resource_offset,
                first_area_offset,
                first_realm_offset,
                first_global_offset: params.first_global_offset,
                writer_epoch: params.writer_epoch,
                events: params.events,
                committed_size_before: params.committed_size_before,
                ingest_metadata: params.ingest_metadata.clone(),
                mode: params.mode,
            });
            if !matches!(&result, Err(error) if error.is_scope_conflict())
                || scope_conflict_retries == MAX_SCOPE_CONFLICT_RETRIES
            {
                return result;
            }
            scope_conflict_retries += 1;
            first_area_offset = self
                .load_area_next_offset_snapshot(params.family, params.realm, params.area)
                .map(|(offset, _)| offset)
                .map_err(PromotionCommitFailure::Retryable)?;
            first_realm_offset = self
                .load_realm_next_offset_snapshot(params.family, params.realm)
                .map(|(offset, _)| offset)
                .map_err(PromotionCommitFailure::Retryable)?;
        }
    }

    fn take_commit_session(
        &self,
        family: u64,
        session_id: SessionId,
    ) -> Result<AppendSession, String> {
        let session = self
            .sessions
            .lock()
            .remove(&session_id)
            .ok_or_else(|| StreamStoreError::SessionNotFound.store_code().to_string())?;
        let validation = if session.family != family {
            Err(ERR_SESSION_ROUTE_FAMILY_MISMATCH.to_string())
        } else if session.event_count == 0 {
            Err("ERR_EMPTY_BATCH".to_string())
        } else {
            self.ensure_layout_activation_for_family(family)
        };
        if let Err(error) = validation {
            self.sessions.lock().insert(session_id, session);
            return Err(error);
        }
        Ok(session)
    }

    fn take_or_reserve_global_range(
        &self,
        family: u64,
        key: &SequenceGuardKey,
        resource_offset: u64,
        event_count: usize,
    ) -> Result<GlobalReservation, String> {
        let pending = self.pending_global_reservations.lock().remove(key);
        if let Some(pending) = pending {
            if pending.resource_offset == resource_offset && pending.event_count == event_count {
                return Ok(pending.reservation);
            }
            self.resolve_pending_global_reservation(key, pending)?;
        }
        self.reserve_global_range(family, event_count)
    }

    pub(crate) fn abandon_pending_global_reservation(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<bool, String> {
        let key = (
            family,
            realm.to_string(),
            area.to_string(),
            resource.to_string(),
        );
        let Some(pending) = self.pending_global_reservations.lock().remove(&key) else {
            return Ok(false);
        };
        self.resolve_pending_global_reservation(&key, pending)?;
        Ok(true)
    }

    fn prepare_session_reservation(
        &self,
        family: u64,
        session: &AppendSession,
        first_resource_offset: u64,
    ) -> Result<PreparedSessionReservation, String> {
        let key = (
            family,
            session.realm.clone(),
            session.area.clone(),
            session.resource.clone(),
        );
        let reservation = self.take_or_reserve_global_range(
            family,
            &key,
            first_resource_offset,
            session.staged_events.len(),
        )?;
        Ok(PreparedSessionReservation { key, reservation })
    }

    pub(super) fn reserve_global_range(
        &self,
        family: u64,
        batch_size: usize,
    ) -> Result<GlobalReservation, String> {
        let guard = self.family_sequence_guard(family);
        let _lock = guard.lock();
        let mut txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("begin global reservation tx failed: {error:?}"))?;
        let key = encode_global_counter_key();
        let writer_epoch = txn
            .get(&encode_family_writer_epoch_key())
            .map_err(|error| format!("read family writer epoch failed: {error:?}"))?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        let first = txn
            .get(&key)
            .map_err(|error| format!("read global counter failed: {error:?}"))?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        let count =
            u64::try_from(batch_size).map_err(|_| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
        let next = first
            .checked_add(count)
            .ok_or_else(|| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
        txn.put(key, RealmCounterValue { next_offset: next }.encode(), None)
            .map_err(|error| format!("write global counter failed: {error:?}"))?;
        txn.commit(self.sync_write_options)
            .map_err(|error| format!("commit global reservation failed: {error:?}"))?;
        Ok(GlobalReservation {
            first_offset: first,
            writer_epoch,
        })
    }

    #[cfg(test)]
    pub(crate) fn advance_family_writer_epoch(&self, family: u64) -> Result<u64, String> {
        let guard = self.family_sequence_guard(family);
        let _lock = guard.lock();
        let mut txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("begin writer epoch fence failed: {error:?}"))?;
        let key = encode_family_writer_epoch_key();
        let current = txn
            .get(&key)
            .map_err(|error| format!("read family writer epoch failed: {error:?}"))?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        let next = current
            .checked_add(1)
            .ok_or_else(|| "ERR_STREAM_WRITER_EPOCH_EXHAUSTED".to_string())?;
        txn.put(key, RealmCounterValue { next_offset: next }.encode(), None)
            .map_err(|error| format!("write family writer epoch failed: {error:?}"))?;
        txn.commit(self.sync_write_options)
            .map_err(|error| format!("commit writer epoch fence failed: {error:?}"))?;
        Ok(next)
    }

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
        // Known scaling limit: strict compact-page ordering keeps this guard
        // across the storage commit (and therefore fsync in Sync mode).
        // Revisit with group commit if per-resource throughput becomes limiting.
        let _sequencing_lock = sequencing_guard.lock();

        let resource_meta_state = self.resource_meta_state(family, realm, area, resource);
        let mut resource_meta_state = resource_meta_state.lock();
        let (resource_meta_before, _) =
            self.load_resource_meta_snapshot(family, realm, area, resource)?;
        resource_meta_state.snapshot = Some(resource_meta_before.clone());
        if resource_meta_before.next_offset != expected_resource_next_offset {
            return Err(StreamStoreError::ConcurrencyConflict
                .store_code()
                .to_string());
        }
        let (first_area_offset, _) = self.load_area_next_offset_snapshot(family, realm, area)?;
        let (first_realm_offset, _) = self.load_realm_next_offset_snapshot(family, realm)?;

        let reservation_key = (
            family,
            realm.to_string(),
            area.to_string(),
            resource.to_string(),
        );
        let global_reservation = self.take_or_reserve_global_range(
            family,
            &reservation_key,
            expected_resource_next_offset,
            events.len(),
        )?;
        let commit_result =
            self.commit_with_scope_conflict_retries(&CommitPromotionFrontierBatchParams {
                family,
                realm,
                area,
                resource,
                first_resource_offset: resource_meta_before.next_offset,
                first_area_offset,
                first_realm_offset,
                first_global_offset: global_reservation.first_offset,
                writer_epoch: global_reservation.writer_epoch,
                events,
                committed_size_before: resource_meta_before.committed_size_bytes,
                ingest_metadata: ingest_metadata.clone(),
                mode,
            });
        let (response, resource_meta_after) = match commit_result {
            Ok(result) => result,
            Err(error) => {
                self.preserve_retryable_global_reservation(
                    reservation_key,
                    PendingGlobalReservation {
                        resource_offset: expected_resource_next_offset,
                        event_count: events.len(),
                        reservation: global_reservation,
                    },
                    &error,
                );
                return Err(error.into_message());
            }
        };

        resource_meta_state.snapshot = Some(resource_meta_after);
        Ok(response)
    }

    /// # Errors
    ///
    /// Returns an error if layout activation fails or metadata scanning cannot
    /// be completed from storage.
    pub fn list_resource_metadata(&self, family: u64) -> Result<Vec<StreamAdminRecord>, String> {
        self.ensure_layout_activation_for_family(family)?;
        let txn = self
            .db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        let resource_meta_query = cntryl_midge::Query::new();
        let resource_meta_iter = txn
            .scan(&resource_meta_query)
            .map_err(|e| format!("scan error: {e:?}"))?;

        let mut values = Vec::new();
        for (key, value) in resource_meta_iter
            .try_collect()
            .map_err(|e| format!("scan error: {e:?}"))?
        {
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
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |current| current.checked_add(1),
            )
            .map_err(|_| "stream session ID space exhausted".to_string())?;

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
            .ok_or_else(|| StreamStoreError::SessionNotFound.store_code().to_string())?;

        if session.family != family {
            return Err(ERR_SESSION_ROUTE_FAMILY_MISMATCH.to_string());
        }

        let next_event_count = session
            .event_count
            .checked_add(1)
            .ok_or_else(|| "ERR_BATCH_TOO_LARGE: event count overflow".to_string())?;
        if next_event_count > self.limits.max_batch_events {
            return Err(format!(
                "ERR_BATCH_TOO_LARGE: event count {} exceeds max_batch_events {}",
                next_event_count, self.limits.max_batch_events
            ));
        }

        let event_bytes = event
            .body
            .len()
            .checked_add(event.metadata.as_ref().map_or(0, bytes::Bytes::len))
            .and_then(|size| {
                size.checked_add(
                    event
                        .discriminator
                        .as_ref()
                        .map_or(0, |value| value.as_str().len()),
                )
            })
            .ok_or_else(|| "ERR_BATCH_TOO_LARGE: event size overflow".to_string())?;
        let next_total_bytes = session
            .total_bytes
            .checked_add(event_bytes)
            .ok_or_else(|| "ERR_BATCH_TOO_LARGE: total byte count overflow".to_string())?;
        if next_total_bytes > self.limits.max_batch_bytes {
            return Err(format!(
                "ERR_BATCH_TOO_LARGE: total {} + event {} exceeds max_batch_bytes {}",
                session.total_bytes, event_bytes, self.limits.max_batch_bytes
            ));
        }

        session.staged_events.push(event);
        session.total_bytes = next_total_bytes;
        session.event_count = next_event_count;

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
        let session = self.take_commit_session(family, session_id)?;

        let sequencing_guard =
            self.resource_sequence_guard(family, &session.realm, &session.area, &session.resource);
        // See commit_records: this deliberately spans the durable commit.
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
            return Err(StreamStoreError::ConcurrencyConflict
                .store_code()
                .to_string());
        }

        let prepared =
            match self.prepare_session_reservation(family, &session, first_resource_offset) {
                Ok(prepared) => prepared,
                Err(error) => {
                    self.sessions.lock().insert(session_id, session);
                    return Err(error);
                }
            };
        let global_reservation = prepared.reservation;
        let reservation_key = prepared.key;

        let result = self.commit_with_scope_conflict_retries(&CommitPromotionFrontierBatchParams {
            family,
            realm: &session.realm,
            area: &session.area,
            resource: &session.resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            first_global_offset: global_reservation.first_offset,
            writer_epoch: global_reservation.writer_epoch,
            events: &session.staged_events,
            committed_size_before: resource_meta_before.committed_size_bytes,
            ingest_metadata: session.ingest_metadata.clone(),
            mode,
        });

        let (response, resource_meta_after) = match result {
            Ok(result) => result,
            Err(error) => {
                if self.preserve_retryable_global_reservation(
                    reservation_key,
                    PendingGlobalReservation {
                        resource_offset: first_resource_offset,
                        event_count: session.staged_events.len(),
                        reservation: global_reservation,
                    },
                    &error,
                ) {
                    self.sessions.lock().insert(session_id, session);
                }
                return Err(error.into_message());
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
        let session = self
            .sessions
            .lock()
            .remove(&session_id)
            .ok_or_else(|| StreamStoreError::SessionNotFound.store_code().to_string())?;
        let key = (
            session.family,
            session.realm.clone(),
            session.area.clone(),
            session.resource.clone(),
        );
        if let Some(pending) = self.pending_global_reservations.lock().remove(&key) {
            if let Err(error) = self.resolve_pending_global_reservation(&key, pending) {
                self.sessions.lock().insert(session_id, session);
                return Err(error);
            }
        }
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
        match self.get_last_resource_offset(family, realm, area, resource)? {
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
        let params = ReadResourceParams {
            family,
            realm,
            area,
            resource,
            from_offset: resource_offset,
            limit: 1,
            max_bytes: None,
        };
        let (items, _) = self.read_resource(&params)?;
        Ok(items.into_iter().find_map(|item| match item {
            StreamReadItem::Event(record) if record.resource_offset == resource_offset => {
                Some(record)
            }
            StreamReadItem::Event(_)
            | StreamReadItem::Filtered { .. }
            | StreamReadItem::FilteredRange { .. } => None,
        }))
    }
}
