use super::{
    decode_resource_offset_from_key, encode_area_counter_key, encode_global_watermark_key,
    encode_realm_counter_key, encode_watermark_key, family_to_storage_partition,
    usize_to_u64_saturating, AreaCounterValue, Bytes, CompactResourcePageValue, RealmCounterValue,
    StreamStore, WatermarkValue,
};

#[cfg(test)]
use super::{
    FAIL_NEXT_AREA_WATERMARK_PERSIST, FAIL_NEXT_GLOBAL_WATERMARK_PERSIST,
    FAIL_NEXT_REALM_WATERMARK_PERSIST,
};

impl StreamStore {
    fn load_persisted_global_watermark(&self, family: u64) -> Result<u64, String> {
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| format!("begin global watermark read failed: {error:?}"))?;
        txn.get(&encode_global_watermark_key())
            .map_err(|error| format!("read global watermark failed: {error:?}"))?
            .map_or(Ok(0), |bytes| {
                WatermarkValue::decode(&bytes).map(|value| value.watermark)
            })
    }

    fn remember_resolved_global_range(&self, family: u64, first: u64, end_exclusive: u64) {
        self.global_completion_state(family)
            .lock()
            .resolved
            .insert(first, end_exclusive);
    }

    fn flush_resolved_global_ranges(&self, family: u64) -> Result<u64, String> {
        let persisted_watermark = self.load_persisted_global_watermark(family)?;
        let handle = self.global_completion_state(family);
        let mut state = handle.lock();
        let previous_watermark = state
            .watermark
            .unwrap_or(persisted_watermark)
            .max(persisted_watermark);
        state.watermark = Some(previous_watermark);
        let mut watermark = previous_watermark;
        let mut consumed = Vec::new();
        while let Some(end) = state.resolved.get(&watermark).copied() {
            consumed.push(watermark);
            watermark = end;
        }
        if watermark != previous_watermark {
            self.set_global_watermark(family, watermark)?;
            for first in consumed {
                state.resolved.remove(&first);
            }
            state.watermark = Some(watermark);
        }
        Ok(watermark)
    }

    /// Loads an explicitly persisted area watermark without applying the
    /// read-side counter fallback used by [`Self::get_watermark`].
    pub(crate) fn get_persisted_area_watermark(
        &self,
        family: u64,
        realm: &str,
        area: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| format!("begin persisted area watermark read failed: {error:?}"))?;
        Self::load_existing_area_watermark_for_guard(&txn, &encode_watermark_key(realm, area))
    }

    /// Loads an explicitly persisted realm watermark without applying the
    /// read-side counter fallback used by [`Self::get_realm_watermark`].
    pub(crate) fn get_persisted_realm_watermark(
        &self,
        family: u64,
        realm: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|error| format!("begin persisted realm watermark read failed: {error:?}"))?;
        Self::load_existing_realm_watermark_for_guard(
            &txn,
            &crate::domains::stream::storage::encode_realm_watermark_key(realm),
        )
    }

    /// Returns the exclusive family-global visibility frontier.
    ///
    /// # Errors
    ///
    /// Returns an error if activation, storage access, or decoding fails.
    pub fn get_global_watermark(&self, family: u64) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;
        self.flush_resolved_global_ranges(family)
    }

    pub(super) fn resolve_global_range(
        &self,
        family: u64,
        first: u64,
        end_exclusive: u64,
    ) -> Result<(), String> {
        self.ensure_layout_activation_for_family(family)?;
        self.remember_resolved_global_range(family, first, end_exclusive);
        self.flush_resolved_global_ranges(family).map(|_| ())
    }

    pub(super) fn resolve_durable_global_range(&self, family: u64, first: u64, end_exclusive: u64) {
        self.remember_resolved_global_range(family, first, end_exclusive);
        if let Err(error) = self.flush_resolved_global_ranges(family) {
            tracing::warn!(
                domain = "stream",
                route_family = family,
                first_global_offset = first,
                end_global_offset = end_exclusive,
                error = %error,
                "Durable Stream commit is awaiting global watermark persistence retry"
            );
        }
    }

    /// Advances the exclusive family-global visibility frontier.
    ///
    /// # Errors
    ///
    /// Returns an error if activation or the durable metadata transaction fails.
    pub fn set_global_watermark(&self, family: u64, watermark: u64) -> Result<(), String> {
        self.ensure_layout_activation_for_family(family)?;
        let key = encode_global_watermark_key();
        let mut txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("begin global watermark write failed: {error:?}"))?;
        if let Some(bytes) = txn
            .get(&key)
            .map_err(|error| format!("read global watermark guard failed: {error:?}"))?
        {
            if watermark <= WatermarkValue::decode(&bytes)?.watermark {
                return Ok(());
            }
        }
        txn.put(key, WatermarkValue { watermark }.encode(), None)
            .map_err(|error| format!("write global watermark failed: {error:?}"))?;
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_GLOBAL_WATERMARK_PERSIST.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected global watermark persistence failure".to_string());
            }
        }
        txn.commit(self.sync_write_options)
            .map_err(|error| format!("commit global watermark failed: {error:?}"))
    }

    #[cfg(test)]
    pub(crate) fn fail_next_global_watermark_persist_for_tests() {
        FAIL_NEXT_GLOBAL_WATERMARK_PERSIST.with(|cell| cell.set(true));
    }

    /// Get the current area watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage reads, counter decoding,
    /// or fallback offset scanning fails.
    pub fn get_watermark(&self, family: u64, realm: &str, area: &str) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = encode_watermark_key(realm, area);
        let counter_key = encode_area_counter_key(realm, area);

        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let persisted = txn
            .get(&key)
            .map_err(|e| format!("midge get error: {e:?}"))?
            .map(|bytes| WatermarkValue::decode(&bytes).map(|value| value.watermark))
            .transpose()?;
        let committed = match txn
            .get(&counter_key)
            .map_err(|e| format!("midge get error: {e:?}"))?
        {
            Some(bytes) => AreaCounterValue::decode(&bytes)?
                .next_offset
                .saturating_sub(1),
            None => self
                .scan_next_area_offset(family, realm, area)?
                .saturating_sub(1),
        };
        Ok(persisted.map_or(committed, |watermark| watermark.max(committed)))
    }

    /// Advance the current area watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage reads or writes,
    /// watermark encoding, or transaction commit fails.
    pub fn set_watermark(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        watermark: u64,
    ) -> Result<(), String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = encode_watermark_key(realm, area);

        let mut txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        // This row is the coordinator's advisory persisted frontier and must
        // not regress relative to an earlier advisory value. The area counter
        // commits atomically with records and remains the authoritative
        // read-side floor in `get_watermark`, so a stale advisory row cannot
        // hide committed data.
        if let Some(current) = Self::load_existing_area_watermark_for_guard(&txn, &key)? {
            if watermark <= current {
                return Ok(());
            }
        }

        let value = WatermarkValue { watermark };
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {e:?}"))?;
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_AREA_WATERMARK_PERSIST.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected area watermark persistence failure".to_string());
            }
        }
        txn.commit(self.sync_write_options)
            .map_err(|e| format!("midge commit error: {e:?}"))
    }

    /// Get the current realm watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage reads, counter decoding,
    /// or fallback offset scanning fails.
    pub fn get_realm_watermark(&self, family: u64, realm: &str) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);
        let counter_key = encode_realm_counter_key(realm);

        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let persisted = txn
            .get(&key)
            .map_err(|e| format!("midge get error: {e:?}"))?
            .map(|bytes| WatermarkValue::decode(&bytes).map(|value| value.watermark))
            .transpose()?;
        let committed = match txn
            .get(&counter_key)
            .map_err(|e| format!("midge get error: {e:?}"))?
        {
            Some(bytes) => RealmCounterValue::decode(&bytes)?
                .next_offset
                .saturating_sub(1),
            None => self
                .scan_next_realm_offset(family, realm)?
                .saturating_sub(1),
        };
        Ok(persisted.map_or(committed, |watermark| watermark.max(committed)))
    }

    /// Advance the current realm watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage reads or writes,
    /// watermark encoding, or transaction commit fails.
    pub fn set_realm_watermark(
        &self,
        family: u64,
        realm: &str,
        watermark: u64,
    ) -> Result<(), String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);

        let mut txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        // This row is the coordinator's advisory persisted frontier and must
        // not regress relative to an earlier advisory value. The realm
        // counter commits atomically with records and remains the
        // authoritative read-side floor in `get_realm_watermark`, so a stale
        // advisory row cannot hide committed data.
        if let Some(current) = Self::load_existing_realm_watermark_for_guard(&txn, &key)? {
            if watermark <= current {
                return Ok(());
            }
        }

        let value = WatermarkValue { watermark };
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {e:?}"))?;
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_REALM_WATERMARK_PERSIST.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected realm watermark persistence failure".to_string());
            }
        }
        txn.commit(self.sync_write_options)
            .map_err(|e| format!("midge commit error: {e:?}"))
    }

    /// Get stream metadata (limits, TTL, offsets, watermarks)
    ///
    /// Used for `describe_stream` / introspection API.
    ///
    /// # Errors
    ///
    /// Returns an error if offset, area watermark, or realm watermark loading
    /// fails.
    pub fn get_metadata(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<crate::domains::stream::protocol::StreamMetadata, String> {
        let first_resource_offset =
            self.get_first_resource_offset(family, realm, area, resource)?;
        let last_resource_offset = self.get_last_resource_offset(family, realm, area, resource)?;
        let area_watermark = self.get_watermark(family, realm, area)?;
        let realm_watermark = self.get_realm_watermark(family, realm)?;
        let resource_count = match (first_resource_offset, last_resource_offset) {
            (Some(first_offset), Some(last_offset)) if last_offset >= first_offset => {
                last_offset.saturating_sub(first_offset).saturating_add(1)
            }
            _ => 0,
        };

        Ok(crate::domains::stream::protocol::StreamMetadata {
            max_batch_events: self.limits.max_batch_events,
            max_batch_bytes: self.limits.max_batch_bytes,
            ttl_seconds: self.ttl.ttl_seconds,
            first_resource_offset,
            last_resource_offset,
            resource_count,
            area_watermark,
            realm_watermark,
        })
    }

    #[cfg(test)]
    pub(crate) fn fail_next_area_watermark_persist_for_tests() {
        FAIL_NEXT_AREA_WATERMARK_PERSIST.with(|cell| cell.set(true));
    }

    #[cfg(test)]
    pub(crate) fn fail_next_realm_watermark_persist_for_tests() {
        FAIL_NEXT_REALM_WATERMARK_PERSIST.with(|cell| cell.set(true));
    }

    /// Get the last committed resource offset for recovery.
    ///
    /// `StreamActor` calls this during initialization to avoid offset reuse.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, transaction creation, page
    /// scanning, or page decoding fails.
    pub fn get_last_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_resource_page_prefix(realm, area, resource),
        ));
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter
            .try_collect()
            .map_err(|e| format!("scan error: {e:?}"))?;

        for (key, value) in results.into_iter().rev() {
            let page_start = decode_resource_offset_from_key(&key)?;
            let page = CompactResourcePageValue::try_decode(&value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, &error)
            })?;

            if let Some(last_record) = page.records.len().checked_sub(1) {
                return Ok(Some(
                    page_start.saturating_add(usize_to_u64_saturating(last_record)),
                ));
            }
        }

        Ok(None)
    }

    /// Get the first committed resource offset that is still readable.
    ///
    /// This is used by exact-resource metadata surfaces that need to remain
    /// truthful when older resource rows are trimmed from the head.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage transaction creation,
    /// page scanning, or page decoding fails.
    pub fn get_first_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_resource_page_prefix(realm, area, resource),
        ));
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter
            .try_collect()
            .map_err(|e| format!("scan error: {e:?}"))?;

        for (key, value) in results {
            let page_start = decode_resource_offset_from_key(&key)?;
            let page = CompactResourcePageValue::try_decode(&value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, &error)
            })?;

            if !page.records.is_empty() {
                return Ok(Some(page_start));
            }
        }

        Ok(None)
    }

    /// Get the next resource offset from durable stream metadata.
    ///
    /// Returns 0 if no metadata exists for the resource yet.
    ///
    /// # Errors
    ///
    /// Returns an error if layout activation, storage transaction creation,
    /// metadata decoding, or fallback offset scanning fails.
    pub fn get_next_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        self.load_next_resource_offset_from_txn(&txn, family, realm, area, resource)
    }
}
