use super::{
    decode_area_offset_from_key, decode_realm_offset_from_key, decode_resource_offset_from_key,
    encode_area_counter_key, encode_realm_counter_key, encode_resource_meta_key,
    family_to_storage_partition, millis_to_u64_saturating, usize_to_u64_saturating,
    AreaCounterValue, Bytes, CompactAreaPageValue, CompactResourcePageValue,
    CompressedCompactRealmPageValue, DiscriminatorWriteRowsParams, Entry, EventPayload,
    RealmCounterValue, RealmSequenceState, RealmSequenceStateHandle, ResourceMetaState,
    ResourceMetaStateHandle, ResourceMetaValue, SequenceGuard, StreamFilterSet, StreamStore,
    WatermarkValue,
};
use parking_lot::Mutex;
use std::sync::Arc;

#[cfg(test)]
use super::{FAIL_NEXT_AREA_WATERMARK_GUARD_READ, FAIL_NEXT_REALM_WATERMARK_GUARD_READ};

impl StreamStore {
    pub(super) fn resource_sequence_guard(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> SequenceGuard {
        let mut guards = self.sequencing_guards.lock();
        let key = (
            family,
            realm.to_string(),
            area.to_string(),
            resource.to_string(),
        );

        match guards.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let guard = Arc::new(Mutex::new(()));
                entry.insert(guard.clone());
                guard
            }
        }
    }

    pub(super) fn realm_sequence_state(
        &self,
        family: u64,
        realm: &str,
    ) -> RealmSequenceStateHandle {
        let mut states = self.realm_sequence_states.lock();
        let key = (family, realm.to_string());

        match states.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let state = Arc::new(Mutex::new(RealmSequenceState::default()));
                entry.insert(state.clone());
                state
            }
        }
    }

    pub(super) fn resource_meta_state(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> ResourceMetaStateHandle {
        let mut states = self.resource_meta_states.lock();
        let key = (
            family,
            realm.to_string(),
            area.to_string(),
            resource.to_string(),
        );

        match states.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let state = Arc::new(Mutex::new(ResourceMetaState::default()));
                entry.insert(state.clone());
                state
            }
        }
    }

    pub(in crate::domains::stream::store) fn now_epoch_ms() -> u64 {
        millis_to_u64_saturating(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
    }

    pub(in crate::domains::stream::store) fn event_size_bytes(event: &EventPayload) -> u64 {
        let total_bytes = event.body.len()
            + event.metadata.as_ref().map_or(0, Bytes::len)
            + event
                .discriminator
                .as_ref()
                .map_or(0, |discriminator| discriminator.as_str().len());
        usize_to_u64_saturating(total_bytes)
    }

    pub(super) fn load_optional_discriminator(
        txn: &cntryl_midge::Transaction,
        key: &[u8],
    ) -> Result<Option<String>, String> {
        match txn.get(key).map_err(|e| format!("get error: {e:?}"))? {
            Some(value_bytes) => String::from_utf8(value_bytes.to_vec())
                .map(Some)
                .map_err(|error| format!("invalid stream discriminator bytes: {error:?}")),
            None => Ok(None),
        }
    }

    pub(in crate::domains::stream::store) fn write_discriminator_rows(
        txn: &mut cntryl_midge::Transaction,
        params: DiscriminatorWriteRowsParams<'_>,
    ) -> Result<(), String> {
        let DiscriminatorWriteRowsParams {
            realm,
            area,
            resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events,
            ttl_opt,
        } = params;
        for (index, event) in events.iter().enumerate() {
            let Some(discriminator) = event.discriminator.as_ref() else {
                continue;
            };

            let index = usize_to_u64_saturating(index);
            let discriminator_bytes = discriminator.as_str().as_bytes().to_vec();

            txn.put(
                crate::domains::stream::storage::encode_resource_discriminator_key(
                    realm,
                    area,
                    resource,
                    first_resource_offset + index,
                ),
                discriminator_bytes.clone(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {e:?}"))?;

            txn.put(
                crate::domains::stream::storage::encode_area_discriminator_key(
                    realm,
                    area,
                    first_area_offset + index,
                ),
                discriminator_bytes.clone(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {e:?}"))?;

            txn.put(
                crate::domains::stream::storage::encode_realm_discriminator_key(
                    realm,
                    first_realm_offset + index,
                ),
                discriminator_bytes,
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {e:?}"))?;
        }

        Ok(())
    }

    pub(in crate::domains::stream::store) fn record_matches_filter(
        filter: Option<&StreamFilterSet>,
        discriminator: Option<&str>,
    ) -> bool {
        match filter {
            Some(filter) => filter.matches(discriminator),
            None => true,
        }
    }

    pub(super) fn load_existing_area_watermark_for_guard(
        txn: &cntryl_midge::Transaction,
        key: &[u8],
    ) -> Result<Option<u64>, String> {
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_AREA_WATERMARK_GUARD_READ.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected area watermark guard read failure".to_string());
            }
        }

        txn.get(key)
            .map_err(|e| format!("midge get error: {e:?}"))
            .and_then(|existing| {
                existing
                    .map(|bytes| WatermarkValue::decode(&bytes).map(|value| value.watermark))
                    .transpose()
            })
    }

    pub(super) fn scan_next_area_offset_from_txn(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        area: &str,
    ) -> Result<u64, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_area_page_prefix(realm, area),
        ));
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_area_offset_from_key(key)?;
            let page = CompactAreaPageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_area_page_error(realm, area, page_start, &error)
            })?;
            Ok(page_start.saturating_add(usize_to_u64_saturating(page.records.len())))
        } else {
            Ok(0)
        }
    }

    pub(super) fn load_effective_area_watermark_for_guard(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        watermark_key: &[u8],
        counter_key: &[u8],
    ) -> Result<u64, String> {
        if let Some(current) = Self::load_existing_area_watermark_for_guard(txn, watermark_key)? {
            return Ok(current);
        }

        match txn
            .get(counter_key)
            .map_err(|e| format!("midge get error: {e:?}"))?
        {
            Some(bytes) => Ok(AreaCounterValue::decode(&bytes)?
                .next_offset
                .saturating_sub(1)),
            None => Ok(Self::scan_next_area_offset_from_txn(txn, realm, area)?.saturating_sub(1)),
        }
    }

    pub(super) fn load_existing_realm_watermark_for_guard(
        txn: &cntryl_midge::Transaction,
        key: &[u8],
    ) -> Result<Option<u64>, String> {
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_REALM_WATERMARK_GUARD_READ.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected realm watermark guard read failure".to_string());
            }
        }

        txn.get(key)
            .map_err(|e| format!("midge get error: {e:?}"))
            .and_then(|existing| {
                existing
                    .map(|bytes| WatermarkValue::decode(&bytes).map(|value| value.watermark))
                    .transpose()
            })
    }

    pub(super) fn scan_next_realm_offset_from_txn(
        txn: &cntryl_midge::Transaction,
        realm: &str,
    ) -> Result<u64, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compressed_compact_realm_page_prefix(realm),
        ));
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start_offset = decode_realm_offset_from_key(key)?;
            let page = CompressedCompactRealmPageValue::try_decode(value)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start_offset, &error))?
                .into_compact_realm_page();
            Ok(page_start_offset.saturating_add(usize_to_u64_saturating(page.records.len())))
        } else {
            Ok(0)
        }
    }

    pub(super) fn load_effective_realm_watermark_for_guard(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        watermark_key: &[u8],
        counter_key: &[u8],
    ) -> Result<u64, String> {
        if let Some(current) = Self::load_existing_realm_watermark_for_guard(txn, watermark_key)? {
            return Ok(current);
        }

        match txn
            .get(counter_key)
            .map_err(|e| format!("midge get error: {e:?}"))?
        {
            Some(bytes) => Ok(RealmCounterValue::decode(&bytes)?
                .next_offset
                .saturating_sub(1)),
            None => Ok(Self::scan_next_realm_offset_from_txn(txn, realm)?.saturating_sub(1)),
        }
    }

    #[cfg(test)]
    pub(super) fn fail_next_area_watermark_guard_read_for_tests() {
        FAIL_NEXT_AREA_WATERMARK_GUARD_READ.with(|cell| cell.set(true));
    }

    #[cfg(test)]
    pub(super) fn fail_next_realm_watermark_guard_read_for_tests() {
        FAIL_NEXT_REALM_WATERMARK_GUARD_READ.with(|cell| cell.set(true));
    }

    #[cfg(test)]
    pub(crate) fn fail_next_promotion_frontier_commit_for_tests(&self) {
        self.fail_next_promotion_frontier_commit
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn resource_identity_from_key(
        expected_prefix: u8,
        key: &[u8],
    ) -> Result<(String, String, String), String> {
        if let Some(body) = crate::utils::storage_key::strip_domain_prefix(
            key,
            crate::utils::storage_key::DomainKeyspace::Stream,
        ) {
            if body.first().copied() != Some(expected_prefix) {
                return Err("unexpected stream metadata key prefix".to_string());
            }

            let realm_end = key
                .iter()
                .position(|byte| *byte == 0)
                .ok_or_else(|| "missing stream realm in key".to_string())?;
            let realm = &key[..realm_end];
            let mut parts = body[1..].splitn(2, |byte| *byte == 0);
            let area = parts
                .next()
                .ok_or_else(|| "missing stream area in key".to_string())?;
            let resource = parts
                .next()
                .ok_or_else(|| "missing stream resource in key".to_string())?;

            return Ok((
                String::from_utf8(realm.to_vec())
                    .map_err(|_| "invalid stream realm key".to_string())?,
                String::from_utf8(area.to_vec())
                    .map_err(|_| "invalid stream area key".to_string())?,
                String::from_utf8(resource.to_vec())
                    .map_err(|_| "invalid stream resource key".to_string())?,
            ));
        }

        if key.first().copied() != Some(expected_prefix) {
            return Err("unexpected stream metadata key prefix".to_string());
        }

        let body = &key[1..];
        let mut parts = body.splitn(3, |byte| *byte == 0);
        let realm = parts
            .next()
            .ok_or_else(|| "missing stream realm in key".to_string())?;
        let area = parts
            .next()
            .ok_or_else(|| "missing stream area in key".to_string())?;
        let resource = parts
            .next()
            .ok_or_else(|| "missing stream resource in key".to_string())?;

        Ok((
            String::from_utf8(realm.to_vec())
                .map_err(|_| "invalid stream realm key".to_string())?,
            String::from_utf8(area.to_vec()).map_err(|_| "invalid stream area key".to_string())?,
            String::from_utf8(resource.to_vec())
                .map_err(|_| "invalid stream resource key".to_string())?,
        ))
    }

    pub(super) fn scan_resource_stats(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<(u64, u64), String> {
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
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter.collect_all();

        let mut next_offset = 0_u64;
        let mut committed_size_bytes = 0_u64;
        for (key_bytes, value_bytes) in results {
            let page_start = decode_resource_offset_from_key(&key_bytes)?;
            let page = CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, &error)
            })?;
            next_offset = next_offset
                .max(page_start.saturating_add(usize_to_u64_saturating(page.records.len())));
            for record in page.records {
                let record_bytes = usize_to_u64_saturating(record.body.len()).saturating_add(
                    usize_to_u64_saturating(record.metadata.as_ref().map_or(0, Bytes::len)),
                );
                committed_size_bytes = committed_size_bytes.saturating_add(record_bytes);
            }
        }

        Ok((next_offset, committed_size_bytes))
    }

    pub(super) fn scan_next_area_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
    ) -> Result<u64, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_area_page_prefix(realm, area),
        ));
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_area_offset_from_key(key)?;
            let page = CompactAreaPageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_area_page_error(realm, area, page_start, &error)
            })?;
            Ok(page_start.saturating_add(usize_to_u64_saturating(page.records.len())))
        } else {
            Ok(0)
        }
    }

    pub(super) fn scan_next_realm_offset(&self, family: u64, realm: &str) -> Result<u64, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compressed_compact_realm_page_prefix(realm),
        ));
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start_offset = decode_realm_offset_from_key(key)?;
            let page = CompressedCompactRealmPageValue::try_decode(value)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start_offset, &error))?
                .into_compact_realm_page();
            Ok(page_start_offset.saturating_add(usize_to_u64_saturating(page.records.len())))
        } else {
            Ok(0)
        }
    }

    pub(super) fn scan_next_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
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
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_resource_offset_from_key(key)?;
            let page = CompactResourcePageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, &error)
            })?;
            Ok(page_start.saturating_add(usize_to_u64_saturating(page.records.len())))
        } else {
            Ok(0)
        }
    }

    pub(super) fn load_resource_meta_snapshot(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<(ResourceMetaValue, bool), String> {
        let meta_key = encode_resource_meta_key(realm, area, resource);
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        if let Some(bytes) = txn
            .get(&meta_key)
            .map_err(|e| format!("get error: {e:?}"))?
        {
            Ok((ResourceMetaValue::decode(&bytes)?, false))
        } else {
            let (next_offset, committed_size_bytes) =
                self.scan_resource_stats(family, realm, area, resource)?;
            Ok((
                ResourceMetaValue {
                    next_offset,
                    committed_size_bytes,
                },
                true,
            ))
        }
    }

    pub(super) fn load_next_resource_offset_from_txn(
        &self,
        txn: &cntryl_midge::Transaction,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        let resource_meta_key = encode_resource_meta_key(realm, area, resource);

        match txn
            .get(&resource_meta_key)
            .map_err(|e| format!("get error: {e:?}"))?
        {
            Some(value_bytes) => Ok(ResourceMetaValue::decode(&value_bytes)?.next_offset),
            None => self.scan_next_resource_offset(family, realm, area, resource),
        }
    }

    pub(super) fn load_area_next_offset_snapshot(
        &self,
        family: u64,
        realm: &str,
        area: &str,
    ) -> Result<(u64, bool), String> {
        let key = encode_area_counter_key(realm, area);
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        match txn.get(&key).map_err(|e| format!("get error: {e:?}"))? {
            Some(bytes) => Ok((AreaCounterValue::decode(&bytes)?.next_offset, false)),
            None => Ok((self.scan_next_area_offset(family, realm, area)?, true)),
        }
    }

    pub(super) fn load_realm_next_offset_snapshot(
        &self,
        family: u64,
        realm: &str,
    ) -> Result<(u64, bool), String> {
        let key = encode_realm_counter_key(realm);
        let txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {e:?}"))?;

        match txn.get(&key).map_err(|e| format!("get error: {e:?}"))? {
            Some(bytes) => Ok((RealmCounterValue::decode(&bytes)?.next_offset, false)),
            None => Ok((self.scan_next_realm_offset(family, realm)?, true)),
        }
    }
}
