use super::{
    decode_staging_value, encode_cursor_state_key, encode_stream_layout_marker_key,
    AreaCounterValue, AreaValue, BatchLimits, CompactAreaPageValue, CompactGlobalPageValue,
    CompactResourcePageValue, CompressedCompactRealmPageValue, KeyPrefix, LayoutActivationFailure,
    OffsetCounterValue, PostingPageValue, RealmCounterValue, RealmValue, ResourceMetaValue,
    ResourceValue, StreamLayoutMarkerValue, StreamStorageLayout, StreamStore, StreamTTL,
    WatermarkValue, REALM_PAGE_RECORD_LIMIT,
};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

impl StreamStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self::new_with_storage(crate::storage::FitzStorageEngine::new(db))
    }

    pub(crate) fn new_with_storage(db: crate::storage::FitzStorageEngine) -> Self {
        Self::with_storage_config_and_layout(
            db,
            BatchLimits::default(),
            StreamTTL::default(),
            StreamStorageLayout::default(),
        )
    }

    pub fn with_layout(db: Arc<cntryl_midge::Engine>, layout: StreamStorageLayout) -> Self {
        Self::with_storage_layout(crate::storage::FitzStorageEngine::new(db), layout)
    }

    pub(crate) fn with_storage_layout(
        db: crate::storage::FitzStorageEngine,
        layout: StreamStorageLayout,
    ) -> Self {
        Self::with_storage_config_and_layout(
            db,
            BatchLimits::default(),
            StreamTTL::default(),
            layout,
        )
    }

    pub fn with_limits(db: Arc<cntryl_midge::Engine>, limits: BatchLimits) -> Self {
        Self::with_storage_limits(crate::storage::FitzStorageEngine::new(db), limits)
    }

    pub(crate) fn with_storage_limits(
        db: crate::storage::FitzStorageEngine,
        limits: BatchLimits,
    ) -> Self {
        Self::with_storage_config_and_layout(
            db,
            limits,
            StreamTTL::default(),
            StreamStorageLayout::default(),
        )
    }

    pub fn with_config(db: Arc<cntryl_midge::Engine>, limits: BatchLimits, ttl: StreamTTL) -> Self {
        Self::with_storage_config(crate::storage::FitzStorageEngine::new(db), limits, ttl)
    }

    pub(crate) fn with_storage_config(
        db: crate::storage::FitzStorageEngine,
        limits: BatchLimits,
        ttl: StreamTTL,
    ) -> Self {
        Self::with_storage_config_and_layout(db, limits, ttl, StreamStorageLayout::default())
    }

    pub fn with_config_and_layout(
        db: Arc<cntryl_midge::Engine>,
        limits: BatchLimits,
        ttl: StreamTTL,
        layout: StreamStorageLayout,
    ) -> Self {
        Self::with_storage_config_and_layout(
            crate::storage::FitzStorageEngine::new(db),
            limits,
            ttl,
            layout,
        )
    }

    #[cfg(test)]
    pub(super) fn with_clock_for_tests(
        mut self,
        clock: Arc<dyn crate::runtime::clock::Clock>,
    ) -> Self {
        self.clock = clock;
        self
    }

    #[cfg(test)]
    pub(super) fn with_maintenance_metrics_for_tests(
        mut self,
        metrics: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.maintenance_metrics = metrics;
        self
    }

    pub(crate) fn with_storage_config_and_layout(
        db: crate::storage::FitzStorageEngine,
        limits: BatchLimits,
        ttl: StreamTTL,
        layout: StreamStorageLayout,
    ) -> Self {
        Self {
            db,
            sync_write_options: cntryl_midge::WriteOptions::sync(),
            buffered_write_options: cntryl_midge::WriteOptions::buffered(),
            limits,
            layout,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            clock: Arc::new(crate::runtime::clock::SystemClock),
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            sequencing_guards: Arc::new(Mutex::new(HashMap::new())),
            realm_sequence_states: Arc::new(Mutex::new(HashMap::new())),
            resource_meta_states: Arc::new(Mutex::new(HashMap::new())),
            family_sequence_guards: Arc::new(Mutex::new(HashMap::new())),
            global_completion_states: Arc::new(Mutex::new(HashMap::new())),
            recovered_global_families: Arc::new(Mutex::new(std::collections::HashSet::new())),
            pending_global_reservations: Arc::new(Mutex::new(HashMap::new())),
            maintenance_queues: Mutex::new(HashMap::new()),
            maintenance_metrics: crate::observability::metrics().as_ref().clone(),
            #[cfg(test)]
            fail_next_promotion_frontier_commit: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fence_next_global_reservation: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            maintenance_failure_stage: std::sync::atomic::AtomicU8::new(0),
            #[cfg(test)]
            maintenance_full_scans: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub(crate) fn with_write_options(
        mut self,
        sync_write_options: cntryl_midge::WriteOptions,
        buffered_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        self.sync_write_options = sync_write_options;
        self.buffered_write_options = buffered_write_options;
        self
    }

    pub fn batch_limits(&self) -> BatchLimits {
        self.limits.clone()
    }

    pub fn storage_layout(&self) -> StreamStorageLayout {
        self.layout
    }

    /// # Errors
    ///
    /// Returns an error if column families cannot be listed or if any existing
    /// stream family cannot be activated for the configured layout.
    pub fn ensure_layout_activation_for_existing_families(&self) -> Result<(), String> {
        let families = self
            .db
            .list_column_families()
            .map_err(|e| format!("list column families failed: {e:?}"))?;

        for family in families {
            if family.id() == 0 {
                continue;
            }
            self.inspect_and_activate_layout_for_family_detailed(u64::from(family.id()))
                .map_err(LayoutActivationFailure::into_string)?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error if column families cannot be listed or if any persisted
    /// stream row fails validation.
    pub fn validate_persisted_state_for_existing_families(&self) -> Result<(), String> {
        let families = self
            .db
            .list_column_families()
            .map_err(|e| format!("list stream column families failed: {e:?}"))?;

        for family in families {
            if family.id() == 0 {
                continue;
            }
            self.validate_persisted_state_for_family(u64::from(family.id()))?;
        }

        Ok(())
    }

    pub(super) fn validate_persisted_state_for_family(&self, family: u64) -> Result<(), String> {
        let txn = self
            .db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("stream validation failed: family={family} begin_tx: {e:?}"))?;
        let iter = txn
            .scan(&cntryl_midge::Query::new())
            .map_err(|e| format!("stream validation failed: family={family} scan: {e:?}"))?;

        for (key, value) in iter
            .try_collect()
            .map_err(|e| format!("stream validation failed: family={family} scan: {e:?}"))?
        {
            let validation = Self::validate_persisted_row(&key, &value);
            if let Err((category, error)) = validation {
                return Err(format!(
                    "stream validation failed: family={family} key_category={category} error={error}"
                ));
            }
        }

        Ok(())
    }

    pub(super) fn validate_persisted_row(
        key: &[u8],
        value: &[u8],
    ) -> Result<(), (&'static str, String)> {
        if key == encode_stream_layout_marker_key() {
            return StreamLayoutMarkerValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("layout_marker", error));
        }
        if key.first().copied() == Some(KeyPrefix::CanonicalResource as u8) {
            return Err((
                "canonical_resource",
                "obsolete D3 prototype row; export/replay or reset the Stream family".to_string(),
            ));
        }
        if key.first().copied() == Some(KeyPrefix::Staging as u8) {
            return decode_staging_value(value)
                .map(|_| ())
                .map_err(|error| ("staging", error));
        }

        let Some(suffix) = crate::utils::storage_key::strip_domain_prefix(
            key,
            crate::utils::storage_key::DomainKeyspace::Stream,
        ) else {
            return Ok(());
        };
        let Some(prefix) = suffix.first().copied() else {
            return Err(("empty_key", "stream key suffix is empty".to_string()));
        };

        Self::validate_domain_row(prefix, value)
    }

    fn validate_domain_row(prefix: u8, value: &[u8]) -> Result<(), (&'static str, String)> {
        match prefix {
            prefix if prefix == KeyPrefix::Resource as u8 => ResourceValue::try_decode(value)
                .map(|_| ())
                .map_err(|error| ("resource", error)),
            prefix if prefix == KeyPrefix::Area as u8 => AreaValue::try_decode(value)
                .map(|_| ())
                .map_err(|error| ("area", error)),
            prefix if prefix == KeyPrefix::Realm as u8 => RealmValue::try_decode(value)
                .map(|_| ())
                .map_err(|error| ("realm", error)),
            prefix if prefix == KeyPrefix::Watermark as u8 => WatermarkValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("watermark", error)),
            prefix if prefix == KeyPrefix::OffsetCounter as u8 => OffsetCounterValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("offset_counter", error)),
            prefix if prefix == KeyPrefix::RealmWatermark as u8 => WatermarkValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("realm_watermark", error)),
            prefix if prefix == KeyPrefix::ResourceMeta as u8 => ResourceMetaValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("resource_meta", error)),
            prefix if prefix == KeyPrefix::AreaCounter as u8 => AreaCounterValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("area_counter", error)),
            prefix if prefix == KeyPrefix::RealmCounter as u8 => RealmCounterValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("realm_counter", error)),
            prefix if prefix == KeyPrefix::CanonicalResource as u8 => Err((
                "canonical_resource",
                "obsolete D3 prototype row; export/replay or reset the Stream family".to_string(),
            )),
            prefix if prefix == KeyPrefix::AreaLocator as u8 => Err((
                "area_locator",
                "obsolete D3 prototype row; export/replay or reset the Stream family".to_string(),
            )),
            prefix if prefix == KeyPrefix::RealmLocator as u8 => Err((
                "realm_locator",
                "obsolete D3 prototype row; export/replay or reset the Stream family".to_string(),
            )),
            prefix if prefix == KeyPrefix::CompactAreaPage as u8 => {
                CompactAreaPageValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("compact_area_page", error))
            }
            prefix if prefix == KeyPrefix::CompressedCompactRealmPage as u8 => {
                CompressedCompactRealmPageValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("compressed_compact_realm_page", error))
            }
            prefix if prefix == KeyPrefix::CompactResourcePage as u8 => {
                CompactResourcePageValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("compact_resource_page", error))
            }
            prefix if prefix == KeyPrefix::CompactGlobalPage as u8 => {
                CompactGlobalPageValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("compact_global_page", error))
            }
            prefix
                if [
                    KeyPrefix::RealmResourcePostingPage as u8,
                    KeyPrefix::GlobalAreaPostingPage as u8,
                    KeyPrefix::GlobalResourcePostingPage as u8,
                    KeyPrefix::GlobalAreaResourcePostingPage as u8,
                ]
                .contains(&prefix) =>
            {
                PostingPageValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("posting_page", error))
            }
            prefix
                if prefix == KeyPrefix::GlobalCounter as u8
                    || prefix == KeyPrefix::FamilyWriterEpoch as u8 =>
            {
                RealmCounterValue::decode(value)
                    .map(|_| ())
                    .map_err(|error| ("global_counter_or_epoch", error))
            }
            prefix if prefix == KeyPrefix::GlobalWatermark as u8 => WatermarkValue::decode(value)
                .map(|_| ())
                .map_err(|error| ("global_watermark", error)),
            prefix if prefix == KeyPrefix::CursorState as u8 => {
                if value.len() == 2 && value[0] == 1 {
                    Ok(())
                } else {
                    Err((
                        "cursor_state",
                        "invalid cursor state version or budget".to_string(),
                    ))
                }
            }
            prefix if prefix == KeyPrefix::PayloadBlob as u8 => super::decode_payload_blob(value)
                .map(|_| ())
                .map_err(|error| ("payload_blob", error)),
            _ => Ok(()),
        }
    }

    pub(super) fn ensure_layout_activation_for_family(&self, family: u64) -> Result<(), String> {
        // Recovery is recorded only after the layout marker and cursor have
        // validated, and Midge holds the family's exclusive writer lease for
        // this store's lifetime. Rechecking those immutable rows on every
        // operation only adds storage transactions to the hot path.
        if self.recovered_global_families.lock().contains(&family) {
            return Ok(());
        }
        self.inspect_and_activate_layout_for_family(family)?;
        self.recover_global_ordering_once(family)
    }

    pub(super) fn inspect_and_activate_layout_for_family(&self, family: u64) -> Result<(), String> {
        self.inspect_and_activate_layout_for_family_detailed(family)
            .map_err(LayoutActivationFailure::into_string)
    }

    pub(super) fn inspect_and_activate_layout_for_family_detailed(
        &self,
        family: u64,
    ) -> Result<(), LayoutActivationFailure> {
        let marker_key = encode_stream_layout_marker_key();
        let mut txn = self
            .db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| LayoutActivationFailure::Other(format!("begin_tx failed: {e:?}")))?;

        if let Some(bytes) = txn
            .get(&marker_key)
            .map_err(|e| LayoutActivationFailure::Other(format!("get error: {e:?}")))?
        {
            if bytes.starts_with(&[0, 0xD3]) {
                return Err(LayoutActivationFailure::ResetRequired(
                    Self::stream_d3_reset_required_error(family),
                ));
            }
            let marker =
                StreamLayoutMarkerValue::decode(&bytes).map_err(LayoutActivationFailure::Other)?;
            if marker.layout != self.layout {
                return Err(LayoutActivationFailure::Mismatch(
                    Self::stream_layout_mismatch_error(family, marker.layout, self.layout),
                ));
            }

            Self::ensure_cursor_state(&self.db, family, self.sync_write_options)?;
            return Ok(());
        }

        if Self::txn_has_stream_data(&txn).map_err(LayoutActivationFailure::Other)? {
            return Err(LayoutActivationFailure::ResetRequired(
                Self::stream_layout_reset_required_error(family, self.layout),
            ));
        }

        txn.put(
            marker_key,
            StreamLayoutMarkerValue::new(self.layout).encode(),
            None,
        )
        .map_err(|e| LayoutActivationFailure::Other(format!("txn put failed: {e:?}")))?;
        txn.commit(self.sync_write_options)
            .map_err(|e| LayoutActivationFailure::Other(format!("midge commit error: {e:?}")))?;

        Self::ensure_cursor_state(&self.db, family, self.sync_write_options)?;

        Ok(())
    }

    fn ensure_cursor_state(
        db: &crate::storage::FitzStorageEngine,
        family: u64,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), LayoutActivationFailure> {
        let mut txn = db
            .begin_tx(
                u64_to_u32_saturating(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| {
                LayoutActivationFailure::Other(format!("cursor tx failed: {error:?}"))
            })?;
        let key = encode_cursor_state_key();
        if txn
            .get(&key)
            .map_err(|error| {
                LayoutActivationFailure::Other(format!("cursor read failed: {error:?}"))
            })?
            .is_none()
        {
            txn.put(key, vec![1, 1], None).map_err(|error| {
                LayoutActivationFailure::Other(format!("cursor write failed: {error:?}"))
            })?;
            txn.commit(write_options).map_err(|error| {
                LayoutActivationFailure::Other(format!("cursor commit failed: {error:?}"))
            })?;
        }
        Ok(())
    }

    pub(super) fn txn_has_stream_data(txn: &cntryl_midge::Transaction) -> Result<bool, String> {
        let stream_prefixes = [
            KeyPrefix::Resource as u8,
            KeyPrefix::Area as u8,
            KeyPrefix::Realm as u8,
            KeyPrefix::Watermark as u8,
            KeyPrefix::OffsetCounter as u8,
            KeyPrefix::RealmWatermark as u8,
            KeyPrefix::ResourceMeta as u8,
            KeyPrefix::AreaCounter as u8,
            KeyPrefix::RealmCounter as u8,
            KeyPrefix::CanonicalResource as u8,
            KeyPrefix::AreaLocator as u8,
            KeyPrefix::RealmLocator as u8,
            KeyPrefix::CompactAreaPage as u8,
            KeyPrefix::CompressedCompactRealmPage as u8,
            KeyPrefix::CompactResourcePage as u8,
            KeyPrefix::GlobalCounter as u8,
            KeyPrefix::GlobalWatermark as u8,
            KeyPrefix::GlobalDiscriminator as u8,
            KeyPrefix::FamilyWriterEpoch as u8,
            KeyPrefix::CompactGlobalPage as u8,
            KeyPrefix::RealmResourcePostingPage as u8,
            KeyPrefix::GlobalAreaPostingPage as u8,
            KeyPrefix::GlobalResourcePostingPage as u8,
            KeyPrefix::GlobalAreaResourcePostingPage as u8,
            KeyPrefix::PayloadBlob as u8,
        ];

        let query = cntryl_midge::Query::new();
        let iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        for entry in iter {
            let (key, _value) = entry.map_err(|e| format!("scan error: {e:?}"))?;
            let suffix = crate::domains::stream::storage::stream_key_suffix(&key);
            if suffix
                .first()
                .is_some_and(|prefix| stream_prefixes.contains(prefix))
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(super) fn stream_layout_mismatch_error(
        family: u64,
        stored_layout: StreamStorageLayout,
        requested_layout: StreamStorageLayout,
    ) -> String {
        format!(
            "ERR_STREAM_STORAGE_LAYOUT_MISMATCH: family={} stored={} requested={} reset required",
            family,
            stored_layout.as_str(),
            requested_layout.as_str()
        )
    }

    pub(super) fn stream_layout_reset_required_error(
        family: u64,
        requested_layout: StreamStorageLayout,
    ) -> String {
        format!(
            "ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED: family={} requested={} existing unmarked stream data requires export/replay into a fresh D4 store or an intentional reset",
            family,
            requested_layout.as_str()
        )
    }

    fn stream_d3_reset_required_error(family: u64) -> String {
        format!(
            "ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED: family={family} stored=D3 requested=D4; export/replay history into a fresh store or intentionally reset Stream data"
        )
    }

    pub(super) fn invalid_compact_realm_page_error(realm_offset: u64, error: &str) -> String {
        format!("ERR_INVALID_COMPACT_REALM_PAGE: realm_offset={realm_offset} {error}")
    }

    pub(super) fn invalid_compact_area_page_error(
        realm: &str,
        area: &str,
        area_offset: u64,
        error: &str,
    ) -> String {
        format!(
            "ERR_INVALID_COMPACT_AREA_PAGE: realm={realm} area={area} area_offset={area_offset} {error}"
        )
    }

    pub(super) fn invalid_compact_resource_page_error(
        realm: &str,
        area: &str,
        resource: &str,
        resource_offset: u64,
        error: &str,
    ) -> String {
        format!(
            "ERR_INVALID_COMPACT_RESOURCE_PAGE: realm={realm} area={area} resource={resource} resource_offset={resource_offset} {error}"
        )
    }

    pub(super) fn build_compact_area_page_prefix(realm: &str, area: &str) -> Vec<u8> {
        let mut encoder = crate::utils::storage_key::domain_marker_encoder(
            realm,
            crate::utils::storage_key::DomainKeyspace::Stream,
            KeyPrefix::CompactAreaPage as u8,
            area.len() + 1,
        );
        crate::utils::storage_key::encode_bytes_segment_into(&mut encoder, area.as_bytes());
        encoder.into_vec()
    }

    pub(super) fn build_compact_resource_page_prefix(
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Vec<u8> {
        let mut encoder = crate::utils::storage_key::domain_marker_encoder(
            realm,
            crate::utils::storage_key::DomainKeyspace::Stream,
            KeyPrefix::CompactResourcePage as u8,
            area.len() + resource.len() + 2,
        );
        crate::utils::storage_key::encode_bytes_segment_into(&mut encoder, area.as_bytes());
        crate::utils::storage_key::encode_bytes_segment_into(&mut encoder, resource.as_bytes());
        encoder.into_vec()
    }

    pub(super) fn build_compressed_compact_realm_page_prefix(realm: &str) -> Vec<u8> {
        crate::utils::storage_key::domain_marker_encoder(
            realm,
            crate::utils::storage_key::DomainKeyspace::Stream,
            KeyPrefix::CompressedCompactRealmPage as u8,
            0,
        )
        .into_vec()
    }

    pub(super) fn page_start_offset(offset: u64) -> u64 {
        offset / REALM_PAGE_RECORD_LIMIT as u64 * REALM_PAGE_RECORD_LIMIT as u64
    }
}
