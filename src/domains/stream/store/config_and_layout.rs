use super::{
    decode_staging_value, encode_stream_layout_marker_key, AreaCounterValue, AreaLocatorValue,
    AreaValue, BatchLimits, CanonicalResourceValue, CompactAreaPageValue, CompactResourcePageValue,
    CompressedCompactRealmPageValue, KeyPrefix, LayoutActivationFailure, OffsetCounterValue,
    RealmCounterValue, RealmLocatorValue, RealmValue, ResourceMetaValue, ResourceValue,
    StreamLayoutMarkerValue, StreamStorageLayout, StreamStore, StreamTTL, WatermarkValue,
    REALM_PAGE_RECORD_LIMIT,
};
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
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

    pub(crate) fn with_storage_config_and_layout(
        db: crate::storage::FitzStorageEngine,
        limits: BatchLimits,
        ttl: StreamTTL,
        layout: StreamStorageLayout,
    ) -> Self {
        Self {
            db,
            limits,
            layout: layout.normalize_requested(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            sequencing_guards: Arc::new(Mutex::new(HashMap::new())),
            realm_sequence_states: Arc::new(Mutex::new(HashMap::new())),
            resource_meta_states: Arc::new(Mutex::new(HashMap::new())),
        }
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
        let mut iter = txn
            .scan(&cntryl_midge::Query::new())
            .map_err(|e| format!("stream validation failed: family={family} scan: {e:?}"))?;

        for (key, value) in iter.collect_all() {
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
            return CanonicalResourceValue::try_decode(value)
                .map(|_| ())
                .map_err(|error| ("canonical_resource", error));
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
            prefix if prefix == KeyPrefix::CanonicalResource as u8 => {
                CanonicalResourceValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("canonical_resource", error))
            }
            prefix if prefix == KeyPrefix::AreaLocator as u8 => AreaLocatorValue::try_decode(value)
                .map(|_| ())
                .map_err(|error| ("area_locator", error)),
            prefix if prefix == KeyPrefix::RealmLocator as u8 => {
                RealmLocatorValue::try_decode(value)
                    .map(|_| ())
                    .map_err(|error| ("realm_locator", error))
            }
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
            _ => Ok(()),
        }
    }

    pub(super) fn ensure_layout_activation_for_family(&self, family: u64) -> Result<(), String> {
        self.inspect_and_activate_layout_for_family(family)
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
            let marker =
                StreamLayoutMarkerValue::decode(&bytes).map_err(LayoutActivationFailure::Other)?;
            if marker.layout != self.layout {
                return Err(LayoutActivationFailure::Mismatch(
                    Self::stream_layout_mismatch_error(family, marker.layout, self.layout),
                ));
            }

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
        txn.commit(cntryl_midge::WriteOptions::sync())
            .map_err(|e| LayoutActivationFailure::Other(format!("midge commit error: {e:?}")))?;

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
        ];

        let query = cntryl_midge::Query::new();
        let mut iter = txn.scan(&query).map_err(|e| format!("scan error: {e:?}"))?;
        while let Some((key, _value)) = iter.next() {
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
            "ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED: family={} requested={} existing unmarked stream data must be reset before opening with promotion-frontier",
            family,
            requested_layout.as_str()
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

    pub(super) fn compact_page_query_limit(from_offset: u64, limit: u64) -> usize {
        let page_start = Self::page_start_offset(from_offset);
        let start_slot = u64_to_usize_saturating(from_offset - page_start);
        let capped_limit = u64_to_usize_saturating(limit);
        start_slot
            .saturating_add(capped_limit)
            .saturating_add(1)
            .div_ceil(REALM_PAGE_RECORD_LIMIT)
            .max(1)
    }
}
