//! Stream storage layer - STORAGE ONLY, NO SEQUENCING

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use super::protocol::{IngestMetadata, StreamRecord, StreamWriteMode};
use super::storage::{
    decode_area_offset_from_key, decode_realm_offset_from_key, decode_resource_offset_from_key,
    encode_area_counter_key, encode_compact_area_page_key, encode_compact_resource_page_key,
    encode_compressed_compact_realm_page_key, encode_realm_counter_key, encode_resource_meta_key,
    encode_stream_layout_marker_key, encode_watermark_key, AreaCounterValue, CompactAreaPageRecord,
    CompactAreaPageValue, CompactRealmPageRecord, CompactResourcePageRecord,
    CompactResourcePageValue, CompressedCompactRealmPageValue, KeyPrefix, RealmCounterValue,
    ResourceMetaValue, StreamLayoutMarkerValue, WatermarkValue, REALM_PAGE_RECORD_LIMIT,
};

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_AREA_WATERMARK_GUARD_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REALM_WATERMARK_GUARD_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Debug, Clone)]
pub struct EventPayload {
    pub body: Bytes,
    pub metadata: Option<Bytes>,
}

/// Active append session buffered until commit.
struct AppendSession {
    realm: String,
    area: String,
    resource: String,
    staged_events: Vec<EventPayload>,
    event_count: usize,
    total_bytes: usize,
    ingest_metadata: Option<IngestMetadata>,
}

pub type SessionId = u64;
type SequenceGuardKey = (u64, String, String, String);
type SequenceGuard = Arc<Mutex<()>>;
type RealmSequenceStateKey = (u64, String);
type RealmSequenceStateHandle = Arc<Mutex<RealmSequenceState>>;
type ResourceMetaStateHandle = Arc<Mutex<ResourceMetaState>>;

#[derive(Default)]
struct RealmSequenceState {
    next_realm_offset: Option<u64>,
    next_area_offsets: HashMap<String, u64>,
}

#[derive(Default)]
struct ResourceMetaState {
    snapshot: Option<ResourceMetaValue>,
}

#[derive(Debug, Clone)]
pub struct BatchLimits {
    pub max_batch_events: usize,
    pub max_batch_bytes: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_batch_events: 10_000,
            max_batch_bytes: 10 * 1024 * 1024,
        }
    }
}

/// Parameters for reading stream resource records
#[derive(Debug, Clone)]
pub struct ReadResourceParams<'a> {
    pub family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub from_offset: u64,
    pub limit: u64,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct CommitRecordsParams<'a> {
    pub family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub expected_resource_next_offset: u64,
    pub events: &'a [EventPayload],
    pub ingest_metadata: Option<IngestMetadata>,
    pub mode: StreamWriteMode,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StreamTTL {
    pub ttl_seconds: Option<u64>,
}

struct PromotionFrontierWriteRowsParams<'a> {
    realm: &'a str,
    area: &'a str,
    resource: &'a str,
    first_resource_offset: u64,
    first_area_offset: u64,
    first_realm_offset: u64,
    events: &'a [EventPayload],
    created_at: u64,
}

struct CommitPromotionFrontierBatchParams<'a> {
    family: u64,
    realm: &'a str,
    area: &'a str,
    resource: &'a str,
    first_resource_offset: u64,
    first_area_offset: u64,
    first_realm_offset: u64,
    events: &'a [EventPayload],
    committed_size_before: u64,
    ingest_metadata: Option<IngestMetadata>,
    mode: StreamWriteMode,
}

enum LayoutActivationFailure {
    Mismatch(String),
    ResetRequired(String),
    Other(String),
}

impl LayoutActivationFailure {
    fn into_string(self) -> String {
        match self {
            Self::Mismatch(message) | Self::ResetRequired(message) | Self::Other(message) => {
                message
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamStorageLayout {
    #[default]
    PromotionFrontier,
    LegacyCovering,
}

impl StreamStorageLayout {
    pub fn from_env() -> Self {
        let raw_value = std::env::var("FITZ_STREAM_STORAGE_LAYOUT")
            .unwrap_or_else(|_| "promotion-frontier".to_string())
            .to_lowercase();

        match raw_value.as_str() {
            "promotion-frontier" | "frontier" => Self::PromotionFrontier,
            "legacy" | "legacy-covering" | "covering" => {
                tracing::warn!(
                    layout = raw_value.as_str(),
                    "Legacy stream storage layout is no longer supported; using promotion frontier layout"
                );
                Self::PromotionFrontier
            }
            _ => {
                tracing::warn!(
                    layout = raw_value,
                    "Unknown stream storage layout, defaulting to promotion frontier layout"
                );
                Self::default()
            }
        }
    }

    pub fn normalize_requested(self) -> Self {
        match self {
            Self::PromotionFrontier => Self::PromotionFrontier,
            Self::LegacyCovering => {
                tracing::warn!(
                    requested = self.as_str(),
                    "Legacy stream storage layout is no longer supported; using promotion frontier layout"
                );
                Self::PromotionFrontier
            }
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LegacyCovering => "legacy-covering",
            Self::PromotionFrontier => "promotion-frontier",
        }
    }
}

impl StreamTTL {
    pub fn with_seconds(seconds: u64) -> Self {
        Self {
            ttl_seconds: Some(seconds),
        }
    }

    pub fn never() -> Self {
        Self { ttl_seconds: None }
    }
}

#[derive(Debug, Clone)]
pub struct CommitResponse {
    pub first_resource_offset: u64,
    pub last_resource_offset: u64,
    pub first_area_offset: u64,
    pub last_area_offset: u64,
    pub first_realm_offset: u64,
    pub last_realm_offset: u64,
    pub batch_size: usize,
    pub ingest_metadata: Option<IngestMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamAdminRecord {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub next_offset: u64,
    pub committed_size_bytes: u64,
}

pub struct StreamStore {
    db: Arc<cntryl_midge::Engine>,
    limits: BatchLimits,
    layout: StreamStorageLayout,
    sessions: Arc<Mutex<HashMap<SessionId, AppendSession>>>,
    ttl: StreamTTL,
    next_session_id: std::sync::atomic::AtomicU64,
    sequencing_guards: Arc<Mutex<HashMap<SequenceGuardKey, SequenceGuard>>>,
    realm_sequence_states: Arc<Mutex<HashMap<RealmSequenceStateKey, RealmSequenceStateHandle>>>,
    resource_meta_states: Arc<Mutex<HashMap<SequenceGuardKey, ResourceMetaStateHandle>>>,
}

impl StreamStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self::with_config_and_layout(
            db,
            BatchLimits::default(),
            StreamTTL::default(),
            StreamStorageLayout::default(),
        )
    }

    pub fn with_layout(db: Arc<cntryl_midge::Engine>, layout: StreamStorageLayout) -> Self {
        Self::with_config_and_layout(db, BatchLimits::default(), StreamTTL::default(), layout)
    }

    pub fn with_limits(db: Arc<cntryl_midge::Engine>, limits: BatchLimits) -> Self {
        Self::with_config_and_layout(
            db,
            limits,
            StreamTTL::default(),
            StreamStorageLayout::default(),
        )
    }

    pub fn with_config(db: Arc<cntryl_midge::Engine>, limits: BatchLimits, ttl: StreamTTL) -> Self {
        Self::with_config_and_layout(db, limits, ttl, StreamStorageLayout::default())
    }

    pub fn with_config_and_layout(
        db: Arc<cntryl_midge::Engine>,
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

    pub fn ensure_layout_activation_for_existing_families(&self) -> Result<(), String> {
        let families = self
            .db
            .list_column_families()
            .map_err(|e| format!("list column families failed: {:?}", e))?;

        for family in families {
            self.inspect_and_activate_layout_for_family_detailed(family.id() as u64)
                .map_err(LayoutActivationFailure::into_string)?;
        }

        Ok(())
    }

    fn ensure_layout_activation_for_family(&self, family: u64) -> Result<(), String> {
        self.inspect_and_activate_layout_for_family(family)
    }

    fn inspect_and_activate_layout_for_family(&self, family: u64) -> Result<(), String> {
        self.inspect_and_activate_layout_for_family_detailed(family)
            .map_err(LayoutActivationFailure::into_string)
    }

    fn inspect_and_activate_layout_for_family_detailed(
        &self,
        family: u64,
    ) -> Result<(), LayoutActivationFailure> {
        let marker_key = encode_stream_layout_marker_key();
        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| LayoutActivationFailure::Other(format!("begin_tx failed: {:?}", e)))?;

        if let Some(bytes) = txn
            .get(&marker_key)
            .map_err(|e| LayoutActivationFailure::Other(format!("get error: {:?}", e)))?
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
        .map_err(|e| LayoutActivationFailure::Other(format!("txn put failed: {:?}", e)))?;
        txn.commit(cntryl_midge::WriteOptions::sync())
            .map_err(|e| LayoutActivationFailure::Other(format!("midge commit error: {:?}", e)))?;

        Ok(())
    }

    fn txn_has_stream_data(txn: &cntryl_midge::Transaction) -> Result<bool, String> {
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

        for prefix in stream_prefixes {
            let query = cntryl_midge::Query::new()
                .prefix(Bytes::from(vec![prefix]))
                .limit(1);
            let mut iter = txn
                .scan(&query)
                .map_err(|e| format!("scan error: {:?}", e))?;
            if !iter.collect_all().is_empty() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn stream_layout_mismatch_error(
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

    fn stream_layout_reset_required_error(
        family: u64,
        requested_layout: StreamStorageLayout,
    ) -> String {
        format!(
            "ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED: family={} requested={} existing unmarked stream data must be reset before opening with promotion-frontier",
            family,
            requested_layout.as_str()
        )
    }

    fn invalid_compact_realm_page_error(realm_offset: u64, error: String) -> String {
        format!(
            "ERR_INVALID_COMPACT_REALM_PAGE: realm_offset={} {}",
            realm_offset, error
        )
    }

    fn invalid_compact_area_page_error(
        realm: &str,
        area: &str,
        area_offset: u64,
        error: String,
    ) -> String {
        format!(
            "ERR_INVALID_COMPACT_AREA_PAGE: realm={} area={} area_offset={} {}",
            realm, area, area_offset, error
        )
    }

    fn invalid_compact_resource_page_error(
        realm: &str,
        area: &str,
        resource: &str,
        resource_offset: u64,
        error: String,
    ) -> String {
        format!(
            "ERR_INVALID_COMPACT_RESOURCE_PAGE: realm={} area={} resource={} resource_offset={} {}",
            realm, area, resource, resource_offset, error
        )
    }

    fn build_compact_area_page_prefix(realm: &str, area: &str) -> Vec<u8> {
        let mut prefix = vec![KeyPrefix::CompactAreaPage as u8];
        prefix.extend_from_slice(realm.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(area.as_bytes());
        prefix.push(0);
        prefix
    }

    fn build_compact_resource_page_prefix(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        let mut prefix = vec![KeyPrefix::CompactResourcePage as u8];
        prefix.extend_from_slice(realm.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(area.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(resource.as_bytes());
        prefix.push(0);
        prefix
    }

    fn build_compressed_compact_realm_page_prefix(realm: &str) -> Vec<u8> {
        let mut prefix = vec![KeyPrefix::CompressedCompactRealmPage as u8];
        prefix.extend_from_slice(realm.as_bytes());
        prefix.push(0);
        prefix
    }

    fn page_start_offset(offset: u64) -> u64 {
        offset / REALM_PAGE_RECORD_LIMIT as u64 * REALM_PAGE_RECORD_LIMIT as u64
    }

    fn compact_page_query_limit(from_offset: u64, limit: u64) -> usize {
        let page_start = Self::page_start_offset(from_offset);
        let start_slot = (from_offset - page_start) as usize;
        let capped_limit = limit.min(usize::MAX as u64) as usize;
        start_slot
            .saturating_add(capped_limit)
            .saturating_add(1)
            .div_ceil(REALM_PAGE_RECORD_LIMIT)
            .max(1)
    }

    fn build_realm_page_records(
        events: &[EventPayload],
        first_resource_offset: u64,
        first_area_offset: u64,
        created_at: u64,
    ) -> Vec<CompactRealmPageRecord> {
        let mut realm_records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            let resource_offset = first_resource_offset + index as u64;
            let area_offset = first_area_offset + index as u64;

            realm_records.push(CompactRealmPageRecord {
                area_offset,
                resource_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }

        realm_records
    }

    fn build_promotion_frontier_area_records(
        events: &[EventPayload],
        first_resource_offset: u64,
        created_at: u64,
    ) -> Vec<CompactAreaPageRecord> {
        let mut records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            records.push(CompactAreaPageRecord {
                resource_offset: first_resource_offset + index as u64,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }

        records
    }

    fn build_promotion_frontier_resource_records(
        events: &[EventPayload],
        first_area_offset: u64,
        first_realm_offset: u64,
        created_at: u64,
    ) -> Vec<CompactResourcePageRecord> {
        let mut records = Vec::with_capacity(events.len());

        for (index, event) in events.iter().enumerate() {
            records.push(CompactResourcePageRecord {
                area_offset: first_area_offset + index as u64,
                realm_offset: first_realm_offset + index as u64,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }

        records
    }

    fn load_compact_area_page_for_write(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        page_start_offset: u64,
    ) -> Result<CompactAreaPageValue, String> {
        match txn
            .get(&encode_compact_area_page_key(
                realm,
                area,
                page_start_offset,
            ))
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => CompactAreaPageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_area_page_error(realm, area, page_start_offset, error)
            }),
            None => Ok(CompactAreaPageValue {
                records: Vec::new(),
            }),
        }
    }

    fn write_compact_area_records(
        txn: &mut cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        first_area_offset: u64,
        records: &[CompactAreaPageRecord],
        ttl_opt: Option<u64>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let mut next_record_index = 0usize;
        let mut current_area_offset = first_area_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_area_offset);
            let page_offset = (current_area_offset - page_start_offset) as usize;
            let mut page =
                Self::load_compact_area_page_for_write(txn, realm, area, page_start_offset)?;

            if page.records.len() != page_offset {
                return Err("ERR_OVERLAPPING_COMPACT_AREA_PAGE_APPEND".to_string());
            }

            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            page.records
                .extend_from_slice(&records[next_record_index..next_record_index + append_count]);

            txn.put(
                encode_compact_area_page_key(realm, area, page_start_offset),
                page.encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {:?}", e))?;

            next_record_index += append_count;
            current_area_offset = current_area_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    fn load_compact_resource_page_for_write(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        resource: &str,
        page_start_offset: u64,
    ) -> Result<CompactResourcePageValue, String> {
        match txn
            .get(&encode_compact_resource_page_key(
                realm,
                area,
                resource,
                page_start_offset,
            ))
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => {
                CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                    Self::invalid_compact_resource_page_error(
                        realm,
                        area,
                        resource,
                        page_start_offset,
                        error,
                    )
                })
            }
            None => Ok(CompactResourcePageValue {
                records: Vec::new(),
            }),
        }
    }

    fn write_compact_resource_records(
        txn: &mut cntryl_midge::Transaction,
        realm: &str,
        area: &str,
        resource: &str,
        first_resource_offset: u64,
        records: &[CompactResourcePageRecord],
        ttl_opt: Option<u64>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let mut next_record_index = 0usize;
        let mut current_resource_offset = first_resource_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_resource_offset);
            let page_offset = (current_resource_offset - page_start_offset) as usize;
            let mut page = Self::load_compact_resource_page_for_write(
                txn,
                realm,
                area,
                resource,
                page_start_offset,
            )?;

            if page.records.len() != page_offset {
                return Err("ERR_OVERLAPPING_COMPACT_RESOURCE_PAGE_APPEND".to_string());
            }

            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            page.records
                .extend_from_slice(&records[next_record_index..next_record_index + append_count]);

            txn.put(
                encode_compact_resource_page_key(realm, area, resource, page_start_offset),
                page.encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {:?}", e))?;

            next_record_index += append_count;
            current_resource_offset = current_resource_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    fn load_compressed_compact_realm_page_for_write(
        txn: &cntryl_midge::Transaction,
        realm: &str,
        page_start_offset: u64,
    ) -> Result<CompressedCompactRealmPageValue, String> {
        match txn
            .get(&encode_compressed_compact_realm_page_key(
                realm,
                page_start_offset,
            ))
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => CompressedCompactRealmPageValue::try_decode(&value_bytes)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start_offset, error)),
            None => Ok(CompressedCompactRealmPageValue {
                records: Vec::new(),
            }),
        }
    }

    fn write_compressed_compact_realm_records(
        txn: &mut cntryl_midge::Transaction,
        realm: &str,
        first_realm_offset: u64,
        records: &[CompactRealmPageRecord],
        ttl_opt: Option<u64>,
    ) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        let mut next_record_index = 0usize;
        let mut current_realm_offset = first_realm_offset;

        while next_record_index < records.len() {
            let page_start_offset = Self::page_start_offset(current_realm_offset);
            let page_offset = (current_realm_offset - page_start_offset) as usize;
            let mut page =
                Self::load_compressed_compact_realm_page_for_write(txn, realm, page_start_offset)?;

            if page.records.len() != page_offset {
                return Err("ERR_OVERLAPPING_COMPRESSED_COMPACT_REALM_PAGE_APPEND".to_string());
            }

            let append_count =
                (REALM_PAGE_RECORD_LIMIT - page_offset).min(records.len() - next_record_index);
            page.records
                .extend_from_slice(&records[next_record_index..next_record_index + append_count]);

            txn.put(
                encode_compressed_compact_realm_page_key(realm, page_start_offset),
                page.encode(),
                ttl_opt,
            )
            .map_err(|e| format!("txn put failed: {:?}", e))?;

            next_record_index += append_count;
            current_realm_offset = current_realm_offset.saturating_add(append_count as u64);
        }

        Ok(())
    }

    fn write_promotion_frontier_event_rows(
        &self,
        txn: &mut cntryl_midge::Transaction,
        params: PromotionFrontierWriteRowsParams<'_>,
    ) -> Result<(), String> {
        let PromotionFrontierWriteRowsParams {
            realm,
            area,
            resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events,
            created_at,
        } = params;
        let resource_records = Self::build_promotion_frontier_resource_records(
            events,
            first_area_offset,
            first_realm_offset,
            created_at,
        );
        Self::write_compact_resource_records(
            txn,
            realm,
            area,
            resource,
            first_resource_offset,
            &resource_records,
            self.ttl.ttl_seconds,
        )?;

        let area_records =
            Self::build_promotion_frontier_area_records(events, first_resource_offset, created_at);
        Self::write_compact_area_records(
            txn,
            realm,
            area,
            first_area_offset,
            &area_records,
            self.ttl.ttl_seconds,
        )?;

        let realm_records = Self::build_realm_page_records(
            events,
            first_resource_offset,
            first_area_offset,
            created_at,
        );
        Self::write_compressed_compact_realm_records(
            txn,
            realm,
            first_realm_offset,
            &realm_records,
            self.ttl.ttl_seconds,
        )
    }

    fn commit_promotion_frontier_batch(
        &self,
        params: CommitPromotionFrontierBatchParams<'_>,
    ) -> Result<(CommitResponse, ResourceMetaValue), String> {
        let CommitPromotionFrontierBatchParams {
            family,
            realm,
            area,
            resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events,
            committed_size_before,
            ingest_metadata,
            mode,
        } = params;
        let created_at = Self::now_epoch_ms();
        let batch_size = events.len();
        let batch_size_u64 = batch_size as u64;
        let last_resource_offset = first_resource_offset + batch_size_u64 - 1;
        let last_area_offset = first_area_offset + batch_size_u64 - 1;
        let last_realm_offset = first_realm_offset + batch_size_u64 - 1;
        let committed_size_delta = events.iter().map(Self::event_size_bytes).sum::<u64>();

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;
        self.write_promotion_frontier_event_rows(
            &mut txn,
            PromotionFrontierWriteRowsParams {
                realm,
                area,
                resource,
                first_resource_offset,
                first_area_offset,
                first_realm_offset,
                events,
                created_at,
            },
        )?;

        let resource_meta_after = ResourceMetaValue {
            next_offset: last_resource_offset.saturating_add(1),
            committed_size_bytes: committed_size_before.saturating_add(committed_size_delta),
        };
        txn.put(
            encode_resource_meta_key(realm, area, resource),
            resource_meta_after.encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        txn.put(
            encode_area_counter_key(realm, area),
            AreaCounterValue {
                next_offset: last_area_offset.saturating_add(1),
            }
            .encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        txn.put(
            encode_realm_counter_key(realm),
            RealmCounterValue {
                next_offset: last_realm_offset.saturating_add(1),
            }
            .encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        let write_options = match mode {
            StreamWriteMode::Sync => cntryl_midge::WriteOptions::sync(),
            StreamWriteMode::Buffered => cntryl_midge::WriteOptions::buffered(),
        };
        txn.commit(write_options)
            .map_err(|e| format!("midge commit error: {:?}", e))?;

        Ok((
            CommitResponse {
                first_resource_offset,
                last_resource_offset,
                first_area_offset,
                last_area_offset,
                first_realm_offset,
                last_realm_offset,
                batch_size,
                ingest_metadata,
            },
            resource_meta_after,
        ))
    }

    fn resource_sequence_guard(
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

    fn realm_sequence_state(&self, family: u64, realm: &str) -> RealmSequenceStateHandle {
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

    fn resource_meta_state(
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

    fn now_epoch_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn event_size_bytes(event: &EventPayload) -> u64 {
        (event.body.len() + event.metadata.as_ref().map(|m| m.len()).unwrap_or(0)) as u64
    }

    fn load_existing_area_watermark_for_guard(
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
            .map_err(|e| format!("midge get error: {:?}", e))
            .map(|existing| existing.map(|bytes| WatermarkValue::decode(&bytes).watermark))
    }

    fn load_existing_realm_watermark_for_guard(
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
            .map_err(|e| format!("midge get error: {:?}", e))
            .map(|existing| existing.map(|bytes| WatermarkValue::decode(&bytes).watermark))
    }

    #[cfg(test)]
    fn fail_next_area_watermark_guard_read_for_tests() {
        FAIL_NEXT_AREA_WATERMARK_GUARD_READ.with(|cell| cell.set(true));
    }

    #[cfg(test)]
    fn fail_next_realm_watermark_guard_read_for_tests() {
        FAIL_NEXT_REALM_WATERMARK_GUARD_READ.with(|cell| cell.set(true));
    }

    fn resource_identity_from_key(
        expected_prefix: u8,
        key: &[u8],
    ) -> Result<(String, String, String), String> {
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

    fn scan_resource_stats(
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
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let mut next_offset = 0_u64;
        let mut committed_size_bytes = 0_u64;
        for (key_bytes, value_bytes) in results {
            let page_start = decode_resource_offset_from_key(&key_bytes)?;
            let page = CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, error)
            })?;
            next_offset = next_offset.max(page_start.saturating_add(page.records.len() as u64));
            for record in page.records {
                committed_size_bytes = committed_size_bytes
                    .saturating_add(record.body.len() as u64)
                    .saturating_add(record.metadata.as_ref().map(|m| m.len()).unwrap_or(0) as u64);
            }
        }

        Ok((next_offset, committed_size_bytes))
    }

    fn scan_next_area_offset(&self, family: u64, realm: &str, area: &str) -> Result<u64, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_area_page_prefix(realm, area),
        ));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_area_offset_from_key(key)?;
            let page = CompactAreaPageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_area_page_error(realm, area, page_start, error)
            })?;
            Ok(page_start.saturating_add(page.records.len() as u64))
        } else {
            Ok(0)
        }
    }

    fn scan_next_realm_offset(&self, family: u64, realm: &str) -> Result<u64, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compressed_compact_realm_page_prefix(realm),
        ));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start_offset = decode_realm_offset_from_key(key)?;
            let page = CompressedCompactRealmPageValue::try_decode(value)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start_offset, error))?
                .into_compact_realm_page();
            Ok(page_start_offset.saturating_add(page.records.len() as u64))
        } else {
            Ok(0)
        }
    }

    fn scan_next_resource_offset(
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
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_resource_offset_from_key(key)?;
            let page = CompactResourcePageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, error)
            })?;
            Ok(page_start.saturating_add(page.records.len() as u64))
        } else {
            Ok(0)
        }
    }

    fn load_resource_meta_snapshot(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<(ResourceMetaValue, bool), String> {
        let meta_key = encode_resource_meta_key(realm, area, resource);
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        match txn
            .get(&meta_key)
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(bytes) => Ok((ResourceMetaValue::decode(&bytes), false)),
            None => {
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
    }

    fn load_next_resource_offset_from_txn(
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
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => Ok(ResourceMetaValue::decode(&value_bytes).next_offset),
            None => self.scan_next_resource_offset(family, realm, area, resource),
        }
    }

    fn load_area_next_offset_snapshot(
        &self,
        family: u64,
        realm: &str,
        area: &str,
    ) -> Result<(u64, bool), String> {
        let key = encode_area_counter_key(realm, area);
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        match txn.get(&key).map_err(|e| format!("get error: {:?}", e))? {
            Some(bytes) => Ok((AreaCounterValue::decode(&bytes).next_offset, false)),
            None => Ok((self.scan_next_area_offset(family, realm, area)?, true)),
        }
    }

    fn load_realm_next_offset_snapshot(
        &self,
        family: u64,
        realm: &str,
    ) -> Result<(u64, bool), String> {
        let key = encode_realm_counter_key(realm);
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        match txn.get(&key).map_err(|e| format!("get error: {:?}", e))? {
            Some(bytes) => Ok((RealmCounterValue::decode(&bytes).next_offset, false)),
            None => Ok((self.scan_next_realm_offset(family, realm)?, true)),
        }
    }

    pub fn commit_records(
        &self,
        params: CommitRecordsParams<'_>,
    ) -> Result<CommitResponse, String> {
        self.ensure_layout_activation_for_family(params.family)?;

        self.commit_records_promotion_frontier(params)
    }

    fn commit_records_promotion_frontier(
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
        let resource_meta_before = match resource_meta_state.snapshot.clone() {
            Some(snapshot) => snapshot,
            None => {
                let (snapshot, _) =
                    self.load_resource_meta_snapshot(family, realm, area, resource)?;
                resource_meta_state.snapshot = Some(snapshot.clone());
                snapshot
            }
        };
        if resource_meta_before.next_offset != expected_resource_next_offset {
            return Err("ERR_CONCURRENCY_CONFLICT".to_string());
        }

        let realm_sequence_state = self.realm_sequence_state(family, realm);
        let mut realm_sequence_state = realm_sequence_state.lock();
        let area_next_offset = match realm_sequence_state.next_area_offsets.get(area).copied() {
            Some(next_offset) => next_offset,
            None => {
                let (next_offset, _) = self.load_area_next_offset_snapshot(family, realm, area)?;
                realm_sequence_state
                    .next_area_offsets
                    .insert(area.to_string(), next_offset);
                next_offset
            }
        };
        let realm_next_offset = match realm_sequence_state.next_realm_offset {
            Some(next_offset) => next_offset,
            None => {
                let (next_offset, _) = self.load_realm_next_offset_snapshot(family, realm)?;
                realm_sequence_state.next_realm_offset = Some(next_offset);
                next_offset
            }
        };

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

    pub fn list_resource_metadata(&self, family: u64) -> Result<Vec<StreamAdminRecord>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.list_resource_metadata_promotion_frontier(family)
    }

    fn list_resource_metadata_promotion_frontier(
        &self,
        family: u64,
    ) -> Result<Vec<StreamAdminRecord>, String> {
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        let resource_meta_query = cntryl_midge::Query::new().prefix(Bytes::from(vec![
            crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8,
        ]));
        let mut resource_meta_iter = txn
            .scan(&resource_meta_query)
            .map_err(|e| format!("scan error: {:?}", e))?;

        let mut values = Vec::new();
        for (key, value) in resource_meta_iter.collect_all() {
            let (realm, area, resource) = Self::resource_identity_from_key(
                crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8,
                &key,
            )?;
            let meta = ResourceMetaValue::decode(&value);
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
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            staged_events: Vec::with_capacity(initial_capacity),
            event_count: 0,
            total_bytes: 0,
            ingest_metadata,
        };

        let _ = family;
        self.sessions.lock().insert(session_id, session);
        Ok(session_id)
    }

    pub fn append_to_session(
        &self,
        _family: u64,
        session_id: &SessionId,
        event: EventPayload,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;

        if session.event_count + 1 > self.limits.max_batch_events {
            return Err(format!(
                "ERR_BATCH_TOO_LARGE: event count {} exceeds max_batch_events {}",
                session.event_count + 1,
                self.limits.max_batch_events
            ));
        }

        let event_bytes = event.body.len() + event.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
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

    /// Commit session with StreamActor-provided first offsets
    ///
    /// **STORAGE ONLY - NO SEQUENCING**
    /// - Accepts first offsets from StreamActor (already sequenced)
    /// - Computes subsequent offsets by index: first + i
    /// - Does NOT validate expected_offset (StreamActor's job)
    /// - Does NOT scan for max offset (StreamActor is sequencer)
    pub fn commit_session(
        &self,
        family: u64,
        session_id: &SessionId,
        first_resource_offset: u64,
        first_area_offset: u64,
        first_realm_offset: u64,
        mode: StreamWriteMode,
    ) -> Result<CommitResponse, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.commit_session_promotion_frontier(
            family,
            session_id,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            mode,
        )
    }

    fn commit_session_promotion_frontier(
        &self,
        family: u64,
        session_id: &SessionId,
        first_resource_offset: u64,
        first_area_offset: u64,
        first_realm_offset: u64,
        mode: StreamWriteMode,
    ) -> Result<CommitResponse, String> {
        let session = {
            let mut sessions = self.sessions.lock();
            sessions
                .remove(session_id)
                .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?
        };

        if session.event_count == 0 {
            self.sessions.lock().insert(*session_id, session);
            return Err("ERR_EMPTY_BATCH".to_string());
        }

        let batch_size = session.event_count;
        let AppendSession {
            realm,
            area,
            resource,
            staged_events,
            total_bytes,
            ingest_metadata,
            ..
        } = session;

        let committed_size_before =
            match self.load_resource_meta_snapshot(family, &realm, &area, &resource) {
                Ok((snapshot, _)) => snapshot.committed_size_bytes,
                Err(error) => {
                    self.sessions.lock().insert(
                        *session_id,
                        AppendSession {
                            realm,
                            area,
                            resource,
                            staged_events,
                            event_count: batch_size,
                            total_bytes,
                            ingest_metadata,
                        },
                    );
                    return Err(error);
                }
            };

        let result = self.commit_promotion_frontier_batch(CommitPromotionFrontierBatchParams {
            family,
            realm: &realm,
            area: &area,
            resource: &resource,
            first_resource_offset,
            first_area_offset,
            first_realm_offset,
            events: &staged_events,
            committed_size_before,
            ingest_metadata: ingest_metadata.clone(),
            mode,
        });

        let (response, resource_meta_after) = match result {
            Ok(result) => result,
            Err(error) => {
                self.sessions.lock().insert(
                    *session_id,
                    AppendSession {
                        realm,
                        area,
                        resource,
                        staged_events,
                        event_count: batch_size,
                        total_bytes,
                        ingest_metadata,
                    },
                );
                return Err(error);
            }
        };

        self.resource_meta_state(family, &realm, &area, &resource)
            .lock()
            .snapshot = Some(resource_meta_after);
        let realm_sequence_state = self.realm_sequence_state(family, &realm);
        let mut realm_sequence_state = realm_sequence_state.lock();
        realm_sequence_state
            .next_area_offsets
            .insert(area.clone(), response.last_area_offset.saturating_add(1));
        realm_sequence_state.next_realm_offset = Some(response.last_realm_offset.saturating_add(1));

        Ok(response)
    }

    pub fn abort_session(&self, session_id: &SessionId) -> Result<(), String> {
        self.sessions
            .lock()
            .remove(session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;
        Ok(())
    }

    pub fn session_event_count(&self, session_id: &SessionId) -> Option<usize> {
        self.sessions.lock().get(session_id).map(|s| s.event_count)
    }

    /// Peek at the last committed record in a resource stream (tail operation)
    ///
    /// **NO WATERMARK GATING**: Resource reads are strictly ordered by StreamActor.
    /// Watermark is for area/realm dimensions only.
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

    fn peek_resource_promotion_frontier(
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

    fn load_compact_resource_record(
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
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        match txn
            .get(&page_key)
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => {
                let page = CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                    Self::invalid_compact_resource_page_error(
                        realm, area, resource, page_start, error,
                    )
                })?;
                let slot = (resource_offset - page_start) as usize;
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

    /// Read resource stream records
    ///
    /// **NO WATERMARK GATING**: Resource reads are strictly ordered by StreamActor.
    /// Each resource offset is durably committed before being visible.
    /// Watermark is only relevant for area/realm dimensions.
    pub fn read_resource(
        &self,
        params: &ReadResourceParams,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        self.ensure_layout_activation_for_family(params.family)?;

        self.read_resource_promotion_frontier(params)
    }

    fn read_resource_promotion_frontier(
        &self,
        params: &ReadResourceParams,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        if params.limit == 1 && params.max_bytes.is_none() {
            if let Some(record) = self.load_compact_resource_record(
                params.family,
                params.realm,
                params.area,
                params.resource,
                params.from_offset,
            )? {
                let next_resource_offset = self.get_next_resource_offset(
                    params.family,
                    params.realm,
                    params.area,
                    params.resource,
                )?;
                let cursor = super::protocol::ReadCursor {
                    last_resource_offset: record.resource_offset,
                    last_area_offset: record.area_offset,
                    last_realm_offset: record.realm_offset,
                    has_more: record.resource_offset.saturating_add(1) < next_resource_offset,
                };
                return Ok((vec![record], cursor));
            }
        }

        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compact_resource_page_key(
                params.realm,
                params.area,
                params.resource,
                Self::page_start_offset(params.from_offset),
            )))
            .prefix(Bytes::from(Self::build_compact_resource_page_prefix(
                params.realm,
                params.area,
                params.resource,
            )))
            .limit(Self::compact_page_query_limit(
                params.from_offset,
                params.limit,
            ));

        let txn = self
            .db
            .begin_tx(
                params.family as u32,
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let mut records = Vec::with_capacity(params.limit.min(1000) as usize);
        let mut total_bytes = 0usize;
        let mut last_resource_offset = params.from_offset;
        let max_bytes_limit = params.max_bytes.unwrap_or(usize::MAX);
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_resource_offset_from_key(&key_bytes)?;
            let page = CompactResourcePageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_resource_page_error(
                    params.realm,
                    params.area,
                    params.resource,
                    page_start,
                    error,
                )
            })?;

            for (slot, page_record) in page.records.into_iter().enumerate() {
                let resource_offset = page_start + slot as u64;
                if resource_offset < params.from_offset {
                    continue;
                }
                if records.len() == params.limit as usize {
                    has_more = true;
                    break 'page_scan;
                }

                let record_bytes = page_record.body.len()
                    + page_record.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                    has_more = true;
                    break 'page_scan;
                }

                last_resource_offset = resource_offset;
                total_bytes += record_bytes;
                records.push(StreamRecord {
                    resource_offset,
                    area_offset: Some(page_record.area_offset),
                    realm_offset: Some(page_record.realm_offset),
                    body: page_record.body,
                    metadata: page_record.metadata,
                    created_at: page_record.created_at,
                });
            }
        }

        let (last_area_offset, last_realm_offset) = if let Some(last_record) = records.last() {
            (last_record.area_offset, last_record.realm_offset)
        } else {
            (None, None)
        };

        let cursor = super::protocol::ReadCursor {
            last_resource_offset,
            last_area_offset,
            last_realm_offset,
            has_more,
        };

        Ok((records, cursor))
    }

    pub fn read_area(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        self.ensure_layout_activation_for_family(family)?;

        self.read_area_promotion_frontier(family, realm, area, from_offset, limit, max_bytes)
    }

    fn read_area_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        let watermark = self.get_watermark(family, realm, area)?;
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compact_area_page_key(
                realm,
                area,
                Self::page_start_offset(from_offset),
            )))
            .prefix(Bytes::from(Self::build_compact_area_page_prefix(
                realm, area,
            )))
            .limit(Self::compact_page_query_limit(from_offset, limit));

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let mut records = Vec::with_capacity(limit.min(1000) as usize);
        let mut total_bytes = 0usize;
        let mut last_area_offset = from_offset;
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);
        let mut stop_scan = false;
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_area_offset_from_key(&key_bytes)?;
            let page = CompactAreaPageValue::try_decode(&value_bytes).map_err(|error| {
                Self::invalid_compact_area_page_error(realm, area, page_start, error)
            })?;

            for (slot, page_record) in page.records.into_iter().enumerate() {
                let area_offset = page_start + slot as u64;
                if area_offset < from_offset {
                    continue;
                }
                if area_offset > watermark {
                    stop_scan = true;
                    break;
                }
                if records.len() == limit as usize {
                    has_more = true;
                    break 'page_scan;
                }

                let record_bytes = page_record.body.len()
                    + page_record.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                    has_more = true;
                    break 'page_scan;
                }

                last_area_offset = area_offset;
                total_bytes += record_bytes;
                records.push(StreamRecord {
                    resource_offset: page_record.resource_offset,
                    area_offset: Some(area_offset),
                    realm_offset: None,
                    body: page_record.body,
                    metadata: page_record.metadata,
                    created_at: page_record.created_at,
                });
            }

            if stop_scan {
                break;
            }
        }

        let (last_resource_offset, last_realm_offset) = if let Some(last_record) = records.last() {
            (last_record.resource_offset, last_record.realm_offset)
        } else {
            (0, None)
        };

        let cursor = super::protocol::ReadCursor {
            last_resource_offset,
            last_area_offset: Some(last_area_offset),
            last_realm_offset,
            has_more,
        };

        Ok((records, cursor))
    }

    pub fn read_realm(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        self.ensure_layout_activation_for_family(family)?;

        self.read_realm_promotion_frontier(family, realm, from_offset, limit, max_bytes)
    }

    fn read_realm_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        let realm_watermark = self.get_realm_watermark(family, realm)?;
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compressed_compact_realm_page_key(
                realm,
                Self::page_start_offset(from_offset),
            )))
            .prefix(Bytes::from(
                Self::build_compressed_compact_realm_page_prefix(realm),
            ))
            .limit(Self::compact_page_query_limit(from_offset, limit));

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let mut records = Vec::with_capacity(limit.min(1000) as usize);
        let mut total_bytes = 0usize;
        let mut last_realm_offset = from_offset;
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);
        let mut stop_scan = false;
        let mut has_more = false;

        'page_scan: for (key_bytes, value_bytes) in results {
            let page_start = decode_realm_offset_from_key(&key_bytes)?;
            let page = CompressedCompactRealmPageValue::try_decode(&value_bytes)
                .map_err(|error| Self::invalid_compact_realm_page_error(page_start, error))?
                .into_compact_realm_page();

            for (slot, page_record) in page.records.into_iter().enumerate() {
                let realm_offset = page_start + slot as u64;
                if realm_offset < from_offset {
                    continue;
                }
                if realm_offset > realm_watermark {
                    stop_scan = true;
                    break;
                }
                if records.len() == limit as usize {
                    has_more = true;
                    break 'page_scan;
                }

                let record_bytes = page_record.body.len()
                    + page_record.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                    has_more = true;
                    break 'page_scan;
                }

                last_realm_offset = realm_offset;
                total_bytes += record_bytes;
                records.push(StreamRecord {
                    resource_offset: page_record.resource_offset,
                    area_offset: Some(page_record.area_offset),
                    realm_offset: Some(realm_offset),
                    body: page_record.body,
                    metadata: page_record.metadata,
                    created_at: page_record.created_at,
                });
            }

            if stop_scan {
                break;
            }
        }

        let (last_resource_offset, last_area_offset) = if let Some(last_record) = records.last() {
            (last_record.resource_offset, last_record.area_offset)
        } else {
            (0, None)
        };

        let cursor = super::protocol::ReadCursor {
            last_resource_offset,
            last_area_offset,
            last_realm_offset: Some(last_realm_offset),
            has_more,
        };

        Ok((records, cursor))
    }

    pub fn get_watermark(&self, family: u64, realm: &str, area: &str) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = encode_watermark_key(realm, area);
        let counter_key = encode_area_counter_key(realm, area);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        match txn
            .get(&key)
            .map_err(|e| format!("midge get error: {:?}", e))?
        {
            Some(bytes) => {
                let value = WatermarkValue::decode(&bytes);
                Ok(value.watermark)
            }
            None => match txn
                .get(&counter_key)
                .map_err(|e| format!("midge get error: {:?}", e))?
            {
                Some(bytes) => Ok(AreaCounterValue::decode(&bytes)
                    .next_offset
                    .saturating_sub(1)),
                None => Ok(self
                    .scan_next_area_offset(family, realm, area)?
                    .saturating_sub(1)),
            },
        }
    }

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
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        // Monotonicity guard: watermarks must only advance, never regress.
        // If the new value is not strictly greater than the current value, no-op.
        if let Some(current) = Self::load_existing_area_watermark_for_guard(&txn, &key)? {
            if watermark <= current {
                return Ok(());
            }
        }

        let value = WatermarkValue { watermark };
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {:?}", e))?;
        let opts = cntryl_midge::WriteOptions::sync();
        txn.commit(opts)
            .map_err(|e| format!("midge commit error: {:?}", e))
    }

    pub fn get_realm_watermark(&self, family: u64, realm: &str) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);
        let counter_key = encode_realm_counter_key(realm);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        match txn
            .get(&key)
            .map_err(|e| format!("midge get error: {:?}", e))?
        {
            Some(bytes) => {
                let value = WatermarkValue::decode(&bytes);
                Ok(value.watermark)
            }
            None => match txn
                .get(&counter_key)
                .map_err(|e| format!("midge get error: {:?}", e))?
            {
                Some(bytes) => Ok(RealmCounterValue::decode(&bytes)
                    .next_offset
                    .saturating_sub(1)),
                None => Ok(self
                    .scan_next_realm_offset(family, realm)?
                    .saturating_sub(1)),
            },
        }
    }

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
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        // Monotonicity guard: realm watermarks must only advance, never regress.
        if let Some(current) = Self::load_existing_realm_watermark_for_guard(&txn, &key)? {
            if watermark <= current {
                return Ok(());
            }
        }

        let value = WatermarkValue { watermark };
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {:?}", e))?;
        let opts = cntryl_midge::WriteOptions::sync();
        txn.commit(opts)
            .map_err(|e| format!("midge commit error: {:?}", e))
    }

    /// Get stream metadata (limits, TTL, offsets, watermarks)
    ///
    /// Used for describe_stream / introspection API
    pub fn get_metadata(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<super::protocol::StreamMetadata, String> {
        let first_resource_offset =
            self.get_first_resource_offset(family, realm, area, resource)?;
        let last_resource_offset = self.get_last_resource_offset(family, realm, area, resource)?;
        let area_watermark = self.get_watermark(family, realm, area)?;
        let realm_watermark = self.get_realm_watermark(family, realm)?;
        let resource_count = match (first_resource_offset, last_resource_offset) {
            (Some(first_offset), Some(last_offset)) if last_offset >= first_offset => {
                last_offset - first_offset + 1
            }
            _ => 0,
        };

        Ok(super::protocol::StreamMetadata {
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

    /// Get the last committed resource offset for recovery
    ///
    /// **CRITICAL**: StreamActor must call this on initialization to recover
    /// next_resource_offset and avoid reusing offsets after restart.
    pub fn get_last_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.get_last_resource_offset_promotion_frontier(family, realm, area, resource)
    }

    fn get_last_resource_offset_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_resource_page_prefix(realm, area, resource),
        ));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, value)) = results.last() {
            let page_start = decode_resource_offset_from_key(key)?;
            let page = CompactResourcePageValue::try_decode(value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, error)
            })?;

            if page.records.is_empty() {
                Ok(None)
            } else {
                Ok(Some(page_start + page.records.len() as u64 - 1))
            }
        } else {
            Ok(None)
        }
    }

    /// Get the first committed resource offset that is still readable.
    ///
    /// This is used by exact-resource metadata surfaces that need to remain
    /// truthful when older resource rows are trimmed from the head.
    pub fn get_first_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.get_first_resource_offset_promotion_frontier(family, realm, area, resource)
    }

    fn get_first_resource_offset_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<u64>, String> {
        let query = cntryl_midge::Query::new().prefix(Bytes::from(
            Self::build_compact_resource_page_prefix(realm, area, resource),
        ));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        for (key, value) in results {
            let page_start = decode_resource_offset_from_key(&key)?;
            let page = CompactResourcePageValue::try_decode(&value).map_err(|error| {
                Self::invalid_compact_resource_page_error(realm, area, resource, page_start, error)
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
    pub fn get_next_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        self.ensure_layout_activation_for_family(family)?;

        self.get_next_resource_offset_promotion_frontier(family, realm, area, resource)
    }

    fn get_next_resource_offset_promotion_frontier(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        self.load_next_resource_offset_from_txn(&txn, family, realm, area, resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::stream::storage::{encode_offset_counter_key, OffsetCounterValue};
    use crate::testkit::create_test_engine_with_cfs;
    use bytes::Bytes;

    fn read_layout_marker(
        engine: &cntryl_midge::Engine,
        family: u32,
    ) -> Option<StreamStorageLayout> {
        let txn = engine
            .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        txn.get(&encode_stream_layout_marker_key())
            .expect("read layout marker")
            .map(|bytes| {
                StreamLayoutMarkerValue::decode(&bytes)
                    .expect("decode layout marker")
                    .layout
            })
    }

    fn write_layout_marker(
        engine: &cntryl_midge::Engine,
        family: u32,
        layout: StreamStorageLayout,
    ) {
        let mut txn = engine
            .begin_tx(family, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        txn.put(
            encode_stream_layout_marker_key(),
            StreamLayoutMarkerValue::new(layout).encode(),
            None,
        )
        .expect("write layout marker");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit layout marker");
    }

    fn single_event(body: &'static [u8]) -> Vec<EventPayload> {
        vec![EventPayload {
            body: Bytes::from_static(body),
            metadata: None,
        }]
    }

    #[test]
    fn should_reuse_sequence_guard_given_same_resource() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

        // Act
        let first = store.resource_sequence_guard(1, "test", "events", "orders");
        let second = store.resource_sequence_guard(1, "test", "events", "orders");

        // Assert
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn should_create_distinct_sequence_guards_given_different_resources() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

        // Act
        let left = store.resource_sequence_guard(1, "test", "events", "orders");
        let right = store.resource_sequence_guard(1, "test", "events", "audits");

        // Assert
        assert!(!Arc::ptr_eq(&left, &right));
    }

    #[test]
    fn should_use_promotion_frontier_stream_storage_layout_by_default() {
        // Arrange

        // Act
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));

        // Assert
        assert_eq!(
            store.storage_layout(),
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    fn should_use_selected_stream_storage_layout_given_explicit_layout() {
        // Arrange

        // Act
        let store = StreamStore::with_layout(
            create_test_engine_with_cfs(vec![1]),
            StreamStorageLayout::PromotionFrontier,
        );

        // Assert
        assert_eq!(
            store.storage_layout(),
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    fn should_normalize_legacy_stream_storage_layout_given_explicit_layout() {
        // Arrange

        // Act
        let store = StreamStore::with_layout(
            create_test_engine_with_cfs(vec![1]),
            StreamStorageLayout::LegacyCovering,
        );

        // Assert
        assert_eq!(
            store.storage_layout(),
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    fn should_persist_promotion_frontier_stream_layout_marker_given_first_real_store_write() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let store = StreamStore::new(db.clone());
        let events = single_event(b"first");

        // Act
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("commit records");

        // Assert
        assert_eq!(
            read_layout_marker(db.as_ref(), 1),
            Some(StreamStorageLayout::PromotionFrontier)
        );
    }

    #[test]
    fn should_mark_existing_families_given_promotion_boot_scan() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1, 2]);
        let store = StreamStore::new(db.clone());

        // Act
        store
            .ensure_layout_activation_for_existing_families()
            .expect("boot scan should succeed for existing families");

        // Assert
        assert_eq!(
            read_layout_marker(db.as_ref(), 1),
            Some(StreamStorageLayout::PromotionFrontier)
        );
        assert_eq!(
            read_layout_marker(db.as_ref(), 2),
            Some(StreamStorageLayout::PromotionFrontier)
        );
    }

    #[test]
    fn should_return_error_given_unmarked_stream_data_on_default_promotion_layout() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let mut txn = db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        txn.put(
            encode_offset_counter_key("test", "events", "orders"),
            OffsetCounterValue { next_offset: 1 }.encode(),
            None,
        )
        .expect("write unmarked stream metadata");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit unmarked stream metadata");
        let store = StreamStore::new(db);

        // Act
        let result = store.get_next_resource_offset(1, "test", "events", "orders");

        // Assert
        let error = result.expect_err("promotion frontier should reject unmarked legacy data");
        assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED"));
    }

    #[test]
    fn should_return_error_given_legacy_layout_marker() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        write_layout_marker(db.as_ref(), 1, StreamStorageLayout::LegacyCovering);
        let store = StreamStore::new(db);

        // Act
        let result = store.get_next_resource_offset(1, "test", "events", "orders");

        // Assert
        let error = result.expect_err("promotion store should reject legacy marker");
        assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_MISMATCH"));
        assert!(error.contains("legacy-covering"));
        assert!(error.contains("promotion-frontier"));
    }

    #[test]
    fn should_return_error_given_legacy_layout_marker_on_existing_families_boot_scan() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1, 2]);
        write_layout_marker(db.as_ref(), 2, StreamStorageLayout::LegacyCovering);
        let store = StreamStore::new(db);

        // Act
        let result = store.ensure_layout_activation_for_existing_families();

        // Assert
        let error = result.expect_err("legacy layout marker should fail boot scan");
        assert!(error.contains("ERR_STREAM_STORAGE_LAYOUT_MISMATCH"));
        assert!(error.contains("family=2"));
    }

    #[test]
    fn should_return_error_when_area_watermark_guard_read_fails() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        store
            .set_watermark(1, "test", "events", 10)
            .expect("seed area watermark");
        StreamStore::fail_next_area_watermark_guard_read_for_tests();

        // Act
        let result = store.set_watermark(1, "test", "events", 11);

        // Assert
        assert!(result.is_err());
        assert_eq!(
            store
                .get_watermark(1, "test", "events")
                .expect("read area watermark"),
            10
        );
    }

    #[test]
    fn should_preserve_area_watermark_given_same_value_update() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        store
            .set_watermark(1, "test", "events", 10)
            .expect("seed area watermark");

        // Act
        store
            .set_watermark(1, "test", "events", 10)
            .expect("rewrite same area watermark");

        // Assert
        assert_eq!(
            store
                .get_watermark(1, "test", "events")
                .expect("read area watermark"),
            10
        );
    }

    #[test]
    fn should_preserve_area_watermark_given_lower_value_update() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        store
            .set_watermark(1, "test", "events", 10)
            .expect("seed area watermark");

        // Act
        store
            .set_watermark(1, "test", "events", 9)
            .expect("rewrite lower area watermark");

        // Assert
        assert_eq!(
            store
                .get_watermark(1, "test", "events")
                .expect("read area watermark"),
            10
        );
    }

    #[test]
    fn should_return_error_when_realm_watermark_guard_read_fails() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        store
            .set_realm_watermark(1, "test", 10)
            .expect("seed realm watermark");
        StreamStore::fail_next_realm_watermark_guard_read_for_tests();

        // Act
        let result = store.set_realm_watermark(1, "test", 11);

        // Assert
        assert!(result.is_err());
        assert_eq!(
            store
                .get_realm_watermark(1, "test")
                .expect("read realm watermark"),
            10
        );
    }

    #[test]
    fn should_preserve_realm_watermark_given_same_value_update() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        store
            .set_realm_watermark(1, "test", 10)
            .expect("seed realm watermark");

        // Act
        store
            .set_realm_watermark(1, "test", 10)
            .expect("rewrite same realm watermark");

        // Assert
        assert_eq!(
            store
                .get_realm_watermark(1, "test")
                .expect("read realm watermark"),
            10
        );
    }

    #[test]
    fn should_preserve_realm_watermark_given_lower_value_update() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        store
            .set_realm_watermark(1, "test", 10)
            .expect("seed realm watermark");

        // Act
        store
            .set_realm_watermark(1, "test", 9)
            .expect("rewrite lower realm watermark");

        // Assert
        assert_eq!(
            store
                .get_realm_watermark(1, "test")
                .expect("read realm watermark"),
            10
        );
    }

    #[test]
    fn should_allocate_sequential_offsets_given_same_process_commits() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");

        // Act
        let first = store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        let second = store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audits",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Assert
        assert_eq!(first.first_area_offset, 0);
        assert_eq!(first.first_realm_offset, 0);
        assert_eq!(second.first_area_offset, 1);
        assert_eq!(second.first_realm_offset, 1);
    }

    #[test]
    fn should_continue_sequential_offsets_given_recreated_store() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let first_store = StreamStore::new(db.clone());
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        first_store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("seed commit");
        let second_store = StreamStore::new(db);

        // Act
        let second = second_store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audits",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Assert
        assert_eq!(second.first_area_offset, 1);
        assert_eq!(second.first_realm_offset, 1);
    }

    #[test]
    fn should_allocate_next_resource_offset_given_same_process_commits() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");

        // Act
        let first = store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        let second = store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 1,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Assert
        assert_eq!(first.first_resource_offset, 0);
        assert_eq!(second.first_resource_offset, 1);
        assert_eq!(
            store
                .get_next_resource_offset(1, "test", "events", "orders")
                .expect("next resource offset"),
            2
        );
    }

    #[test]
    fn should_continue_next_resource_offset_given_recreated_store() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let first_store = StreamStore::new(db.clone());
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        first_store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("seed commit");
        let second_store = StreamStore::new(db);

        // Act
        let second = second_store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 1,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Assert
        assert_eq!(second.first_resource_offset, 1);
        assert_eq!(
            second_store
                .get_next_resource_offset(1, "test", "events", "orders")
                .expect("next resource offset"),
            2
        );
    }

    #[test]
    fn should_reject_future_expected_resource_offset_given_store_commit() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let future_events = single_event(b"future");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("seed commit");

        // Act
        let result = store.commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 2,
            events: &future_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        });

        // Assert
        let error = result.expect_err("future expected offset should fail store commit");
        assert_eq!(error, "ERR_CONCURRENCY_CONFLICT");
        assert_eq!(
            store
                .get_next_resource_offset(1, "test", "events", "orders")
                .expect("next resource offset"),
            1
        );
    }

    #[test]
    fn should_report_has_more_given_single_record_resource_fast_path() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 1,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: 0,
                limit: 1,
                max_bytes: None,
            })
            .expect("read first resource record");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].resource_offset, 0);
        assert!(cursor.has_more);
        assert_eq!(cursor.last_resource_offset, 0);
    }

    #[test]
    fn should_not_report_has_more_given_single_record_resource_fast_path_at_end() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 1,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: 1,
                limit: 1,
                max_bytes: None,
            })
            .expect("read last resource record");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].resource_offset, 1);
        assert!(!cursor.has_more);
        assert_eq!(cursor.last_resource_offset, 1);
    }

    #[test]
    fn should_return_next_available_resource_record_given_trimmed_compact_resource_page_on_ttl_store(
    ) {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let store = StreamStore::with_config(
            db.clone(),
            BatchLimits::default(),
            StreamTTL::with_seconds(1),
        );
        let first_page_events = vec![
            EventPayload {
                body: Bytes::from_static(b"first-page"),
                metadata: None,
            };
            REALM_PAGE_RECORD_LIMIT
        ];
        let second_page_events = single_event(b"second-page");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_page_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first page commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: REALM_PAGE_RECORD_LIMIT as u64,
                events: &second_page_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second page commit");
        let mut txn = db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin ttl trim tx");
        txn.delete(encode_compact_resource_page_key(
            "test", "events", "orders", 0,
        ))
        .expect("delete trimmed resource page");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit ttl trim simulation");

        // Act
        let (records, cursor) = store
            .read_resource(&ReadResourceParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                from_offset: 0,
                limit: 1,
                max_bytes: None,
            })
            .expect("read ttl-trimmed resource stream");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].resource_offset, REALM_PAGE_RECORD_LIMIT as u64);
        assert_eq!(records[0].body, Bytes::from_static(b"second-page"));
        assert_eq!(cursor.last_resource_offset, REALM_PAGE_RECORD_LIMIT as u64);
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_peek_resource_given_missing_offset_counter_and_present_resource_meta() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let store = StreamStore::new(db.clone());
        let events = single_event(b"first");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("commit record");
        let mut txn = db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin cleanup tx");
        txn.delete(encode_offset_counter_key("test", "events", "orders"))
            .expect("delete legacy offset counter");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit legacy offset counter removal");

        // Act
        let record = store
            .peek_resource(1, "test", "events", "orders")
            .expect("peek exact resource")
            .expect("expected tail record");

        // Assert
        assert_eq!(record.resource_offset, 0);
        assert_eq!(record.body, Bytes::from_static(b"first"));
    }

    #[test]
    fn should_not_report_has_more_given_area_read_at_end() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audits",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_area(1, "test", "events", 0, 2, None)
            .expect("read area stream");

        // Assert
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].area_offset, Some(0));
        assert_eq!(records[1].area_offset, Some(1));
        assert_eq!(cursor.last_area_offset, Some(1));
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_return_record_given_area_read_at_watermark_boundary() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audits",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_area(1, "test", "events", 1, 1, None)
            .expect("read area at watermark boundary");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].area_offset, Some(1));
        assert_eq!(records[0].body, Bytes::from_static(b"second"));
        assert_eq!(cursor.last_area_offset, Some(1));
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_truncate_area_read_given_max_bytes() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"abcd");
        let second_events = single_event(b"efgh");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audits",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_area(1, "test", "events", 0, 10, Some(4))
            .expect("read area with max_bytes");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].body, Bytes::from_static(b"abcd"));
        assert_eq!(records[0].area_offset, Some(0));
        assert_eq!(cursor.last_area_offset, Some(0));
        assert!(cursor.has_more);
    }

    #[test]
    fn should_return_first_area_record_given_max_bytes_below_record_size() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let events = single_event(b"abcde");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("commit record");

        // Act
        let (records, cursor) = store
            .read_area(1, "test", "events", 0, 10, Some(4))
            .expect("read area with tight max_bytes");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].body, Bytes::from_static(b"abcde"));
        assert_eq!(cursor.last_area_offset, Some(0));
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_not_report_has_more_given_realm_read_at_end() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "audit",
                resource: "entries",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_realm(1, "test", 0, 2, None)
            .expect("read realm stream");

        // Assert
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].realm_offset, Some(0));
        assert_eq!(records[1].realm_offset, Some(1));
        assert_eq!(cursor.last_realm_offset, Some(1));
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_return_record_given_realm_read_at_watermark_boundary() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "audit",
                resource: "entries",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_realm(1, "test", 1, 1, None)
            .expect("read realm at watermark boundary");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].realm_offset, Some(1));
        assert_eq!(records[0].body, Bytes::from_static(b"second"));
        assert_eq!(cursor.last_realm_offset, Some(1));
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_truncate_realm_read_given_max_bytes() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let first_events = single_event(b"abcd");
        let second_events = single_event(b"efgh");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("first commit");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "audit",
                resource: "entries",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let (records, cursor) = store
            .read_realm(1, "test", 0, 10, Some(4))
            .expect("read realm with max_bytes");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].body, Bytes::from_static(b"abcd"));
        assert_eq!(records[0].realm_offset, Some(0));
        assert_eq!(cursor.last_realm_offset, Some(0));
        assert!(cursor.has_more);
    }

    #[test]
    fn should_return_first_realm_record_given_max_bytes_below_record_size() {
        // Arrange
        let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
        let events = single_event(b"abcde");
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("commit record");

        // Act
        let (records, cursor) = store
            .read_realm(1, "test", 0, 10, Some(4))
            .expect("read realm with tight max_bytes");

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].body, Bytes::from_static(b"abcde"));
        assert_eq!(cursor.last_realm_offset, Some(0));
        assert!(!cursor.has_more);
    }

    #[test]
    fn should_read_realm_records_given_recreated_partial_compact_page() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let first_store = StreamStore::new(db.clone());
        let second_store = StreamStore::new(db);
        let first_events = single_event(b"first");
        let second_events = single_event(b"second");
        first_store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "orders",
                expected_resource_next_offset: 0,
                events: &first_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("seed commit");
        second_store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "test",
                area: "events",
                resource: "audits",
                expected_resource_next_offset: 0,
                events: &second_events,
                ingest_metadata: None,
                mode: StreamWriteMode::Buffered,
            })
            .expect("second commit");

        // Act
        let records = second_store
            .read_realm(1, "test", 1, 10, None)
            .expect("read realm from offset one")
            .0;

        // Assert
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].realm_offset, Some(1));
        assert_eq!(records[0].body, Bytes::from_static(b"second"));
    }

    #[test]
    fn should_return_error_given_malformed_compact_realm_page_when_reading_realm() {
        // Arrange
        let db = create_test_engine_with_cfs(vec![1]);
        let store = StreamStore::new(db.clone());
        store
            .set_realm_watermark(1, "test", 0)
            .expect("seed realm watermark");
        let mut txn = db
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        txn.put(
            encode_compressed_compact_realm_page_key("test", 0),
            vec![0, 0xB2, 1, 0, 0, 0],
            None,
        )
        .expect("write malformed compact realm page");
        txn.commit(cntryl_midge::WriteOptions::sync())
            .expect("commit malformed compact realm page");

        // Act
        let result = store.read_realm(1, "test", 0, 10, None);

        // Assert
        let error = result.expect_err("malformed compact realm page should fail read");
        assert!(error.contains("ERR_INVALID_COMPACT_REALM_PAGE"));
    }
}
