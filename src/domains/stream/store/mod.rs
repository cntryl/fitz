//! Stream storage layer - STORAGE ONLY, NO SEQUENCING

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use super::protocol::{
    IngestMetadata, StreamDiscriminator, StreamFilterSet, StreamFilteredReason, StreamReadItem,
    StreamRecord, StreamWriteMode,
};
use super::storage::{
    decode_area_offset_from_key, decode_realm_offset_from_key, decode_resource_offset_from_key,
    decode_staging_value, encode_area_counter_key, encode_compact_area_page_key,
    encode_compact_resource_page_key, encode_compressed_compact_realm_page_key,
    encode_realm_counter_key, encode_resource_meta_key, encode_stream_layout_marker_key,
    encode_watermark_key, AreaCounterValue, AreaLocatorValue, AreaValue, CanonicalResourceValue,
    CompactAreaPageRecord, CompactAreaPageValue, CompactRealmPageRecord, CompactResourcePageRecord,
    CompactResourcePageValue, CompressedCompactRealmPageValue, KeyPrefix, OffsetCounterValue,
    RealmCounterValue, RealmLocatorValue, RealmValue, ResourceMetaValue, ResourceValue,
    StreamLayoutMarkerValue, WatermarkValue, REALM_PAGE_RECORD_LIMIT,
};

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_AREA_WATERMARK_GUARD_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REALM_WATERMARK_GUARD_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_AREA_WATERMARK_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REALM_WATERMARK_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_PROMOTION_FRONTIER_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Debug, Clone)]
pub struct EventPayload {
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub discriminator: Option<StreamDiscriminator>,
}

/// Active append session buffered until commit.
struct AppendSession {
    family: u64,
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

const ERR_SESSION_ROUTE_FAMILY_MISMATCH: &str = "ERR_SESSION_ROUTE_FAMILY_MISMATCH";

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
pub(crate) struct ReadAreaParams<'a> {
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) from_offset: u64,
    pub(crate) limit: u64,
    pub(crate) max_bytes: Option<usize>,
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
struct DiscriminatorWriteRowsParams<'a> {
    realm: &'a str,
    area: &'a str,
    resource: &'a str,
    first_resource_offset: u64,
    first_area_offset: u64,
    first_realm_offset: u64,
    events: &'a [EventPayload],
    ttl_opt: Option<u64>,
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

    #[must_use]
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

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LegacyCovering => "legacy-covering",
            Self::PromotionFrontier => "promotion-frontier",
        }
    }
}

impl StreamTTL {
    #[must_use]
    pub fn with_seconds(seconds: u64) -> Self {
        Self {
            ttl_seconds: Some(seconds),
        }
    }

    #[must_use]
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
    db: crate::storage::FitzStorageEngine,
    limits: BatchLimits,
    layout: StreamStorageLayout,
    sessions: Arc<Mutex<HashMap<SessionId, AppendSession>>>,
    ttl: StreamTTL,
    next_session_id: std::sync::atomic::AtomicU64,
    sequencing_guards: Arc<Mutex<HashMap<SequenceGuardKey, SequenceGuard>>>,
    realm_sequence_states: Arc<Mutex<HashMap<RealmSequenceStateKey, RealmSequenceStateHandle>>>,
    resource_meta_states: Arc<Mutex<HashMap<SequenceGuardKey, ResourceMetaStateHandle>>>,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct ReadCursorState {
    last_resource_offset: u64,
    last_area_offset: Option<u64>,
    last_realm_offset: Option<u64>,
}

struct ReadPageState<'a> {
    from_offset: u64,
    limit: usize,
    max_bytes_limit: usize,
    watermark: Option<u64>,
    filter: Option<&'a StreamFilterSet>,
    cursor: &'a mut ReadCursorState,
    total_bytes: &'a mut usize,
    items: &'a mut Vec<StreamReadItem>,
    has_more: &'a mut bool,
}

fn resource_page_record_bytes(page_record: &CompactResourcePageRecord) -> usize {
    page_record.body.len() + page_record.metadata.as_ref().map_or(0, Bytes::len)
}

fn update_resource_cursor(
    state: &mut ReadCursorState,
    resource_offset: u64,
    page_record: &CompactResourcePageRecord,
) {
    state.last_resource_offset = resource_offset;
    state.last_area_offset = Some(page_record.area_offset);
    state.last_realm_offset = Some(page_record.realm_offset);
}

fn area_page_record_bytes(page_record: &CompactAreaPageRecord) -> usize {
    page_record.body.len() + page_record.metadata.as_ref().map_or(0, Bytes::len)
}

fn update_area_cursor(
    state: &mut ReadCursorState,
    area_offset: u64,
    page_record: &CompactAreaPageRecord,
) {
    state.last_resource_offset = page_record.resource_offset;
    state.last_area_offset = Some(area_offset);
    state.last_realm_offset = None;
}

fn realm_page_record_bytes(page_record: &CompactRealmPageRecord) -> usize {
    page_record.body.len() + page_record.metadata.as_ref().map_or(0, Bytes::len)
}

fn update_realm_cursor(
    state: &mut ReadCursorState,
    realm_offset: u64,
    page_record: &CompactRealmPageRecord,
) {
    state.last_resource_offset = page_record.resource_offset;
    state.last_area_offset = Some(page_record.area_offset);
    state.last_realm_offset = Some(realm_offset);
}

fn family_to_storage_partition(family: u64) -> u32 {
    u32::try_from(family).unwrap_or(u32::MAX)
}

fn read_limit_to_usize(limit: u64) -> usize {
    usize::try_from(limit).unwrap_or(usize::MAX)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn millis_to_u64_saturating(millis: u128) -> u64 {
    u64::try_from(millis).unwrap_or(u64::MAX)
}

impl ReadCursorState {
    fn into_cursor(self, has_more: bool) -> super::protocol::ReadCursor {
        super::protocol::ReadCursor {
            last_resource_offset: self.last_resource_offset,
            last_area_offset: self.last_area_offset,
            last_realm_offset: self.last_realm_offset,
            has_more,
        }
    }
}

fn collect_filtered_read_page_items<
    R,
    I,
    FLoadDiscriminator,
    FRecordBytes,
    FUpdateCursor,
    FFilteredItem,
    FEventItem,
>(
    records: I,
    state: &mut ReadPageState<'_>,
    mut load_discriminator: FLoadDiscriminator,
    mut record_bytes: FRecordBytes,
    mut update_cursor: FUpdateCursor,
    mut filtered_item: FFilteredItem,
    mut event_item: FEventItem,
) -> Result<bool, String>
where
    I: IntoIterator<Item = (u64, R)>,
    FLoadDiscriminator: FnMut(u64, &R) -> Result<Option<String>, String>,
    FRecordBytes: FnMut(&R) -> usize,
    FUpdateCursor: FnMut(&mut ReadCursorState, u64, &R),
    FFilteredItem: FnMut(u64) -> StreamReadItem,
    FEventItem: FnMut(u64, R) -> StreamReadItem,
{
    let mut stop_scan = false;

    for (offset, record) in records {
        if offset < state.from_offset {
            continue;
        }

        if let Some(watermark) = state.watermark {
            if offset > watermark {
                stop_scan = true;
                break;
            }
        }

        let discriminator = if state.filter.is_some() {
            load_discriminator(offset, &record)?
        } else {
            None
        };

        if !StreamStore::record_matches_filter(state.filter, discriminator.as_deref()) {
            if state.items.len() == state.limit {
                *state.has_more = true;
                return Ok(true);
            }

            update_cursor(state.cursor, offset, &record);
            state.items.push(filtered_item(offset));
            continue;
        }

        if state.items.len() == state.limit {
            *state.has_more = true;
            return Ok(true);
        }

        let record_bytes = record_bytes(&record);
        if *state.total_bytes + record_bytes > state.max_bytes_limit && !state.items.is_empty() {
            *state.has_more = true;
            return Ok(true);
        }

        update_cursor(state.cursor, offset, &record);
        *state.total_bytes += record_bytes;
        state.items.push(event_item(offset, record));
    }

    Ok(stop_scan)
}

mod commits_and_sessions;
mod compact_page_writes;
mod config_and_layout;
mod reads;
mod sequence_and_filters;
mod watermarks_and_metadata;

#[cfg(test)]
mod tests;
