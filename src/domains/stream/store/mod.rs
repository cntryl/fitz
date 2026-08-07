//! Stream storage layer - STORAGE ONLY, NO SEQUENCING

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use super::protocol::{
    IngestMetadata, StreamDiscriminator, StreamFilterSet, StreamFilteredReason, StreamReadItem,
    StreamRecord, StreamWriteMode,
};
use super::storage::{
    decode_area_offset_from_key, decode_realm_offset_from_key, decode_resource_offset_from_key,
    decode_staging_value, encode_area_counter_key, encode_compact_area_page_key,
    encode_compact_global_page_key, encode_compact_resource_page_key,
    encode_compressed_compact_realm_page_key, encode_cursor_state_key,
    encode_family_writer_epoch_key, encode_global_area_posting_key,
    encode_global_area_resource_posting_key, encode_global_counter_key,
    encode_global_discriminator_key, encode_global_resource_posting_key,
    encode_global_watermark_key, encode_payload_blob_key, encode_realm_counter_key,
    encode_realm_resource_posting_key, encode_resource_meta_key, encode_stream_layout_marker_key,
    encode_watermark_key, AreaCounterValue, AreaValue, CompactAreaPageRecord, CompactAreaPageValue,
    CompactGlobalPageRecord, CompactGlobalPageValue, CompactRealmPageRecord,
    CompactResourcePageRecord, CompactResourcePageValue, CompressedCompactRealmPageValue,
    KeyPrefix, OffsetCounterValue, PostingEntry, PostingPageValue, RealmCounterValue, RealmValue,
    ResourceMetaValue, ResourceValue, StreamLayoutMarkerValue, WatermarkValue,
    GLOBAL_PAGE_RECORD_LIMIT, REALM_PAGE_RECORD_LIMIT,
};

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_AREA_WATERMARK_GUARD_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REALM_WATERMARK_GUARD_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_GLOBAL_WATERMARK_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_AREA_WATERMARK_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REALM_WATERMARK_PERSIST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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

#[derive(Default)]
struct GlobalCompletionState {
    watermark: Option<u64>,
    resolved: BTreeMap<u64, u64>,
}

#[derive(Clone, Copy)]
struct GlobalReservation {
    first_offset: u64,
    writer_epoch: u64,
}

#[derive(Clone, Copy)]
struct PendingGlobalReservation {
    resource_offset: u64,
    event_count: usize,
    reservation: GlobalReservation,
}

enum PromotionCommitFailure {
    ScopeConflict,
    Retryable(String),
    Resolved(String),
}

impl PromotionCommitFailure {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::ScopeConflict | Self::Retryable(_))
    }

    fn is_scope_conflict(&self) -> bool {
        matches!(self, Self::ScopeConflict)
    }

    fn into_message(self) -> String {
        match self {
            Self::ScopeConflict => "ERR_CONCURRENCY_CONFLICT".to_string(),
            Self::Retryable(message) | Self::Resolved(message) => message,
        }
    }
}

enum PromotionWriteFailure {
    ScopeConflict,
    WriterFenced,
    Other(String),
}

impl From<String> for PromotionWriteFailure {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

enum PromotionTransactionFailure {
    WriteConflict,
    Other(String),
}

type GlobalCompletionStateHandle = Arc<Mutex<GlobalCompletionState>>;
type PendingGlobalReservations = Arc<Mutex<HashMap<SequenceGuardKey, PendingGlobalReservation>>>;

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

#[derive(Clone, Copy)]
pub(crate) struct ReadRealmPostingParams<'a> {
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) from_offset: u64,
    pub(crate) limit: u64,
    pub(crate) max_bytes: Option<usize>,
}

#[derive(Clone, Copy)]
pub(crate) struct ReadGlobalPostingParams<'a> {
    pub(crate) family: u64,
    pub(crate) from_offset: u64,
    pub(crate) limit: u64,
    pub(crate) max_bytes: Option<usize>,
    pub(crate) area: Option<&'a str>,
    pub(crate) resource: Option<&'a str>,
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
    first_global_offset: u64,
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
    first_global_offset: u64,
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
    first_global_offset: u64,
    writer_epoch: u64,
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
}

impl StreamStorageLayout {
    pub fn from_env() -> Self {
        let raw_value = std::env::var("FITZ_STREAM_STORAGE_LAYOUT")
            .unwrap_or_else(|_| "promotion-frontier".to_string())
            .to_lowercase();

        match raw_value.as_str() {
            "promotion-frontier" | "frontier" => Self::PromotionFrontier,
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
    pub fn as_str(&self) -> &'static str {
        match self {
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
    pub first_global_offset: u64,
    pub last_global_offset: u64,
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
    sync_write_options: cntryl_midge::WriteOptions,
    buffered_write_options: cntryl_midge::WriteOptions,
    limits: BatchLimits,
    layout: StreamStorageLayout,
    sessions: Arc<Mutex<HashMap<SessionId, AppendSession>>>,
    ttl: StreamTTL,
    clock: Arc<dyn crate::runtime::clock::Clock>,
    next_session_id: std::sync::atomic::AtomicU64,
    sequencing_guards: Arc<Mutex<HashMap<SequenceGuardKey, SequenceGuard>>>,
    realm_sequence_states: Arc<Mutex<HashMap<RealmSequenceStateKey, RealmSequenceStateHandle>>>,
    resource_meta_states: Arc<Mutex<HashMap<SequenceGuardKey, ResourceMetaStateHandle>>>,
    family_sequence_guards: Arc<Mutex<HashMap<u64, SequenceGuard>>>,
    global_completion_states: Arc<Mutex<HashMap<u64, GlobalCompletionStateHandle>>>,
    recovered_global_families: Arc<Mutex<HashSet<u64>>>,
    pending_global_reservations: PendingGlobalReservations,
    maintenance_queues: Mutex<HashMap<u64, MaintenanceQueueState>>,
    maintenance_metrics: crate::observability::metrics::MetricsCollector,
    #[cfg(test)]
    fail_next_promotion_frontier_commit: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fence_next_global_reservation: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    maintenance_failure_stage: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    maintenance_full_scans: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
struct MaintenanceQueueState {
    initialized: bool,
    buckets: BTreeSet<Vec<u8>>,
    retry_buckets: BTreeSet<Vec<u8>>,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Copy)]
struct ReadCursorState {
    last_resource_offset: u64,
    last_area_offset: Option<u64>,
    last_realm_offset: Option<u64>,
    last_global_offset: Option<u64>,
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
    page_record
        .body
        .len()
        .saturating_add(page_record.metadata.as_ref().map_or(0, Bytes::len))
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
    page_record
        .body
        .len()
        .saturating_add(page_record.metadata.as_ref().map_or(0, Bytes::len))
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
    page_record
        .body
        .len()
        .saturating_add(page_record.metadata.as_ref().map_or(0, Bytes::len))
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
    usize::try_from(limit)
        .unwrap_or(usize::MAX)
        .min(crate::domains::stream::MAX_READ_ITEMS)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn record_is_expired(expires_at: Option<u64>, now_epoch_ms: u64) -> bool {
    expires_at.is_some_and(|deadline| deadline <= now_epoch_ms)
}

const PAYLOAD_LOCATOR_MARKER: [u8; 3] = [0, 0xD4, 0x4C];
const PAYLOAD_BLOB_REF_MARKER: [u8; 3] = [0, 0xD4, 0xB0];
const PAYLOAD_BLOB_VALUE_MARKER: [u8; 3] = [0, 0xD4, 0xB1];
const INLINE_PAYLOAD_LIMIT: usize = 16 * 1024;

fn encode_payload_locator(global_offset: u64, parent_fragment_start: u64) -> Bytes {
    let mut bytes = Vec::with_capacity(19);
    bytes.extend_from_slice(&PAYLOAD_LOCATOR_MARKER);
    bytes.extend_from_slice(&global_offset.to_le_bytes());
    bytes.extend_from_slice(&parent_fragment_start.to_le_bytes());
    Bytes::from(bytes)
}

fn decode_payload_locator(bytes: &[u8]) -> Result<Option<(u64, u64)>, String> {
    if !bytes.starts_with(&PAYLOAD_LOCATOR_MARKER) {
        return Ok(None);
    }
    if bytes.len() != 19 {
        return Err("ERR_STREAM_CORRUPT_LOCATOR: invalid payload locator length".to_string());
    }
    let global_offset = u64::from_le_bytes(
        bytes[3..11]
            .try_into()
            .map_err(|_| "ERR_STREAM_CORRUPT_LOCATOR: invalid global offset".to_string())?,
    );
    let parent_fragment_start = u64::from_le_bytes(
        bytes[11..19]
            .try_into()
            .map_err(|_| "ERR_STREAM_CORRUPT_LOCATOR: invalid parent offset".to_string())?,
    );
    Ok(Some((global_offset, parent_fragment_start)))
}

fn payload_checksum(body: &[u8], metadata: Option<&Bytes>) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update((body.len() as u64).to_le_bytes());
    digest.update(body);
    match metadata {
        Some(metadata) => {
            digest.update([1]);
            digest.update((metadata.len() as u64).to_le_bytes());
            digest.update(metadata);
        }
        None => digest.update([0]),
    }
    digest.finalize().into()
}

fn encode_payload_blob_ref(global_offset: u64, checksum: [u8; 32]) -> Bytes {
    let mut bytes = Vec::with_capacity(43);
    bytes.extend_from_slice(&PAYLOAD_BLOB_REF_MARKER);
    bytes.extend_from_slice(&global_offset.to_le_bytes());
    bytes.extend_from_slice(&checksum);
    Bytes::from(bytes)
}

fn decode_payload_blob_ref(bytes: &[u8]) -> Result<Option<(u64, [u8; 32])>, String> {
    if !bytes.starts_with(&PAYLOAD_BLOB_REF_MARKER) {
        return Ok(None);
    }
    if bytes.len() != 43 {
        return Err("ERR_STREAM_CORRUPT_BLOB: invalid blob reference length".to_string());
    }
    let global_offset = u64::from_le_bytes(
        bytes[3..11]
            .try_into()
            .map_err(|_| "ERR_STREAM_CORRUPT_BLOB: invalid blob offset".to_string())?,
    );
    let checksum = bytes[11..43]
        .try_into()
        .map_err(|_| "ERR_STREAM_CORRUPT_BLOB: invalid blob checksum".to_string())?;
    Ok(Some((global_offset, checksum)))
}

fn encode_payload_blob(
    body: &Bytes,
    metadata: Option<&Bytes>,
    checksum: [u8; 32],
) -> Result<Vec<u8>, String> {
    let body_len = u32::try_from(body.len())
        .map_err(|_| "ERR_STREAM_PAYLOAD_TOO_LARGE: body exceeds blob format".to_string())?;
    let metadata_len = metadata
        .map(|value| {
            u32::try_from(value.len()).map_err(|_| {
                "ERR_STREAM_PAYLOAD_TOO_LARGE: metadata exceeds blob format".to_string()
            })
        })
        .transpose()?
        .unwrap_or(u32::MAX);
    let mut bytes =
        Vec::with_capacity(3 + 32 + 4 + 4 + body.len() + metadata.map_or(0, Bytes::len));
    bytes.extend_from_slice(&PAYLOAD_BLOB_VALUE_MARKER);
    bytes.extend_from_slice(&checksum);
    bytes.extend_from_slice(&body_len.to_le_bytes());
    bytes.extend_from_slice(&metadata_len.to_le_bytes());
    bytes.extend_from_slice(body);
    if let Some(metadata) = metadata {
        bytes.extend_from_slice(metadata);
    }
    Ok(bytes)
}

fn decode_payload_blob(bytes: &[u8]) -> Result<(Bytes, Option<Bytes>, [u8; 32]), String> {
    if bytes.len() < 43 || !bytes.starts_with(&PAYLOAD_BLOB_VALUE_MARKER) {
        return Err("ERR_STREAM_CORRUPT_BLOB: invalid blob header".to_string());
    }
    let checksum: [u8; 32] = bytes[3..35]
        .try_into()
        .map_err(|_| "ERR_STREAM_CORRUPT_BLOB: invalid checksum".to_string())?;
    let body_len = u32::from_le_bytes(bytes[35..39].try_into().unwrap()) as usize;
    let metadata_raw = u32::from_le_bytes(bytes[39..43].try_into().unwrap());
    let metadata_len = (metadata_raw != u32::MAX).then_some(metadata_raw as usize);
    let expected = 43usize
        .checked_add(body_len)
        .and_then(|value| value.checked_add(metadata_len.unwrap_or(0)))
        .ok_or_else(|| "ERR_STREAM_CORRUPT_BLOB: length overflow".to_string())?;
    if bytes.len() != expected {
        return Err("ERR_STREAM_CORRUPT_BLOB: invalid payload length".to_string());
    }
    let body = Bytes::copy_from_slice(&bytes[43..43 + body_len]);
    let metadata = metadata_len
        .map(|length| Bytes::copy_from_slice(&bytes[43 + body_len..43 + body_len + length]));
    if payload_checksum(&body, metadata.as_ref()) != checksum {
        return Err("ERR_STREAM_CORRUPT_BLOB: checksum mismatch".to_string());
    }
    Ok((body, metadata, checksum))
}

impl ReadCursorState {
    fn into_cursor(self, has_more: bool) -> super::protocol::ReadCursor {
        super::protocol::ReadCursor {
            last_resource_offset: self.last_resource_offset,
            last_area_offset: self.last_area_offset,
            last_realm_offset: self.last_realm_offset,
            last_global_offset: self.last_global_offset,
            has_more,
            cursor_fingerprint: None,
            captured_watermark: None,
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
    FFilteredItem: FnMut(u64, &R) -> StreamReadItem,
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
            state.items.push(filtered_item(offset, &record));
            continue;
        }

        if state.items.len() == state.limit {
            *state.has_more = true;
            return Ok(true);
        }

        let record_bytes = record_bytes(&record);
        let next_total_bytes = state.total_bytes.saturating_add(record_bytes);
        if next_total_bytes > state.max_bytes_limit && !state.items.is_empty() {
            *state.has_more = true;
            return Ok(true);
        }

        update_cursor(state.cursor, offset, &record);
        *state.total_bytes = next_total_bytes;
        state.items.push(event_item(offset, record));
    }

    Ok(stop_scan)
}

mod commits_and_sessions;
mod compact_page_writes;
mod config_and_layout;
mod global_recovery;
mod maintenance;
#[cfg(test)]
pub(super) use maintenance::MaintenanceFailureStage;
pub use maintenance::StreamMaintenanceResult;
mod ordered_reads;
mod read_support;
mod reads;
mod sequence_and_filters;
mod watermarks_and_metadata;

#[cfg(test)]
mod tests;
