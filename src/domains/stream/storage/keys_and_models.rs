use bytes::Bytes;
use std::sync::Arc;

use super::super::store::StreamStorageLayout;
use crate::utils::storage_key::{self, DomainKeyspace};

/// Storage key prefixes for stream data
#[derive(Debug, Clone, Copy)]
pub enum KeyPrefix {
    /// Resource stream entry: [RF][realm][area][resource][`resource_offset`]
    Resource = 0x01,
    /// Area index entry: [RF][realm][area][area_offset]
    Area = 0x02,
    /// Realm index entry: [RF][realm][`realm_offset`]
    Realm = 0x03,
    /// Watermark entry: [RF][realm][area]
    Watermark = 0x04,
    /// Staging entry for active sessions: [`session_id`][event_index]
    Staging = 0x05,
    /// Offset counter: [RF][realm][area][resource] - stores next offset independent of TTL
    OffsetCounter = 0x06,
    /// Realm watermark: [RF][realm] - stores realm-level watermark
    RealmWatermark = 0x07,
    /// Resource metadata: [RF][realm][area][resource]
    ResourceMeta = 0x08,
    /// Area offset counter: [RF][realm][area]
    AreaCounter = 0x09,
    /// Realm offset counter: [RF][realm]
    RealmCounter = 0x0A,
    /// Resource discriminator sidecar: [RF][realm][area][resource][`resource_offset`]
    ResourceDiscriminator = 0x0F,
    /// Area discriminator sidecar: [RF][realm][area][area_offset]
    AreaDiscriminator = 0x10,
    /// Realm discriminator sidecar: [RF][realm][`realm_offset`]
    RealmDiscriminator = 0x11,
    /// Family-global allocation head.
    GlobalCounter = 0x12,
    /// Family-global contiguous visibility frontier.
    GlobalWatermark = 0x13,
    /// Family-global discriminator sidecar.
    GlobalDiscriminator = 0x14,
    /// Durable per-family cursor integrity/version state.
    CursorState = 0x16,
    /// Durable generation fencing pre-recovery writers.
    FamilyWriterEpoch = 0x15,
    /// Prototype canonical resource row for storage redesign research: [`stream_id`][resource_offset]
    CanonicalResource = 0x0B,
    /// Prototype area locator row for storage redesign research: [RF][realm][area][area_offset]
    AreaLocator = 0x0C,
    /// Prototype realm locator row for storage redesign research: [RF][realm][`realm_offset`]
    RealmLocator = 0x0D,
    /// Stream storage layout marker for the route family
    LayoutMarker = 0x0E,
    /// Promotion-frontier area page row: [realm][area][`page_start_area_offset`]
    CompactAreaPage = 0xE4,
    /// Promotion-frontier compressed compact realm page row: [realm][page_start_realm_offset]
    CompressedCompactRealmPage = 0xE8,
    /// D4 immutable exact-resource fragment:
    /// [realm][area][resource][64-offset bucket][first resource offset]
    CompactResourcePage = 0xEA,
    /// Immutable family-global commit fragment.
    CompactGlobalPage = 0xEB,
    /// Realm offsets for one resource name across a realm.
    RealmResourcePostingPage = 0xEC,
    /// Global offsets for one area name across all realms.
    GlobalAreaPostingPage = 0xED,
    /// Global offsets for one resource name across all realms and areas.
    GlobalResourcePostingPage = 0xEE,
    /// Global offsets for one area/resource pair across all realms.
    GlobalAreaResourcePostingPage = 0xEF,
    /// D4 immutable large-payload blob, keyed by family-global offset.
    PayloadBlob = 0xF0,
}

pub(super) fn stream_kind_encoder(
    realm: &str,
    kind: KeyPrefix,
    extra_capacity: usize,
) -> lexkey::Encoder {
    storage_key::domain_marker_encoder(realm, DomainKeyspace::Stream, kind as u8, extra_capacity)
}

pub(super) fn stream_kind_key(realm: &str, kind: KeyPrefix, extra_capacity: usize) -> Vec<u8> {
    stream_kind_encoder(realm, kind, extra_capacity).into_vec()
}

const FAMILY_SCOPE_REALM: &str = "";

#[must_use]
pub fn encode_global_counter_key() -> Vec<u8> {
    stream_kind_key(FAMILY_SCOPE_REALM, KeyPrefix::GlobalCounter, 0)
}

#[must_use]
pub fn encode_global_watermark_key() -> Vec<u8> {
    stream_kind_key(FAMILY_SCOPE_REALM, KeyPrefix::GlobalWatermark, 0)
}

#[must_use]
pub fn encode_global_discriminator_key(global_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(FAMILY_SCOPE_REALM, KeyPrefix::GlobalDiscriminator, 8);
    encoder.encode_u64_into(global_offset);
    encoder.into_vec()
}

#[must_use]
pub fn encode_payload_blob_key(global_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(FAMILY_SCOPE_REALM, KeyPrefix::PayloadBlob, 8);
    encoder.encode_u64_into(global_offset);
    encoder.into_vec()
}

#[must_use]
pub fn encode_family_writer_epoch_key() -> Vec<u8> {
    stream_kind_key(FAMILY_SCOPE_REALM, KeyPrefix::FamilyWriterEpoch, 0)
}

#[must_use]
pub fn encode_cursor_state_key() -> Vec<u8> {
    stream_kind_key(FAMILY_SCOPE_REALM, KeyPrefix::CursorState, 0)
}

#[must_use]
pub fn encode_compact_global_page_key(first_global_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(FAMILY_SCOPE_REALM, KeyPrefix::CompactGlobalPage, 24);
    encoder.encode_u64_into(first_global_offset / 64 * 64);
    encoder.encode_u64_into(first_global_offset);
    encoder.encode_u64_into(0);
    encoder.into_vec()
}

fn encode_posting_key(realm: &str, kind: KeyPrefix, segments: &[&str], offset: u64) -> Vec<u8> {
    let capacity = segments
        .iter()
        .map(|segment| segment.len() + 2)
        .sum::<usize>()
        + 24;
    let mut encoder = stream_kind_encoder(realm, kind, capacity);
    for segment in segments {
        storage_key::encode_segment_into(&mut encoder, segment);
    }
    encoder.encode_u64_into(offset / 64 * 64);
    encoder.encode_u64_into(offset);
    encoder.encode_u64_into(0);
    encoder.into_vec()
}

#[must_use]
pub fn encode_realm_resource_posting_key(realm: &str, resource: &str, offset: u64) -> Vec<u8> {
    encode_posting_key(
        realm,
        KeyPrefix::RealmResourcePostingPage,
        &[resource],
        offset,
    )
}

#[must_use]
pub fn encode_global_area_posting_key(area: &str, offset: u64) -> Vec<u8> {
    encode_posting_key(
        FAMILY_SCOPE_REALM,
        KeyPrefix::GlobalAreaPostingPage,
        &[area],
        offset,
    )
}

#[must_use]
pub fn encode_global_resource_posting_key(resource: &str, offset: u64) -> Vec<u8> {
    encode_posting_key(
        FAMILY_SCOPE_REALM,
        KeyPrefix::GlobalResourcePostingPage,
        &[resource],
        offset,
    )
}

#[must_use]
pub fn encode_global_area_resource_posting_key(area: &str, resource: &str, offset: u64) -> Vec<u8> {
    encode_posting_key(
        FAMILY_SCOPE_REALM,
        KeyPrefix::GlobalAreaResourcePostingPage,
        &[area, resource],
        offset,
    )
}

#[must_use]
pub fn stream_key_suffix(key: &[u8]) -> &[u8] {
    storage_key::strip_domain_prefix(key, DomainKeyspace::Stream).unwrap_or(key)
}

pub(super) fn encode_single_u64_value(marker: u8, value: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9);
    bytes.push(marker);
    bytes.extend_from_slice(&value.to_le_bytes());
    bytes
}

pub(super) fn decode_single_u64_value(
    bytes: &[u8],
    marker: u8,
    context: &str,
) -> Result<u64, String> {
    if bytes.len() != 9 {
        return Err(format!("{context}: invalid length"));
    }
    if bytes[0] != marker {
        return Err(format!("{context}: missing marker"));
    }

    let mut value = [0u8; 8];
    value.copy_from_slice(&bytes[1..9]);
    Ok(u64::from_le_bytes(value))
}

pub(super) fn encode_two_u64_value(marker: u8, first: u64, second: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(17);
    bytes.push(marker);
    bytes.extend_from_slice(&first.to_le_bytes());
    bytes.extend_from_slice(&second.to_le_bytes());
    bytes
}

pub(super) fn decode_two_u64_value(
    bytes: &[u8],
    marker: u8,
    context: &str,
) -> Result<(u64, u64), String> {
    if bytes.len() != 17 {
        return Err(format!("{context}: invalid length"));
    }
    if bytes[0] != marker {
        return Err(format!("{context}: missing marker"));
    }

    let mut first = [0u8; 8];
    first.copy_from_slice(&bytes[1..9]);
    let mut second = [0u8; 8];
    second.copy_from_slice(&bytes[9..17]);
    Ok((u64::from_le_bytes(first), u64::from_le_bytes(second)))
}

/// Encodes a resource stream key
#[must_use]
pub fn encode_resource_key(
    realm: &str,
    area: &str,
    resource: &str,
    resource_offset: u64,
) -> Vec<u8> {
    let mut encoder =
        stream_kind_encoder(realm, KeyPrefix::Resource, area.len() + resource.len() + 10);
    storage_key::encode_segment_into(&mut encoder, area);
    storage_key::encode_segment_into(&mut encoder, resource);
    encoder.encode_u64_into(resource_offset);
    encoder.into_vec()
}

/// Encodes an area index key
#[must_use]
pub fn encode_area_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::Area, area.len() + 9);
    storage_key::encode_segment_into(&mut encoder, area);
    encoder.encode_u64_into(area_offset);
    encoder.into_vec()
}

/// Encodes a realm index key
#[must_use]
pub fn encode_realm_key(realm: &str, realm_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::Realm, 9);
    encoder.encode_u64_into(realm_offset);
    encoder.into_vec()
}

/// Decode `area_offset` from area key.
///
/// # Errors
///
/// Returns an error if the key is too short to contain the trailing encoded
/// offset.
pub fn decode_area_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 16 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 16..key.len() - 8];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Decode `realm_offset` from realm key.
///
/// # Errors
///
/// Returns an error if the key is too short to contain the trailing encoded
/// offset.
pub fn decode_realm_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 16 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 16..key.len() - 8];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Encodes a watermark key
#[must_use]
pub fn encode_watermark_key(realm: &str, area: &str) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::Watermark, area.len());
    encoder.encode_string_into(area);
    encoder.into_vec()
}

/// Encodes an offset counter key (metadata, independent of TTL)
#[must_use]
pub fn encode_offset_counter_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(
        realm,
        KeyPrefix::OffsetCounter,
        area.len() + resource.len() + 1,
    );
    storage_key::encode_segment_into(&mut encoder, area);
    encoder.encode_string_into(resource);
    encoder.into_vec()
}

/// Encodes a realm watermark key (metadata, independent of TTL)
#[must_use]
pub fn encode_realm_watermark_key(realm: &str) -> Vec<u8> {
    stream_kind_key(realm, KeyPrefix::RealmWatermark, 0)
}

/// Encodes a resource metadata key.
#[must_use]
pub fn encode_resource_meta_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(
        realm,
        KeyPrefix::ResourceMeta,
        area.len() + resource.len() + 1,
    );
    storage_key::encode_segment_into(&mut encoder, area);
    encoder.encode_string_into(resource);
    encoder.into_vec()
}

/// Encodes an area offset counter key.
#[must_use]
pub fn encode_area_counter_key(realm: &str, area: &str) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::AreaCounter, area.len());
    encoder.encode_string_into(area);
    encoder.into_vec()
}

/// Encodes a realm offset counter key.
#[must_use]
pub fn encode_realm_counter_key(realm: &str) -> Vec<u8> {
    stream_kind_key(realm, KeyPrefix::RealmCounter, 0)
}

/// Encodes a resource discriminator sidecar key.
#[must_use]
pub fn encode_resource_discriminator_key(
    realm: &str,
    area: &str,
    resource: &str,
    resource_offset: u64,
) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(
        realm,
        KeyPrefix::ResourceDiscriminator,
        area.len() + resource.len() + 10,
    );
    storage_key::encode_segment_into(&mut encoder, area);
    storage_key::encode_segment_into(&mut encoder, resource);
    encoder.encode_u64_into(resource_offset);
    encoder.into_vec()
}

/// Encodes an area discriminator sidecar key.
#[must_use]
pub fn encode_area_discriminator_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::AreaDiscriminator, area.len() + 10);
    storage_key::encode_segment_into(&mut encoder, area);
    encoder.encode_u64_into(area_offset);
    encoder.into_vec()
}

/// Encodes a realm discriminator sidecar key.
#[must_use]
pub fn encode_realm_discriminator_key(realm: &str, realm_offset: u64) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::RealmDiscriminator, 9);
    encoder.encode_u64_into(realm_offset);
    encoder.into_vec()
}

/// Encodes a promotion-frontier compact area page key.
#[must_use]
pub fn encode_compact_area_page_key(
    realm: &str,
    area: &str,
    area_page_start_offset: u64,
) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::CompactAreaPage, area.len() + 26);
    storage_key::encode_segment_into(&mut encoder, area);
    encoder.encode_u64_into(area_page_start_offset / 64 * 64);
    encoder.encode_u64_into(area_page_start_offset);
    encoder.encode_u64_into(0);
    encoder.into_vec()
}

/// Encodes a promotion-frontier compressed compact realm page key.
#[must_use]
pub fn encode_compressed_compact_realm_page_key(
    realm: &str,
    page_start_realm_offset: u64,
) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(realm, KeyPrefix::CompressedCompactRealmPage, 24);
    encoder.encode_u64_into(page_start_realm_offset / 64 * 64);
    encoder.encode_u64_into(page_start_realm_offset);
    encoder.encode_u64_into(0);
    encoder.into_vec()
}

/// Encodes a promotion-frontier compact resource mini-page key.
#[must_use]
pub fn encode_compact_resource_page_key(
    realm: &str,
    area: &str,
    resource: &str,
    page_start_resource_offset: u64,
) -> Vec<u8> {
    encode_compact_resource_fragment_key(realm, area, resource, page_start_resource_offset, 0)
}

#[must_use]
pub(crate) fn encode_compact_resource_fragment_key(
    realm: &str,
    area: &str,
    resource: &str,
    first_resource_offset: u64,
    generation: u64,
) -> Vec<u8> {
    let mut encoder = stream_kind_encoder(
        realm,
        KeyPrefix::CompactResourcePage,
        area.len() + resource.len() + 26,
    );
    storage_key::encode_segment_into(&mut encoder, area);
    storage_key::encode_segment_into(&mut encoder, resource);
    encoder.encode_u64_into(first_resource_offset / 64 * 64);
    encoder.encode_u64_into(first_resource_offset);
    encoder.encode_u64_into(generation);
    encoder.into_vec()
}

/// Decode `resource_offset` from compact resource page key.
///
/// # Errors
///
/// Returns an error if the key is too short to contain the trailing encoded
/// offset.
pub fn decode_resource_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 16 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 16..key.len() - 8];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Encodes the per-family stream storage layout marker key.
#[must_use]
pub fn encode_stream_layout_marker_key() -> Vec<u8> {
    vec![KeyPrefix::LayoutMarker as u8]
}

/// Value stored in resource index (full record)
///
/// `area_offset` and `realm_offset` are always written as `Some` at commit time.
/// They remain options in memory so the record shape stays explicit at the
/// storage boundary.
#[derive(Debug, Clone)]
pub struct ResourceValue {
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
    /// Area offset — always `Some` when written at commit time.
    pub area_offset: Option<u64>,
    /// Realm offset — always `Some` when written at commit time.
    pub realm_offset: Option<u64>,
}

/// Value stored in area index (covering index with full event)
#[derive(Debug, Clone)]
pub struct AreaValue {
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

/// Value stored in realm index rows, carrying the full event inline
#[derive(Debug, Clone)]
pub struct RealmValue {
    pub area_offset: u64,
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct CompactRealmPageRecord {
    /// Concrete area identity.
    pub area: Arc<str>,
    /// Concrete resource identity.
    pub resource: Arc<str>,
    pub area_offset: u64,
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
    /// Absolute Unix epoch deadline in milliseconds.
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CompactRealmPageValue {
    pub records: Vec<CompactRealmPageRecord>,
}

/// Promotion-frontier area wildcard page. Bodies stay local to the area plane.
#[derive(Debug, Clone)]
pub struct CompactAreaPageRecord {
    /// Concrete resource identity.
    pub resource: Arc<str>,
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
    /// Absolute Unix epoch deadline in milliseconds.
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CompactAreaPageValue {
    pub records: Vec<CompactAreaPageRecord>,
}

/// Promotion-frontier exact-resource mini-page. Exact replay keeps direct locality.
#[derive(Debug, Clone)]
pub struct CompactResourcePageRecord {
    pub area_offset: u64,
    pub realm_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
    /// Absolute Unix epoch deadline in milliseconds.
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CompactResourcePageValue {
    pub records: Vec<CompactResourcePageRecord>,
}

/// Promotion-frontier compressed realm page. Compression is storage-only and
/// must not change the replay semantics represented by the underlying page.
#[derive(Debug, Clone)]
pub struct CompressedCompactRealmPageValue {
    pub records: Vec<CompactRealmPageRecord>,
}

#[derive(Debug, Clone)]
pub struct CompactGlobalPageRecord {
    pub realm: Arc<str>,
    pub area: Arc<str>,
    pub resource: Arc<str>,
    pub resource_offset: u64,
    pub area_offset: u64,
    pub realm_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
    /// Absolute Unix epoch deadline in milliseconds.
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CompactGlobalPageValue {
    pub records: Vec<CompactGlobalPageRecord>,
}

/// Sparse immutable fragment referencing offsets in a parent ordering scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingPageValue {
    pub entries: Vec<PostingEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingEntry {
    pub offset: u64,
    pub parent_fragment_start: u64,
    /// Absolute Unix epoch deadline in milliseconds.
    pub expires_at: Option<u64>,
}

pub(super) const AREA_VALUE_V2_MARKER: [u8; 2] = [0, 0xA1];
pub(super) const REALM_VALUE_V2_MARKER: [u8; 2] = [0, 0xB1];
pub(super) const COMPACT_REALM_PAGE_VALUE_V2_MARKER: [u8; 2] = [0, 0xB3];
pub(super) const RESOURCE_VALUE_V2_MARKER: [u8; 2] = [0, 0x91];
/// D4 is an intentional clean on-disk break. The public layout selection name
/// remains stable, but the marker and layout id do not accept D3 stores.
pub(super) const STREAM_LAYOUT_MARKER_VALUE_V2_MARKER: [u8; 2] = [0, 0xD4];
pub(super) const COMPACT_AREA_PAGE_VALUE_V2_MARKER: [u8; 2] = [0, 0xE5];
pub(super) const COMPRESSED_COMPACT_REALM_PAGE_VALUE_V2_MARKER: [u8; 2] = [0, 0xE9];
pub(super) const COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xEA];
pub(super) const COMPACT_GLOBAL_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xEB];
pub(super) const COMPACT_GLOBAL_PAGE_VALUE_V2_MARKER: [u8; 2] = [0, 0xED];
pub const GLOBAL_PAGE_RECORD_LIMIT: u64 = 64;
pub(super) const OPTIONAL_BYTES_ABSENT: u32 = u32::MAX;
pub(super) const OPTIONAL_OFFSET_ABSENT: u64 = u64::MAX;

pub const REALM_PAGE_RECORD_LIMIT: usize = 64;

/// Watermark value
#[derive(Debug, Clone)]
pub struct WatermarkValue {
    pub watermark: u64,
}

/// Offset counter value (metadata, not subject to TTL)
#[derive(Debug, Clone)]
pub struct OffsetCounterValue {
    pub next_offset: u64,
}

/// Durable metadata for a resource stream.
#[derive(Debug, Clone)]
pub struct ResourceMetaValue {
    pub next_offset: u64,
    pub committed_size_bytes: u64,
}

/// Durable next area offset counter.
#[derive(Debug, Clone)]
pub struct AreaCounterValue {
    pub next_offset: u64,
}

/// Durable next realm offset counter.
#[derive(Debug, Clone)]
pub struct RealmCounterValue {
    pub next_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamLayoutMarkerValue {
    pub layout: StreamStorageLayout,
}
