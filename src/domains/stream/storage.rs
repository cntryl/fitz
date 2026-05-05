use bincode::{deserialize, serialize};
use bytes::Bytes;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use serde::{Deserialize, Serialize};

use super::store::{EventPayload, StreamStorageLayout};

/// Storage key prefixes for stream data
#[derive(Debug, Clone, Copy)]
pub enum KeyPrefix {
    /// Resource stream entry: [RF][realm][area][resource][resource_offset]
    Resource = 0x01,
    /// Area index entry: [RF][realm][area][area_offset]
    Area = 0x02,
    /// Realm index entry: [RF][realm][realm_offset]
    Realm = 0x03,
    /// Watermark entry: [RF][realm][area]
    Watermark = 0x04,
    /// Staging entry for active sessions: [session_id][event_index]
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
    /// Resource discriminator sidecar: [RF][realm][area][resource][resource_offset]
    ResourceDiscriminator = 0x0F,
    /// Area discriminator sidecar: [RF][realm][area][area_offset]
    AreaDiscriminator = 0x10,
    /// Realm discriminator sidecar: [RF][realm][realm_offset]
    RealmDiscriminator = 0x11,
    /// Prototype canonical resource row for storage redesign research: [stream_id][resource_offset]
    CanonicalResource = 0x0B,
    /// Prototype area locator row for storage redesign research: [RF][realm][area][area_offset]
    AreaLocator = 0x0C,
    /// Prototype realm locator row for storage redesign research: [RF][realm][realm_offset]
    RealmLocator = 0x0D,
    /// Stream storage layout marker for the route family
    LayoutMarker = 0x0E,
    /// Promotion-frontier area page row: [realm][area][page_start_area_offset]
    CompactAreaPage = 0xE4,
    /// Promotion-frontier compressed compact realm page row: [realm][page_start_realm_offset]
    CompressedCompactRealmPage = 0xE8,
    /// Promotion-frontier exact-resource mini-page row: [realm][area][resource][page_start_resource_offset]
    CompactResourcePage = 0xEA,
}

/// Encodes a resource stream key
pub fn encode_resource_key(
    realm: &str,
    area: &str,
    resource: &str,
    resource_offset: u64,
) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Resource as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0); // separator
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key.push(0);
    key.extend_from_slice(&resource_offset.to_be_bytes());
    key
}

/// Encodes an area index key
pub fn encode_area_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Area as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_offset.to_be_bytes());
    key
}

/// Encodes a realm index key
pub fn encode_realm_key(realm: &str, realm_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Realm as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&realm_offset.to_be_bytes());
    key
}

/// Decode area_offset from area key
pub fn decode_area_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 8..];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Decode realm_offset from realm key
pub fn decode_realm_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 8..];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Encodes a watermark key
pub fn encode_watermark_key(realm: &str, area: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Watermark as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key
}

/// Encodes an offset counter key (metadata, independent of TTL)
pub fn encode_offset_counter_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::OffsetCounter as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key
}

/// Encodes a realm watermark key (metadata, independent of TTL)
pub fn encode_realm_watermark_key(realm: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::RealmWatermark as u8];
    key.extend_from_slice(realm.as_bytes());
    key
}

/// Encodes a resource metadata key.
pub fn encode_resource_meta_key(realm: &str, area: &str, resource: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::ResourceMeta as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key
}

/// Encodes an area offset counter key.
pub fn encode_area_counter_key(realm: &str, area: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::AreaCounter as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key
}

/// Encodes a realm offset counter key.
pub fn encode_realm_counter_key(realm: &str) -> Vec<u8> {
    let mut key = vec![KeyPrefix::RealmCounter as u8];
    key.extend_from_slice(realm.as_bytes());
    key
}

/// Encodes a resource discriminator sidecar key.
pub fn encode_resource_discriminator_key(
    realm: &str,
    area: &str,
    resource: &str,
    resource_offset: u64,
) -> Vec<u8> {
    let mut key = vec![KeyPrefix::ResourceDiscriminator as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key.push(0);
    key.extend_from_slice(&resource_offset.to_be_bytes());
    key
}

/// Encodes an area discriminator sidecar key.
pub fn encode_area_discriminator_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::AreaDiscriminator as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_offset.to_be_bytes());
    key
}

/// Encodes a realm discriminator sidecar key.
pub fn encode_realm_discriminator_key(realm: &str, realm_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::RealmDiscriminator as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&realm_offset.to_be_bytes());
    key
}

/// Encodes a prototype canonical resource key.
pub fn encode_canonical_resource_key(stream_id: u64, resource_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::CanonicalResource as u8];
    key.extend_from_slice(&stream_id.to_be_bytes());
    key.extend_from_slice(&resource_offset.to_be_bytes());
    key
}

/// Encodes a prototype area locator key.
pub fn encode_area_locator_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::AreaLocator as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_offset.to_be_bytes());
    key
}

/// Encodes a prototype realm locator key.
pub fn encode_realm_locator_key(realm: &str, realm_offset: u64) -> Vec<u8> {
    let mut key = vec![KeyPrefix::RealmLocator as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&realm_offset.to_be_bytes());
    key
}

/// Encodes a promotion-frontier compact area page key.
pub fn encode_compact_area_page_key(
    realm: &str,
    area: &str,
    area_page_start_offset: u64,
) -> Vec<u8> {
    let mut key = vec![KeyPrefix::CompactAreaPage as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_page_start_offset.to_be_bytes());
    key
}

/// Encodes a promotion-frontier compressed compact realm page key.
pub fn encode_compressed_compact_realm_page_key(
    realm: &str,
    page_start_realm_offset: u64,
) -> Vec<u8> {
    let mut key = vec![KeyPrefix::CompressedCompactRealmPage as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

/// Encodes a promotion-frontier compact resource mini-page key.
pub fn encode_compact_resource_page_key(
    realm: &str,
    area: &str,
    resource: &str,
    page_start_resource_offset: u64,
) -> Vec<u8> {
    let mut key = vec![KeyPrefix::CompactResourcePage as u8];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_resource_offset.to_be_bytes());
    key
}

/// Decode resource_offset from compact resource page key.
pub fn decode_resource_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }
    let offset_bytes = &key[key.len() - 8..];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

/// Encodes the per-family stream storage layout marker key.
pub fn encode_stream_layout_marker_key() -> Vec<u8> {
    vec![KeyPrefix::LayoutMarker as u8]
}

/// Value stored in resource index (full record)
///
/// `area_offset` and `realm_offset` are always written as `Some` at commit time.
/// They are typed as `Option<u64>` solely for bincode format compatibility with
/// existing on-disk data. Changing these to `u64` would be a breaking storage
/// migration and must not be done without a migration plan.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyResourceValue {
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
    area_offset: Option<u64>,
    realm_offset: Option<u64>,
}

/// Value stored in area index (covering index with full event)
#[derive(Debug, Clone)]
pub struct AreaValue {
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

/// Value stored in legacy realm index rows (covering index with full event)
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
    pub area_offset: u64,
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

#[derive(Debug, Clone)]
pub struct CompactRealmPageValue {
    pub records: Vec<CompactRealmPageRecord>,
}

/// Promotion-frontier area wildcard page. Bodies stay local to the area plane.
#[derive(Debug, Clone)]
pub struct CompactAreaPageRecord {
    pub resource_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyAreaValue {
    realm: String,
    area: String,
    resource: String,
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyRealmValue {
    realm: String,
    area: String,
    area_offset: u64,
    resource: String,
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

const AREA_VALUE_V2_MARKER: [u8; 2] = [0, 0xA1];
const REALM_VALUE_V2_MARKER: [u8; 2] = [0, 0xB1];
const COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xB2];
const RESOURCE_VALUE_V2_MARKER: [u8; 2] = [0, 0x91];
const CANONICAL_RESOURCE_VALUE_V1_MARKER: [u8; 2] = [0, 0xC1];
const AREA_LOCATOR_VALUE_V1_MARKER: [u8; 2] = [0, 0xC2];
const REALM_LOCATOR_VALUE_V1_MARKER: [u8; 2] = [0, 0xC3];
const STREAM_LAYOUT_MARKER_VALUE_V1_MARKER: [u8; 2] = [0, 0xD1];
const COMPACT_AREA_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE4];
const COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE8];
const COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xEA];
const OPTIONAL_BYTES_ABSENT: u32 = u32::MAX;
const OPTIONAL_OFFSET_ABSENT: u64 = u64::MAX;

pub const REALM_PAGE_RECORD_LIMIT: usize = 64;

/// Watermark value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkValue {
    pub watermark: u64,
}

/// Offset counter value (metadata, not subject to TTL)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCounterValue {
    pub next_offset: u64,
}

/// Durable metadata for a resource stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetaValue {
    pub next_offset: u64,
    pub committed_size_bytes: u64,
}

/// Durable next area offset counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AreaCounterValue {
    pub next_offset: u64,
}

/// Durable next realm offset counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmCounterValue {
    pub next_offset: u64,
}

/// Prototype canonical body row used to benchmark canonical-body plus locator layouts.
#[derive(Debug, Clone)]
pub struct CanonicalResourceValue {
    pub area_offset: u64,
    pub realm_offset: u64,
    pub body: Bytes,
    pub metadata: Option<Bytes>,
    pub created_at: u64,
}

/// Prototype area locator row used to benchmark batched wildcard hydration.
#[derive(Debug, Clone)]
pub struct AreaLocatorValue {
    pub stream_id: u64,
    pub resource_offset: u64,
}

/// Prototype realm locator row used to benchmark batched wildcard hydration.
#[derive(Debug, Clone)]
pub struct RealmLocatorValue {
    pub area_offset: u64,
    pub stream_id: u64,
    pub resource_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamLayoutMarkerValue {
    pub layout: StreamStorageLayout,
}

impl StreamLayoutMarkerValue {
    pub fn new(layout: StreamStorageLayout) -> Self {
        Self { layout }
    }

    pub fn encode(&self) -> Vec<u8> {
        vec![
            STREAM_LAYOUT_MARKER_VALUE_V1_MARKER[0],
            STREAM_LAYOUT_MARKER_VALUE_V1_MARKER[1],
            match self.layout {
                StreamStorageLayout::LegacyCovering => 0,
                StreamStorageLayout::PromotionFrontier => 1,
            },
        ]
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 3 || !bytes.starts_with(&STREAM_LAYOUT_MARKER_VALUE_V1_MARKER) {
            return Err("decode stream layout marker: invalid encoding".to_string());
        }

        let layout = match bytes[2] {
            0 => StreamStorageLayout::LegacyCovering,
            1 => StreamStorageLayout::PromotionFrontier,
            other => {
                return Err(format!(
                    "decode stream layout marker: unknown layout id {}",
                    other
                ))
            }
        };

        Ok(Self { layout })
    }
}

impl ResourceValue {
    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let mut buf = Vec::with_capacity(42 + self.body.len() + metadata_len);
        buf.extend_from_slice(&RESOURCE_VALUE_V2_MARKER);
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(
            &self
                .area_offset
                .unwrap_or(OPTIONAL_OFFSET_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(
            &self
                .realm_offset
                .unwrap_or(OPTIONAL_OFFSET_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map(|m| m.len() as u32)
                .unwrap_or(OPTIONAL_BYTES_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Self {
        if bytes.starts_with(&RESOURCE_VALUE_V2_MARKER) {
            return Self::decode_v2(bytes).expect("deserialize compact resource value");
        }

        let legacy: LegacyResourceValue =
            deserialize(bytes).expect("deserialize legacy resource value");
        Self {
            resource_offset: legacy.resource_offset,
            body: legacy.body,
            metadata: legacy.metadata,
            created_at: legacy.created_at,
            area_offset: legacy.area_offset,
            realm_offset: legacy.realm_offset,
        }
    }

    fn decode_v2(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 42 {
            return Err("decode resource value: header too short".to_string());
        }

        let resource_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let area_offset_raw = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
        let realm_offset_raw = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[34..38].try_into().unwrap()) as usize;
        let metadata_len_raw = u32::from_le_bytes(bytes[38..42].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(metadata_len_raw as usize)
        };

        let mut offset = 42;
        if bytes.len() < offset + body_len {
            return Err("decode resource value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len() < offset + metadata_len {
                return Err("decode resource value: truncated metadata".to_string());
            }
            Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ))
        } else {
            None
        };

        Ok(Self {
            resource_offset,
            body,
            metadata,
            created_at,
            area_offset: (area_offset_raw != OPTIONAL_OFFSET_ABSENT).then_some(area_offset_raw),
            realm_offset: (realm_offset_raw != OPTIONAL_OFFSET_ABSENT).then_some(realm_offset_raw),
        })
    }
}

impl AreaValue {
    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let mut buf = Vec::with_capacity(26 + self.body.len() + metadata_len);
        buf.extend_from_slice(&AREA_VALUE_V2_MARKER);
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map(|m| m.len() as u32)
                .unwrap_or(OPTIONAL_BYTES_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Self {
        if bytes.starts_with(&AREA_VALUE_V2_MARKER) {
            return Self::decode_v2(bytes).expect("deserialize compact area value");
        }

        let legacy: LegacyAreaValue = deserialize(bytes).expect("deserialize legacy area value");
        Self {
            resource_offset: legacy.resource_offset,
            body: legacy.body,
            metadata: legacy.metadata,
            created_at: legacy.created_at,
        }
    }

    fn decode_v2(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 26 {
            return Err("decode area value: header too short".to_string());
        }

        let resource_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[18..22].try_into().unwrap()) as usize;
        let metadata_len_raw = u32::from_le_bytes(bytes[22..26].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(metadata_len_raw as usize)
        };

        let mut offset = 26;
        if bytes.len() < offset + body_len {
            return Err("decode area value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len() < offset + metadata_len {
                return Err("decode area value: truncated metadata".to_string());
            }
            Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ))
        } else {
            None
        };

        Ok(Self {
            resource_offset,
            body,
            metadata,
            created_at,
        })
    }
}

impl RealmValue {
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.starts_with(&REALM_VALUE_V2_MARKER) {
            return Self::decode_v2(bytes);
        }

        let legacy: LegacyRealmValue = deserialize(bytes)
            .map_err(|error| format!("deserialize legacy realm value: {error}"))?;
        Ok(Self {
            area_offset: legacy.area_offset,
            resource_offset: legacy.resource_offset,
            body: legacy.body,
            metadata: legacy.metadata,
            created_at: legacy.created_at,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let mut buf = Vec::with_capacity(34 + self.body.len() + metadata_len);
        buf.extend_from_slice(&REALM_VALUE_V2_MARKER);
        buf.extend_from_slice(&self.area_offset.to_le_bytes());
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map(|m| m.len() as u32)
                .unwrap_or(OPTIONAL_BYTES_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize realm value")
    }

    fn decode_v2(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 34 {
            return Err("decode realm value: header too short".to_string());
        }

        let area_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let resource_offset = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[26..30].try_into().unwrap()) as usize;
        let metadata_len_raw = u32::from_le_bytes(bytes[30..34].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(metadata_len_raw as usize)
        };

        let mut offset = 34;
        if bytes.len() < offset + body_len {
            return Err("decode realm value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len() < offset + metadata_len {
                return Err("decode realm value: truncated metadata".to_string());
            }
            Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ))
        } else {
            None
        };

        Ok(Self {
            area_offset,
            resource_offset,
            body,
            metadata,
            created_at,
        })
    }
}

impl CompactRealmPageValue {
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_REALM_PAGE_VALUE_V1_MARKER)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record
                .metadata
                .as_ref()
                .map(|metadata| metadata.len())
                .unwrap_or(0);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_REALM_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&(record.body.len() as u32).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.len() as u32)
                    .unwrap_or(OPTIONAL_BYTES_ABSENT)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compact realm page value")
    }

    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compact realm page value: missing marker".to_string());
        }
        if bytes.len() < 6 {
            return Err("decode compact realm page value: header too short".to_string());
        }

        let mut offset = 2usize;
        let record_count =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            if bytes.len() < offset + 32 {
                return Err("decode compact realm page value: record header truncated".to_string());
            }

            let area_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let resource_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let created_at = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let body_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let metadata_len_raw =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(metadata_len_raw as usize)
            };

            if bytes.len() < offset + body_len {
                return Err("decode compact realm page value: truncated body".to_string());
            }
            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;

            let metadata = if let Some(metadata_len) = metadata_len {
                if bytes.len() < offset + metadata_len {
                    return Err("decode compact realm page value: truncated metadata".to_string());
                }
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(CompactRealmPageRecord {
                area_offset,
                resource_offset,
                body,
                metadata,
                created_at,
            });
        }

        Ok(Self { records })
    }
}

impl CompactAreaPageValue {
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_AREA_PAGE_VALUE_V1_MARKER)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 4 + 4 + record.body.len();
            total_len += record
                .metadata
                .as_ref()
                .map(|metadata| metadata.len())
                .unwrap_or(0);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_AREA_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&(record.body.len() as u32).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.len() as u32)
                    .unwrap_or(OPTIONAL_BYTES_ABSENT)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compact area page value")
    }

    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compact area page value: missing marker".to_string());
        }
        if bytes.len() < 6 {
            return Err("decode compact area page value: header too short".to_string());
        }

        let mut offset = 2usize;
        let record_count =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let resource_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let created_at = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let body_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let metadata_len_raw =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(metadata_len_raw as usize)
            };

            if bytes.len() < offset + body_len {
                return Err("decode compact area page value: truncated body".to_string());
            }
            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;

            let metadata = if let Some(metadata_len) = metadata_len {
                if bytes.len() < offset + metadata_len {
                    return Err("decode compact area page value: truncated metadata".to_string());
                }
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(CompactAreaPageRecord {
                resource_offset,
                body,
                metadata,
                created_at,
            });
        }

        Ok(Self { records })
    }
}

impl CompactResourcePageValue {
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record
                .metadata
                .as_ref()
                .map(|metadata| metadata.len())
                .unwrap_or(0);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.realm_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&(record.body.len() as u32).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.len() as u32)
                    .unwrap_or(OPTIONAL_BYTES_ABSENT)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compact resource page value")
    }

    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compact resource page value: missing marker".to_string());
        }
        if bytes.len() < 6 {
            return Err("decode compact resource page value: header too short".to_string());
        }

        let mut offset = 2usize;
        let record_count =
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let area_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let realm_offset = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let created_at = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let body_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let metadata_len_raw =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(metadata_len_raw as usize)
            };

            if bytes.len() < offset + body_len {
                return Err("decode compact resource page value: truncated body".to_string());
            }
            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;

            let metadata = if let Some(metadata_len) = metadata_len {
                if bytes.len() < offset + metadata_len {
                    return Err(
                        "decode compact resource page value: truncated metadata".to_string()
                    );
                }
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(CompactResourcePageRecord {
                area_offset,
                realm_offset,
                body,
                metadata,
                created_at,
            });
        }

        Ok(Self { records })
    }
}

impl CompressedCompactRealmPageValue {
    pub fn is_encoded(bytes: &[u8]) -> bool {
        bytes.starts_with(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER)
    }

    pub fn encode(&self) -> Vec<u8> {
        let compact_page = CompactRealmPageValue {
            records: self.records.clone(),
        };
        let compressed_payload = compress_prepend_size(&compact_page.encode());

        let mut bytes = Vec::with_capacity(2 + compressed_payload.len());
        bytes.extend_from_slice(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&compressed_payload);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("deserialize compressed compact realm page value")
    }

    pub fn into_compact_realm_page(self) -> CompactRealmPageValue {
        CompactRealmPageValue {
            records: self.records,
        }
    }

    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        if !Self::is_encoded(bytes) {
            return Err("decode compressed compact realm page value: missing marker".to_string());
        }
        if bytes.len() <= 2 {
            return Err("decode compressed compact realm page value: payload missing".to_string());
        }

        let decompressed = decompress_size_prepended(&bytes[2..]).map_err(|error| {
            format!("decode compressed compact realm page value: decompress failed: {error}")
        })?;
        let page = CompactRealmPageValue::try_decode(&decompressed)?;

        Ok(Self {
            records: page.records,
        })
    }
}

impl WatermarkValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize watermark value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize watermark value")
    }
}

impl OffsetCounterValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize offset counter value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize offset counter value")
    }
}

impl ResourceMetaValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize resource metadata value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize resource metadata value")
    }
}

impl AreaCounterValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize area counter value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize area counter value")
    }
}

impl RealmCounterValue {
    pub fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize realm counter value")
    }

    pub fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize realm counter value")
    }
}

impl CanonicalResourceValue {
    pub fn encode(&self) -> Vec<u8> {
        let metadata_len = self.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let mut buf = Vec::with_capacity(34 + self.body.len() + metadata_len);
        buf.extend_from_slice(&CANONICAL_RESOURCE_VALUE_V1_MARKER);
        buf.extend_from_slice(&self.area_offset.to_le_bytes());
        buf.extend_from_slice(&self.realm_offset.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());
        buf.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        buf.extend_from_slice(
            &self
                .metadata
                .as_ref()
                .map(|m| m.len() as u32)
                .unwrap_or(OPTIONAL_BYTES_ABSENT)
                .to_le_bytes(),
        );
        buf.extend_from_slice(&self.body);
        if let Some(metadata) = &self.metadata {
            buf.extend_from_slice(metadata);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::decode_v1(bytes).expect("deserialize canonical resource value")
    }

    fn decode_v1(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&CANONICAL_RESOURCE_VALUE_V1_MARKER) {
            return Err("decode canonical resource value: missing marker".to_string());
        }
        if bytes.len() < 34 {
            return Err("decode canonical resource value: header too short".to_string());
        }

        let area_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let realm_offset = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let created_at = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[26..30].try_into().unwrap()) as usize;
        let metadata_len_raw = u32::from_le_bytes(bytes[30..34].try_into().unwrap());
        let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
            None
        } else {
            Some(metadata_len_raw as usize)
        };

        let mut offset = 34;
        if bytes.len() < offset + body_len {
            return Err("decode canonical resource value: truncated body".to_string());
        }
        let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
        offset += body_len;

        let metadata = if let Some(metadata_len) = metadata_len {
            if bytes.len() < offset + metadata_len {
                return Err("decode canonical resource value: truncated metadata".to_string());
            }
            Some(Bytes::copy_from_slice(
                &bytes[offset..offset + metadata_len],
            ))
        } else {
            None
        };

        Ok(Self {
            area_offset,
            realm_offset,
            body,
            metadata,
            created_at,
        })
    }
}

impl AreaLocatorValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(18);
        buf.extend_from_slice(&AREA_LOCATOR_VALUE_V1_MARKER);
        buf.extend_from_slice(&self.stream_id.to_le_bytes());
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::decode_v1(bytes).expect("deserialize area locator value")
    }

    fn decode_v1(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&AREA_LOCATOR_VALUE_V1_MARKER) {
            return Err("decode area locator value: missing marker".to_string());
        }
        if bytes.len() < 18 {
            return Err("decode area locator value: header too short".to_string());
        }

        Ok(Self {
            stream_id: u64::from_le_bytes(bytes[2..10].try_into().unwrap()),
            resource_offset: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
        })
    }
}

impl RealmLocatorValue {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(26);
        buf.extend_from_slice(&REALM_LOCATOR_VALUE_V1_MARKER);
        buf.extend_from_slice(&self.area_offset.to_le_bytes());
        buf.extend_from_slice(&self.stream_id.to_le_bytes());
        buf.extend_from_slice(&self.resource_offset.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Self {
        Self::decode_v1(bytes).expect("deserialize realm locator value")
    }

    fn decode_v1(bytes: &[u8]) -> Result<Self, String> {
        if !bytes.starts_with(&REALM_LOCATOR_VALUE_V1_MARKER) {
            return Err("decode realm locator value: missing marker".to_string());
        }
        if bytes.len() < 26 {
            return Err("decode realm locator value: header too short".to_string());
        }

        Ok(Self {
            area_offset: u64::from_le_bytes(bytes[2..10].try_into().unwrap()),
            stream_id: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
            resource_offset: u64::from_le_bytes(bytes[18..26].try_into().unwrap()),
        })
    }
}

/// Create an in-memory Midge database for tests
#[cfg(test)]
pub fn create_test_db() -> std::sync::Arc<cntryl_midge::Engine> {
    use cntryl_midge::testkit::MidgeOptions;
    use std::sync::Arc;
    Arc::new(
        cntryl_midge::Engine::open_with_options(MidgeOptions::default())
            .expect("create in-memory db"),
    )
}

/// Staging key/value encoding for Transaction
pub fn encode_staging_key(session_id: u64, event_index: usize) -> Vec<u8> {
    let mut key = vec![KeyPrefix::Staging as u8];
    key.extend_from_slice(&session_id.to_be_bytes());
    key.push(0);
    key.extend_from_slice(&(event_index as u64).to_be_bytes());
    key
}

pub fn encode_staging_value(event: &EventPayload) -> Vec<u8> {
    let metadata_len = event.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
    let mut buf = Vec::with_capacity(8 + event.body.len() + metadata_len);
    buf.extend_from_slice(&(event.body.len() as u32).to_le_bytes());
    buf.extend_from_slice(
        &event
            .metadata
            .as_ref()
            .map(|m| m.len() as u32)
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    buf.extend_from_slice(&event.body);
    if let Some(metadata) = &event.metadata {
        buf.extend_from_slice(metadata);
    }
    buf
}

pub fn decode_staging_value(data: &[u8]) -> Result<EventPayload, String> {
    if data.len() < 8 {
        return Err("decode_staging_value: header too short".to_string());
    }

    let body_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let metadata_len_raw = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let metadata_len = if metadata_len_raw == u32::MAX {
        None
    } else {
        Some(metadata_len_raw as usize)
    };

    let mut offset = 8;
    if data.len() < offset + body_len {
        return Err("decode_staging_value: truncated body".to_string());
    }
    let body = data[offset..offset + body_len].to_vec();
    offset += body_len;

    let metadata = if let Some(metadata_len) = metadata_len {
        if data.len() < offset + metadata_len {
            return Err("decode_staging_value: truncated metadata".to_string());
        }
        Some(data[offset..offset + metadata_len].to_vec())
    } else {
        None
    };

    Ok(EventPayload {
        body: Bytes::from(body),
        metadata: metadata.map(Bytes::from),
        discriminator: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_resource_key_with_proper_ordering() {
        // Arrange
        let key1 = encode_resource_key("realm", "area", "res1", 0);
        let key2 = encode_resource_key("realm", "area", "res1", 1);
        let key3 = encode_resource_key("realm", "area", "res1", 10);

        // Act
        // (keys are already encoded)

        // Assert
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn should_encode_area_key_with_proper_ordering() {
        // Arrange
        let key1 = encode_area_key("realm", "area", 0);
        let key2 = encode_area_key("realm", "area", 1);
        let key3 = encode_area_key("realm", "area", 100);

        // Act
        // (keys are already encoded)

        // Assert
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn should_isolate_keys_by_prefix() {
        // Arrange
        let resource_key = encode_resource_key("realm", "area", "res", 0);
        let area_key = encode_area_key("realm", "area", 0);
        let realm_key = encode_realm_key("realm", 0);

        // Act
        // (keys are already encoded)

        // Assert
        assert_eq!(resource_key[0], KeyPrefix::Resource as u8);
        assert_eq!(area_key[0], KeyPrefix::Area as u8);
        assert_eq!(realm_key[0], KeyPrefix::Realm as u8);
    }

    #[test]
    fn should_encode_compact_area_page_key_with_proper_ordering() {
        // Arrange
        let key1 = encode_compact_area_page_key("realm", "area", 0);
        let key2 = encode_compact_area_page_key("realm", "area", 1);
        let key3 = encode_compact_area_page_key("realm", "area", 10);

        // Act
        // (keys are already encoded)

        // Assert
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn should_encode_compact_resource_page_key_with_proper_ordering() {
        // Arrange
        let key1 = encode_compact_resource_page_key("realm", "area", "resource", 0);
        let key2 = encode_compact_resource_page_key("realm", "area", "resource", 1);
        let key3 = encode_compact_resource_page_key("realm", "area", "resource", 10);

        // Act
        // (keys are already encoded)

        // Assert
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn should_decode_resource_offset_from_compact_resource_page_key() {
        // Arrange
        let key = encode_compact_resource_page_key("realm", "area", "resource", 42);

        // Act
        let decoded = decode_resource_offset_from_key(&key).expect("decode resource offset");

        // Assert
        assert_eq!(decoded, 42);
    }

    #[test]
    fn should_roundtrip_resource_value() {
        // Arrange
        let value = ResourceValue {
            resource_offset: 42,
            body: Bytes::from("test"),
            metadata: Some(Bytes::from("meta")),
            created_at: 1234567890,
            area_offset: Some(10),
            realm_offset: Some(5),
        };

        // Act
        let encoded = value.encode();
        let decoded = ResourceValue::decode(&encoded);

        // Assert

        assert_eq!(decoded.resource_offset, 42);
        assert_eq!(decoded.body, Bytes::from("test"));
        assert_eq!(decoded.area_offset, Some(10));
    }

    #[test]
    fn should_decode_legacy_resource_value() {
        // Arrange
        let encoded = serialize(&LegacyResourceValue {
            resource_offset: 42,
            body: Bytes::from("test"),
            metadata: Some(Bytes::from("meta")),
            created_at: 1234567890,
            area_offset: Some(10),
            realm_offset: Some(5),
        })
        .expect("serialize legacy resource value");

        // Act
        let decoded = ResourceValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.resource_offset, 42);
        assert_eq!(decoded.body, Bytes::from("test"));
        assert_eq!(decoded.metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded.created_at, 1234567890);
        assert_eq!(decoded.area_offset, Some(10));
        assert_eq!(decoded.realm_offset, Some(5));
    }

    #[test]
    fn should_roundtrip_staging_value_with_metadata() {
        // Arrange
        let event = EventPayload {
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
            discriminator: None,
        };

        // Act
        let encoded = encode_staging_value(&event);
        let decoded = decode_staging_value(&encoded).expect("decode staging value");

        // Assert
        assert_eq!(decoded.body, event.body);
        assert_eq!(decoded.metadata, event.metadata);
    }

    #[test]
    fn should_roundtrip_staging_value_without_metadata() {
        // Arrange
        let event = EventPayload {
            body: Bytes::from("body"),
            metadata: None,
            discriminator: None,
        };

        // Act
        let encoded = encode_staging_value(&event);
        let decoded = decode_staging_value(&encoded).expect("decode staging value");

        // Assert
        assert_eq!(decoded.body, event.body);
        assert_eq!(decoded.metadata, None);
    }

    #[test]
    fn should_roundtrip_compact_area_value() {
        // Arrange
        let value = AreaValue {
            resource_offset: 42,
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
            created_at: 123,
        };

        // Act
        let encoded = value.encode();
        let decoded = AreaValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.resource_offset, value.resource_offset);
        assert_eq!(decoded.body, value.body);
        assert_eq!(decoded.metadata, value.metadata);
        assert_eq!(decoded.created_at, value.created_at);
    }

    #[test]
    fn should_decode_legacy_area_value() {
        // Arrange
        let encoded = serialize(&LegacyAreaValue {
            realm: "realm".to_string(),
            area: "area".to_string(),
            resource: "resource".to_string(),
            resource_offset: 7,
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
            created_at: 321,
        })
        .expect("serialize legacy area value");

        // Act
        let decoded = AreaValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.resource_offset, 7);
        assert_eq!(decoded.body, Bytes::from("body"));
        assert_eq!(decoded.metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded.created_at, 321);
    }

    #[test]
    fn should_roundtrip_compact_realm_value() {
        // Arrange
        let value = RealmValue {
            area_offset: 11,
            resource_offset: 42,
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
            created_at: 123,
        };

        // Act
        let encoded = value.encode();
        let decoded = RealmValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.area_offset, value.area_offset);
        assert_eq!(decoded.resource_offset, value.resource_offset);
        assert_eq!(decoded.body, value.body);
        assert_eq!(decoded.metadata, value.metadata);
        assert_eq!(decoded.created_at, value.created_at);
    }

    #[test]
    fn should_decode_legacy_realm_value() {
        // Arrange
        let encoded = serialize(&LegacyRealmValue {
            realm: "realm".to_string(),
            area: "area".to_string(),
            area_offset: 11,
            resource: "resource".to_string(),
            resource_offset: 7,
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
            created_at: 321,
        })
        .expect("serialize legacy realm value");

        // Act
        let decoded = RealmValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.area_offset, 11);
        assert_eq!(decoded.resource_offset, 7);
        assert_eq!(decoded.body, Bytes::from("body"));
        assert_eq!(decoded.metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded.created_at, 321);
    }

    #[test]
    fn should_roundtrip_compact_realm_page_value() {
        // Arrange
        let value = CompactRealmPageValue {
            records: vec![
                CompactRealmPageRecord {
                    area_offset: 11,
                    resource_offset: 42,
                    body: Bytes::from("body"),
                    metadata: Some(Bytes::from("meta")),
                    created_at: 123,
                },
                CompactRealmPageRecord {
                    area_offset: 12,
                    resource_offset: 43,
                    body: Bytes::from("body-2"),
                    metadata: None,
                    created_at: 124,
                },
            ],
        };

        // Act
        let encoded = value.encode();
        let decoded = CompactRealmPageValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].area_offset, 11);
        assert_eq!(decoded.records[0].resource_offset, 42);
        assert_eq!(decoded.records[0].body, Bytes::from("body"));
        assert_eq!(decoded.records[0].metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded.records[1].area_offset, 12);
        assert_eq!(decoded.records[1].resource_offset, 43);
        assert_eq!(decoded.records[1].body, Bytes::from("body-2"));
        assert_eq!(decoded.records[1].metadata, None);
    }

    #[test]
    fn should_return_error_given_truncated_compact_realm_page_value() {
        // Arrange
        let encoded = vec![
            COMPACT_REALM_PAGE_VALUE_V1_MARKER[0],
            COMPACT_REALM_PAGE_VALUE_V1_MARKER[1],
            1,
            0,
            0,
            0,
        ];

        // Act
        let result = CompactRealmPageValue::try_decode(&encoded);

        // Assert
        let error = result.expect_err("truncated compact realm page should fail to decode");
        assert!(error.contains("record header truncated"));
    }

    #[test]
    fn should_roundtrip_compact_area_page_value() {
        // Arrange
        let value = CompactAreaPageValue {
            records: vec![
                CompactAreaPageRecord {
                    resource_offset: 42,
                    body: Bytes::from("body"),
                    metadata: Some(Bytes::from("meta")),
                    created_at: 123,
                },
                CompactAreaPageRecord {
                    resource_offset: 43,
                    body: Bytes::from("body-2"),
                    metadata: None,
                    created_at: 124,
                },
            ],
        };

        // Act
        let encoded = value.encode();
        let decoded = CompactAreaPageValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].resource_offset, 42);
        assert_eq!(decoded.records[0].body, Bytes::from("body"));
        assert_eq!(decoded.records[0].metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded.records[1].resource_offset, 43);
        assert_eq!(decoded.records[1].body, Bytes::from("body-2"));
        assert_eq!(decoded.records[1].metadata, None);
    }

    #[test]
    fn should_roundtrip_compact_resource_page_value() {
        // Arrange
        let value = CompactResourcePageValue {
            records: vec![
                CompactResourcePageRecord {
                    area_offset: 11,
                    realm_offset: 21,
                    body: Bytes::from("body"),
                    metadata: Some(Bytes::from("meta")),
                    created_at: 123,
                },
                CompactResourcePageRecord {
                    area_offset: 12,
                    realm_offset: 22,
                    body: Bytes::from("body-2"),
                    metadata: None,
                    created_at: 124,
                },
            ],
        };

        // Act
        let encoded = value.encode();
        let decoded = CompactResourcePageValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].area_offset, 11);
        assert_eq!(decoded.records[0].realm_offset, 21);
        assert_eq!(decoded.records[0].body, Bytes::from("body"));
        assert_eq!(decoded.records[0].metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded.records[1].area_offset, 12);
        assert_eq!(decoded.records[1].realm_offset, 22);
        assert_eq!(decoded.records[1].body, Bytes::from("body-2"));
        assert_eq!(decoded.records[1].metadata, None);
    }

    #[test]
    fn should_roundtrip_compressed_compact_realm_page_value() {
        // Arrange
        let value = CompressedCompactRealmPageValue {
            records: vec![
                CompactRealmPageRecord {
                    area_offset: 11,
                    resource_offset: 42,
                    body: Bytes::from("body"),
                    metadata: Some(Bytes::from("meta")),
                    created_at: 123,
                },
                CompactRealmPageRecord {
                    area_offset: 12,
                    resource_offset: 43,
                    body: Bytes::from("body-2"),
                    metadata: None,
                    created_at: 124,
                },
            ],
        };

        // Act
        let encoded = value.encode();
        let decoded = CompressedCompactRealmPageValue::decode(&encoded);
        let decoded_page = decoded.into_compact_realm_page();

        // Assert
        assert_eq!(decoded_page.records.len(), 2);
        assert_eq!(decoded_page.records[0].area_offset, 11);
        assert_eq!(decoded_page.records[0].resource_offset, 42);
        assert_eq!(decoded_page.records[0].body, Bytes::from("body"));
        assert_eq!(decoded_page.records[0].metadata, Some(Bytes::from("meta")));
        assert_eq!(decoded_page.records[1].area_offset, 12);
        assert_eq!(decoded_page.records[1].resource_offset, 43);
        assert_eq!(decoded_page.records[1].body, Bytes::from("body-2"));
        assert_eq!(decoded_page.records[1].metadata, None);
    }

    #[test]
    fn should_roundtrip_canonical_resource_value() {
        // Arrange
        let value = CanonicalResourceValue {
            area_offset: 11,
            realm_offset: 19,
            body: Bytes::from("body"),
            metadata: Some(Bytes::from("meta")),
            created_at: 123,
        };

        // Act
        let encoded = value.encode();
        let decoded = CanonicalResourceValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.area_offset, value.area_offset);
        assert_eq!(decoded.realm_offset, value.realm_offset);
        assert_eq!(decoded.body, value.body);
        assert_eq!(decoded.metadata, value.metadata);
        assert_eq!(decoded.created_at, value.created_at);
    }

    #[test]
    fn should_roundtrip_area_locator_value() {
        // Arrange
        let value = AreaLocatorValue {
            stream_id: 7,
            resource_offset: 42,
        };

        // Act
        let encoded = value.encode();
        let decoded = AreaLocatorValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.stream_id, value.stream_id);
        assert_eq!(decoded.resource_offset, value.resource_offset);
    }

    #[test]
    fn should_roundtrip_realm_locator_value() {
        // Arrange
        let value = RealmLocatorValue {
            area_offset: 17,
            stream_id: 9,
            resource_offset: 42,
        };

        // Act
        let encoded = value.encode();
        let decoded = RealmLocatorValue::decode(&encoded);

        // Assert
        assert_eq!(decoded.area_offset, value.area_offset);
        assert_eq!(decoded.stream_id, value.stream_id);
        assert_eq!(decoded.resource_offset, value.resource_offset);
    }
}
