use bincode::{deserialize, serialize};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_bench_store;
use fitz::domains::stream::protocol::{StreamRecord, StreamWriteMode};
use fitz::domains::stream::storage::{
    decode_area_offset_from_key, decode_realm_offset_from_key, encode_area_key,
    encode_area_locator_key, encode_canonical_resource_key, encode_realm_locator_key,
    AreaLocatorValue, AreaValue, CanonicalResourceValue, KeyPrefix, RealmLocatorValue,
};
use fitz::domains::stream::store::{
    CommitRecordsParams, EventPayload, ReadResourceParams, StreamStore,
};
use fitz::domains::stream::StreamReadItem;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

const FAMILY: u64 = 1;
const REALM: &str = "bench-realm";
const BODY_BYTES: usize = 128;
const METADATA_BYTES: usize = 24;
const PRODUCTION_LIKE_SMALL_EVENT_BYTES: usize = 40;
const PRODUCTION_LIKE_JSON_BODY_BYTES: usize = 160;
const PRODUCTION_LIKE_BINARY_BODY_BYTES: usize = 192;
const PRODUCTION_LIKE_LOG_BODY_BYTES: usize = 120;
const PRODUCTION_LIKE_JSON_METADATA_BYTES: usize = 48;
const PRODUCTION_LIKE_BINARY_METADATA_BYTES: usize = 16;
const PRODUCTION_LIKE_LOG_METADATA_BYTES: usize = 32;
const REPLAY_PAGE_RECORD_LIMIT: usize = 64;
const PAGED_REALM_KEY_PREFIX: u8 = 0xE0;
const PAGED_AREA_LOCATOR_KEY_PREFIX: u8 = 0xE1;
const COMPACT_PAGED_REALM_KEY_PREFIX: u8 = 0xE2;
const COMPACT_REALM_AREA_REF_KEY_PREFIX: u8 = 0xE3;
const COMPACT_AREA_PAGE_KEY_PREFIX: u8 = 0xE4;
const COMPACT_REALM_AREA_PAGE_REF_KEY_PREFIX: u8 = 0xE5;
const COMPACT_REALM_PAGE_ID_REF_KEY_PREFIX: u8 = 0xE6;
const COMPACT_REALM_PAGE_RUN_REF_KEY_PREFIX: u8 = 0xE7;
const COMPRESSED_COMPACT_PAGED_REALM_KEY_PREFIX: u8 = 0xE8;
const COMPACT_RESOURCE_AREA_PAGE_REF_KEY_PREFIX: u8 = 0xE9;
const COMPACT_RESOURCE_PAGE_KEY_PREFIX: u8 = 0xEA;
const COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xB2];
const COMPACT_AREA_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE4];
const COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE8];
const COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xEA];
const OPTIONAL_BYTES_ABSENT: u32 = u32::MAX;
const ASCII_TOKEN_BANK: [&str; 12] = [
    "stream", "event", "commit", "cursor", "tenant", "region", "audit", "batch", "order", "delta",
    "notify", "writer",
];

#[derive(Clone, Copy)]
enum PayloadProfile {
    LowEntropy,
    HighEntropy,
    ProductionLike,
}

#[derive(Clone)]
struct PrototypeStream {
    stream_id: u64,
    area_index: usize,
    area: String,
    resource: String,
}

struct PrototypeRowWrite {
    key: Vec<u8>,
    value: Vec<u8>,
}

fn event_records(items: Vec<StreamReadItem>) -> Vec<StreamRecord> {
    items
        .into_iter()
        .filter_map(|item| match item {
            StreamReadItem::Event(record) => Some(record),
            _ => None,
        })
        .collect()
}

#[derive(Clone)]
struct PagedSeedRecord {
    stream_index: usize,
    area_index: usize,
    area: String,
    resource_offset: u64,
    area_offset: u64,
    realm_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PagedReplayRecord {
    resource_offset: u64,
    area_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PagedRealmValue {
    records: Vec<PagedReplayRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PagedAreaLocatorValue {
    page_start_realm_offset: u64,
    slot: u16,
}

#[derive(Debug, Clone)]
struct CompactPagedRealmValue {
    records: Vec<PagedReplayRecord>,
}

#[derive(Debug, Clone)]
struct CompactRealmAreaRefPageValue {
    records: Vec<CompactRealmAreaRefRecord>,
}

#[derive(Debug, Clone)]
struct CompactRealmAreaRefRecord {
    area_index: u16,
    area_offset: u64,
}

#[derive(Debug, Clone)]
struct CompactAreaPageValue {
    records: Vec<CompactAreaPageRecord>,
}

#[derive(Debug, Clone)]
struct CompactAreaPageRecord {
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug, Clone)]
struct CompactRealmAreaPageRefValue {
    records: Vec<CompactRealmAreaPageRefRecord>,
}

#[derive(Debug, Clone)]
struct CompactRealmAreaPageRefRecord {
    area_index: u16,
    area_page_start_offset: u64,
    slot: u16,
}

#[derive(Debug, Clone)]
struct CompactRealmPageIdRefValue {
    records: Vec<CompactRealmPageIdRefRecord>,
}

#[derive(Debug, Clone)]
struct CompactRealmPageIdRefRecord {
    page_id: u32,
    slot: u16,
}

#[derive(Debug, Clone)]
struct CompactRealmPageRunRefValue {
    runs: Vec<CompactRealmPageRunRefRecord>,
}

#[derive(Debug, Clone)]
struct CompactRealmPageRunRefRecord {
    page_id: u32,
    start_slot: u16,
    len: u16,
}

#[derive(Debug, Clone)]
struct CompactResourceAreaPageRefValue {
    records: Vec<CompactResourceAreaPageRefRecord>,
}

#[derive(Debug, Clone)]
struct CompactResourceAreaPageRefRecord {
    area_page_start_offset: u64,
    slot: u16,
    realm_offset: u64,
}

#[derive(Debug, Clone)]
struct CompactResourcePageValue {
    records: Vec<CompactResourcePageRecord>,
}

#[derive(Debug, Clone)]
struct CompactResourcePageRecord {
    area_offset: u64,
    realm_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug, Clone)]
struct FlatCompactAreaPage {
    page_start_offset: u64,
    value: CompactAreaPageValue,
}

#[derive(Debug, Clone)]
struct PageRunAssignment {
    realm_output_start: usize,
    area_start_slot: usize,
    len: usize,
}

struct ReplayCase {
    store: StreamStore,
    db: Arc<cntryl_midge::Engine>,
    areas: Vec<String>,
    streams: Vec<PrototypeStream>,
    stream_positions: HashMap<u64, usize>,
    expected_records: usize,
    expected_payload_bytes: usize,
}

struct HydrationLocator {
    stream_index: usize,
    resource_offset: u64,
    area_offset: u64,
    realm_offset: Option<u64>,
}

impl PagedRealmValue {
    fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize paged realm value")
    }

    fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize paged realm value")
    }
}

impl PagedAreaLocatorValue {
    fn encode(&self) -> Vec<u8> {
        serialize(self).expect("serialize paged area locator")
    }

    fn decode(bytes: &[u8]) -> Self {
        deserialize(bytes).expect("deserialize paged area locator")
    }
}

impl CompactPagedRealmValue {
    fn encode(&self) -> Vec<u8> {
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

    fn decode(bytes: &[u8]) -> Self {
        assert!(
            bytes.starts_with(&COMPACT_REALM_PAGE_VALUE_V1_MARKER),
            "deserialize compact realm page value: missing marker"
        );
        assert!(
            bytes.len() >= 6,
            "deserialize compact realm page value: header too short"
        );

        let mut offset = 2usize;
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };
        let read_u64 = |input: &[u8], cursor: &mut usize| -> u64 {
            let value = u64::from_le_bytes(input[*cursor..*cursor + 8].try_into().unwrap());
            *cursor += 8;
            value
        };

        let record_count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            let area_offset = read_u64(bytes, &mut offset);
            let resource_offset = read_u64(bytes, &mut offset);
            let created_at = read_u64(bytes, &mut offset);
            let body_len = read_u32(bytes, &mut offset) as usize;
            let metadata_len_raw = read_u32(bytes, &mut offset);
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(metadata_len_raw as usize)
            };

            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;
            let metadata = if let Some(metadata_len) = metadata_len {
                let metadata = Bytes::copy_from_slice(&bytes[offset..offset + metadata_len]);
                offset += metadata_len;
                Some(metadata)
            } else {
                None
            };

            records.push(PagedReplayRecord {
                resource_offset,
                area_offset,
                body,
                metadata,
                created_at,
            });
        }

        Self { records }
    }
}

impl CompactRealmAreaRefPageValue {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + (self.records.len() * (2 + 8)));
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for record in &self.records {
            bytes.extend_from_slice(&record.area_index.to_le_bytes());
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut offset = 0usize;
        let read_u16 = |input: &[u8], cursor: &mut usize| -> u16 {
            let value = u16::from_le_bytes(input[*cursor..*cursor + 2].try_into().unwrap());
            *cursor += 2;
            value
        };
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };
        let read_u64 = |input: &[u8], cursor: &mut usize| -> u64 {
            let value = u64::from_le_bytes(input[*cursor..*cursor + 8].try_into().unwrap());
            *cursor += 8;
            value
        };

        let count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(CompactRealmAreaRefRecord {
                area_index: read_u16(bytes, &mut offset),
                area_offset: read_u64(bytes, &mut offset),
            });
        }
        Self { records }
    }
}

impl CompactAreaPageValue {
    fn encode(&self) -> Vec<u8> {
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

    fn decode(bytes: &[u8]) -> Self {
        assert!(
            bytes.starts_with(&COMPACT_AREA_PAGE_VALUE_V1_MARKER),
            "deserialize compact area page value: missing marker"
        );
        assert!(
            bytes.len() >= 6,
            "deserialize compact area page value: header too short"
        );

        let mut offset = 2usize;
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };
        let read_u64 = |input: &[u8], cursor: &mut usize| -> u64 {
            let value = u64::from_le_bytes(input[*cursor..*cursor + 8].try_into().unwrap());
            *cursor += 8;
            value
        };

        let record_count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            let resource_offset = read_u64(bytes, &mut offset);
            let created_at = read_u64(bytes, &mut offset);
            let body_len = read_u32(bytes, &mut offset) as usize;
            let metadata_len_raw = read_u32(bytes, &mut offset);
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(metadata_len_raw as usize)
            };

            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;
            let metadata = if let Some(metadata_len) = metadata_len {
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

        Self { records }
    }
}

impl CompactRealmAreaPageRefValue {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + (self.records.len() * (2 + 8 + 2)));
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for record in &self.records {
            bytes.extend_from_slice(&record.area_index.to_le_bytes());
            bytes.extend_from_slice(&record.area_page_start_offset.to_le_bytes());
            bytes.extend_from_slice(&record.slot.to_le_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut offset = 0usize;
        let read_u16 = |input: &[u8], cursor: &mut usize| -> u16 {
            let value = u16::from_le_bytes(input[*cursor..*cursor + 2].try_into().unwrap());
            *cursor += 2;
            value
        };
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };
        let read_u64 = |input: &[u8], cursor: &mut usize| -> u64 {
            let value = u64::from_le_bytes(input[*cursor..*cursor + 8].try_into().unwrap());
            *cursor += 8;
            value
        };

        let record_count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            records.push(CompactRealmAreaPageRefRecord {
                area_index: read_u16(bytes, &mut offset),
                area_page_start_offset: read_u64(bytes, &mut offset),
                slot: read_u16(bytes, &mut offset),
            });
        }

        Self { records }
    }
}

impl CompactRealmPageIdRefValue {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + (self.records.len() * (4 + 2)));
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for record in &self.records {
            bytes.extend_from_slice(&record.page_id.to_le_bytes());
            bytes.extend_from_slice(&record.slot.to_le_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut offset = 0usize;
        let read_u16 = |input: &[u8], cursor: &mut usize| -> u16 {
            let value = u16::from_le_bytes(input[*cursor..*cursor + 2].try_into().unwrap());
            *cursor += 2;
            value
        };
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };

        let record_count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            records.push(CompactRealmPageIdRefRecord {
                page_id: read_u32(bytes, &mut offset),
                slot: read_u16(bytes, &mut offset),
            });
        }

        Self { records }
    }
}

impl CompactRealmPageRunRefValue {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + (self.runs.len() * (4 + 2 + 2)));
        bytes.extend_from_slice(&(self.runs.len() as u32).to_le_bytes());
        for run in &self.runs {
            bytes.extend_from_slice(&run.page_id.to_le_bytes());
            bytes.extend_from_slice(&run.start_slot.to_le_bytes());
            bytes.extend_from_slice(&run.len.to_le_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut offset = 0usize;
        let read_u16 = |input: &[u8], cursor: &mut usize| -> u16 {
            let value = u16::from_le_bytes(input[*cursor..*cursor + 2].try_into().unwrap());
            *cursor += 2;
            value
        };
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };

        let run_count = read_u32(bytes, &mut offset) as usize;
        let mut runs = Vec::with_capacity(run_count);

        for _ in 0..run_count {
            runs.push(CompactRealmPageRunRefRecord {
                page_id: read_u32(bytes, &mut offset),
                start_slot: read_u16(bytes, &mut offset),
                len: read_u16(bytes, &mut offset),
            });
        }

        Self { runs }
    }
}

impl CompactResourceAreaPageRefValue {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + (self.records.len() * (8 + 2 + 8)));
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for record in &self.records {
            bytes.extend_from_slice(&record.area_page_start_offset.to_le_bytes());
            bytes.extend_from_slice(&record.slot.to_le_bytes());
            bytes.extend_from_slice(&record.realm_offset.to_le_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut offset = 0usize;
        let read_u16 = |input: &[u8], cursor: &mut usize| -> u16 {
            let value = u16::from_le_bytes(input[*cursor..*cursor + 2].try_into().unwrap());
            *cursor += 2;
            value
        };
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };
        let read_u64 = |input: &[u8], cursor: &mut usize| -> u64 {
            let value = u64::from_le_bytes(input[*cursor..*cursor + 8].try_into().unwrap());
            *cursor += 8;
            value
        };

        let record_count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            records.push(CompactResourceAreaPageRefRecord {
                area_page_start_offset: read_u64(bytes, &mut offset),
                slot: read_u16(bytes, &mut offset),
                realm_offset: read_u64(bytes, &mut offset),
            });
        }

        Self { records }
    }
}

impl CompactResourcePageValue {
    fn encode(&self) -> Vec<u8> {
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

    fn decode(bytes: &[u8]) -> Self {
        assert!(
            bytes.starts_with(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER),
            "deserialize compact resource page value: missing marker"
        );
        assert!(
            bytes.len() >= 6,
            "deserialize compact resource page value: header too short"
        );

        let mut offset = 2usize;
        let read_u32 = |input: &[u8], cursor: &mut usize| -> u32 {
            let value = u32::from_le_bytes(input[*cursor..*cursor + 4].try_into().unwrap());
            *cursor += 4;
            value
        };
        let read_u64 = |input: &[u8], cursor: &mut usize| -> u64 {
            let value = u64::from_le_bytes(input[*cursor..*cursor + 8].try_into().unwrap());
            *cursor += 8;
            value
        };

        let record_count = read_u32(bytes, &mut offset) as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            let area_offset = read_u64(bytes, &mut offset);
            let realm_offset = read_u64(bytes, &mut offset);
            let created_at = read_u64(bytes, &mut offset);
            let body_len = read_u32(bytes, &mut offset) as usize;
            let metadata_len_raw = read_u32(bytes, &mut offset);
            let metadata_len = if metadata_len_raw == OPTIONAL_BYTES_ABSENT {
                None
            } else {
                Some(metadata_len_raw as usize)
            };

            let body = Bytes::copy_from_slice(&bytes[offset..offset + body_len]);
            offset += body_len;
            let metadata = if let Some(metadata_len) = metadata_len {
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

        Self { records }
    }
}

fn deterministic_seed(stream_index: usize, record_index: usize, salt: u64) -> u64 {
    let base = ((stream_index as u64) << 32) ^ record_index as u64 ^ salt;
    base | 1
}

fn next_deterministic_state(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}

fn build_low_entropy_bytes(len: usize, seed: u64) -> Bytes {
    Bytes::from(vec![(seed & 0xFF) as u8; len])
}

fn build_high_entropy_bytes(len: usize, seed: u64) -> Bytes {
    let mut state = seed;
    let mut bytes = Vec::with_capacity(len);

    while bytes.len() < len {
        state = next_deterministic_state(state);
        bytes.push((state & 0xFF) as u8);
    }

    Bytes::from(bytes)
}

fn build_ascii_fill(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    let mut bytes = Vec::with_capacity(len);

    while bytes.len() < len {
        state = next_deterministic_state(state);
        let token = ASCII_TOKEN_BANK[(state as usize) % ASCII_TOKEN_BANK.len()].as_bytes();
        let hex = format!("{:08x}", state as u32);

        for chunk in [token, b" ", hex.as_bytes(), b" "] {
            for byte in chunk {
                if bytes.len() == len {
                    break;
                }
                bytes.push(*byte);
            }

            if bytes.len() == len {
                break;
            }
        }
    }

    bytes
}

fn build_padded_text(prefix: String, len: usize, seed: u64) -> Bytes {
    let mut bytes = prefix.into_bytes();
    if bytes.len() < len {
        bytes.extend_from_slice(&build_ascii_fill(len - bytes.len(), seed));
    }
    bytes.truncate(len);
    Bytes::from(bytes)
}

fn build_json_like_bytes(prefix: String, len: usize, seed: u64) -> Bytes {
    let suffix = b"\"}";
    let mut bytes = prefix.into_bytes();
    if bytes.len() + suffix.len() < len {
        bytes.extend_from_slice(&build_ascii_fill(len - bytes.len() - suffix.len(), seed));
    }
    bytes.extend_from_slice(suffix);
    if bytes.len() < len {
        bytes.resize(len, b' ');
    }
    bytes.truncate(len);
    Bytes::from(bytes)
}

fn build_tag_bytes(len: usize, seed: u64, stream_index: usize, record_index: usize) -> Bytes {
    build_padded_text(
        format!("fam=1 stream={stream_index} seq={record_index} "),
        len,
        seed,
    )
}

fn build_production_like_payload(stream_index: usize, record_index: usize) -> EventPayload {
    let body_seed = deterministic_seed(stream_index, record_index, 0x51_37);
    let metadata_seed = deterministic_seed(stream_index, record_index, 0xA5_A5);

    match ((stream_index * 17) + record_index) % 4 {
        0 => EventPayload {
            body: build_padded_text(
                format!("event-{record_index:04} stream-{stream_index:02} "),
                PRODUCTION_LIKE_SMALL_EVENT_BYTES,
                body_seed,
            ),
            metadata: None,
            discriminator: None,
        },
        1 => EventPayload {
            body: build_json_like_bytes(
                format!(
                    "{{\"event\":\"commit\",\"stream\":{stream_index},\"seq\":{record_index},\"message\":\""
                ),
                PRODUCTION_LIKE_JSON_BODY_BYTES,
                body_seed,
            ),
            metadata: Some(build_tag_bytes(
                PRODUCTION_LIKE_JSON_METADATA_BYTES,
                metadata_seed,
                stream_index,
                record_index,
            )),
            discriminator: None,
        },
        2 => EventPayload {
            body: build_high_entropy_bytes(PRODUCTION_LIKE_BINARY_BODY_BYTES, body_seed),
            metadata: Some(build_high_entropy_bytes(
                PRODUCTION_LIKE_BINARY_METADATA_BYTES,
                metadata_seed,
            )),
            discriminator: None,
        },
        _ => EventPayload {
            body: build_padded_text(
                format!(
                    "ts={:08x} lvl=info stream={stream_index} seq={record_index} msg=",
                    body_seed as u32
                ),
                PRODUCTION_LIKE_LOG_BODY_BYTES,
                body_seed ^ 0xDE_AD_BE_EF,
            ),
            metadata: Some(build_tag_bytes(
                PRODUCTION_LIKE_LOG_METADATA_BYTES,
                metadata_seed ^ 0xC6_A4_A7_93,
                stream_index,
                record_index,
            )),
            discriminator: None,
        },
    }
}

fn build_event_payload(
    stream_index: usize,
    record_index: usize,
    profile: PayloadProfile,
) -> EventPayload {
    let body_seed = ((stream_index as u8).wrapping_mul(17)).wrapping_add(record_index as u8);
    let metadata_seed = body_seed.wrapping_add(53);

    match profile {
        PayloadProfile::LowEntropy => EventPayload {
            body: build_low_entropy_bytes(
                BODY_BYTES,
                deterministic_seed(stream_index, record_index, body_seed as u64),
            ),
            metadata: Some(build_low_entropy_bytes(
                METADATA_BYTES,
                deterministic_seed(stream_index, record_index, metadata_seed as u64 ^ 0xA5A5),
            )),
            discriminator: None,
        },
        PayloadProfile::HighEntropy => EventPayload {
            body: build_high_entropy_bytes(
                BODY_BYTES,
                deterministic_seed(stream_index, record_index, body_seed as u64),
            ),
            metadata: Some(build_high_entropy_bytes(
                METADATA_BYTES,
                deterministic_seed(stream_index, record_index, metadata_seed as u64 ^ 0xA5A5),
            )),
            discriminator: None,
        },
        PayloadProfile::ProductionLike => build_production_like_payload(stream_index, record_index),
    }
}

fn build_canonical_prefix(stream_id: u64) -> Bytes {
    let mut prefix = vec![KeyPrefix::CanonicalResource as u8];
    prefix.extend_from_slice(&stream_id.to_be_bytes());
    Bytes::from(prefix)
}

fn encode_paged_realm_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![PAGED_REALM_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_paged_realm_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![PAGED_REALM_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_paged_realm_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_PAGED_REALM_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_compact_paged_realm_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![COMPACT_PAGED_REALM_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compressed_compact_paged_realm_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPRESSED_COMPACT_PAGED_REALM_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_compressed_compact_paged_realm_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![COMPRESSED_COMPACT_PAGED_REALM_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_realm_area_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_AREA_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_compact_realm_area_ref_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![COMPACT_REALM_AREA_REF_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_area_page_key(realm: &str, area: &str, area_page_start_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_AREA_PAGE_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_page_start_offset.to_be_bytes());
    key
}

fn build_compact_area_page_prefix(realm: &str, area: &str) -> Bytes {
    let mut prefix = vec![COMPACT_AREA_PAGE_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(area.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_realm_area_page_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_AREA_PAGE_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_compact_realm_area_page_ref_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![COMPACT_REALM_AREA_PAGE_REF_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_realm_page_id_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_PAGE_ID_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_compact_realm_page_id_ref_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![COMPACT_REALM_PAGE_ID_REF_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_realm_page_run_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_PAGE_RUN_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn build_compact_realm_page_run_ref_prefix(realm: &str) -> Bytes {
    let mut prefix = vec![COMPACT_REALM_PAGE_RUN_REF_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_resource_area_page_ref_key(
    realm: &str,
    area: &str,
    resource: &str,
    page_start_resource_offset: u64,
) -> Vec<u8> {
    let mut key = vec![COMPACT_RESOURCE_AREA_PAGE_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_resource_offset.to_be_bytes());
    key
}

fn build_compact_resource_area_page_ref_prefix(realm: &str, area: &str, resource: &str) -> Bytes {
    let mut prefix = vec![COMPACT_RESOURCE_AREA_PAGE_REF_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(area.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(resource.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn encode_compact_resource_page_key(
    realm: &str,
    area: &str,
    resource: &str,
    page_start_resource_offset: u64,
) -> Vec<u8> {
    let mut key = vec![COMPACT_RESOURCE_PAGE_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(resource.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_resource_offset.to_be_bytes());
    key
}

fn build_compact_resource_page_prefix(realm: &str, area: &str, resource: &str) -> Bytes {
    let mut prefix = vec![COMPACT_RESOURCE_PAGE_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(area.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(resource.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn decode_resource_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }

    let offset_bytes = &key[key.len() - 8..];
    let mut arr = [0u8; 8];
    arr.copy_from_slice(offset_bytes);
    Ok(u64::from_be_bytes(arr))
}

fn encode_paged_area_locator_key(realm: &str, area: &str, area_offset: u64) -> Vec<u8> {
    let mut key = vec![PAGED_AREA_LOCATOR_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&area_offset.to_be_bytes());
    key
}

fn build_paged_area_locator_prefix(realm: &str, area: &str) -> Bytes {
    let mut prefix = vec![PAGED_AREA_LOCATOR_KEY_PREFIX];
    prefix.extend_from_slice(realm.as_bytes());
    prefix.push(0);
    prefix.extend_from_slice(area.as_bytes());
    prefix.push(0);
    Bytes::from(prefix)
}

fn persist_prototype_rows(
    db: &Arc<cntryl_midge::Engine>,
    writes: &[PrototypeRowWrite],
) -> Result<(), String> {
    let mut txn = db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadWrite)
        .map_err(|error| format!("begin_tx failed: {error:?}"))?;

    for row in writes {
        txn.put(row.key.clone(), row.value.clone(), None)
            .map_err(|error| format!("txn put failed: {error:?}"))?;
    }

    txn.commit(cntryl_midge::WriteOptions::buffered())
        .map_err(|error| format!("txn commit failed: {error:?}"))
}

fn seed_replay_case(
    area_count: usize,
    streams_per_area: usize,
    records_per_stream: usize,
    profile: PayloadProfile,
) -> ReplayCase {
    let db = create_bench_store();
    let store = StreamStore::new(db.clone());
    let areas = (0..area_count)
        .map(|area_index| format!("area-{area_index}"))
        .collect::<Vec<_>>();
    let mut streams = Vec::with_capacity(area_count * streams_per_area);

    for (area_index, area) in areas.iter().cloned().enumerate() {
        for resource_index in 0..streams_per_area {
            streams.push(PrototypeStream {
                stream_id: (streams.len() + 1) as u64,
                area_index,
                area: area.clone(),
                resource: format!("resource-{area_index}-{resource_index}"),
            });
        }
    }

    let mut prototype_rows =
        Vec::with_capacity(area_count * streams_per_area * records_per_stream * 4);
    let mut next_resource_offsets = vec![0u64; streams.len()];
    let mut paged_seed_records =
        Vec::with_capacity(area_count * streams_per_area * records_per_stream);
    let mut expected_payload_bytes = 0usize;

    for record_index in 0..records_per_stream {
        for (stream_index, stream) in streams.iter().enumerate() {
            let event = build_event_payload(stream_index, record_index, profile);
            expected_payload_bytes +=
                event.body.len() + event.metadata.as_ref().map(|meta| meta.len()).unwrap_or(0);
            let commit = store
                .commit_records(CommitRecordsParams {
                    family: FAMILY,
                    realm: REALM,
                    area: &stream.area,
                    resource: &stream.resource,
                    expected_resource_next_offset: next_resource_offsets[stream_index],
                    events: std::slice::from_ref(&event),
                    ingest_metadata: None,
                    mode: StreamWriteMode::Buffered,
                })
                .expect("seed stream commit");
            next_resource_offsets[stream_index] += 1;

            let created_at = ((stream_index as u64) << 32) | record_index as u64;
            prototype_rows.push(PrototypeRowWrite {
                key: encode_canonical_resource_key(stream.stream_id, commit.first_resource_offset),
                value: CanonicalResourceValue {
                    area_offset: commit.first_area_offset,
                    realm_offset: commit.first_realm_offset,
                    body: event.body.clone(),
                    metadata: event.metadata.clone(),
                    created_at,
                }
                .encode(),
            });
            prototype_rows.push(PrototypeRowWrite {
                key: encode_area_key(REALM, &stream.area, commit.first_area_offset),
                value: AreaValue {
                    resource_offset: commit.first_resource_offset,
                    body: event.body.clone(),
                    metadata: event.metadata.clone(),
                    created_at,
                }
                .encode(),
            });
            prototype_rows.push(PrototypeRowWrite {
                key: encode_area_locator_key(REALM, &stream.area, commit.first_area_offset),
                value: AreaLocatorValue {
                    stream_id: stream.stream_id,
                    resource_offset: commit.first_resource_offset,
                }
                .encode(),
            });
            prototype_rows.push(PrototypeRowWrite {
                key: encode_realm_locator_key(REALM, commit.first_realm_offset),
                value: RealmLocatorValue {
                    area_offset: commit.first_area_offset,
                    stream_id: stream.stream_id,
                    resource_offset: commit.first_resource_offset,
                }
                .encode(),
            });
            paged_seed_records.push(PagedSeedRecord {
                stream_index,
                area_index: stream.area_index,
                area: stream.area.clone(),
                resource_offset: commit.first_resource_offset,
                area_offset: commit.first_area_offset,
                realm_offset: commit.first_realm_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            });
        }
    }

    let mut area_seed_records = vec![Vec::new(); areas.len()];
    for record in &paged_seed_records {
        area_seed_records[record.area_index].push(record.clone());
    }

    let mut area_page_ids = HashMap::<(usize, u64), u32>::new();
    let mut next_area_page_id = 0u32;

    for (area_index, area_records) in area_seed_records.iter().enumerate() {
        let area_name = &areas[area_index];
        for page in area_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
            let page_start_area_offset = page[0].area_offset;
            area_page_ids.insert((area_index, page_start_area_offset), next_area_page_id);
            next_area_page_id += 1;
            prototype_rows.push(PrototypeRowWrite {
                key: encode_compact_area_page_key(REALM, area_name, page_start_area_offset),
                value: CompactAreaPageValue {
                    records: page
                        .iter()
                        .map(|record| CompactAreaPageRecord {
                            resource_offset: record.resource_offset,
                            body: record.body.clone(),
                            metadata: record.metadata.clone(),
                            created_at: record.created_at,
                        })
                        .collect(),
                }
                .encode(),
            });
        }
    }

    let mut stream_seed_records = vec![Vec::new(); streams.len()];
    for record in &paged_seed_records {
        stream_seed_records[record.stream_index].push(record.clone());
    }

    for (stream_index, resource_records) in stream_seed_records.iter().enumerate() {
        let stream = &streams[stream_index];
        for page in resource_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
            prototype_rows.push(PrototypeRowWrite {
                key: encode_compact_resource_area_page_ref_key(
                    REALM,
                    &stream.area,
                    &stream.resource,
                    page[0].resource_offset,
                ),
                value: CompactResourceAreaPageRefValue {
                    records: page
                        .iter()
                        .map(|record| CompactResourceAreaPageRefRecord {
                            area_page_start_offset: record.area_offset
                                / REPLAY_PAGE_RECORD_LIMIT as u64
                                * REPLAY_PAGE_RECORD_LIMIT as u64,
                            slot: (record.area_offset % REPLAY_PAGE_RECORD_LIMIT as u64) as u16,
                            realm_offset: record.realm_offset,
                        })
                        .collect(),
                }
                .encode(),
            });
            prototype_rows.push(PrototypeRowWrite {
                key: encode_compact_resource_page_key(
                    REALM,
                    &stream.area,
                    &stream.resource,
                    page[0].resource_offset,
                ),
                value: CompactResourcePageValue {
                    records: page
                        .iter()
                        .map(|record| CompactResourcePageRecord {
                            area_offset: record.area_offset,
                            realm_offset: record.realm_offset,
                            body: record.body.clone(),
                            metadata: record.metadata.clone(),
                            created_at: record.created_at,
                        })
                        .collect(),
                }
                .encode(),
            });
        }
    }

    for page in paged_seed_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
        let page_start_realm_offset = page[0].realm_offset;
        let page_records = page
            .iter()
            .map(|record| PagedReplayRecord {
                resource_offset: record.resource_offset,
                area_offset: record.area_offset,
                body: record.body.clone(),
                metadata: record.metadata.clone(),
                created_at: record.created_at,
            })
            .collect::<Vec<_>>();
        prototype_rows.push(PrototypeRowWrite {
            key: encode_paged_realm_key(REALM, page_start_realm_offset),
            value: PagedRealmValue {
                records: page_records.clone(),
            }
            .encode(),
        });
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compact_paged_realm_key(REALM, page_start_realm_offset),
            value: CompactPagedRealmValue {
                records: page_records,
            }
            .encode(),
        });
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compressed_compact_paged_realm_key(REALM, page_start_realm_offset),
            value: {
                let compressed_payload = compress_prepend_size(
                    &CompactPagedRealmValue {
                        records: page
                            .iter()
                            .map(|record| PagedReplayRecord {
                                resource_offset: record.resource_offset,
                                area_offset: record.area_offset,
                                body: record.body.clone(),
                                metadata: record.metadata.clone(),
                                created_at: record.created_at,
                            })
                            .collect(),
                    }
                    .encode(),
                );
                let mut bytes = Vec::with_capacity(
                    COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER.len() + compressed_payload.len(),
                );
                bytes.extend_from_slice(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER);
                bytes.extend_from_slice(&compressed_payload);
                bytes
            },
        });
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compact_realm_area_ref_key(REALM, page_start_realm_offset),
            value: CompactRealmAreaRefPageValue {
                records: page
                    .iter()
                    .map(|record| CompactRealmAreaRefRecord {
                        area_index: record.area_index as u16,
                        area_offset: record.area_offset,
                    })
                    .collect(),
            }
            .encode(),
        });
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compact_realm_area_page_ref_key(REALM, page_start_realm_offset),
            value: CompactRealmAreaPageRefValue {
                records: page
                    .iter()
                    .map(|record| CompactRealmAreaPageRefRecord {
                        area_index: record.area_index as u16,
                        area_page_start_offset: record.area_offset
                            / REPLAY_PAGE_RECORD_LIMIT as u64
                            * REPLAY_PAGE_RECORD_LIMIT as u64,
                        slot: (record.area_offset % REPLAY_PAGE_RECORD_LIMIT as u64) as u16,
                    })
                    .collect(),
            }
            .encode(),
        });
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compact_realm_page_id_ref_key(REALM, page_start_realm_offset),
            value: CompactRealmPageIdRefValue {
                records: page
                    .iter()
                    .map(|record| {
                        let page_start_area_offset = record.area_offset
                            / REPLAY_PAGE_RECORD_LIMIT as u64
                            * REPLAY_PAGE_RECORD_LIMIT as u64;
                        let page_id = area_page_ids
                            .get(&(record.area_index, page_start_area_offset))
                            .copied()
                            .expect("missing compact area page id");
                        CompactRealmPageIdRefRecord {
                            page_id,
                            slot: (record.area_offset % REPLAY_PAGE_RECORD_LIMIT as u64) as u16,
                        }
                    })
                    .collect(),
            }
            .encode(),
        });
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compact_realm_page_run_ref_key(REALM, page_start_realm_offset),
            value: CompactRealmPageRunRefValue {
                runs: page
                    .iter()
                    .map(|record| {
                        let page_start_area_offset = record.area_offset
                            / REPLAY_PAGE_RECORD_LIMIT as u64
                            * REPLAY_PAGE_RECORD_LIMIT as u64;
                        let page_id = area_page_ids
                            .get(&(record.area_index, page_start_area_offset))
                            .copied()
                            .expect("missing compact area page id");
                        (
                            page_id,
                            (record.area_offset % REPLAY_PAGE_RECORD_LIMIT as u64) as u16,
                        )
                    })
                    .fold(
                        Vec::<CompactRealmPageRunRefRecord>::new(),
                        |mut runs, (page_id, slot)| {
                            if let Some(last_run) = runs.last_mut() {
                                let expected_slot = last_run.start_slot + last_run.len;
                                if last_run.page_id == page_id && expected_slot == slot {
                                    last_run.len += 1;
                                    return runs;
                                }
                            }

                            runs.push(CompactRealmPageRunRefRecord {
                                page_id,
                                start_slot: slot,
                                len: 1,
                            });
                            runs
                        },
                    ),
            }
            .encode(),
        });

        for (slot, record) in page.iter().enumerate() {
            prototype_rows.push(PrototypeRowWrite {
                key: encode_paged_area_locator_key(REALM, &record.area, record.area_offset),
                value: PagedAreaLocatorValue {
                    page_start_realm_offset,
                    slot: slot as u16,
                }
                .encode(),
            });
        }
    }

    persist_prototype_rows(&db, &prototype_rows).expect("persist prototype stream rows");

    let stream_positions = streams
        .iter()
        .enumerate()
        .map(|(index, stream)| (stream.stream_id, index))
        .collect();

    ReplayCase {
        store,
        db,
        areas,
        streams,
        stream_positions,
        expected_records: area_count * streams_per_area * records_per_stream,
        expected_payload_bytes,
    }
}

fn hydrate_area_batches(
    txn: &cntryl_midge::Transaction,
    case: &ReplayCase,
    requested_offsets: &[Vec<u64>],
) -> Result<Vec<HashMap<u64, AreaValue>>, String> {
    let mut hydrated = Vec::with_capacity(case.areas.len());

    for (area_index, offsets) in requested_offsets.iter().enumerate() {
        if offsets.is_empty() {
            hydrated.push(HashMap::new());
            continue;
        }

        let mut sorted_offsets = offsets.clone();
        sorted_offsets.sort_unstable();
        let first_offset = sorted_offsets[0];
        let last_offset = *sorted_offsets.last().expect("sorted area offsets");

        let area = &case.areas[area_index];
        let mut prefix_key = encode_area_key(REALM, area, 0);
        prefix_key.truncate(prefix_key.len() - 8);

        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_area_key(REALM, area, first_offset)))
            .prefix(Bytes::from(prefix_key))
            .limit((last_offset - first_offset + 1) as usize);
        let mut iter = txn
            .scan(&query)
            .map_err(|error| format!("scan error: {error:?}"))?;
        let wanted = sorted_offsets
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let mut area_records = HashMap::with_capacity(wanted.len());

        for (key, value) in iter.collect_all() {
            let area_offset = decode_area_offset_from_key(&key)?;
            if wanted.contains(&area_offset) {
                area_records.insert(area_offset, AreaValue::decode(&value));
            }
        }

        if area_records.len() != offsets.len() {
            return Err(format!(
                "area hydration count mismatch for {}: expected {}, got {}",
                area,
                offsets.len(),
                area_records.len()
            ));
        }

        hydrated.push(area_records);
    }

    Ok(hydrated)
}

fn load_area_page_cached<'a>(
    txn: &cntryl_midge::Transaction,
    case: &ReplayCase,
    cache: &'a mut HashMap<(u16, u64), CompactAreaPageValue>,
    area_index: u16,
    page_start_offset: u64,
) -> Result<&'a CompactAreaPageValue, String> {
    let cache_key = (area_index, page_start_offset);
    if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry(cache_key) {
        let area_name = &case.areas[area_index as usize];
        let page_key = encode_compact_area_page_key(REALM, area_name, page_start_offset);
        let page_bytes = txn
            .get(&page_key)
            .map_err(|error| format!("get error: {error:?}"))?
            .ok_or_else(|| {
                format!(
                    "missing compact area page for area {} page {}",
                    area_name, page_start_offset
                )
            })?;
        entry.insert(CompactAreaPageValue::decode(&page_bytes));
    }

    Ok(cache.get(&cache_key).expect("cached compact area page"))
}

fn scan_compact_area_pages(
    txn: &cntryl_midge::Transaction,
    case: &ReplayCase,
) -> Result<HashMap<(u16, u64), CompactAreaPageValue>, String> {
    let mut pages = HashMap::new();

    for (area_index, area_name) in case.areas.iter().enumerate() {
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compact_area_page_key(
                REALM, area_name, 0,
            )))
            .prefix(build_compact_area_page_prefix(REALM, area_name))
            .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
        let mut iter = txn
            .scan(&query)
            .map_err(|error| format!("scan error: {error:?}"))?;

        for (key, value) in iter.collect_all() {
            let page_start = decode_area_offset_from_key(&key)?;
            pages.insert(
                (area_index as u16, page_start),
                CompactAreaPageValue::decode(&value),
            );
        }
    }

    Ok(pages)
}

fn scan_compact_area_pages_flat(
    txn: &cntryl_midge::Transaction,
    case: &ReplayCase,
) -> Result<Vec<FlatCompactAreaPage>, String> {
    let mut pages = Vec::new();

    for area_name in &case.areas {
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_compact_area_page_key(
                REALM, area_name, 0,
            )))
            .prefix(build_compact_area_page_prefix(REALM, area_name))
            .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
        let mut iter = txn
            .scan(&query)
            .map_err(|error| format!("scan error: {error:?}"))?;

        for (key, value) in iter.collect_all() {
            pages.push(FlatCompactAreaPage {
                page_start_offset: decode_area_offset_from_key(&key)?,
                value: CompactAreaPageValue::decode(&value),
            });
        }
    }

    Ok(pages)
}

fn hydrate_canonical_batches(
    txn: &cntryl_midge::Transaction,
    case: &ReplayCase,
    requested_offsets: &[Vec<u64>],
) -> Result<Vec<Vec<CanonicalResourceValue>>, String> {
    let mut hydrated = Vec::with_capacity(case.streams.len());

    for (stream_index, offsets) in requested_offsets.iter().enumerate() {
        if offsets.is_empty() {
            hydrated.push(Vec::new());
            continue;
        }

        let stream = &case.streams[stream_index];
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(encode_canonical_resource_key(
                stream.stream_id,
                offsets[0],
            )))
            .prefix(build_canonical_prefix(stream.stream_id))
            .limit(offsets.len());

        let mut iter = txn
            .scan(&query)
            .map_err(|error| format!("scan error: {error:?}"))?;
        let values = iter
            .collect_all()
            .into_iter()
            .map(|(_, value)| CanonicalResourceValue::decode(&value))
            .collect::<Vec<_>>();

        if values.len() != offsets.len() {
            return Err(format!(
                "hydration count mismatch for stream {}: expected {}, got {}",
                stream.stream_id,
                offsets.len(),
                values.len()
            ));
        }

        hydrated.push(values);
    }

    Ok(hydrated)
}

fn read_resource_covering(
    case: &ReplayCase,
    stream: &PrototypeStream,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
    let (records, _) = case.store.read_resource(&ReadResourceParams {
        family: FAMILY,
        realm: REALM,
        area: &stream.area,
        resource: &stream.resource,
        from_offset: 0,
        limit: limit as u64,
        max_bytes: None,
    })?;

    Ok(event_records(records))
}

fn read_resource_compact_paged(
    case: &ReplayCase,
    stream: &PrototypeStream,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_resource_page_key(
            REALM,
            &stream.area,
            &stream.resource,
            0,
        )))
        .prefix(build_compact_resource_page_prefix(
            REALM,
            &stream.area,
            &stream.resource,
        ))
        .limit(limit.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(limit);

    for (key, value) in raw_rows {
        let resource_page_start = decode_resource_offset_from_key(&key)?;
        let page = CompactResourcePageValue::decode(&value);

        for (resource_page_slot, record) in page.records.iter().enumerate() {
            if records.len() == limit {
                return Ok(records);
            }

            records.push(StreamRecord {
                resource_offset: resource_page_start + resource_page_slot as u64,
                area_offset: Some(record.area_offset),
                realm_offset: Some(record.realm_offset),
                body: record.body.clone(),
                metadata: record.metadata.clone(),
                created_at: record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_resource_area_page_ref(
    case: &ReplayCase,
    stream: &PrototypeStream,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_resource_area_page_ref_key(
            REALM,
            &stream.area,
            &stream.resource,
            0,
        )))
        .prefix(build_compact_resource_area_page_ref_prefix(
            REALM,
            &stream.area,
            &stream.resource,
        ))
        .limit(limit.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut page_cache = HashMap::<(u16, u64), CompactAreaPageValue>::new();
    let mut records = Vec::with_capacity(limit);

    for (key, value) in raw_rows {
        let resource_page_start = decode_resource_offset_from_key(&key)?;
        let page = CompactResourceAreaPageRefValue::decode(&value);

        for (resource_page_slot, record) in page.records.iter().enumerate() {
            if records.len() == limit {
                return Ok(records);
            }

            let area_page = load_area_page_cached(
                &txn,
                case,
                &mut page_cache,
                stream.area_index as u16,
                record.area_page_start_offset,
            )?;
            let area_record = area_page
                .records
                .get(record.slot as usize)
                .ok_or_else(|| format!("invalid compact area page slot {}", record.slot))?;
            let area_offset = record.area_page_start_offset + record.slot as u64;

            records.push(StreamRecord {
                resource_offset: resource_page_start + resource_page_slot as u64,
                area_offset: Some(area_offset),
                realm_offset: Some(record.realm_offset),
                body: area_record.body.clone(),
                metadata: area_record.metadata.clone(),
                created_at: area_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_resource_area_page_ref_scanned(
    case: &ReplayCase,
    stream: &PrototypeStream,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_resource_area_page_ref_key(
            REALM,
            &stream.area,
            &stream.resource,
            0,
        )))
        .prefix(build_compact_resource_area_page_ref_prefix(
            REALM,
            &stream.area,
            &stream.resource,
        ))
        .limit(limit.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();
    let page_cache = scan_compact_area_pages(&txn, case)?;

    let mut records = Vec::with_capacity(limit);

    for (key, value) in raw_rows {
        let resource_page_start = decode_resource_offset_from_key(&key)?;
        let page = CompactResourceAreaPageRefValue::decode(&value);

        for (resource_page_slot, record) in page.records.iter().enumerate() {
            if records.len() == limit {
                return Ok(records);
            }

            let area_page = page_cache
                .get(&(stream.area_index as u16, record.area_page_start_offset))
                .ok_or_else(|| {
                    format!(
                        "missing scanned compact area page for area {} page {}",
                        stream.area, record.area_page_start_offset
                    )
                })?;
            let area_record = area_page
                .records
                .get(record.slot as usize)
                .ok_or_else(|| format!("invalid compact area page slot {}", record.slot))?;
            let area_offset = record.area_page_start_offset + record.slot as u64;

            records.push(StreamRecord {
                resource_offset: resource_page_start + resource_page_slot as u64,
                area_offset: Some(area_offset),
                realm_offset: Some(record.realm_offset),
                body: area_record.body.clone(),
                metadata: area_record.metadata.clone(),
                created_at: area_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_area_hydrated(case: &ReplayCase, area: &str) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_watermark(FAMILY, REALM, area)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let mut prefix_key = encode_area_locator_key(REALM, area, 0);
    prefix_key.truncate(prefix_key.len() - 8);

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_area_locator_key(REALM, area, 0)))
        .prefix(Bytes::from(prefix_key))
        .limit(case.expected_records);
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut locators = Vec::with_capacity(raw_rows.len());
    let mut requested_offsets = vec![Vec::new(); case.streams.len()];

    for (key, value) in raw_rows {
        let area_offset = decode_area_offset_from_key(&key)?;
        if area_offset > watermark {
            break;
        }

        let locator = AreaLocatorValue::decode(&value);
        let stream_index = *case
            .stream_positions
            .get(&locator.stream_id)
            .ok_or_else(|| format!("unknown stream id {}", locator.stream_id))?;
        requested_offsets[stream_index].push(locator.resource_offset);
        locators.push(HydrationLocator {
            stream_index,
            resource_offset: locator.resource_offset,
            area_offset,
            realm_offset: None,
        });
    }

    let hydrated = hydrate_canonical_batches(&txn, case, &requested_offsets)?;
    let mut stream_cursors = vec![0usize; case.streams.len()];
    let mut records = Vec::with_capacity(locators.len());

    for locator in locators {
        let value = &hydrated[locator.stream_index][stream_cursors[locator.stream_index]];
        stream_cursors[locator.stream_index] += 1;

        records.push(StreamRecord {
            resource_offset: locator.resource_offset,
            area_offset: Some(locator.area_offset),
            realm_offset: None,
            body: value.body.clone(),
            metadata: value.metadata.clone(),
            created_at: value.created_at,
        });
    }

    Ok(records)
}

fn read_realm_hydrated(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let mut prefix_key = encode_realm_locator_key(REALM, 0);
    prefix_key.truncate(prefix_key.len() - 8);

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_realm_locator_key(REALM, 0)))
        .prefix(Bytes::from(prefix_key))
        .limit(case.expected_records);
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut locators = Vec::with_capacity(raw_rows.len());
    let mut requested_offsets = vec![Vec::new(); case.streams.len()];

    for (key, value) in raw_rows {
        let realm_offset = decode_realm_offset_from_key(&key)?;
        if realm_offset > watermark {
            break;
        }

        let locator = RealmLocatorValue::decode(&value);
        let stream_index = *case
            .stream_positions
            .get(&locator.stream_id)
            .ok_or_else(|| format!("unknown stream id {}", locator.stream_id))?;
        requested_offsets[stream_index].push(locator.resource_offset);
        locators.push(HydrationLocator {
            stream_index,
            resource_offset: locator.resource_offset,
            area_offset: locator.area_offset,
            realm_offset: Some(realm_offset),
        });
    }

    let hydrated = hydrate_canonical_batches(&txn, case, &requested_offsets)?;
    let mut stream_cursors = vec![0usize; case.streams.len()];
    let mut records = Vec::with_capacity(locators.len());

    for locator in locators {
        let value = &hydrated[locator.stream_index][stream_cursors[locator.stream_index]];
        stream_cursors[locator.stream_index] += 1;

        records.push(StreamRecord {
            resource_offset: locator.resource_offset,
            area_offset: Some(locator.area_offset),
            realm_offset: locator.realm_offset,
            body: value.body.clone(),
            metadata: value.metadata.clone(),
            created_at: value.created_at,
        });
    }

    Ok(records)
}

fn read_area_paged(case: &ReplayCase, area: &str) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_watermark(FAMILY, REALM, area)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_paged_area_locator_key(REALM, area, 0)))
        .prefix(build_paged_area_locator_prefix(REALM, area))
        .limit(case.expected_records);
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut page_cache = HashMap::<u64, PagedRealmValue>::new();
    let mut records = Vec::with_capacity(raw_rows.len());

    for (key, value) in raw_rows {
        let area_offset = decode_area_offset_from_key(&key)?;
        if area_offset > watermark {
            break;
        }

        let locator = PagedAreaLocatorValue::decode(&value);
        let page = if let Some(page) = page_cache.get(&locator.page_start_realm_offset) {
            page
        } else {
            let page_key = encode_paged_realm_key(REALM, locator.page_start_realm_offset);
            let page_bytes = txn
                .get(&page_key)
                .map_err(|error| format!("get error: {error:?}"))?
                .ok_or_else(|| {
                    format!(
                        "missing paged realm row for {}",
                        locator.page_start_realm_offset
                    )
                })?;
            let page = PagedRealmValue::decode(&page_bytes);
            page_cache.insert(locator.page_start_realm_offset, page);
            page_cache
                .get(&locator.page_start_realm_offset)
                .expect("cached paged realm row")
        };
        let page_record = page
            .records
            .get(locator.slot as usize)
            .ok_or_else(|| format!("invalid page slot {}", locator.slot))?;

        records.push(StreamRecord {
            resource_offset: page_record.resource_offset,
            area_offset: Some(area_offset),
            realm_offset: None,
            body: page_record.body.clone(),
            metadata: page_record.metadata.clone(),
            created_at: page_record.created_at,
        });
    }

    Ok(records)
}

fn read_area_compact_paged(case: &ReplayCase, area: &str) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_watermark(FAMILY, REALM, area)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_area_page_key(REALM, area, 0)))
        .prefix(build_compact_area_page_prefix(REALM, area))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_area_offset_from_key(&key)?;
        let page = CompactAreaPageValue::decode(&value);

        for (slot, page_record) in page.records.iter().enumerate() {
            let area_offset = page_start + slot as u64;
            if area_offset > watermark {
                return Ok(records);
            }

            records.push(StreamRecord {
                resource_offset: page_record.resource_offset,
                area_offset: Some(area_offset),
                realm_offset: None,
                body: page_record.body.clone(),
                metadata: page_record.metadata.clone(),
                created_at: page_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_paged(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_paged_realm_key(REALM, 0)))
        .prefix(build_paged_realm_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = PagedRealmValue::decode(&value);

        for (slot, page_record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            records.push(StreamRecord {
                resource_offset: page_record.resource_offset,
                area_offset: Some(page_record.area_offset),
                realm_offset: Some(realm_offset),
                body: page_record.body.clone(),
                metadata: page_record.metadata.clone(),
                created_at: page_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_compact_paged(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_paged_realm_key(REALM, 0)))
        .prefix(build_compact_paged_realm_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = CompactPagedRealmValue::decode(&value);

        for (slot, page_record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            records.push(StreamRecord {
                resource_offset: page_record.resource_offset,
                area_offset: Some(page_record.area_offset),
                realm_offset: Some(realm_offset),
                body: page_record.body.clone(),
                metadata: page_record.metadata.clone(),
                created_at: page_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_compressed_compact_paged(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compressed_compact_paged_realm_key(
            REALM, 0,
        )))
        .prefix(build_compressed_compact_paged_realm_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        if !value.starts_with(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER) {
            return Err("decode compressed compact realm page value: missing marker".to_string());
        }
        if value.len() <= COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER.len() {
            return Err("decode compressed compact realm page value: payload missing".to_string());
        }
        let decompressed = decompress_size_prepended(&value[2..])
            .map_err(|error| format!("lz4 decompress error: {error}"))?;
        let page = CompactPagedRealmValue::decode(&decompressed);

        for (slot, page_record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            records.push(StreamRecord {
                resource_offset: page_record.resource_offset,
                area_offset: Some(page_record.area_offset),
                realm_offset: Some(realm_offset),
                body: page_record.body.clone(),
                metadata: page_record.metadata.clone(),
                created_at: page_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_area_ref_paged(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_realm_area_ref_key(REALM, 0)))
        .prefix(build_compact_realm_area_ref_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = CompactRealmAreaRefPageValue::decode(&value);
        let mut requested_offsets = vec![Vec::new(); case.areas.len()];

        for record in &page.records {
            requested_offsets[record.area_index as usize].push(record.area_offset);
        }

        let hydrated = hydrate_area_batches(&txn, case, &requested_offsets)?;

        for (slot, record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            let area_record = hydrated[record.area_index as usize]
                .get(&record.area_offset)
                .ok_or_else(|| {
                    format!(
                        "missing hydrated area record for area index {} offset {}",
                        record.area_index, record.area_offset
                    )
                })?;

            records.push(StreamRecord {
                resource_offset: area_record.resource_offset,
                area_offset: Some(record.area_offset),
                realm_offset: Some(realm_offset),
                body: area_record.body.clone(),
                metadata: area_record.metadata.clone(),
                created_at: area_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_area_page_ref_paged(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_realm_area_page_ref_key(
            REALM, 0,
        )))
        .prefix(build_compact_realm_area_page_ref_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut page_cache = HashMap::<(u16, u64), CompactAreaPageValue>::new();
    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = CompactRealmAreaPageRefValue::decode(&value);

        for (slot, record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            let area_page = load_area_page_cached(
                &txn,
                case,
                &mut page_cache,
                record.area_index,
                record.area_page_start_offset,
            )?;
            let area_record = area_page
                .records
                .get(record.slot as usize)
                .ok_or_else(|| format!("invalid compact area page slot {}", record.slot))?;
            let area_offset = record.area_page_start_offset + record.slot as u64;

            records.push(StreamRecord {
                resource_offset: area_record.resource_offset,
                area_offset: Some(area_offset),
                realm_offset: Some(realm_offset),
                body: area_record.body.clone(),
                metadata: area_record.metadata.clone(),
                created_at: area_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_area_page_ref_scanned(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_realm_area_page_ref_key(
            REALM, 0,
        )))
        .prefix(build_compact_realm_area_page_ref_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();
    let page_cache = scan_compact_area_pages(&txn, case)?;

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = CompactRealmAreaPageRefValue::decode(&value);

        for (slot, record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            let area_page = page_cache
                .get(&(record.area_index, record.area_page_start_offset))
                .ok_or_else(|| {
                    format!(
                        "missing scanned compact area page for area {} page {}",
                        record.area_index, record.area_page_start_offset
                    )
                })?;
            let area_record = area_page
                .records
                .get(record.slot as usize)
                .ok_or_else(|| format!("invalid compact area page slot {}", record.slot))?;
            let area_offset = record.area_page_start_offset + record.slot as u64;

            records.push(StreamRecord {
                resource_offset: area_record.resource_offset,
                area_offset: Some(area_offset),
                realm_offset: Some(realm_offset),
                body: area_record.body.clone(),
                metadata: area_record.metadata.clone(),
                created_at: area_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_page_id_ref_scanned(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_realm_page_id_ref_key(REALM, 0)))
        .prefix(build_compact_realm_page_id_ref_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();
    let page_cache = scan_compact_area_pages_flat(&txn, case)?;

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = CompactRealmPageIdRefValue::decode(&value);

        for (slot, record) in page.records.iter().enumerate() {
            let realm_offset = page_start + slot as u64;
            if realm_offset > watermark {
                return Ok(records);
            }

            let area_page = page_cache.get(record.page_id as usize).ok_or_else(|| {
                format!("missing compact area page for page id {}", record.page_id)
            })?;
            let area_record = area_page
                .value
                .records
                .get(record.slot as usize)
                .ok_or_else(|| format!("invalid compact area page slot {}", record.slot))?;
            let area_offset = area_page.page_start_offset + record.slot as u64;

            records.push(StreamRecord {
                resource_offset: area_record.resource_offset,
                area_offset: Some(area_offset),
                realm_offset: Some(realm_offset),
                body: area_record.body.clone(),
                metadata: area_record.metadata.clone(),
                created_at: area_record.created_at,
            });
        }
    }

    Ok(records)
}

fn read_realm_page_run_ref_scanned(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_realm_page_run_ref_key(REALM, 0)))
        .prefix(build_compact_realm_page_run_ref_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();
    let page_cache = scan_compact_area_pages_flat(&txn, case)?;

    let mut records = Vec::with_capacity(case.expected_records);

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)?;
        let page = CompactRealmPageRunRefValue::decode(&value);
        let mut next_realm_offset = page_start;

        for run in page.runs {
            let area_page = page_cache
                .get(run.page_id as usize)
                .ok_or_else(|| format!("missing compact area page for page id {}", run.page_id))?;
            let start_slot = run.start_slot as usize;
            let end_slot = start_slot + run.len as usize;

            if end_slot > area_page.value.records.len() {
                return Err(format!(
                    "invalid compact area page run {}..{} for page id {}",
                    start_slot, end_slot, run.page_id
                ));
            }

            for (slot, area_record) in area_page.value.records[start_slot..end_slot]
                .iter()
                .enumerate()
            {
                if next_realm_offset > watermark {
                    return Ok(records);
                }

                let absolute_slot = start_slot + slot;
                let area_offset = area_page.page_start_offset + absolute_slot as u64;

                records.push(StreamRecord {
                    resource_offset: area_record.resource_offset,
                    area_offset: Some(area_offset),
                    realm_offset: Some(next_realm_offset),
                    body: area_record.body.clone(),
                    metadata: area_record.metadata.clone(),
                    created_at: area_record.created_at,
                });
                next_realm_offset += 1;
            }
        }
    }

    Ok(records)
}

fn read_realm_page_run_ref_clustered(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_realm_page_run_ref_key(REALM, 0)))
        .prefix(build_compact_realm_page_run_ref_prefix(REALM))
        .limit(case.expected_records.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();
    let page_cache = scan_compact_area_pages_flat(&txn, case)?;
    let output_len = (watermark as usize)
        .saturating_add(1)
        .min(case.expected_records);
    let mut assignments_by_page = vec![Vec::<PageRunAssignment>::new(); page_cache.len()];

    for (key, value) in raw_rows {
        let page_start = decode_realm_offset_from_key(&key)? as usize;
        if page_start >= output_len {
            break;
        }

        let page = CompactRealmPageRunRefValue::decode(&value);
        let mut next_realm_output = page_start;

        for run in page.runs {
            if next_realm_output >= output_len {
                break;
            }

            let page_id = run.page_id as usize;
            if page_id >= assignments_by_page.len() {
                return Err(format!(
                    "missing compact area page for page id {}",
                    run.page_id
                ));
            }

            let available_len = output_len - next_realm_output;
            let assignment_len = available_len.min(run.len as usize);
            assignments_by_page[page_id].push(PageRunAssignment {
                realm_output_start: next_realm_output,
                area_start_slot: run.start_slot as usize,
                len: assignment_len,
            });
            next_realm_output += assignment_len;
        }
    }

    let mut output = vec![None; output_len];

    for (page_id, assignments) in assignments_by_page.into_iter().enumerate() {
        if assignments.is_empty() {
            continue;
        }

        let area_page = &page_cache[page_id];
        for assignment in assignments {
            let end_slot = assignment.area_start_slot + assignment.len;
            if end_slot > area_page.value.records.len() {
                return Err(format!(
                    "invalid clustered page run {}..{} for page id {}",
                    assignment.area_start_slot, end_slot, page_id
                ));
            }

            for (slot_delta, area_record) in area_page.value.records
                [assignment.area_start_slot..end_slot]
                .iter()
                .enumerate()
            {
                let output_index = assignment.realm_output_start + slot_delta;
                let absolute_slot = assignment.area_start_slot + slot_delta;
                let area_offset = area_page.page_start_offset + absolute_slot as u64;

                output[output_index] = Some(StreamRecord {
                    resource_offset: area_record.resource_offset,
                    area_offset: Some(area_offset),
                    realm_offset: Some(output_index as u64),
                    body: area_record.body.clone(),
                    metadata: area_record.metadata.clone(),
                    created_at: area_record.created_at,
                });
            }
        }
    }

    output
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            record.ok_or_else(|| format!("missing clustered run record at realm offset {index}"))
        })
        .collect()
}

fn assert_matching_records(left: &[StreamRecord], right: &[StreamRecord]) {
    assert_eq!(left.len(), right.len(), "record count mismatch");

    for (index, (left_record, right_record)) in left.iter().zip(right).enumerate() {
        if left_record.resource_offset != right_record.resource_offset
            || left_record.area_offset != right_record.area_offset
            || left_record.realm_offset != right_record.realm_offset
            || left_record.body != right_record.body
            || left_record.metadata != right_record.metadata
        {
            panic!(
                "record mismatch at index {index}: left(resource={:?}, area={:?}, realm={:?}) right(resource={:?}, area={:?}, realm={:?})",
                left_record.resource_offset,
                left_record.area_offset,
                left_record.realm_offset,
                right_record.resource_offset,
                right_record.area_offset,
                right_record.realm_offset,
            );
        }
    }
}

fn assert_total_payload_bytes(records: &[StreamRecord], expected_payload_bytes: usize) {
    let observed = records
        .iter()
        .map(|record| {
            record.body.len() + record.metadata.as_ref().map(|meta| meta.len()).unwrap_or(0)
        })
        .sum::<usize>();

    assert_eq!(observed, expected_payload_bytes);
}

fn validate_resource_case(case: &ReplayCase, stream: &PrototypeStream, expected_records: usize) {
    let covering_records =
        read_resource_covering(case, stream, expected_records).expect("covering resource replay");
    let compact_paged_records = read_resource_compact_paged(case, stream, expected_records)
        .expect("resource compact paged replay");
    let area_page_ref_records = read_resource_area_page_ref(case, stream, expected_records)
        .expect("resource area-page-ref replay");
    let scanned_area_page_ref_records =
        read_resource_area_page_ref_scanned(case, stream, expected_records)
            .expect("resource area-page-ref scanned replay");

    assert_eq!(covering_records.len(), expected_records);
    assert_eq!(compact_paged_records.len(), expected_records);
    assert_eq!(area_page_ref_records.len(), expected_records);
    assert_eq!(scanned_area_page_ref_records.len(), expected_records);
    assert_matching_records(&covering_records, &compact_paged_records);
    assert_matching_records(&covering_records, &area_page_ref_records);
    assert_matching_records(&covering_records, &scanned_area_page_ref_records);
}

fn validate_area_case(case: &ReplayCase, area: &str) {
    let (covering_records, _) = case
        .store
        .read_area(FAMILY, REALM, area, 0, case.expected_records as u64, None)
        .expect("covering area replay");
    let covering_records = event_records(covering_records);
    let hydrated_records = read_area_hydrated(case, area).expect("hydrated area replay");
    let paged_records = read_area_paged(case, area).expect("paged area replay");
    let compact_paged_records =
        read_area_compact_paged(case, area).expect("compact paged area replay");

    assert_eq!(covering_records.len(), case.expected_records);
    assert_eq!(hydrated_records.len(), case.expected_records);
    assert_eq!(paged_records.len(), case.expected_records);
    assert_eq!(compact_paged_records.len(), case.expected_records);
    assert_total_payload_bytes(&covering_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&hydrated_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&paged_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&compact_paged_records, case.expected_payload_bytes);
    assert_matching_records(&covering_records, &hydrated_records);
    assert_matching_records(&covering_records, &paged_records);
    assert_matching_records(&covering_records, &compact_paged_records);
}

fn validate_realm_case(case: &ReplayCase) {
    let (covering_records, _) = case
        .store
        .read_realm(FAMILY, REALM, 0, case.expected_records as u64, None)
        .expect("covering realm replay");
    let covering_records = event_records(covering_records);
    let hydrated_records = read_realm_hydrated(case).expect("hydrated realm replay");
    let paged_records = read_realm_paged(case).expect("paged realm replay");
    let compact_paged_records = read_realm_compact_paged(case).expect("compact paged realm replay");
    let compressed_compact_paged_records =
        read_realm_compressed_compact_paged(case).expect("compressed compact paged realm replay");
    let area_ref_paged_records =
        read_realm_area_ref_paged(case).expect("area-ref paged realm replay");
    let area_page_ref_paged_records =
        read_realm_area_page_ref_paged(case).expect("area-page-ref paged realm replay");
    let area_page_ref_scanned_records =
        read_realm_area_page_ref_scanned(case).expect("area-page-ref scanned realm replay");
    let page_id_ref_scanned_records =
        read_realm_page_id_ref_scanned(case).expect("page-id-ref scanned realm replay");
    let page_run_ref_scanned_records =
        read_realm_page_run_ref_scanned(case).expect("page-run-ref scanned realm replay");
    let page_run_ref_clustered_records =
        read_realm_page_run_ref_clustered(case).expect("page-run-ref clustered realm replay");

    assert_eq!(covering_records.len(), case.expected_records);
    assert_eq!(hydrated_records.len(), case.expected_records);
    assert_eq!(paged_records.len(), case.expected_records);
    assert_eq!(compact_paged_records.len(), case.expected_records);
    assert_eq!(
        compressed_compact_paged_records.len(),
        case.expected_records
    );
    assert_eq!(area_ref_paged_records.len(), case.expected_records);
    assert_eq!(area_page_ref_paged_records.len(), case.expected_records);
    assert_eq!(area_page_ref_scanned_records.len(), case.expected_records);
    assert_eq!(page_id_ref_scanned_records.len(), case.expected_records);
    assert_eq!(page_run_ref_scanned_records.len(), case.expected_records);
    assert_eq!(page_run_ref_clustered_records.len(), case.expected_records);
    assert_total_payload_bytes(&covering_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&hydrated_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&paged_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&compact_paged_records, case.expected_payload_bytes);
    assert_total_payload_bytes(
        &compressed_compact_paged_records,
        case.expected_payload_bytes,
    );
    assert_total_payload_bytes(&area_ref_paged_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&area_page_ref_paged_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&area_page_ref_scanned_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&page_id_ref_scanned_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&page_run_ref_scanned_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&page_run_ref_clustered_records, case.expected_payload_bytes);
    assert_matching_records(&covering_records, &hydrated_records);
    assert_matching_records(&covering_records, &paged_records);
    assert_matching_records(&covering_records, &compact_paged_records);
    assert_matching_records(&covering_records, &compressed_compact_paged_records);
    assert_matching_records(&covering_records, &area_ref_paged_records);
    assert_matching_records(&covering_records, &area_page_ref_paged_records);
    assert_matching_records(&covering_records, &area_page_ref_scanned_records);
    assert_matching_records(&covering_records, &page_id_ref_scanned_records);
    assert_matching_records(&covering_records, &page_run_ref_scanned_records);
    assert_matching_records(&covering_records, &page_run_ref_clustered_records);
}

fn validate_realm_local_body_case(case: &ReplayCase) {
    let (covering_records, _) = case
        .store
        .read_realm(FAMILY, REALM, 0, case.expected_records as u64, None)
        .expect("covering realm replay");
    let covering_records = event_records(covering_records);
    let compact_paged_records = read_realm_compact_paged(case).expect("compact paged realm replay");
    let compressed_compact_paged_records =
        read_realm_compressed_compact_paged(case).expect("compressed compact paged realm replay");

    assert_eq!(covering_records.len(), case.expected_records);
    assert_eq!(compact_paged_records.len(), case.expected_records);
    assert_eq!(
        compressed_compact_paged_records.len(),
        case.expected_records
    );
    assert_total_payload_bytes(&covering_records, case.expected_payload_bytes);
    assert_total_payload_bytes(&compact_paged_records, case.expected_payload_bytes);
    assert_total_payload_bytes(
        &compressed_compact_paged_records,
        case.expected_payload_bytes,
    );
    assert_matching_records(&covering_records, &compact_paged_records);
    assert_matching_records(&covering_records, &compressed_compact_paged_records);
}

fn bench_stream_replay_hydration(c: &mut Criterion) {
    let area_case = seed_replay_case(1, 16, 128, PayloadProfile::LowEntropy);
    let area_name = area_case.streams[0].area.clone();
    let resource_stream = area_case.streams[0].clone();
    let resource_expected_records = area_case.expected_records / area_case.streams.len();
    validate_resource_case(&area_case, &resource_stream, resource_expected_records);
    validate_area_case(&area_case, &area_name);

    let production_like_resource_case =
        seed_replay_case(1, 16, 128, PayloadProfile::ProductionLike);
    let production_like_resource_stream = production_like_resource_case.streams[0].clone();
    let production_like_resource_expected_records = production_like_resource_case.expected_records
        / production_like_resource_case.streams.len();
    validate_resource_case(
        &production_like_resource_case,
        &production_like_resource_stream,
        production_like_resource_expected_records,
    );

    let realm_case = seed_replay_case(4, 8, 64, PayloadProfile::LowEntropy);
    validate_realm_case(&realm_case);
    let high_entropy_realm_case = seed_replay_case(4, 8, 64, PayloadProfile::HighEntropy);
    validate_realm_local_body_case(&high_entropy_realm_case);
    let production_like_realm_case = seed_replay_case(4, 8, 64, PayloadProfile::ProductionLike);
    validate_realm_local_body_case(&production_like_realm_case);

    let mut group = c.benchmark_group("subsystem_stream_replay");
    group.sampling_mode(SamplingMode::Flat);

    group.throughput(Throughput::Elements(resource_expected_records as u64));
    group.bench_function("covering_resource_replay_128_records_1_stream", |b| {
        b.iter(|| {
            black_box(
                read_resource_covering(&area_case, &resource_stream, resource_expected_records)
                    .expect("covering resource replay"),
            );
        })
    });
    group.bench_function("resource_mini_page_replay_128_records_1_stream", |b| {
        b.iter(|| {
            black_box(
                read_resource_compact_paged(
                    &area_case,
                    &resource_stream,
                    resource_expected_records,
                )
                .expect("resource mini-page replay"),
            );
        })
    });
    group.bench_function("area_page_ref_resource_replay_128_records_1_stream", |b| {
        b.iter(|| {
            black_box(
                read_resource_area_page_ref(
                    &area_case,
                    &resource_stream,
                    resource_expected_records,
                )
                .expect("resource area-page-ref replay"),
            );
        })
    });
    group.bench_function(
        "area_page_ref_scanned_resource_replay_128_records_1_stream",
        |b| {
            b.iter(|| {
                black_box(
                    read_resource_area_page_ref_scanned(
                        &area_case,
                        &resource_stream,
                        resource_expected_records,
                    )
                    .expect("resource area-page-ref scanned replay"),
                );
            })
        },
    );
    group.bench_function(
        "covering_resource_replay_128_records_1_stream_production_like",
        |b| {
            b.iter(|| {
                black_box(
                    read_resource_covering(
                        &production_like_resource_case,
                        &production_like_resource_stream,
                        production_like_resource_expected_records,
                    )
                    .expect("covering resource replay production-like"),
                );
            })
        },
    );
    group.bench_function(
        "resource_mini_page_replay_128_records_1_stream_production_like",
        |b| {
            b.iter(|| {
                black_box(
                    read_resource_compact_paged(
                        &production_like_resource_case,
                        &production_like_resource_stream,
                        production_like_resource_expected_records,
                    )
                    .expect("resource mini-page replay production-like"),
                );
            })
        },
    );

    group.throughput(Throughput::Elements(area_case.expected_records as u64));
    group.bench_function("covering_area_replay_2048_records_16_streams", |b| {
        b.iter(|| {
            black_box(
                area_case
                    .store
                    .read_area(
                        FAMILY,
                        REALM,
                        &area_name,
                        0,
                        area_case.expected_records as u64,
                        None,
                    )
                    .expect("covering area replay"),
            );
        })
    });
    group.bench_function("hydrated_area_replay_2048_records_16_streams", |b| {
        b.iter(|| {
            black_box(read_area_hydrated(&area_case, &area_name).expect("hydrated area replay"));
        })
    });
    group.bench_function("paged_area_replay_2048_records_16_streams", |b| {
        b.iter(|| {
            black_box(read_area_paged(&area_case, &area_name).expect("paged area replay"));
        })
    });
    group.bench_function("compact_paged_area_replay_2048_records_16_streams", |b| {
        b.iter(|| {
            black_box(
                read_area_compact_paged(&area_case, &area_name).expect("compact paged area replay"),
            );
        })
    });

    group.throughput(Throughput::Elements(realm_case.expected_records as u64));
    group.bench_function("covering_realm_replay_2048_records_32_streams", |b| {
        b.iter(|| {
            black_box(
                realm_case
                    .store
                    .read_realm(FAMILY, REALM, 0, realm_case.expected_records as u64, None)
                    .expect("covering realm replay"),
            );
        })
    });
    group.bench_function("hydrated_realm_replay_2048_records_32_streams", |b| {
        b.iter(|| {
            black_box(read_realm_hydrated(&realm_case).expect("hydrated realm replay"));
        })
    });
    group.bench_function("paged_realm_replay_2048_records_32_streams", |b| {
        b.iter(|| {
            black_box(read_realm_paged(&realm_case).expect("paged realm replay"));
        })
    });
    group.bench_function("compact_paged_realm_replay_2048_records_32_streams", |b| {
        b.iter(|| {
            black_box(read_realm_compact_paged(&realm_case).expect("compact paged realm replay"));
        })
    });
    group.bench_function(
        "compressed_compact_paged_realm_replay_2048_records_32_streams",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_compressed_compact_paged(&realm_case)
                        .expect("compressed compact paged realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "covering_realm_replay_2048_records_32_streams_high_entropy",
        |b| {
            b.iter(|| {
                black_box(
                    high_entropy_realm_case
                        .store
                        .read_realm(
                            FAMILY,
                            REALM,
                            0,
                            high_entropy_realm_case.expected_records as u64,
                            None,
                        )
                        .expect("covering realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "compact_paged_realm_replay_2048_records_32_streams_high_entropy",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_compact_paged(&high_entropy_realm_case)
                        .expect("compact paged realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "compressed_compact_paged_realm_replay_2048_records_32_streams_high_entropy",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_compressed_compact_paged(&high_entropy_realm_case)
                        .expect("compressed compact paged realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "covering_realm_replay_2048_records_32_streams_production_like",
        |b| {
            b.iter(|| {
                black_box(
                    production_like_realm_case
                        .store
                        .read_realm(
                            FAMILY,
                            REALM,
                            0,
                            production_like_realm_case.expected_records as u64,
                            None,
                        )
                        .expect("covering realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "compact_paged_realm_replay_2048_records_32_streams_production_like",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_compact_paged(&production_like_realm_case)
                        .expect("compact paged realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "compressed_compact_paged_realm_replay_2048_records_32_streams_production_like",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_compressed_compact_paged(&production_like_realm_case)
                        .expect("compressed compact paged realm replay"),
                );
            })
        },
    );
    group.bench_function("area_ref_paged_realm_replay_2048_records_32_streams", |b| {
        b.iter(|| {
            black_box(read_realm_area_ref_paged(&realm_case).expect("area-ref paged realm replay"));
        })
    });
    group.bench_function(
        "area_page_ref_paged_realm_replay_2048_records_32_streams",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_area_page_ref_paged(&realm_case)
                        .expect("area-page-ref paged realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "area_page_ref_scanned_realm_replay_2048_records_32_streams",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_area_page_ref_scanned(&realm_case)
                        .expect("area-page-ref scanned realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "page_id_ref_scanned_realm_replay_2048_records_32_streams",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_page_id_ref_scanned(&realm_case)
                        .expect("page-id-ref scanned realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "page_run_ref_scanned_realm_replay_2048_records_32_streams",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_page_run_ref_scanned(&realm_case)
                        .expect("page-run-ref scanned realm replay"),
                );
            })
        },
    );
    group.bench_function(
        "page_run_ref_clustered_realm_replay_2048_records_32_streams",
        |b| {
            b.iter(|| {
                black_box(
                    read_realm_page_run_ref_clustered(&realm_case)
                        .expect("page-run-ref clustered realm replay"),
                );
            })
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_stream_replay_hydration
}
criterion_main!(benches);
