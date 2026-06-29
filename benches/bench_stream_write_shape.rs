#![allow(deprecated)]
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::stream::storage::{
    encode_area_key, encode_realm_key, encode_resource_key, AreaValue, RealmValue, ResourceValue,
};
use lz4_flex::block::compress_prepend_size;
use std::collections::HashMap;

#[path = "criterion_config.rs"]
mod criterion_config;

const REALM: &str = "bench-realm";
const AREA_COUNT: usize = 4;
const STREAMS_PER_AREA: usize = 8;
const RECORDS_PER_STREAM: usize = 64;
const REALM_PAGE_RECORD_LIMIT: usize = 64;
const COMPACT_PAGED_REALM_KEY_PREFIX: u8 = 0xE2;
const COMPACT_REALM_AREA_REF_KEY_PREFIX: u8 = 0xE3;
const COMPACT_AREA_PAGE_KEY_PREFIX: u8 = 0xE4;
const COMPACT_REALM_AREA_PAGE_REF_KEY_PREFIX: u8 = 0xE5;
const COMPACT_REALM_PAGE_ID_REF_KEY_PREFIX: u8 = 0xE6;
const COMPACT_REALM_PAGE_RUN_REF_KEY_PREFIX: u8 = 0xE7;
const COMPACT_RESOURCE_AREA_PAGE_REF_KEY_PREFIX: u8 = 0xE9;
const COMPACT_RESOURCE_PAGE_KEY_PREFIX: u8 = 0xEA;
const BODY_BYTES: usize = 128;
const METADATA_BYTES: usize = 24;
const PRODUCTION_LIKE_SMALL_EVENT_BYTES: usize = 40;
const PRODUCTION_LIKE_JSON_BODY_BYTES: usize = 160;
const PRODUCTION_LIKE_BINARY_BODY_BYTES: usize = 192;
const PRODUCTION_LIKE_LOG_BODY_BYTES: usize = 120;
const PRODUCTION_LIKE_JSON_METADATA_BYTES: usize = 48;
const PRODUCTION_LIKE_BINARY_METADATA_BYTES: usize = 16;
const PRODUCTION_LIKE_LOG_METADATA_BYTES: usize = 32;
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
struct LayoutRecord {
    area: String,
    resource: String,
    resource_offset: u64,
    area_offset: u64,
    realm_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Clone)]
struct CompactPagedRealmValue {
    records: Vec<CompactPagedRealmRecord>,
}

#[derive(Clone)]
struct CompactPagedRealmRecord {
    resource_offset: u64,
    area_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

struct LayoutSummary {
    total_bytes: usize,
    resource_plane_bytes: usize,
    area_plane_bytes: usize,
    realm_plane_bytes: usize,
    bytes_per_event: f64,
}

#[derive(Clone)]
struct CompactRealmAreaRefPageValue {
    records: Vec<CompactRealmAreaRefRecord>,
}

#[derive(Clone)]
struct CompactRealmAreaRefRecord {
    area_index: u16,
    area_offset: u64,
}

#[derive(Clone)]
struct CompactAreaPageValue {
    records: Vec<CompactAreaPageRecord>,
}

#[derive(Clone)]
struct CompactAreaPageRecord {
    resource_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Clone)]
struct CompactRealmAreaPageRefValue {
    records: Vec<CompactRealmAreaPageRefRecord>,
}

#[derive(Clone)]
struct CompactRealmAreaPageRefRecord {
    area_index: u16,
    area_page_start_offset: u64,
    slot: u16,
}

#[derive(Clone)]
struct CompactRealmPageIdRefValue {
    records: Vec<CompactRealmPageIdRefRecord>,
}

#[derive(Clone)]
struct CompactRealmPageIdRefRecord {
    page_id: u32,
    slot: u16,
}

#[derive(Clone)]
struct CompactRealmPageRunRefValue {
    runs: Vec<CompactRealmPageRunRefRecord>,
}

#[derive(Clone)]
struct CompactRealmPageRunRefRecord {
    page_id: u32,
    start_slot: u16,
    len: u16,
}

#[derive(Clone)]
struct CompactResourceAreaPageRefValue {
    records: Vec<CompactResourceAreaPageRefRecord>,
}

#[derive(Clone)]
struct CompactResourceAreaPageRefRecord {
    area_page_start_offset: u64,
    slot: u16,
    realm_offset: u64,
}

#[derive(Clone)]
struct CompactResourcePageValue {
    records: Vec<CompactResourcePageRecord>,
}

#[derive(Clone)]
struct CompactResourcePageRecord {
    area_offset: u64,
    realm_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

impl CompactPagedRealmValue {
    fn encode(&self) -> Vec<u8> {
        let mut total_len = 4;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, bytes::Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&(record.body.len() as u32).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(u32::MAX, |metadata| metadata.len() as u32)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
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
}

impl CompactAreaPageValue {
    fn encode(&self) -> Vec<u8> {
        let mut total_len = 4;
        for record in &self.records {
            total_len += 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, bytes::Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&(record.body.len() as u32).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(u32::MAX, |metadata| metadata.len() as u32)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
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
}

impl CompactResourcePageValue {
    fn encode(&self) -> Vec<u8> {
        let mut total_len = 4;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, bytes::Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
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
                    .map_or(u32::MAX, |metadata| metadata.len() as u32)
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&record.body);
            if let Some(metadata) = &record.metadata {
                bytes.extend_from_slice(metadata);
            }
        }

        bytes
    }
}

fn encode_compact_paged_realm_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_PAGED_REALM_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn encode_compact_realm_area_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_AREA_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn encode_compact_area_page_key(realm: &str, area: &str, page_start_area_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_AREA_PAGE_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_area_offset.to_be_bytes());
    key
}

fn encode_compact_realm_area_page_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_AREA_PAGE_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn encode_compact_realm_page_id_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_PAGE_ID_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
}

fn encode_compact_realm_page_run_ref_key(realm: &str, page_start_realm_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_REALM_PAGE_RUN_REF_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_realm_offset.to_be_bytes());
    key
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

fn parse_area_index(area: &str) -> u16 {
    area.strip_prefix("area-")
        .and_then(|value| value.parse::<u16>().ok())
        .expect("parse area index")
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

fn build_record_payload(
    stream_index: usize,
    record_index: usize,
    profile: PayloadProfile,
) -> (Bytes, Option<Bytes>) {
    let body_seed = ((stream_index as u8).wrapping_mul(17)).wrapping_add(record_index as u8);
    let metadata_seed = body_seed.wrapping_add(53);
    let body_seed = deterministic_seed(stream_index, record_index, body_seed as u64);
    let metadata_seed =
        deterministic_seed(stream_index, record_index, metadata_seed as u64 ^ 0xA5A5);

    match profile {
        PayloadProfile::LowEntropy => (
            build_low_entropy_bytes(BODY_BYTES, body_seed),
            Some(build_low_entropy_bytes(METADATA_BYTES, metadata_seed)),
        ),
        PayloadProfile::HighEntropy => (
            build_high_entropy_bytes(BODY_BYTES, body_seed),
            Some(build_high_entropy_bytes(METADATA_BYTES, metadata_seed)),
        ),
        PayloadProfile::ProductionLike => match ((stream_index * 17) + record_index) % 4 {
            0 => (
                build_padded_text(
                    format!("event-{record_index:04} stream-{stream_index:02} "),
                    PRODUCTION_LIKE_SMALL_EVENT_BYTES,
                    body_seed,
                ),
                None,
            ),
            1 => (
                build_json_like_bytes(
                    format!(
                        "{{\"event\":\"commit\",\"stream\":{stream_index},\"seq\":{record_index},\"message\":\""
                    ),
                    PRODUCTION_LIKE_JSON_BODY_BYTES,
                    body_seed,
                ),
                Some(build_tag_bytes(
                    PRODUCTION_LIKE_JSON_METADATA_BYTES,
                    metadata_seed,
                    stream_index,
                    record_index,
                )),
            ),
            2 => (
                build_high_entropy_bytes(PRODUCTION_LIKE_BINARY_BODY_BYTES, body_seed),
                Some(build_high_entropy_bytes(
                    PRODUCTION_LIKE_BINARY_METADATA_BYTES,
                    metadata_seed,
                )),
            ),
            _ => (
                build_padded_text(
                    format!(
                        "ts={:08x} lvl=info stream={stream_index} seq={record_index} msg=",
                        body_seed as u32
                    ),
                    PRODUCTION_LIKE_LOG_BODY_BYTES,
                    body_seed ^ 0xDE_AD_BE_EF,
                ),
                Some(build_tag_bytes(
                    PRODUCTION_LIKE_LOG_METADATA_BYTES,
                    metadata_seed ^ 0xC6_A4_A7_93,
                    stream_index,
                    record_index,
                )),
            ),
        },
    }
}

fn build_records(profile: PayloadProfile) -> Vec<LayoutRecord> {
    let mut records = Vec::with_capacity(AREA_COUNT * STREAMS_PER_AREA * RECORDS_PER_STREAM);
    let mut next_area_offsets = [0u64; AREA_COUNT];
    let mut next_realm_offset = 0u64;

    for record_index in 0..RECORDS_PER_STREAM {
        for (area_index, area_next_offset) in next_area_offsets.iter_mut().enumerate() {
            let area = format!("area-{area_index}");
            for stream_index in 0..STREAMS_PER_AREA {
                let global_stream_index = area_index * STREAMS_PER_AREA + stream_index;
                let area_offset = *area_next_offset;
                *area_next_offset += 1;
                let (body, metadata) =
                    build_record_payload(global_stream_index, record_index, profile);

                records.push(LayoutRecord {
                    area: area.clone(),
                    resource: format!("resource-{area_index}-{stream_index}"),
                    resource_offset: record_index as u64,
                    area_offset,
                    realm_offset: next_realm_offset,
                    body,
                    metadata,
                    created_at: ((global_stream_index as u64) << 32) | record_index as u64,
                });
                next_realm_offset += 1;
            }
        }
    }

    records
}

fn summarize_current_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();

        area_plane_bytes += encode_area_key(REALM, &record.area, record.area_offset).len();
        area_plane_bytes += AreaValue {
            resource_offset: record.resource_offset,
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();

        realm_plane_bytes += encode_realm_key(REALM, record.realm_offset).len();
        realm_plane_bytes += RealmValue {
            area_offset: record.area_offset,
            resource_offset: record.resource_offset,
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_hybrid_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();

        area_plane_bytes += encode_area_key(REALM, &record.area, record.area_offset).len();
        area_plane_bytes += AreaValue {
            resource_offset: record.resource_offset,
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes += encode_compact_paged_realm_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactPagedRealmValue {
            records: page
                .iter()
                .map(|record| CompactPagedRealmRecord {
                    resource_offset: record.resource_offset,
                    area_offset: record.area_offset,
                    body: record.body.clone(),
                    metadata: record.metadata.clone(),
                    created_at: record.created_at,
                })
                .collect(),
        }
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_area_paged_realm_body_hybrid_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page[0].area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes += encode_compact_paged_realm_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactPagedRealmValue {
            records: page
                .iter()
                .map(|record| CompactPagedRealmRecord {
                    resource_offset: record.resource_offset,
                    area_offset: record.area_offset,
                    body: record.body.clone(),
                    metadata: record.metadata.clone(),
                    created_at: record.created_at,
                })
                .collect(),
        }
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_two_body_hybrid_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();

        area_plane_bytes += encode_area_key(REALM, &record.area, record.area_offset).len();
        area_plane_bytes += AreaValue {
            resource_offset: record.resource_offset,
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes += encode_compact_realm_area_ref_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactRealmAreaRefPageValue {
            records: page
                .iter()
                .map(|record| CompactRealmAreaRefRecord {
                    area_index: record
                        .area
                        .strip_prefix("area-")
                        .and_then(|value| value.parse::<u16>().ok())
                        .expect("parse area index"),
                    area_offset: record.area_offset,
                })
                .collect(),
        }
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_area_page_ref_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page[0].area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes +=
            encode_compact_realm_area_page_ref_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactRealmAreaPageRefValue {
            records: page
                .iter()
                .map(|record| CompactRealmAreaPageRefRecord {
                    area_index: parse_area_index(&record.area),
                    area_page_start_offset: record.area_offset / REALM_PAGE_RECORD_LIMIT as u64
                        * REALM_PAGE_RECORD_LIMIT as u64,
                    slot: (record.area_offset % REALM_PAGE_RECORD_LIMIT as u64) as u16,
                })
                .collect(),
        }
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_area_page_id_ref_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    let mut area_page_ids = HashMap::<(u16, u64), u32>::new();
    let mut next_area_page_id = 0u32;

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            let page_start_area_offset = page[0].area_offset;
            area_page_ids.insert(
                (area_index as u16, page_start_area_offset),
                next_area_page_id,
            );
            next_area_page_id += 1;

            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page_start_area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes +=
            encode_compact_realm_page_id_ref_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactRealmPageIdRefValue {
            records: page
                .iter()
                .map(|record| {
                    let area_index = parse_area_index(&record.area);
                    let area_page_start_offset = record.area_offset
                        / REALM_PAGE_RECORD_LIMIT as u64
                        * REALM_PAGE_RECORD_LIMIT as u64;
                    let page_id = area_page_ids
                        .get(&(area_index, area_page_start_offset))
                        .copied()
                        .expect("missing area page id for write-shape layout");
                    CompactRealmPageIdRefRecord {
                        page_id,
                        slot: (record.area_offset % REALM_PAGE_RECORD_LIMIT as u64) as u16,
                    }
                })
                .collect(),
        }
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_area_page_run_ref_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    let mut area_page_ids = HashMap::<(u16, u64), u32>::new();
    let mut next_area_page_id = 0u32;

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            let page_start_area_offset = page[0].area_offset;
            area_page_ids.insert(
                (area_index as u16, page_start_area_offset),
                next_area_page_id,
            );
            next_area_page_id += 1;

            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page_start_area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes +=
            encode_compact_realm_page_run_ref_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactRealmPageRunRefValue {
            runs: page
                .iter()
                .map(|record| {
                    let area_index = parse_area_index(&record.area);
                    let area_page_start_offset = record.area_offset
                        / REALM_PAGE_RECORD_LIMIT as u64
                        * REALM_PAGE_RECORD_LIMIT as u64;
                    let page_id = area_page_ids
                        .get(&(area_index, area_page_start_offset))
                        .copied()
                        .expect("missing area page id for run-ref layout");
                    (
                        page_id,
                        (record.area_offset % REALM_PAGE_RECORD_LIMIT as u64) as u16,
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
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_area_body_canonical_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    let mut area_page_ids = HashMap::<(u16, u64), u32>::new();
    let mut next_area_page_id = 0u32;

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            let page_start_area_offset = page[0].area_offset;
            area_page_ids.insert(
                (area_index as u16, page_start_area_offset),
                next_area_page_id,
            );
            next_area_page_id += 1;

            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page_start_area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for area_index in 0..AREA_COUNT {
        let area_name = format!("area-{area_index}");
        for stream_index in 0..STREAMS_PER_AREA {
            let resource_name = format!("resource-{area_index}-{stream_index}");
            let resource_records = records
                .iter()
                .filter(|record| record.area == area_name && record.resource == resource_name)
                .collect::<Vec<_>>();

            for page in resource_records.chunks(REALM_PAGE_RECORD_LIMIT) {
                resource_plane_bytes += encode_compact_resource_area_page_ref_key(
                    REALM,
                    &area_name,
                    &resource_name,
                    page[0].resource_offset,
                )
                .len();
                resource_plane_bytes += CompactResourceAreaPageRefValue {
                    records: page
                        .iter()
                        .map(|record| CompactResourceAreaPageRefRecord {
                            area_page_start_offset: record.area_offset
                                / REALM_PAGE_RECORD_LIMIT as u64
                                * REALM_PAGE_RECORD_LIMIT as u64,
                            slot: (record.area_offset % REALM_PAGE_RECORD_LIMIT as u64) as u16,
                            realm_offset: record.realm_offset,
                        })
                        .collect(),
                }
                .encode()
                .len();
            }
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes +=
            encode_compact_realm_page_run_ref_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactRealmPageRunRefValue {
            runs: page
                .iter()
                .map(|record| {
                    let area_index = parse_area_index(&record.area);
                    let area_page_start_offset = record.area_offset
                        / REALM_PAGE_RECORD_LIMIT as u64
                        * REALM_PAGE_RECORD_LIMIT as u64;
                    let page_id = area_page_ids
                        .get(&(area_index, area_page_start_offset))
                        .copied()
                        .expect("missing area page id for area-body canonical layout");
                    (
                        page_id,
                        (record.area_offset % REALM_PAGE_RECORD_LIMIT as u64) as u16,
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
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_resource_mini_page_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    let mut area_page_ids = HashMap::<(u16, u64), u32>::new();
    let mut next_area_page_id = 0u32;

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            let page_start_area_offset = page[0].area_offset;
            area_page_ids.insert(
                (area_index as u16, page_start_area_offset),
                next_area_page_id,
            );
            next_area_page_id += 1;

            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page_start_area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for area_index in 0..AREA_COUNT {
        let area_name = format!("area-{area_index}");
        for stream_index in 0..STREAMS_PER_AREA {
            let resource_name = format!("resource-{area_index}-{stream_index}");
            let resource_records = records
                .iter()
                .filter(|record| record.area == area_name && record.resource == resource_name)
                .collect::<Vec<_>>();

            for page in resource_records.chunks(REALM_PAGE_RECORD_LIMIT) {
                resource_plane_bytes += encode_compact_resource_page_key(
                    REALM,
                    &area_name,
                    &resource_name,
                    page[0].resource_offset,
                )
                .len();
                resource_plane_bytes += CompactResourcePageValue {
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
                .encode()
                .len();
            }
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes +=
            encode_compact_realm_page_run_ref_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += CompactRealmPageRunRefValue {
            runs: page
                .iter()
                .map(|record| {
                    let area_index = parse_area_index(&record.area);
                    let area_page_start_offset = record.area_offset
                        / REALM_PAGE_RECORD_LIMIT as u64
                        * REALM_PAGE_RECORD_LIMIT as u64;
                    let page_id = area_page_ids
                        .get(&(area_index, area_page_start_offset))
                        .copied()
                        .expect("missing area page id for resource mini-page layout");
                    (
                        page_id,
                        (record.area_offset % REALM_PAGE_RECORD_LIMIT as u64) as u16,
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
        .encode()
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_resource_mini_page_compressed_realm_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let resource_mini_page = summarize_resource_mini_page_layout(records);
    let compressed_realm = summarize_area_paged_compressed_realm_body_layout(records);

    debug_assert_eq!(
        resource_mini_page.area_plane_bytes, compressed_realm.area_plane_bytes,
        "resource mini-page and compressed realm layouts should share the same area plane"
    );

    let total_bytes = resource_mini_page.resource_plane_bytes
        + resource_mini_page.area_plane_bytes
        + compressed_realm.realm_plane_bytes;

    LayoutSummary {
        total_bytes,
        resource_plane_bytes: resource_mini_page.resource_plane_bytes,
        area_plane_bytes: resource_mini_page.area_plane_bytes,
        realm_plane_bytes: compressed_realm.realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn summarize_area_paged_compressed_realm_body_layout(records: &[LayoutRecord]) -> LayoutSummary {
    let mut resource_plane_bytes = 0usize;
    let mut area_plane_bytes = 0usize;
    let mut realm_plane_bytes = 0usize;

    for record in records {
        resource_plane_bytes += encode_resource_key(
            REALM,
            &record.area,
            &record.resource,
            record.resource_offset,
        )
        .len();
        resource_plane_bytes += ResourceValue {
            resource_offset: record.resource_offset,
            area_offset: Some(record.area_offset),
            realm_offset: Some(record.realm_offset),
            body: record.body.clone(),
            metadata: record.metadata.clone(),
            created_at: record.created_at,
        }
        .encode()
        .len();
    }

    let mut area_records = vec![Vec::new(); AREA_COUNT];
    for record in records {
        area_records[parse_area_index(&record.area) as usize].push(record);
    }

    for (area_index, records_for_area) in area_records.iter().enumerate() {
        let area_name = format!("area-{area_index}");
        for page in records_for_area.chunks(REALM_PAGE_RECORD_LIMIT) {
            area_plane_bytes +=
                encode_compact_area_page_key(REALM, &area_name, page[0].area_offset).len();
            area_plane_bytes += CompactAreaPageValue {
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
            .encode()
            .len();
        }
    }

    for page in records.chunks(REALM_PAGE_RECORD_LIMIT) {
        realm_plane_bytes += encode_compact_paged_realm_key(REALM, page[0].realm_offset).len();
        realm_plane_bytes += compress_prepend_size(
            &CompactPagedRealmValue {
                records: page
                    .iter()
                    .map(|record| CompactPagedRealmRecord {
                        resource_offset: record.resource_offset,
                        area_offset: record.area_offset,
                        body: record.body.clone(),
                        metadata: record.metadata.clone(),
                        created_at: record.created_at,
                    })
                    .collect(),
            }
            .encode(),
        )
        .len();
    }

    let total_bytes = resource_plane_bytes + area_plane_bytes + realm_plane_bytes;
    LayoutSummary {
        total_bytes,
        resource_plane_bytes,
        area_plane_bytes,
        realm_plane_bytes,
        bytes_per_event: total_bytes as f64 / records.len() as f64,
    }
}

fn bench_stream_write_shape(c: &mut Criterion) {
    let records = build_records(PayloadProfile::LowEntropy);
    let high_entropy_records = build_records(PayloadProfile::HighEntropy);
    let production_like_records = build_records(PayloadProfile::ProductionLike);
    let current = summarize_current_layout(&records);
    let hybrid = summarize_hybrid_layout(&records);
    let area_paged_realm_body = summarize_area_paged_realm_body_hybrid_layout(&records);
    let area_paged_compressed_realm_body =
        summarize_area_paged_compressed_realm_body_layout(&records);
    let high_entropy_area_paged_compressed_realm_body =
        summarize_area_paged_compressed_realm_body_layout(&high_entropy_records);
    let production_like_current = summarize_current_layout(&production_like_records);
    let production_like_area_paged_realm_body =
        summarize_area_paged_realm_body_hybrid_layout(&production_like_records);
    let production_like_area_paged_compressed_realm_body =
        summarize_area_paged_compressed_realm_body_layout(&production_like_records);
    let area_body_canonical = summarize_area_body_canonical_layout(&records);
    let resource_mini_page = summarize_resource_mini_page_layout(&records);
    let resource_mini_page_compressed_realm =
        summarize_resource_mini_page_compressed_realm_layout(&records);
    let production_like_resource_mini_page_compressed_realm =
        summarize_resource_mini_page_compressed_realm_layout(&production_like_records);
    let two_body_hybrid = summarize_two_body_hybrid_layout(&records);
    let area_page_ref = summarize_area_page_ref_layout(&records);
    let area_page_id_ref = summarize_area_page_id_ref_layout(&records);
    let area_page_run_ref = summarize_area_page_run_ref_layout(&records);
    let reduction = 100.0 * (1.0 - (hybrid.total_bytes as f64 / current.total_bytes as f64));
    let area_paged_realm_body_reduction =
        100.0 * (1.0 - (area_paged_realm_body.total_bytes as f64 / current.total_bytes as f64));
    let area_paged_compressed_realm_body_reduction = 100.0
        * (1.0
            - (area_paged_compressed_realm_body.total_bytes as f64 / current.total_bytes as f64));
    let two_body_reduction =
        100.0 * (1.0 - (two_body_hybrid.total_bytes as f64 / current.total_bytes as f64));
    let area_page_ref_reduction =
        100.0 * (1.0 - (area_page_ref.total_bytes as f64 / current.total_bytes as f64));
    let area_page_id_ref_reduction =
        100.0 * (1.0 - (area_page_id_ref.total_bytes as f64 / current.total_bytes as f64));
    let area_page_run_ref_reduction =
        100.0 * (1.0 - (area_page_run_ref.total_bytes as f64 / current.total_bytes as f64));
    let area_body_canonical_reduction =
        100.0 * (1.0 - (area_body_canonical.total_bytes as f64 / current.total_bytes as f64));
    let resource_mini_page_reduction =
        100.0 * (1.0 - (resource_mini_page.total_bytes as f64 / current.total_bytes as f64));
    let resource_mini_page_compressed_realm_reduction = 100.0
        * (1.0
            - (resource_mini_page_compressed_realm.total_bytes as f64
                / current.total_bytes as f64));
    let production_like_reduction = 100.0
        * (1.0
            - (production_like_area_paged_compressed_realm_body.total_bytes as f64
                / production_like_current.total_bytes as f64));
    let production_like_vs_uncompressed_reduction = 100.0
        * (1.0
            - (production_like_area_paged_compressed_realm_body.total_bytes as f64
                / production_like_area_paged_realm_body.total_bytes as f64));
    let production_like_resource_mini_page_compressed_realm_reduction = 100.0
        * (1.0
            - (production_like_resource_mini_page_compressed_realm.total_bytes as f64
                / production_like_current.total_bytes as f64));
    let event_count = records.len() as u64;

    eprintln!(
        "stream write-shape economics: current {:.2} B/event (resource {} area {} realm {}), hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, area-paged realm-body hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, area-paged compressed realm-body hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, two-body hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, area-page-ref hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, area-page-id-ref hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, area-page-run-ref hybrid {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, area-body canonical {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, resource mini-page {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%, resource mini-page + compressed realm {:.2} B/event (resource {} area {} realm {}), reduction {:.2}%",
        current.bytes_per_event,
        current.resource_plane_bytes,
        current.area_plane_bytes,
        current.realm_plane_bytes,
        hybrid.bytes_per_event,
        hybrid.resource_plane_bytes,
        hybrid.area_plane_bytes,
        hybrid.realm_plane_bytes,
        reduction,
        area_paged_realm_body.bytes_per_event,
        area_paged_realm_body.resource_plane_bytes,
        area_paged_realm_body.area_plane_bytes,
        area_paged_realm_body.realm_plane_bytes,
        area_paged_realm_body_reduction,
        area_paged_compressed_realm_body.bytes_per_event,
        area_paged_compressed_realm_body.resource_plane_bytes,
        area_paged_compressed_realm_body.area_plane_bytes,
        area_paged_compressed_realm_body.realm_plane_bytes,
        area_paged_compressed_realm_body_reduction,
        two_body_hybrid.bytes_per_event,
        two_body_hybrid.resource_plane_bytes,
        two_body_hybrid.area_plane_bytes,
        two_body_hybrid.realm_plane_bytes,
        two_body_reduction,
        area_page_ref.bytes_per_event,
        area_page_ref.resource_plane_bytes,
        area_page_ref.area_plane_bytes,
        area_page_ref.realm_plane_bytes,
        area_page_ref_reduction,
        area_page_id_ref.bytes_per_event,
        area_page_id_ref.resource_plane_bytes,
        area_page_id_ref.area_plane_bytes,
        area_page_id_ref.realm_plane_bytes,
        area_page_id_ref_reduction,
        area_page_run_ref.bytes_per_event,
        area_page_run_ref.resource_plane_bytes,
        area_page_run_ref.area_plane_bytes,
        area_page_run_ref.realm_plane_bytes,
        area_page_run_ref_reduction,
        area_body_canonical.bytes_per_event,
        area_body_canonical.resource_plane_bytes,
        area_body_canonical.area_plane_bytes,
        area_body_canonical.realm_plane_bytes,
        area_body_canonical_reduction,
        resource_mini_page.bytes_per_event,
        resource_mini_page.resource_plane_bytes,
        resource_mini_page.area_plane_bytes,
        resource_mini_page.realm_plane_bytes,
        resource_mini_page_reduction,
        resource_mini_page_compressed_realm.bytes_per_event,
        resource_mini_page_compressed_realm.resource_plane_bytes,
        resource_mini_page_compressed_realm.area_plane_bytes,
        resource_mini_page_compressed_realm.realm_plane_bytes,
        resource_mini_page_compressed_realm_reduction,
    );
    eprintln!(
        "stream write-shape compression sensitivity: uncompressed area-paged realm-body {:.2} B/event, compressed low-entropy {:.2} B/event, compressed high-entropy {:.2} B/event",
        area_paged_realm_body.bytes_per_event,
        area_paged_compressed_realm_body.bytes_per_event,
        high_entropy_area_paged_compressed_realm_body.bytes_per_event,
    );
    eprintln!(
        "stream write-shape production-like corpus: current {:.2} B/event, uncompressed area-paged realm-body {:.2} B/event, compressed realm-body {:.2} B/event, resource mini-page + compressed realm {:.2} B/event, compressed reduction vs current {:.2}%, compressed reduction vs uncompressed {:.2}%, mini-page + compressed reduction vs current {:.2}%",
        production_like_current.bytes_per_event,
        production_like_area_paged_realm_body.bytes_per_event,
        production_like_area_paged_compressed_realm_body.bytes_per_event,
        production_like_resource_mini_page_compressed_realm.bytes_per_event,
        production_like_reduction,
        production_like_vs_uncompressed_reduction,
        production_like_resource_mini_page_compressed_realm_reduction,
    );

    assert!(
        hybrid.total_bytes < current.total_bytes,
        "hybrid layout should write fewer bytes than current layout"
    );
    assert!(
        area_paged_realm_body.total_bytes < hybrid.total_bytes,
        "area-paged realm-body hybrid should write fewer bytes than covering-area hybrid"
    );
    assert!(
        area_paged_compressed_realm_body.total_bytes < area_paged_realm_body.total_bytes,
        "compressed realm-body hybrid should write fewer bytes than uncompressed realm-body hybrid"
    );
    assert!(
        production_like_area_paged_compressed_realm_body.total_bytes
            < production_like_area_paged_realm_body.total_bytes,
        "compressed realm-body hybrid should still beat the uncompressed realm-body layout on the production-like corpus"
    );
    assert!(
        two_body_hybrid.total_bytes < hybrid.total_bytes,
        "two-body hybrid should write fewer bytes than realm-body hybrid layout"
    );
    assert!(
        area_page_id_ref.total_bytes < area_page_ref.total_bytes,
        "page-id ref layout should write fewer bytes than page-offset ref layout"
    );
    assert!(
        area_page_run_ref.total_bytes < area_page_id_ref.total_bytes,
        "run-ref layout should write fewer bytes than page-id ref layout"
    );
    assert!(
        area_body_canonical.total_bytes < area_page_run_ref.total_bytes,
        "area-body canonical layout should write fewer bytes than the current area-page run-ref hybrid"
    );
    assert!(
        resource_mini_page.total_bytes > area_body_canonical.total_bytes,
        "resource mini-page layout should cost more than the no-resource-body canonical layout"
    );
    assert!(
        resource_mini_page.total_bytes < current.total_bytes,
        "resource mini-page layout should still reduce bytes versus current layout"
    );
    assert!(
        resource_mini_page_compressed_realm.total_bytes > resource_mini_page.total_bytes,
        "adding compressed realm bodies should cost more than the pure run-ref resource mini-page layout"
    );
    assert!(
        resource_mini_page_compressed_realm.total_bytes
            < area_paged_compressed_realm_body.total_bytes,
        "resource mini-page plus compressed realm layout should still beat the current resource covering compressed-realm hybrid"
    );
    assert!(
        production_like_resource_mini_page_compressed_realm.total_bytes
            < production_like_area_paged_compressed_realm_body.total_bytes,
        "resource mini-page plus compressed realm layout should still beat the covering-resource compressed-realm hybrid on the production-like corpus"
    );

    let mut group = c.benchmark_group("subsystem_stream_write_shape");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(event_count));

    group.bench_function("assemble_current_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_current_layout(black_box(&records))))
    });

    group.bench_function("assemble_hybrid_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_hybrid_layout(black_box(&records))))
    });

    group.bench_function(
        "assemble_area_paged_realm_body_hybrid_layout_2048_records",
        |b| {
            b.iter(|| {
                black_box(summarize_area_paged_realm_body_hybrid_layout(black_box(
                    &records,
                )))
            })
        },
    );

    group.bench_function(
        "assemble_area_paged_compressed_realm_body_hybrid_layout_2048_records",
        |b| {
            b.iter(|| {
                black_box(summarize_area_paged_compressed_realm_body_layout(
                    black_box(&records),
                ))
            })
        },
    );
    group.bench_function(
        "assemble_area_paged_compressed_realm_body_hybrid_layout_2048_records_high_entropy",
        |b| {
            b.iter(|| {
                black_box(summarize_area_paged_compressed_realm_body_layout(
                    black_box(&high_entropy_records),
                ))
            })
        },
    );
    group.bench_function(
        "assemble_area_paged_realm_body_hybrid_layout_2048_records_production_like",
        |b| {
            b.iter(|| {
                black_box(summarize_area_paged_realm_body_hybrid_layout(black_box(
                    &production_like_records,
                )))
            })
        },
    );
    group.bench_function(
        "assemble_area_paged_compressed_realm_body_hybrid_layout_2048_records_production_like",
        |b| {
            b.iter(|| {
                black_box(summarize_area_paged_compressed_realm_body_layout(
                    black_box(&production_like_records),
                ))
            })
        },
    );

    group.bench_function("assemble_two_body_hybrid_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_two_body_hybrid_layout(black_box(&records))))
    });

    group.bench_function("assemble_area_page_ref_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_area_page_ref_layout(black_box(&records))))
    });

    group.bench_function("assemble_area_page_id_ref_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_area_page_id_ref_layout(black_box(&records))))
    });

    group.bench_function("assemble_area_page_run_ref_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_area_page_run_ref_layout(black_box(&records))))
    });

    group.bench_function("assemble_area_body_canonical_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_area_body_canonical_layout(black_box(&records))))
    });

    group.bench_function("assemble_resource_mini_page_layout_2048_records", |b| {
        b.iter(|| black_box(summarize_resource_mini_page_layout(black_box(&records))))
    });

    group.bench_function(
        "assemble_resource_mini_page_compressed_realm_layout_2048_records",
        |b| {
            b.iter(|| {
                black_box(summarize_resource_mini_page_compressed_realm_layout(
                    black_box(&records),
                ))
            })
        },
    );
    group.bench_function(
        "assemble_resource_mini_page_compressed_realm_layout_2048_records_production_like",
        |b| {
            b.iter(|| {
                black_box(summarize_resource_mini_page_compressed_realm_layout(
                    black_box(&production_like_records),
                ))
            })
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_stream_write_shape
}
criterion_main!(benches);
