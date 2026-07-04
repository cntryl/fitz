#![allow(deprecated)]
//! Stream storage-model tier 3 benchmarks using stress.
//!
//! These are bench-only prototype replay measurements for the current Tier 2
//! Stream redesign frontier. They do not exercise the live domain sink. They
//! measure the current covering replay surfaces against the combined area-first
//! candidate: compact area pages, compact resource mini-pages, and compressed
//! compact realm-body pages.

#[path = "stress_config.rs"]
mod stress_config;

use bytes::{BufMut, Bytes};
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    count_stream_read_records_from_payload, create_local_bench_store, extract_single_tlv_field,
    register_session_queue_sink, route_raw_frame, FrameQueueSink,
};
use fitz::domains::stream::protocol::{
    StreamClientFrame, StreamClientResponse, StreamClientResponseBody, StreamMessage, StreamRecord,
    StreamWriteMode,
};
use fitz::domains::stream::storage::{decode_area_offset_from_key, decode_realm_offset_from_key};
use fitz::domains::stream::store::{
    CommitRecordsParams, EventPayload, ReadResourceParams, StreamStore,
};
use fitz::domains::stream::{StreamReadItem, StreamRecord as DomainStreamRecord};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{DeliveryError, MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::SessionId;
use fitz::testkit::transport::TlvFrameBuilder;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const FAMILY: u64 = 1;
const CLIENT_SESSION_ID: u64 = 1;
const REALM: &str = "bench-realm";
const PRODUCTION_LIKE_SMALL_EVENT_BYTES: usize = 40;
const PRODUCTION_LIKE_JSON_BODY_BYTES: usize = 160;
const PRODUCTION_LIKE_BINARY_BODY_BYTES: usize = 192;
const PRODUCTION_LIKE_LOG_BODY_BYTES: usize = 120;
const PRODUCTION_LIKE_JSON_METADATA_BYTES: usize = 48;
const PRODUCTION_LIKE_BINARY_METADATA_BYTES: usize = 16;
const PRODUCTION_LIKE_LOG_METADATA_BYTES: usize = 32;
const REPLAY_PAGE_RECORD_LIMIT: usize = 64;
const COMPACT_AREA_PAGE_KEY_PREFIX: u8 = 0xE4;
const COMPRESSED_COMPACT_PAGED_REALM_KEY_PREFIX: u8 = 0xE8;
const COMPACT_RESOURCE_PAGE_KEY_PREFIX: u8 = 0xEA;
const ASCII_TOKEN_BANK: [&str; 12] = [
    "stream", "event", "commit", "cursor", "tenant", "region", "audit", "batch", "order", "delta",
    "notify", "writer",
];

#[derive(Clone)]
struct PrototypeStream {
    area: String,
    resource: String,
}

struct PrototypeRowWrite {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct SeedRecord {
    stream_index: usize,
    area_index: usize,
    resource_offset: u64,
    area_offset: u64,
    realm_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
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
struct CompactPagedRealmValue {
    records: Vec<CompactPagedRealmRecord>,
}

#[derive(Debug, Clone)]
struct CompactPagedRealmRecord {
    resource_offset: u64,
    area_offset: u64,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

struct ReplayCase {
    store: StreamStore,
    db: Arc<cntryl_midge::Engine>,
    _temp_dir: tempfile::TempDir,
    areas: Vec<String>,
    streams: Vec<PrototypeStream>,
    expected_records: usize,
}

#[derive(Clone, Copy)]
enum RoutedReadLayout {
    CurrentCovering,
    PromotionFrontier,
}

struct RoutedBenchContext {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
}

struct PrototypeStreamReadSink {
    router: Arc<Router>,
    case: Arc<ReplayCase>,
    layout: RoutedReadLayout,
}

const COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xB2];
const COMPACT_AREA_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE4];
const COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE8];
const COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xEA];
const OPTIONAL_BYTES_ABSENT: u32 = u32::MAX;

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn u64_to_u32_saturating(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn low_u32(value: u64) -> u32 {
    u32::try_from(value & u64::from(u32::MAX)).expect("masked value fits in u32")
}

impl CompactAreaPageValue {
    fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, bytes::Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_AREA_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.records.len()).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&usize_to_u32_saturating(record.body.len()).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(OPTIONAL_BYTES_ABSENT, |metadata| {
                        usize_to_u32_saturating(metadata.len())
                    })
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

impl CompactResourcePageValue {
    fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, bytes::Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.records.len()).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.realm_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&usize_to_u32_saturating(record.body.len()).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(OPTIONAL_BYTES_ABSENT, |metadata| {
                        usize_to_u32_saturating(metadata.len())
                    })
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

impl CompactPagedRealmValue {
    fn encode(&self) -> Vec<u8> {
        let mut total_len = 6;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map_or(0, bytes::Bytes::len);
        }

        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&COMPACT_REALM_PAGE_VALUE_V1_MARKER);
        bytes.extend_from_slice(&usize_to_u32_saturating(self.records.len()).to_le_bytes());

        for record in &self.records {
            bytes.extend_from_slice(&record.area_offset.to_le_bytes());
            bytes.extend_from_slice(&record.resource_offset.to_le_bytes());
            bytes.extend_from_slice(&record.created_at.to_le_bytes());
            bytes.extend_from_slice(&usize_to_u32_saturating(record.body.len()).to_le_bytes());
            bytes.extend_from_slice(
                &record
                    .metadata
                    .as_ref()
                    .map_or(OPTIONAL_BYTES_ABSENT, |metadata| {
                        usize_to_u32_saturating(metadata.len())
                    })
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

            records.push(CompactPagedRealmRecord {
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

fn build_ascii_fill(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    let mut bytes = Vec::with_capacity(len);

    while bytes.len() < len {
        state = next_deterministic_state(state);
        let token_count = usize_to_u64_saturating(ASCII_TOKEN_BANK.len());
        let token = ASCII_TOKEN_BANK[u64_to_usize_saturating(state % token_count)].as_bytes();
        let hex = format!("{:08x}", low_u32(state));

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
            body: {
                let mut state = body_seed;
                let mut bytes = Vec::with_capacity(PRODUCTION_LIKE_BINARY_BODY_BYTES);
                while bytes.len() < PRODUCTION_LIKE_BINARY_BODY_BYTES {
                    state = next_deterministic_state(state);
                    bytes.push((state & 0xFF) as u8);
                }
                Bytes::from(bytes)
            },
            metadata: Some({
                let mut state = metadata_seed;
                let mut bytes = Vec::with_capacity(PRODUCTION_LIKE_BINARY_METADATA_BYTES);
                while bytes.len() < PRODUCTION_LIKE_BINARY_METADATA_BYTES {
                    state = next_deterministic_state(state);
                    bytes.push((state & 0xFF) as u8);
                }
                Bytes::from(bytes)
            }),
            discriminator: None,
        },
        _ => EventPayload {
            body: build_padded_text(
                format!(
                    "ts={:08x} lvl=info stream={stream_index} seq={record_index} msg=",
                    low_u32(body_seed)
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

fn encode_compact_area_page_key(realm: &str, area: &str, page_start_area_offset: u64) -> Vec<u8> {
    let mut key = vec![COMPACT_AREA_PAGE_KEY_PREFIX];
    key.extend_from_slice(realm.as_bytes());
    key.push(0);
    key.extend_from_slice(area.as_bytes());
    key.push(0);
    key.extend_from_slice(&page_start_area_offset.to_be_bytes());
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

fn decode_resource_offset_from_key(key: &[u8]) -> Result<u64, String> {
    if key.len() < 8 {
        return Err("key too short".to_string());
    }

    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[key.len() - 8..]);
    Ok(u64::from_be_bytes(bytes))
}

fn persist_prototype_rows(
    db: &Arc<cntryl_midge::Engine>,
    writes: &[PrototypeRowWrite],
) -> Result<(), String> {
    let mut txn = db
        .begin_tx(
            u64_to_u32_saturating(FAMILY),
            cntryl_midge::TransactionMode::ReadWrite,
        )
        .map_err(|error| format!("begin_tx failed: {error:?}"))?;

    for row in writes {
        txn.put(row.key.clone(), row.value.clone(), None)
            .map_err(|error| format!("txn put failed: {error:?}"))?;
    }

    txn.commit(cntryl_midge::WriteOptions::buffered())
        .map_err(|error| format!("txn commit failed: {error:?}"))
}

#[allow(clippy::too_many_lines)]
fn seed_replay_case(
    area_count: usize,
    streams_per_area: usize,
    records_per_stream: usize,
) -> ReplayCase {
    let (db, temp_dir) = create_local_bench_store();
    let store = StreamStore::new(db.clone());
    let areas = (0..area_count)
        .map(|area_index| format!("area-{area_index}"))
        .collect::<Vec<_>>();
    let mut streams = Vec::with_capacity(area_count * streams_per_area);

    for (area_index, area) in areas.iter().cloned().enumerate() {
        for resource_index in 0..streams_per_area {
            let _ = area_index;
            streams.push(PrototypeStream {
                area: area.clone(),
                resource: format!("resource-{area_index}-{resource_index}"),
            });
        }
    }

    let mut prototype_rows =
        Vec::with_capacity(area_count * streams_per_area * records_per_stream * 3);
    let mut next_resource_offsets = vec![0u64; streams.len()];
    let mut seed_records = Vec::with_capacity(area_count * streams_per_area * records_per_stream);

    for record_index in 0..records_per_stream {
        for (stream_index, stream) in streams.iter().enumerate() {
            let event = build_production_like_payload(stream_index, record_index);
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

            seed_records.push(SeedRecord {
                stream_index,
                area_index: stream_index / streams_per_area,
                resource_offset: commit.first_resource_offset,
                area_offset: commit.first_area_offset,
                realm_offset: commit.first_realm_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at: ((stream_index as u64) << 32) | record_index as u64,
            });
        }
    }

    let mut area_seed_records = vec![Vec::new(); areas.len()];
    for record in &seed_records {
        area_seed_records[record.area_index].push(record.clone());
    }

    for (area_index, area_records) in area_seed_records.iter().enumerate() {
        let area_name = &areas[area_index];
        for page in area_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
            prototype_rows.push(PrototypeRowWrite {
                key: encode_compact_area_page_key(REALM, area_name, page[0].area_offset),
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
    for record in &seed_records {
        stream_seed_records[record.stream_index].push(record.clone());
    }

    for (stream_index, resource_records) in stream_seed_records.iter().enumerate() {
        let stream = &streams[stream_index];
        for page in resource_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
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

    for page in seed_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
        prototype_rows.push(PrototypeRowWrite {
            key: encode_compressed_compact_paged_realm_key(REALM, page[0].realm_offset),
            value: {
                let compressed_payload = compress_prepend_size(
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
                );
                let mut bytes = Vec::with_capacity(
                    COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER.len() + compressed_payload.len(),
                );
                bytes.extend_from_slice(&COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER);
                bytes.extend_from_slice(&compressed_payload);
                bytes
            },
        });
    }

    persist_prototype_rows(&db, &prototype_rows).expect("persist prototype rows");

    ReplayCase {
        store,
        db,
        _temp_dir: temp_dir,
        areas,
        streams,
        expected_records: area_count * streams_per_area * records_per_stream,
    }
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
        limit: usize_to_u64_saturating(limit),
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
        .begin_tx(
            u64_to_u32_saturating(FAMILY),
            cntryl_midge::TransactionMode::ReadOnly,
        )
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

fn read_area_covering(case: &ReplayCase, area: &str) -> Result<Vec<StreamRecord>, String> {
    let (records, _) = case.store.read_area(
        FAMILY,
        REALM,
        area,
        0,
        usize_to_u64_saturating(case.expected_records),
        None,
    )?;
    Ok(event_records(records))
}

fn read_area_compact_paged(case: &ReplayCase, area: &str) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_watermark(FAMILY, REALM, area)?;
    let txn = case
        .db
        .begin_tx(
            u64_to_u32_saturating(FAMILY),
            cntryl_midge::TransactionMode::ReadOnly,
        )
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

fn read_realm_covering(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let (records, _) = case.store.read_realm(
        FAMILY,
        REALM,
        0,
        usize_to_u64_saturating(case.expected_records),
        None,
    )?;
    Ok(event_records(records))
}

fn event_records(items: Vec<StreamReadItem>) -> Vec<DomainStreamRecord> {
    items
        .into_iter()
        .filter_map(|item| match item {
            StreamReadItem::Event(record) => Some(record),
            _ => None,
        })
        .collect()
}

fn read_realm_compressed_compact_paged(case: &ReplayCase) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_realm_watermark(FAMILY, REALM)?;
    let txn = case
        .db
        .begin_tx(
            u64_to_u32_saturating(FAMILY),
            cntryl_midge::TransactionMode::ReadOnly,
        )
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

fn encode_optional_bytes(encoder: &mut PayloadEncoder, value: Option<&Bytes>) {
    match value {
        Some(bytes) => {
            encoder.put_u8(1);
            encoder.put_bytes(bytes.as_ref());
        }
        None => encoder.put_u8(0),
    }
}

fn encode_stream_record(encoder: &mut PayloadEncoder, record: &StreamRecord) {
    encoder.put_u64(record.resource_offset);
    encoder.put_optional_u64(record.area_offset);
    encoder.put_optional_u64(record.realm_offset);
    encoder.put_bytes(record.body.as_ref());
    encode_optional_bytes(encoder, record.metadata.as_ref());
    encoder.put_u64(record.created_at);
}

fn encode_stream_read_data(
    records: &[StreamRecord],
    from_offset: u64,
    limit: u64,
    max_bytes: Option<usize>,
) -> Vec<u8> {
    let mut selected = Vec::new();
    let mut total_bytes = 0usize;
    let mut has_more = false;

    for record in records
        .iter()
        .filter(|record| record.resource_offset >= from_offset)
    {
        if selected.len() >= u64_to_usize_saturating(limit) {
            has_more = true;
            break;
        }

        if let Some(max_bytes) = max_bytes {
            let projected = total_bytes
                + record.body.len()
                + record.metadata.as_ref().map_or(0, bytes::Bytes::len);
            if !selected.is_empty() && projected > max_bytes {
                has_more = true;
                break;
            }
            total_bytes = projected;
        }

        selected.push(record);
    }

    let last_resource_offset = selected
        .last()
        .map_or(from_offset, |record| record.resource_offset);
    let last_area_offset = selected.last().and_then(|record| record.area_offset);
    let last_realm_offset = selected.last().and_then(|record| record.realm_offset);

    let mut encoder = PayloadEncoder::new();
    encoder.put_u32(usize_to_u32_saturating(selected.len()));
    for record in selected {
        encoder.put_u8(0); // StreamReadItem::Event
        encode_stream_record(&mut encoder, record);
    }
    encoder.put_u64(last_resource_offset);
    encoder.put_optional_u64(last_area_offset);
    encoder.put_optional_u64(last_realm_offset);
    encoder.put_u8(u8::from(has_more));
    encoder.finish()
}

fn parse_stream_read_route(route: &str) -> Result<(String, String, String), String> {
    let raw = if let Some(rest) = route.split_once("://") {
        rest.1
    } else {
        route.trim_start_matches('/')
    };

    let mut parts = raw.split('/');
    let realm = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| "stream read route missing realm".to_string())?;
    let area = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| "stream read route missing area".to_string())?;
    let resource = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| "stream read route missing resource".to_string())?;

    if parts.next().is_some() {
        return Err("stream read routes require exactly 3 segments".to_string());
    }

    Ok((realm.to_string(), area.to_string(), resource.to_string()))
}

fn find_stream<'a>(
    case: &'a ReplayCase,
    area: &str,
    resource: &str,
) -> Result<&'a PrototypeStream, String> {
    case.streams
        .iter()
        .find(|stream| stream.area == area && stream.resource == resource)
        .ok_or_else(|| format!("missing prototype stream for {area}/{resource}"))
}

impl PrototypeStreamReadSink {
    fn new(router: Arc<Router>, case: Arc<ReplayCase>, layout: RoutedReadLayout) -> Self {
        Self {
            router,
            case,
            layout,
        }
    }

    fn encode_read_response_data(
        &self,
        route: &Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>, String> {
        if limit == 0 {
            return Ok(encode_stream_read_data(&[], from_offset, 0, max_bytes));
        }

        let (realm, area, resource) = parse_stream_read_route(route.as_str())?;
        if realm != REALM {
            return Err(format!("unsupported prototype realm {realm}"));
        }

        let records = match self.layout {
            RoutedReadLayout::CurrentCovering => {
                if area == "*" && resource == "*" {
                    read_realm_covering(&self.case)?
                } else if resource == "*" {
                    read_area_covering(&self.case, &area)?
                } else {
                    let stream = find_stream(&self.case, &area, &resource)?;
                    read_resource_covering(&self.case, stream, u64_to_usize_saturating(limit))?
                }
            }
            RoutedReadLayout::PromotionFrontier => {
                if area == "*" && resource == "*" {
                    read_realm_compressed_compact_paged(&self.case)?
                } else if resource == "*" {
                    read_area_compact_paged(&self.case, &area)?
                } else {
                    let stream = find_stream(&self.case, &area, &resource)?;
                    read_resource_compact_paged(&self.case, stream, u64_to_usize_saturating(limit))?
                }
            }
        };

        Ok(encode_stream_read_data(
            &records,
            from_offset,
            limit,
            max_bytes,
        ))
    }
}

impl MailboxSink for PrototypeStreamReadSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let request = Self::request_from_envelope(&envelope)?;
        let meta = request.meta;
        let parsed = request.frame.map_err(|_| DeliveryError::ActorStopped)?;

        let response = match parsed {
            StreamClientFrame::Op(StreamMessage::Read {
                route,
                from_offset,
                limit,
                max_bytes,
                ..
            }) => match self.encode_read_response_data(&route, from_offset, limit, max_bytes) {
                Ok(data) => StreamClientResponseBody::Ok {
                    session_id: None,
                    data,
                },
                Err(error) => StreamClientResponseBody::Error(error),
            },
            _ => StreamClientResponseBody::Error(
                "prototype routed stream sink currently supports only READ operations".to_string(),
            ),
        };

        if let Some(response_envelope) =
            envelope.try_reply_to(StreamClientResponse::new(meta, response))
        {
            let _ = self.router.route(response_envelope);
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl PrototypeStreamReadSink {
    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Result<fitz::domains::stream::protocol::StreamClientRequest, DeliveryError> {
        if let Some(request) =
            envelope.payload::<fitz::domains::stream::protocol::StreamClientRequest>()
        {
            return Ok(request.clone());
        }

        let frame_ctx = envelope
            .payload::<FrameContext>()
            .cloned()
            .ok_or(DeliveryError::ActorStopped)?;
        let subscriber = envelope.source().cloned().unwrap_or_else(|| {
            RouteAddress::new(
                *envelope.destination().family(),
                Route::new(format!("inbox://session/{}", frame_ctx.session_id)),
            )
        });
        let meta = fitz::runtime::ClientFrameMeta::new(
            frame_ctx.session_id,
            client_channel_from_protocol(frame_ctx.channel_id),
            frame_ctx.msg_type.as_u16(),
            frame_ctx.route_family,
        );
        let parsed = fitz::protocol::stream_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            SessionId(frame_ctx.session_id),
            subscriber,
        );

        Ok(fitz::domains::stream::protocol::StreamClientRequest::new(
            meta, parsed,
        ))
    }
}

fn client_channel_from_protocol(channel: ChannelId) -> fitz::runtime::ClientChannel {
    match channel {
        ChannelId::Control => fitz::runtime::ClientChannel::Control,
        ChannelId::Pub => fitz::runtime::ClientChannel::Pub,
        ChannelId::Sub => fitz::runtime::ClientChannel::Sub,
        ChannelId::Rpc => fitz::runtime::ClientChannel::Rpc,
        ChannelId::Lease => fitz::runtime::ClientChannel::Lease,
        ChannelId::Internal => fitz::runtime::ClientChannel::Internal,
    }
}

fn setup_routed_context(case: Arc<ReplayCase>, layout: RoutedReadLayout) -> RoutedBenchContext {
    let family = RouteFamily::new(FAMILY);
    let router = Arc::new(Router::new());
    let sink = Arc::new(PrototypeStreamReadSink::new(router.clone(), case, layout));
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    RoutedBenchContext {
        router,
        family,
        source,
        inbox,
    }
}

fn build_stream_read_with_limit(route: &str, start_offset: u64, limit: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(usize_to_u32_saturating(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(limit);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

fn routed_request(
    context: &RoutedBenchContext,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_raw_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("prototype stream route");

    let responses = context.inbox.drain_after_count(1, Duration::from_secs(1));
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("prototype stream response")
}

fn prepare_validated_routed_read(
    context: &RoutedBenchContext,
    route: &str,
    expected_count: usize,
    limit: u64,
) -> (u16, Bytes) {
    let read_frame = build_stream_read_with_limit(route, 0, limit);
    let (msg_type, payload) = extract_single_tlv_field(&read_frame);
    let response = routed_request(context, route, msg_type, payload.clone());
    let count = count_stream_read_records_from_payload(response.as_ref())
        .expect("prototype routed stream read count");
    assert_eq!(
        count, expected_count,
        "unexpected routed read count for {route}"
    );
    (msg_type, payload)
}

fn validate_resource_case(case: &ReplayCase, stream: &PrototypeStream, expected_records: usize) {
    let covering =
        read_resource_covering(case, stream, expected_records).expect("covering resource replay");
    let candidate = read_resource_compact_paged(case, stream, expected_records)
        .expect("resource mini-page replay");
    assert_eq!(covering.len(), expected_records);
    assert_eq!(candidate.len(), expected_records);
    assert_matching_records(&covering, &candidate);
}

fn validate_area_case(case: &ReplayCase, area: &str) {
    let covering = read_area_covering(case, area).expect("covering area replay");
    let candidate = read_area_compact_paged(case, area).expect("compact area replay");
    assert_eq!(covering.len(), case.expected_records);
    assert_eq!(candidate.len(), case.expected_records);
    assert_matching_records(&covering, &candidate);
}

fn validate_realm_case(case: &ReplayCase) {
    let covering = read_realm_covering(case).expect("covering realm replay");
    let candidate =
        read_realm_compressed_compact_paged(case).expect("compressed compact realm replay");
    assert_eq!(covering.len(), case.expected_records);
    assert_eq!(candidate.len(), case.expected_records);
    assert_matching_records(&covering, &candidate);
}

fn prepare_resource_area_case() -> (ReplayCase, PrototypeStream, usize, String) {
    let case = seed_replay_case(1, 16, 128);
    let stream = case.streams[0].clone();
    let area = case.areas[0].clone();
    let resource_expected_records = case.expected_records / case.streams.len();
    validate_resource_case(&case, &stream, resource_expected_records);
    validate_area_case(&case, &area);
    (case, stream, resource_expected_records, area)
}

fn prepare_realm_case() -> ReplayCase {
    let case = seed_replay_case(4, 8, 64);
    validate_realm_case(&case);
    case
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_covering_resource_replay_production_like_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "prototype_storage_model");
    ctx.parameter("candidate", "current_covering");
    ctx.parameter("payload_profile", "production_like");

    let (case, stream, resource_expected_records, _) = prepare_resource_area_case();

    let iterations = ctx.measure_workload(|| {
        let records = read_resource_covering(&case, &stream, resource_expected_records)
            .expect("covering resource replay");
        black_box(records);
    });
    stress_config::record_completed(ctx, resource_expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_resource_mini_page_replay_production_like_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "prototype_storage_model");
    ctx.parameter("candidate", "resource_mini_page");
    ctx.parameter("payload_profile", "production_like");

    let (case, stream, resource_expected_records, _) = prepare_resource_area_case();

    let iterations = ctx.measure_workload(|| {
        let records = read_resource_compact_paged(&case, &stream, resource_expected_records)
            .expect("resource mini-page replay");
        black_box(records);
    });
    stress_config::record_completed(ctx, resource_expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_covering_area_replay_production_like_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "prototype_storage_model");
    ctx.parameter("candidate", "current_covering");
    ctx.parameter("payload_profile", "production_like");

    let (case, _, _, area) = prepare_resource_area_case();

    let iterations = ctx.measure_workload(|| {
        let records = read_area_covering(&case, &area).expect("covering area replay");
        black_box(records);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_compact_area_page_replay_production_like_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "prototype_storage_model");
    ctx.parameter("candidate", "compact_area_pages");
    ctx.parameter("payload_profile", "production_like");

    let (case, _, _, area) = prepare_resource_area_case();

    let iterations = ctx.measure_workload(|| {
        let records = read_area_compact_paged(&case, &area).expect("compact area replay");
        black_box(records);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_covering_realm_replay_production_like_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "prototype_storage_model");
    ctx.parameter("candidate", "current_covering");
    ctx.parameter("payload_profile", "production_like");

    let case = prepare_realm_case();

    let iterations = ctx.measure_workload(|| {
        let records = read_realm_covering(&case).expect("covering realm replay");
        black_box(records);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_compressed_realm_replay_production_like_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "prototype_storage_model");
    ctx.parameter("candidate", "compressed_realm_body");
    ctx.parameter("payload_profile", "production_like");

    let case = prepare_realm_case();

    let iterations = ctx.measure_workload(|| {
        let records =
            read_realm_compressed_compact_paged(&case).expect("compressed compact realm replay");
        black_box(records);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_covering_resource_replay_production_like_routed_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "prototype_routed_model");
    ctx.parameter("candidate", "current_covering");
    ctx.parameter("payload_profile", "production_like");

    let (case, stream, resource_expected_records, _) = prepare_resource_area_case();
    let case = Arc::new(case);
    let route = format!("stream://{REALM}/{}/{}", stream.area, stream.resource);
    let context = setup_routed_context(case, RoutedReadLayout::CurrentCovering);
    let (read_msg_type, read_payload) = prepare_validated_routed_read(
        &context,
        &route,
        resource_expected_records,
        resource_expected_records as u64,
    );

    let iterations = ctx.measure_workload(|| {
        let response = routed_request(&context, &route, read_msg_type, read_payload.clone());
        black_box(response);
    });
    stress_config::record_completed(ctx, resource_expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_promotion_frontier_resource_replay_production_like_routed_model(
    ctx: &mut StressContext,
) {
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "prototype_routed_model");
    ctx.parameter("candidate", "promotion_frontier");
    ctx.parameter("payload_profile", "production_like");

    let (case, stream, resource_expected_records, _) = prepare_resource_area_case();
    let case = Arc::new(case);
    let route = format!("stream://{REALM}/{}/{}", stream.area, stream.resource);
    let context = setup_routed_context(case, RoutedReadLayout::PromotionFrontier);
    let (read_msg_type, read_payload) = prepare_validated_routed_read(
        &context,
        &route,
        resource_expected_records,
        resource_expected_records as u64,
    );

    let iterations = ctx.measure_workload(|| {
        let response = routed_request(&context, &route, read_msg_type, read_payload.clone());
        black_box(response);
    });
    stress_config::record_completed(ctx, resource_expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_covering_area_replay_production_like_routed_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "prototype_routed_model");
    ctx.parameter("candidate", "current_covering");
    ctx.parameter("payload_profile", "production_like");

    let (case, _, _, area) = prepare_resource_area_case();
    let case = Arc::new(case);
    let route = format!("stream://{REALM}/{area}/*");
    let context = setup_routed_context(case.clone(), RoutedReadLayout::CurrentCovering);
    let (read_msg_type, read_payload) = prepare_validated_routed_read(
        &context,
        &route,
        case.expected_records,
        case.expected_records as u64,
    );

    let iterations = ctx.measure_workload(|| {
        let response = routed_request(&context, &route, read_msg_type, read_payload.clone());
        black_box(response);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_compact_area_page_replay_production_like_routed_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "prototype_routed_model");
    ctx.parameter("candidate", "promotion_frontier");
    ctx.parameter("payload_profile", "production_like");

    let (case, _, _, area) = prepare_resource_area_case();
    let case = Arc::new(case);
    let route = format!("stream://{REALM}/{area}/*");
    let context = setup_routed_context(case.clone(), RoutedReadLayout::PromotionFrontier);
    let (read_msg_type, read_payload) = prepare_validated_routed_read(
        &context,
        &route,
        case.expected_records,
        case.expected_records as u64,
    );

    let iterations = ctx.measure_workload(|| {
        let response = routed_request(&context, &route, read_msg_type, read_payload.clone());
        black_box(response);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_covering_realm_replay_production_like_routed_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "prototype_routed_model");
    ctx.parameter("candidate", "current_covering");
    ctx.parameter("payload_profile", "production_like");

    let case = Arc::new(prepare_realm_case());
    let route = format!("stream://{REALM}/*/*");
    let context = setup_routed_context(case.clone(), RoutedReadLayout::CurrentCovering);
    let (read_msg_type, read_payload) = prepare_validated_routed_read(
        &context,
        &route,
        case.expected_records,
        case.expected_records as u64,
    );

    let iterations = ctx.measure_workload(|| {
        let response = routed_request(&context, &route, read_msg_type, read_payload.clone());
        black_box(response);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_compressed_realm_replay_production_like_routed_model(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "prototype_routed_model");
    ctx.parameter("candidate", "promotion_frontier");
    ctx.parameter("payload_profile", "production_like");

    let case = Arc::new(prepare_realm_case());
    let route = format!("stream://{REALM}/*/*");
    let context = setup_routed_context(case.clone(), RoutedReadLayout::PromotionFrontier);
    let (read_msg_type, read_payload) = prepare_validated_routed_read(
        &context,
        &route,
        case.expected_records,
        case.expected_records as u64,
    );

    let iterations = ctx.measure_workload(|| {
        let response = routed_request(&context, &route, read_msg_type, read_payload.clone());
        black_box(response);
    });
    stress_config::record_completed(ctx, case.expected_records as u64 * iterations);
}

stress_main!();
