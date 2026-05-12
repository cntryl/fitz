use bytes::Bytes;
use fitz::benchkit::create_local_bench_store;
use fitz::domains::stream::protocol::{StreamRecord, StreamWriteMode};
use fitz::domains::stream::storage::{decode_area_offset_from_key, decode_realm_offset_from_key};
use fitz::domains::stream::store::{
    CommitRecordsParams, EventPayload, ReadResourceParams, StreamStore,
};
use fitz::domains::stream::StreamReadItem;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{DeliveryError, MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress};
use fitz::session::SessionId;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use std::sync::Arc;

const FAMILY: u64 = 1;
pub const PROTOTYPE_ROUTE_FAMILY: u64 = FAMILY;
const REALM: &str = "tier4";
const READ_LIMIT: usize = 100;
const REPLAY_PAGE_RECORD_LIMIT: usize = 64;
const COMPACT_AREA_PAGE_KEY_PREFIX: u8 = 0xE4;
const COMPRESSED_COMPACT_PAGED_REALM_KEY_PREFIX: u8 = 0xE8;
const COMPACT_RESOURCE_PAGE_KEY_PREFIX: u8 = 0xEA;

pub const RESOURCE_ROUTE: &str = "stream://tier4/resource/orders";
pub const AREA_ROUTE: &str = "stream://tier4/stream-area/*";
pub const REALM_ROUTE: &str = "stream://tier4/*/*";

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

#[derive(Clone)]
struct PrototypeStream {
    area: String,
    resource: String,
    record_count: usize,
}

struct PrototypeRowWrite {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct SeedRecord {
    area: String,
    resource: String,
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

pub struct ReplayCase {
    store: StreamStore,
    db: Arc<cntryl_midge::Engine>,
    _temp_dir: tempfile::TempDir,
    streams: Vec<PrototypeStream>,
}

pub struct PrototypeReadCase {
    pub replay_case: Arc<ReplayCase>,
    pub route: &'static str,
    pub expected_count: usize,
}

struct PrototypeStreamReadSink {
    router: Arc<Router>,
    case: Arc<ReplayCase>,
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

const COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xB2];
const COMPACT_AREA_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE4];
const COMPRESSED_COMPACT_REALM_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xE8];
const COMPACT_RESOURCE_PAGE_VALUE_V1_MARKER: [u8; 2] = [0, 0xEA];
const OPTIONAL_BYTES_ABSENT: u32 = u32::MAX;

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
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadWrite)
        .map_err(|error| format!("begin_tx failed: {error:?}"))?;

    for row in writes {
        txn.put(row.key.clone(), row.value.clone(), None)
            .map_err(|error| format!("txn put failed: {error:?}"))?;
    }

    txn.commit(cntryl_midge::WriteOptions::buffered())
        .map_err(|error| format!("txn commit failed: {error:?}"))
}

fn seed_replay_case(streams: &[PrototypeStream]) -> ReplayCase {
    let (db, temp_dir) = create_local_bench_store();
    let store = StreamStore::new(db.clone());
    let mut prototype_rows = Vec::new();
    let mut seed_records = Vec::new();

    for (stream_index, stream) in streams.iter().enumerate() {
        for record_index in 0..stream.record_count {
            let event = build_production_like_payload(stream_index, record_index);
            let commit = store
                .commit_records(CommitRecordsParams {
                    family: FAMILY,
                    realm: REALM,
                    area: &stream.area,
                    resource: &stream.resource,
                    expected_resource_next_offset: record_index as u64,
                    events: std::slice::from_ref(&event),
                    ingest_metadata: None,
                    mode: StreamWriteMode::Buffered,
                })
                .expect("seed stream commit");

            seed_records.push(SeedRecord {
                area: stream.area.clone(),
                resource: stream.resource.clone(),
                resource_offset: commit.first_resource_offset,
                area_offset: commit.first_area_offset,
                realm_offset: commit.first_realm_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at: ((stream_index as u64) << 32) | record_index as u64,
            });
        }
    }

    let mut area_names = streams
        .iter()
        .map(|stream| stream.area.clone())
        .collect::<Vec<_>>();
    area_names.sort();
    area_names.dedup();

    for area in area_names {
        let area_records = seed_records
            .iter()
            .filter(|record| record.area == area)
            .cloned()
            .collect::<Vec<_>>();
        for page in area_records.chunks(REPLAY_PAGE_RECORD_LIMIT) {
            prototype_rows.push(PrototypeRowWrite {
                key: encode_compact_area_page_key(REALM, &area, page[0].area_offset),
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

    for stream in streams {
        let resource_records = seed_records
            .iter()
            .filter(|record| record.area == stream.area && record.resource == stream.resource)
            .cloned()
            .collect::<Vec<_>>();
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
        streams: streams.to_vec(),
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

fn read_area_covering(
    case: &ReplayCase,
    area: &str,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
    let (records, _) = case
        .store
        .read_area(FAMILY, REALM, area, 0, limit as u64, None)?;
    Ok(event_records(records))
}

fn read_area_compact_paged(
    case: &ReplayCase,
    area: &str,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_watermark(FAMILY, REALM, area)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let query = cntryl_midge::Query::new()
        .start_key(Bytes::from(encode_compact_area_page_key(REALM, area, 0)))
        .prefix(build_compact_area_page_prefix(REALM, area))
        .limit(limit.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(limit);

    for (key, value) in raw_rows {
        let page_start = decode_area_offset_from_key(&key)?;
        let page = CompactAreaPageValue::decode(&value);

        for (slot, page_record) in page.records.iter().enumerate() {
            let area_offset = page_start + slot as u64;
            if area_offset > watermark {
                return Ok(records);
            }
            if records.len() == limit {
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

fn read_realm_covering(case: &ReplayCase, limit: usize) -> Result<Vec<StreamRecord>, String> {
    let (records, _) = case
        .store
        .read_realm(FAMILY, REALM, 0, limit as u64, None)?;
    Ok(event_records(records))
}

fn read_realm_compressed_compact_paged(
    case: &ReplayCase,
    limit: usize,
) -> Result<Vec<StreamRecord>, String> {
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
        .limit(limit.div_ceil(REPLAY_PAGE_RECORD_LIMIT));
    let mut iter = txn
        .scan(&query)
        .map_err(|error| format!("scan error: {error:?}"))?;
    let raw_rows = iter.collect_all();

    let mut records = Vec::with_capacity(limit);

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
            if records.len() == limit {
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
        if selected.len() >= limit as usize {
            has_more = true;
            break;
        }

        if let Some(max_bytes) = max_bytes {
            let projected = total_bytes
                + record.body.len()
                + record
                    .metadata
                    .as_ref()
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
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
        .map(|record| record.resource_offset)
        .unwrap_or(from_offset);
    let last_area_offset = selected.last().and_then(|record| record.area_offset);
    let last_realm_offset = selected.last().and_then(|record| record.realm_offset);

    let mut encoder = PayloadEncoder::new();
    encoder.put_u32(selected.len() as u32);
    for record in selected {
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
    fn new(router: Arc<Router>, case: Arc<ReplayCase>) -> Self {
        Self { router, case }
    }

    fn encode_read_response_data(
        &self,
        route: &Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<Vec<u8>, String> {
        if limit == 0 {
            let mut encoder = PayloadEncoder::new();
            encoder.put_u32(0);
            return Ok(encoder.finish());
        }

        let (realm, area, resource) = parse_stream_read_route(route.as_str())?;
        if realm != REALM {
            return Err(format!("unsupported prototype realm {realm}"));
        }

        let records = if area == "*" && resource == "*" {
            read_realm_compressed_compact_paged(&self.case, limit as usize)?
        } else if resource == "*" {
            read_area_compact_paged(&self.case, &area, limit as usize)?
        } else {
            let stream = find_stream(&self.case, &area, &resource)?;
            read_resource_compact_paged(&self.case, stream, limit as usize)?
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
        let frame_ctx = match envelope.payload::<fitz::protocol::frame_context::FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => return Err(DeliveryError::ActorStopped),
        };

        let parsed = fitz::protocol::stream_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            SessionId(frame_ctx.session_id),
            envelope.source().cloned().unwrap_or_else(|| {
                RouteAddress::new(
                    *envelope.destination().family(),
                    Route::new(format!("inbox://session/{}", frame_ctx.session_id)),
                )
            }),
        )
        .map_err(|_| DeliveryError::ActorStopped)?;

        use fitz::domains::stream::protocol::StreamMessage;
        use fitz::protocol::stream_codec::{ParsedStreamFrame, StreamResponse};

        let response = match parsed {
            ParsedStreamFrame::Op(StreamMessage::Read {
                route,
                from_offset,
                limit,
                max_bytes,
                ..
            }) => match self.encode_read_response_data(&route, from_offset, limit, max_bytes) {
                Ok(data) => StreamResponse::Ok {
                    session_id: None,
                    data,
                },
                Err(error) => StreamResponse::Error(error),
            },
            _ => StreamResponse::Error(
                "prototype routed stream sink currently supports only READ operations".to_string(),
            ),
        };

        let response_bytes = fitz::protocol::stream_codec::encode_response(&response);
        let response_ctx = fitz::protocol::frame_context::FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            fitz::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            Bytes::from(response_bytes),
            frame_ctx.route_family,
        );

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn validate_resource_case(case: &ReplayCase) {
    let stream = find_stream(case, "resource", "orders").expect("resource prototype stream");
    let covering =
        read_resource_covering(case, stream, READ_LIMIT).expect("covering resource replay");
    let candidate =
        read_resource_compact_paged(case, stream, READ_LIMIT).expect("resource mini-page replay");
    assert_eq!(covering.len(), READ_LIMIT);
    assert_eq!(candidate.len(), READ_LIMIT);
    assert_matching_records(&covering, &candidate);
}

fn validate_area_case(case: &ReplayCase) {
    let covering =
        read_area_covering(case, "stream-area", READ_LIMIT).expect("covering area replay");
    let candidate =
        read_area_compact_paged(case, "stream-area", READ_LIMIT).expect("compact area replay");
    assert_eq!(covering.len(), READ_LIMIT);
    assert_eq!(candidate.len(), READ_LIMIT);
    assert_matching_records(&covering, &candidate);
}

fn validate_realm_case(case: &ReplayCase) {
    let covering = read_realm_covering(case, READ_LIMIT).expect("covering realm replay");
    let candidate = read_realm_compressed_compact_paged(case, READ_LIMIT)
        .expect("compressed compact realm replay");
    assert_eq!(covering.len(), READ_LIMIT);
    assert_eq!(candidate.len(), READ_LIMIT);
    assert_matching_records(&covering, &candidate);
}

pub fn prepare_resource_read_case() -> PrototypeReadCase {
    let case = Arc::new(seed_replay_case(&[PrototypeStream {
        area: "resource".to_string(),
        resource: "orders".to_string(),
        record_count: READ_LIMIT,
    }]));
    validate_resource_case(case.as_ref());
    PrototypeReadCase {
        replay_case: case,
        route: RESOURCE_ROUTE,
        expected_count: READ_LIMIT,
    }
}

pub fn prepare_area_read_case() -> PrototypeReadCase {
    let case = Arc::new(seed_replay_case(&[
        PrototypeStream {
            area: "stream-area".to_string(),
            resource: "orders".to_string(),
            record_count: READ_LIMIT / 2,
        },
        PrototypeStream {
            area: "stream-area".to_string(),
            resource: "audits".to_string(),
            record_count: READ_LIMIT / 2,
        },
    ]));
    validate_area_case(case.as_ref());
    PrototypeReadCase {
        replay_case: case,
        route: AREA_ROUTE,
        expected_count: READ_LIMIT,
    }
}

pub fn prepare_realm_read_case() -> PrototypeReadCase {
    let case = Arc::new(seed_replay_case(&[
        PrototypeStream {
            area: "events".to_string(),
            resource: "orders".to_string(),
            record_count: READ_LIMIT / 2,
        },
        PrototypeStream {
            area: "audit".to_string(),
            resource: "ledger".to_string(),
            record_count: READ_LIMIT / 2,
        },
    ]));
    validate_realm_case(case.as_ref());
    PrototypeReadCase {
        replay_case: case,
        route: REALM_ROUTE,
        expected_count: READ_LIMIT,
    }
}

pub fn install_stream_read_prototype_sink(router: Arc<Router>, case: Arc<ReplayCase>) {
    let sink = Arc::new(PrototypeStreamReadSink::new(router.clone(), case));
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
}
