use bincode::{deserialize, serialize};
use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::create_local_bench_store;
use fitz::domains::stream::protocol::{StreamRecord, StreamWriteMode};
use fitz::domains::stream::storage::{
    decode_area_offset_from_key, decode_realm_offset_from_key, encode_area_locator_key,
    encode_canonical_resource_key, encode_realm_locator_key, AreaLocatorValue,
    CanonicalResourceValue, KeyPrefix, RealmLocatorValue,
};
use fitz::domains::stream::store::{CommitRecordsParams, EventPayload, StreamStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[path = "criterion_config.rs"]
mod criterion_config;

const FAMILY: u64 = 1;
const REALM: &str = "bench-realm";
const BODY_BYTES: usize = 128;
const METADATA_BYTES: usize = 24;
const REPLAY_PAGE_RECORD_LIMIT: usize = 64;
const PAGED_REALM_KEY_PREFIX: u8 = 0xE0;
const PAGED_AREA_LOCATOR_KEY_PREFIX: u8 = 0xE1;
const COMPACT_PAGED_REALM_KEY_PREFIX: u8 = 0xE2;

struct PrototypeStream {
    stream_id: u64,
    area: String,
    resource: String,
}

struct PrototypeRowWrite {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Clone)]
struct PagedSeedRecord {
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

struct ReplayCase {
    store: StreamStore,
    db: Arc<cntryl_midge::Engine>,
    _temp_dir: tempfile::TempDir,
    streams: Vec<PrototypeStream>,
    stream_positions: HashMap<u64, usize>,
    expected_records: usize,
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
        let mut total_len = 4;
        for record in &self.records {
            total_len += 8 + 8 + 8 + 4 + 4 + record.body.len();
            total_len += record.metadata.as_ref().map(|metadata| metadata.len()).unwrap_or(0);
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
                    .map(|metadata| metadata.len() as u32)
                    .unwrap_or(u32::MAX)
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
        let mut offset = 0usize;
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
            let area_offset = read_u64(bytes, &mut offset);
            let created_at = read_u64(bytes, &mut offset);
            let body_len = read_u32(bytes, &mut offset) as usize;
            let metadata_len_raw = read_u32(bytes, &mut offset);
            let metadata_len = if metadata_len_raw == u32::MAX {
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

fn payload_bytes() -> usize {
    BODY_BYTES + METADATA_BYTES
}

fn build_event_payload(stream_index: usize, record_index: usize) -> EventPayload {
    let body_seed = ((stream_index as u8).wrapping_mul(17)).wrapping_add(record_index as u8);
    let metadata_seed = body_seed.wrapping_add(53);

    EventPayload {
        body: Bytes::from(vec![body_seed; BODY_BYTES]),
        metadata: Some(Bytes::from(vec![metadata_seed; METADATA_BYTES])),
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
) -> ReplayCase {
    let (db, temp_dir) = create_local_bench_store();
    let store = StreamStore::new(db.clone());
    let mut streams = Vec::with_capacity(area_count * streams_per_area);

    for area_index in 0..area_count {
        let area = format!("area-{area_index}");
        for resource_index in 0..streams_per_area {
            streams.push(PrototypeStream {
                stream_id: (streams.len() + 1) as u64,
                area: area.clone(),
                resource: format!("resource-{area_index}-{resource_index}"),
            });
        }
    }

    let mut prototype_rows =
        Vec::with_capacity(area_count * streams_per_area * records_per_stream * 3);
    let mut next_resource_offsets = vec![0u64; streams.len()];
    let mut paged_seed_records = Vec::with_capacity(area_count * streams_per_area * records_per_stream);

    for record_index in 0..records_per_stream {
        for (stream_index, stream) in streams.iter().enumerate() {
            let event = build_event_payload(stream_index, record_index);
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
        _temp_dir: temp_dir,
        streams,
        stream_positions,
        expected_records: area_count * streams_per_area * records_per_stream,
    }
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

fn read_area_hydrated(case: &ReplayCase, area: &str) -> Result<Vec<StreamRecord>, String> {
    let watermark = case.store.get_watermark(FAMILY, REALM, area)?;
    let txn = case
        .db
        .begin_tx(FAMILY as u32, cntryl_midge::TransactionMode::ReadOnly)
        .map_err(|error| format!("failed to begin tx: {error:?}"))?;

    let mut prefix_key = vec![KeyPrefix::AreaLocator as u8];
    prefix_key.extend_from_slice(REALM.as_bytes());
    prefix_key.push(0);
    prefix_key.extend_from_slice(area.as_bytes());
    prefix_key.push(0);

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

    let mut prefix_key = vec![KeyPrefix::RealmLocator as u8];
    prefix_key.extend_from_slice(REALM.as_bytes());
    prefix_key.push(0);

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

fn assert_matching_records(left: &[StreamRecord], right: &[StreamRecord]) {
    assert_eq!(left.len(), right.len(), "record count mismatch");

    for (left_record, right_record) in left.iter().zip(right) {
        assert_eq!(left_record.resource_offset, right_record.resource_offset);
        assert_eq!(left_record.area_offset, right_record.area_offset);
        assert_eq!(left_record.realm_offset, right_record.realm_offset);
        assert_eq!(left_record.body, right_record.body);
        assert_eq!(left_record.metadata, right_record.metadata);
    }
}

fn assert_total_payload_bytes(records: &[StreamRecord], expected_records: usize) {
    let observed = records
        .iter()
        .map(|record| {
            record.body.len() + record.metadata.as_ref().map(|meta| meta.len()).unwrap_or(0)
        })
        .sum::<usize>();

    assert_eq!(observed, expected_records * payload_bytes());
}

fn validate_area_case(case: &ReplayCase, area: &str) {
    let (covering_records, _) = case
        .store
        .read_area(FAMILY, REALM, area, 0, case.expected_records as u64, None)
        .expect("covering area replay");
    let hydrated_records = read_area_hydrated(case, area).expect("hydrated area replay");
    let paged_records = read_area_paged(case, area).expect("paged area replay");

    assert_eq!(covering_records.len(), case.expected_records);
    assert_eq!(hydrated_records.len(), case.expected_records);
    assert_eq!(paged_records.len(), case.expected_records);
    assert_total_payload_bytes(&covering_records, case.expected_records);
    assert_total_payload_bytes(&hydrated_records, case.expected_records);
    assert_total_payload_bytes(&paged_records, case.expected_records);
    assert_matching_records(&covering_records, &hydrated_records);
    assert_matching_records(&covering_records, &paged_records);
}

fn validate_realm_case(case: &ReplayCase) {
    let (covering_records, _) = case
        .store
        .read_realm(FAMILY, REALM, 0, case.expected_records as u64, None)
        .expect("covering realm replay");
    let hydrated_records = read_realm_hydrated(case).expect("hydrated realm replay");
    let paged_records = read_realm_paged(case).expect("paged realm replay");
    let compact_paged_records =
        read_realm_compact_paged(case).expect("compact paged realm replay");

    assert_eq!(covering_records.len(), case.expected_records);
    assert_eq!(hydrated_records.len(), case.expected_records);
    assert_eq!(paged_records.len(), case.expected_records);
    assert_eq!(compact_paged_records.len(), case.expected_records);
    assert_total_payload_bytes(&covering_records, case.expected_records);
    assert_total_payload_bytes(&hydrated_records, case.expected_records);
    assert_total_payload_bytes(&paged_records, case.expected_records);
    assert_total_payload_bytes(&compact_paged_records, case.expected_records);
    assert_matching_records(&covering_records, &hydrated_records);
    assert_matching_records(&covering_records, &paged_records);
    assert_matching_records(&covering_records, &compact_paged_records);
}

fn bench_stream_replay_hydration(c: &mut Criterion) {
    let area_case = seed_replay_case(1, 16, 128);
    let area_name = area_case.streams[0].area.clone();
    validate_area_case(&area_case, &area_name);

    let realm_case = seed_replay_case(4, 8, 64);
    validate_realm_case(&realm_case);

    let mut group = c.benchmark_group("subsystem_stream_replay");
    group.sampling_mode(SamplingMode::Flat);

    group.throughput(Throughput::Elements(area_case.expected_records as u64));
    group.bench_function("covering_area_replay_2048_records_16_streams", |b| {
        b.iter(|| {
            black_box(
                area_case
                    .store
                    .read_area(FAMILY, REALM, &area_name, 0, area_case.expected_records as u64, None)
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
            black_box(
                read_realm_compact_paged(&realm_case).expect("compact paged realm replay"),
            );
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_stream_replay_hydration
}
criterion_main!(benches);