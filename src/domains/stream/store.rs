//! Stream storage layer - STORAGE ONLY, NO SEQUENCING

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use super::protocol::{IngestMetadata, StreamRecord, StreamWriteMode};
use super::storage::{
    decode_area_offset_from_key, decode_realm_offset_from_key, encode_area_counter_key,
    encode_area_key, encode_offset_counter_key, encode_realm_counter_key, encode_realm_key,
    encode_resource_key, encode_resource_meta_key, encode_watermark_key, AreaCounterValue,
    AreaValue, OffsetCounterValue, RealmCounterValue, RealmValue, ResourceMetaValue, ResourceValue,
    WatermarkValue,
};

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
    sessions: Arc<Mutex<HashMap<SessionId, AppendSession>>>,
    ttl: StreamTTL,
    next_session_id: std::sync::atomic::AtomicU64,
    sequencing_guards: Arc<Mutex<HashMap<u64, Arc<Mutex<()>>>>>,
}

impl StreamStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self::with_config(db, BatchLimits::default(), StreamTTL::default())
    }

    pub fn with_limits(db: Arc<cntryl_midge::Engine>, limits: BatchLimits) -> Self {
        Self::with_config(db, limits, StreamTTL::default())
    }

    pub fn with_config(db: Arc<cntryl_midge::Engine>, limits: BatchLimits, ttl: StreamTTL) -> Self {
        Self {
            db,
            limits,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            sequencing_guards: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn batch_limits(&self) -> BatchLimits {
        self.limits.clone()
    }

    fn family_sequence_guard(&self, family: u64) -> Arc<Mutex<()>> {
        let mut guards = self.sequencing_guards.lock();
        match guards.entry(family) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let guard = Arc::new(Mutex::new(()));
                entry.insert(guard.clone());
                guard
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
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Resource as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(area.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(resource.as_bytes());
        prefix_key.push(0);

        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix_key));
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
        for (_, value_bytes) in results {
            let value = ResourceValue::decode(&value_bytes);
            next_offset = next_offset.max(value.resource_offset.saturating_add(1));
            committed_size_bytes = committed_size_bytes
                .saturating_add(value.body.len() as u64)
                .saturating_add(value.metadata.as_ref().map(|m| m.len()).unwrap_or(0) as u64);
        }

        Ok((next_offset, committed_size_bytes))
    }

    fn scan_next_area_offset(&self, family: u64, realm: &str, area: &str) -> Result<u64, String> {
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Area as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(area.as_bytes());
        prefix_key.push(0);

        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix_key));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, _)) = results.last() {
            Ok(decode_area_offset_from_key(key)?.saturating_add(1))
        } else {
            Ok(0)
        }
    }

    fn scan_next_realm_offset(&self, family: u64, realm: &str) -> Result<u64, String> {
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Realm as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);

        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix_key));
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        if let Some((key, _)) = results.last() {
            Ok(decode_realm_offset_from_key(key)?.saturating_add(1))
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
                let next_offset = match txn
                    .get(&encode_offset_counter_key(realm, area, resource))
                    .map_err(|e| format!("get error: {:?}", e))?
                {
                    Some(bytes) => OffsetCounterValue::decode(&bytes).next_offset,
                    None => self.scan_resource_stats(family, realm, area, resource)?.0,
                };
                let (_, committed_size_bytes) =
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

        let sequencing_guard = self.family_sequence_guard(family);
        let _sequencing_lock = sequencing_guard.lock();

        let (resource_meta_before, upgrade_resource_meta) =
            self.load_resource_meta_snapshot(family, realm, area, resource)?;
        if resource_meta_before.next_offset != expected_resource_next_offset {
            return Err("ERR_CONCURRENCY_CONFLICT".to_string());
        }

        let (area_next_offset, _persist_area_counter) =
            self.load_area_next_offset_snapshot(family, realm, area)?;
        let (realm_next_offset, _persist_realm_counter) =
            self.load_realm_next_offset_snapshot(family, realm)?;

        let created_at = Self::now_epoch_ms();
        let batch_size = events.len();
        let batch_size_u64 = batch_size as u64;
        let first_resource_offset = resource_meta_before.next_offset;
        let first_area_offset = area_next_offset;
        let first_realm_offset = realm_next_offset;
        let last_resource_offset = first_resource_offset + batch_size_u64 - 1;
        let last_area_offset = first_area_offset + batch_size_u64 - 1;
        let last_realm_offset = first_realm_offset + batch_size_u64 - 1;
        let committed_size_delta = events.iter().map(Self::event_size_bytes).sum::<u64>();

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for (index, event) in events.iter().enumerate() {
            let resource_offset = first_resource_offset + index as u64;
            let area_offset = first_area_offset + index as u64;
            let realm_offset = first_realm_offset + index as u64;

            let resource_key = encode_resource_key(realm, area, resource, resource_offset);
            let resource_value = ResourceValue {
                resource_offset,
                area_offset: Some(area_offset),
                realm_offset: Some(realm_offset),
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            };
            let ttl_opt = self.ttl.ttl_seconds;
            txn.put(resource_key, resource_value.encode(), ttl_opt)
                .map_err(|e| format!("txn put failed: {:?}", e))?;

            let area_key = encode_area_key(realm, area, area_offset);
            let area_value = AreaValue {
                realm: realm.to_string(),
                area: area.to_string(),
                resource: resource.to_string(),
                resource_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            };
            txn.put(area_key, area_value.encode(), ttl_opt)
                .map_err(|e| format!("txn put failed: {:?}", e))?;

            let realm_key = encode_realm_key(realm, realm_offset);
            let realm_value = RealmValue {
                resource: resource.to_string(),
                resource_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
                realm: realm.to_string(),
                area: area.to_string(),
                area_offset,
            };
            txn.put(realm_key, realm_value.encode(), ttl_opt)
                .map_err(|e| format!("txn put failed: {:?}", e))?;
        }

        let resource_meta_after = ResourceMetaValue {
            next_offset: last_resource_offset.saturating_add(1),
            committed_size_bytes: resource_meta_before
                .committed_size_bytes
                .saturating_add(committed_size_delta),
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

        txn.put(
            encode_watermark_key(realm, area),
            WatermarkValue {
                watermark: last_area_offset,
            }
            .encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        txn.put(
            crate::domains::stream::storage::encode_realm_watermark_key(realm),
            WatermarkValue {
                watermark: last_realm_offset,
            }
            .encode(),
            None,
        )
        .map_err(|e| format!("txn put failed: {:?}", e))?;

        if upgrade_resource_meta {
            txn.delete(encode_offset_counter_key(realm, area, resource))
                .map_err(|e| format!("txn delete failed: {:?}", e))?;
        }

        let write_options = match mode {
            StreamWriteMode::Sync => cntryl_midge::WriteOptions::sync(),
            StreamWriteMode::Buffered => cntryl_midge::WriteOptions::buffered(),
        };
        txn.commit(write_options)
            .map_err(|e| format!("midge commit error: {:?}", e))?;

        Ok(CommitResponse {
            first_resource_offset,
            last_resource_offset,
            first_area_offset,
            last_area_offset,
            first_realm_offset,
            last_realm_offset,
            batch_size,
            ingest_metadata,
        })
    }

    pub fn list_resource_metadata(&self, family: u64) -> Result<Vec<StreamAdminRecord>, String> {
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        let mut records = HashMap::<(String, String, String), StreamAdminRecord>::new();

        let resource_meta_query = cntryl_midge::Query::new().prefix(Bytes::from(vec![
            crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8,
        ]));
        let mut resource_meta_iter = txn
            .scan(&resource_meta_query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        for (key, value) in resource_meta_iter.collect_all() {
            let (realm, area, resource) = Self::resource_identity_from_key(
                crate::domains::stream::storage::KeyPrefix::ResourceMeta as u8,
                &key,
            )?;
            let meta = ResourceMetaValue::decode(&value);
            if meta.next_offset == 0 {
                continue;
            }
            records.insert(
                (realm.clone(), area.clone(), resource.clone()),
                StreamAdminRecord {
                    realm,
                    area,
                    resource,
                    next_offset: meta.next_offset,
                    committed_size_bytes: meta.committed_size_bytes,
                },
            );
        }

        let legacy_counter_query = cntryl_midge::Query::new().prefix(Bytes::from(vec![
            crate::domains::stream::storage::KeyPrefix::OffsetCounter as u8,
        ]));
        let mut legacy_counter_iter = txn
            .scan(&legacy_counter_query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        for (key, value) in legacy_counter_iter.collect_all() {
            let identity = Self::resource_identity_from_key(
                crate::domains::stream::storage::KeyPrefix::OffsetCounter as u8,
                &key,
            )?;
            if records.contains_key(&identity) {
                continue;
            }

            let next_offset = OffsetCounterValue::decode(&value).next_offset;
            if next_offset == 0 {
                continue;
            }

            let (realm, area, resource) = identity;
            let (_, committed_size_bytes) =
                self.scan_resource_stats(family, &realm, &area, &resource)?;
            records.insert(
                (realm.clone(), area.clone(), resource.clone()),
                StreamAdminRecord {
                    realm,
                    area,
                    resource,
                    next_offset,
                    committed_size_bytes,
                },
            );
        }

        let mut values = records.into_values().collect::<Vec<_>>();
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

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut txn = match self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(txn) => txn,
            Err(e) => {
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
                return Err(format!("begin_tx failed: {:?}", e));
            }
        };

        for (i, event) in staged_events.iter().enumerate() {
            let resource_offset = first_resource_offset + i as u64;
            let area_offset = first_area_offset + i as u64;
            let realm_offset = first_realm_offset + i as u64;

            let resource_key = encode_resource_key(&realm, &area, &resource, resource_offset);
            let resource_value = ResourceValue {
                resource_offset,
                area_offset: Some(area_offset),
                realm_offset: Some(realm_offset),
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            };
            let ttl_opt = self.ttl.ttl_seconds;
            txn.put(resource_key, resource_value.encode(), ttl_opt)
                .map_err(|e| format!("txn put failed: {:?}", e))?;

            let area_key = encode_area_key(&realm, &area, area_offset);
            let area_value = AreaValue {
                realm: realm.clone(),
                area: area.clone(),
                resource: resource.clone(),
                resource_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            };
            txn.put(area_key, area_value.encode(), ttl_opt)
                .map_err(|e| format!("txn put failed: {:?}", e))?;

            let realm_key = encode_realm_key(&realm, realm_offset);
            let realm_value = RealmValue {
                resource: resource.clone(),
                resource_offset,
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
                realm: realm.clone(),
                area: area.clone(),
                area_offset,
            };
            txn.put(realm_key, realm_value.encode(), ttl_opt)
                .map_err(|e| format!("txn put failed: {:?}", e))?;
        }

        // **CRITICAL**: Persist offset counter metadata (no TTL!)
        // This ensures next_resource_offset survives TTL expiry and restarts
        let next_offset = first_resource_offset + batch_size as u64;
        let counter_key =
            crate::domains::stream::storage::encode_offset_counter_key(&realm, &area, &resource);
        let counter_value = crate::domains::stream::storage::OffsetCounterValue { next_offset };

        txn.put(counter_key, counter_value.encode(), None)
            .map_err(|e| format!("txn put failed: {:?}", e))?;

        let opts = match mode {
            StreamWriteMode::Sync => cntryl_midge::WriteOptions::sync(),
            StreamWriteMode::Buffered => cntryl_midge::WriteOptions::buffered(),
        };
        if let Err(e) = txn.commit(opts) {
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
            return Err(format!("midge commit error: {:?}", e));
        }

        Ok(CommitResponse {
            first_resource_offset,
            last_resource_offset: first_resource_offset + batch_size as u64 - 1,
            first_area_offset,
            last_area_offset: first_area_offset + batch_size as u64 - 1,
            first_realm_offset,
            last_realm_offset: first_realm_offset + batch_size as u64 - 1,
            batch_size,
            ingest_metadata,
        })
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
        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;

        // Use offset counter to find last committed offset without opening a second transaction.
        let counter_key =
            crate::domains::stream::storage::encode_offset_counter_key(realm, area, resource);
        let next_offset = match txn
            .get(&counter_key)
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(value_bytes) => {
                crate::domains::stream::storage::OffsetCounterValue::decode(&value_bytes)
                    .next_offset
            }
            None => return Ok(None),
        };
        if next_offset == 0 {
            return Ok(None);
        }

        let last_offset = next_offset - 1;

        // Use an inline buffer to avoid heap allocation on the tail-read hot path.
        let mut key_buf = [0u8; 256];
        let key_len = {
            let mut pos = 0;
            key_buf[pos] = crate::domains::stream::storage::KeyPrefix::Resource as u8;
            pos += 1;

            let realm_bytes = realm.as_bytes();
            key_buf[pos..pos + realm_bytes.len()].copy_from_slice(realm_bytes);
            pos += realm_bytes.len();
            key_buf[pos] = 0;
            pos += 1;

            let area_bytes = area.as_bytes();
            key_buf[pos..pos + area_bytes.len()].copy_from_slice(area_bytes);
            pos += area_bytes.len();
            key_buf[pos] = 0;
            pos += 1;

            let resource_bytes = resource.as_bytes();
            key_buf[pos..pos + resource_bytes.len()].copy_from_slice(resource_bytes);
            pos += resource_bytes.len();
            key_buf[pos] = 0;
            pos += 1;

            let offset_bytes = last_offset.to_be_bytes();
            key_buf[pos..pos + 8].copy_from_slice(&offset_bytes);
            pos + 8
        };

        let value_bytes = match txn
            .get(&key_buf[..key_len])
            .map_err(|e| format!("get error: {:?}", e))?
        {
            Some(bytes) => bytes.to_vec(),
            None => return Ok(None), // Record expired or deleted
        };

        let resource_value = ResourceValue::decode(&value_bytes);

        Ok(Some(StreamRecord {
            resource_offset: resource_value.resource_offset,
            area_offset: resource_value.area_offset,
            realm_offset: resource_value.realm_offset,
            body: resource_value.body,
            metadata: resource_value.metadata,
            created_at: resource_value.created_at,
        }))
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
        // Fast path for single-record reads (limit=1, no byte limit)
        if params.limit == 1 && params.max_bytes.is_none() {
            return self.read_resource_single(
                params.family,
                params.realm,
                params.area,
                params.resource,
                params.from_offset,
            );
        }

        // Build prefix for this resource
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Resource as u8];
        prefix_key.extend_from_slice(params.realm.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(params.area.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(params.resource.as_bytes());
        prefix_key.push(0);

        let start_key = encode_resource_key(
            params.realm,
            params.area,
            params.resource,
            params.from_offset,
        );

        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(start_key))
            .prefix(Bytes::from(prefix_key))
            .limit(params.limit as usize);

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
        let mut total_bytes = 0;
        let mut last_offset = params.from_offset;
        let max_bytes_limit = params.max_bytes.unwrap_or(usize::MAX);

        for (_, value_bytes) in results {
            let resource_value = ResourceValue::decode(&value_bytes);

            let record_bytes = resource_value.body.len()
                + resource_value
                    .metadata
                    .as_ref()
                    .map(|m| m.len())
                    .unwrap_or(0);

            // Byte limit enforcement
            if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                break;
            }

            last_offset = resource_value.resource_offset;
            total_bytes += record_bytes;

            records.push(StreamRecord {
                resource_offset: resource_value.resource_offset,
                area_offset: resource_value.area_offset,
                realm_offset: resource_value.realm_offset,
                body: resource_value.body,
                metadata: resource_value.metadata,
                created_at: resource_value.created_at,
            });
        }

        let has_more = records.len() == params.limit as usize || total_bytes >= max_bytes_limit;

        // Cache last record to avoid repeated last() lookups
        let (last_area_offset, last_realm_offset) = if let Some(last_rec) = records.last() {
            (last_rec.area_offset, last_rec.realm_offset)
        } else {
            (None, None)
        };

        let cursor = super::protocol::ReadCursor {
            last_resource_offset: last_offset,
            last_area_offset,
            last_realm_offset,
            has_more,
        };

        Ok((records, cursor))
    }

    /// Optimized fast-path for single-record reads (limit=1, no byte limit)
    /// Avoids scan overhead by using direct get with inline key buffer
    fn read_resource_single(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        from_offset: u64,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        // Use inline buffer for key to avoid heap allocation
        // Most keys are <200 bytes, use 256 for safety
        let mut key_buf = [0u8; 256];
        let key_len = {
            let mut pos = 0;
            key_buf[pos] = crate::domains::stream::storage::KeyPrefix::Resource as u8;
            pos += 1;

            let realm_bytes = realm.as_bytes();
            key_buf[pos..pos + realm_bytes.len()].copy_from_slice(realm_bytes);
            pos += realm_bytes.len();
            key_buf[pos] = 0;
            pos += 1;

            let area_bytes = area.as_bytes();
            key_buf[pos..pos + area_bytes.len()].copy_from_slice(area_bytes);
            pos += area_bytes.len();
            key_buf[pos] = 0;
            pos += 1;

            let resource_bytes = resource.as_bytes();
            key_buf[pos..pos + resource_bytes.len()].copy_from_slice(resource_bytes);
            pos += resource_bytes.len();
            key_buf[pos] = 0;
            pos += 1;

            let offset_bytes = from_offset.to_be_bytes();
            key_buf[pos..pos + 8].copy_from_slice(&offset_bytes);
            pos + 8
        };

        let key = &key_buf[..key_len];

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        match txn.get(key).map_err(|e| format!("get error: {:?}", e))? {
            Some(value_bytes) => {
                let resource_value = ResourceValue::decode(&value_bytes);

                let record = StreamRecord {
                    resource_offset: resource_value.resource_offset,
                    area_offset: resource_value.area_offset,
                    realm_offset: resource_value.realm_offset,
                    body: resource_value.body,
                    metadata: resource_value.metadata,
                    created_at: resource_value.created_at,
                };

                let cursor = super::protocol::ReadCursor {
                    last_resource_offset: resource_value.resource_offset,
                    last_area_offset: resource_value.area_offset,
                    last_realm_offset: resource_value.realm_offset,
                    has_more: false, // Single read never has more
                };

                Ok((vec![record], cursor))
            }
            None => {
                // No record at this offset
                let cursor = super::protocol::ReadCursor {
                    last_resource_offset: from_offset,
                    last_area_offset: None,
                    last_realm_offset: None,
                    has_more: false,
                };
                Ok((vec![], cursor))
            }
        }
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
        let watermark = self.get_watermark(family, realm, area)?;
        // Area index is pre-interleaved by writes - no K-way merge needed!
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Area as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(area.as_bytes());
        prefix_key.push(0);

        let start_key = encode_area_key(realm, area, from_offset);

        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(start_key))
            .prefix(Bytes::from(prefix_key))
            .limit(limit as usize);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let mut records = Vec::with_capacity(limit.min(1000) as usize);
        let mut total_bytes = 0;
        let mut last_area_offset = from_offset;
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);

        for (key_bytes, area_value_bytes) in results {
            let area_offset = decode_area_offset_from_key(&key_bytes)?;
            let area_value = AreaValue::decode(&area_value_bytes);

            // Watermark enforcement at area level
            if area_offset > watermark {
                break;
            }

            // Area index is now a covering index - read directly!
            let record_bytes =
                area_value.body.len() + area_value.metadata.as_ref().map(|m| m.len()).unwrap_or(0);

            if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                break;
            }

            last_area_offset = area_offset;
            total_bytes += record_bytes;

            records.push(StreamRecord {
                resource_offset: area_value.resource_offset,
                area_offset: Some(area_offset),
                realm_offset: None, // Not available in area index
                body: area_value.body,
                metadata: area_value.metadata,
                created_at: area_value.created_at,
            });
        }

        let has_more = records.len() == limit as usize || total_bytes >= max_bytes_limit;

        let (last_resource_offset, last_realm_offset) = if let Some(last_rec) = records.last() {
            (last_rec.resource_offset, last_rec.realm_offset)
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
        let realm_watermark = self.get_realm_watermark(family, realm)?;

        // Realm index is pre-interleaved by writes - scan sequentially!
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Realm as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);

        let start_key = encode_realm_key(realm, from_offset);

        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(start_key))
            .prefix(Bytes::from(prefix_key))
            .limit(limit as usize);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        let mut records = Vec::with_capacity(limit.min(1000) as usize);
        let mut total_bytes = 0;
        let mut last_realm_offset = from_offset;
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);

        for (key_bytes, realm_value_bytes) in results {
            let realm_offset = decode_realm_offset_from_key(&key_bytes)?;
            let realm_value = RealmValue::decode(&realm_value_bytes);

            // Watermark enforcement at realm level
            if realm_offset > realm_watermark {
                break;
            }

            // Realm index is now a covering index - read directly!
            let record_bytes = realm_value.body.len()
                + realm_value.metadata.as_ref().map(|m| m.len()).unwrap_or(0);

            if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                break;
            }

            last_realm_offset = realm_offset;
            total_bytes += record_bytes;

            records.push(StreamRecord {
                resource_offset: realm_value.resource_offset,
                area_offset: Some(realm_value.area_offset),
                realm_offset: Some(realm_offset),
                body: realm_value.body,
                metadata: realm_value.metadata,
                created_at: realm_value.created_at,
            });
        }

        let has_more = records.len() == limit as usize || total_bytes >= max_bytes_limit;

        let (last_resource_offset, last_area_offset) = if let Some(last_rec) = records.last() {
            (last_rec.resource_offset, last_rec.area_offset)
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
        let key = encode_watermark_key(realm, area);

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
            None => Ok(self
                .scan_next_area_offset(family, realm, area)?
                .saturating_sub(1)),
        }
    }

    pub fn set_watermark(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        watermark: u64,
    ) -> Result<(), String> {
        let key = encode_watermark_key(realm, area);
        let value = WatermarkValue { watermark };

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        txn.put(key, value.encode(), None)
            .map_err(|e| format!("txn put failed: {:?}", e))?;
        let opts = cntryl_midge::WriteOptions::sync();
        txn.commit(opts)
            .map_err(|e| format!("midge commit error: {:?}", e))
    }

    pub fn get_realm_watermark(&self, family: u64, realm: &str) -> Result<u64, String> {
        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);

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
            None => Ok(self
                .scan_next_realm_offset(family, realm)?
                .saturating_sub(1)),
        }
    }

    pub fn set_realm_watermark(
        &self,
        family: u64,
        realm: &str,
        watermark: u64,
    ) -> Result<(), String> {
        let key = crate::domains::stream::storage::encode_realm_watermark_key(realm);
        let value = WatermarkValue { watermark };

        let mut txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
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
        let last_resource_offset = self.get_last_resource_offset(family, realm, area, resource)?;
        let area_watermark = self.get_watermark(family, realm, area)?;
        let realm_watermark = self.get_realm_watermark(family, realm)?;

        Ok(super::protocol::StreamMetadata {
            max_batch_events: self.limits.max_batch_events,
            max_batch_bytes: self.limits.max_batch_bytes,
            ttl_seconds: self.ttl.ttl_seconds,
            last_resource_offset,
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
        // Build prefix for this resource
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Resource as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(area.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(resource.as_bytes());
        prefix_key.push(0);

        // Scan to get all keys (Midge returns Vec), take last
        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix_key));

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        let results = iter.collect_all();

        // Get last element from results
        if let Some((_, value_bytes)) = results.last() {
            let resource_value = ResourceValue::decode(value_bytes);
            Ok(Some(resource_value.resource_offset))
        } else {
            Ok(None)
        }
    }

    /// Get the next resource offset from metadata (TTL-safe)
    ///
    /// **CRITICAL**: Reads from OffsetCounter metadata, NOT from scanning
    /// TTL-governed data. This ensures offsets never reset after TTL expiry.
    ///
    /// Returns 0 if no counter exists (new resource), otherwise returns
    /// the next offset to use.
    pub fn get_next_resource_offset(
        &self,
        family: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        let resource_meta_key = encode_resource_meta_key(realm, area, resource);
        let counter_key = encode_offset_counter_key(realm, area, resource);

        let txn = self
            .db
            .begin_tx(family as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("failed to begin tx: {:?}", e))?;
        match txn.get(&resource_meta_key) {
            Ok(Some(value_bytes)) => Ok(ResourceMetaValue::decode(&value_bytes).next_offset),
            Ok(None) => match txn.get(&counter_key) {
                Ok(Some(value_bytes)) => Ok(OffsetCounterValue::decode(&value_bytes).next_offset),
                Ok(None) => Ok(0),
                Err(e) => Err(format!("get_next_resource_offset error: {:?}", e)),
            },
            Err(e) => Err(format!("get_next_resource_offset error: {:?}", e)),
        }
    }
}
