//! Stream storage layer - STORAGE ONLY, NO SEQUENCING

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use bytes::Bytes;
use uuid::Uuid;
use cntryl_midge::{WriteBatch, KvTransaction};

use super::protocol::{StreamRecord, IngestMetadata};
use super::storage::{
    encode_resource_key, encode_area_key, encode_realm_key, encode_watermark_key,
    encode_staging_key, encode_staging_value, decode_staging_value,
    decode_area_offset_from_key, decode_realm_offset_from_key,
    ResourceValue, AreaValue, RealmValue, WatermarkValue,
};

#[derive(Debug, Clone)]
pub struct EventPayload {
    pub body: Bytes,
    pub metadata: Option<Bytes>,
}

/// Session using KvTransaction for O(1) memory streaming
struct AppendSession {
    realm: String,
    area: String,
    resource: String,
    session_id: String,
    txn: Box<dyn KvTransaction>,
    event_count: usize,
    total_bytes: usize,
    ingest_metadata: Option<IngestMetadata>,
}

pub type SessionId = String;

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

#[derive(Debug, Clone, Copy)]
pub struct StreamTTL {
    pub ttl_seconds: Option<u64>,
}

impl Default for StreamTTL {
    fn default() -> Self {
        Self { ttl_seconds: None }
    }
}

impl StreamTTL {
    pub fn with_seconds(seconds: u64) -> Self {
        Self { ttl_seconds: Some(seconds) }
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

pub struct StreamStore {
    db: Arc<cntryl_midge::MidgeEngine>,
    limits: BatchLimits,
    sessions: Arc<Mutex<HashMap<SessionId, AppendSession>>>,
    ttl: StreamTTL,
}

impl StreamStore {
    pub fn new(db: Arc<cntryl_midge::MidgeEngine>) -> Self {
        Self::with_config(db, BatchLimits::default(), StreamTTL::default())
    }
    
    pub fn with_limits(db: Arc<cntryl_midge::MidgeEngine>, limits: BatchLimits) -> Self {
        Self::with_config(db, limits, StreamTTL::default())
    }
    
    pub fn with_config(db: Arc<cntryl_midge::MidgeEngine>, limits: BatchLimits, ttl: StreamTTL) -> Self {
        Self { 
            db, 
            limits,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            ttl,
        }
    }
    
    pub fn begin_session(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        ingest_metadata: Option<IngestMetadata>,
    ) -> Result<SessionId, String> {
        let session_id = Uuid::new_v4().to_string();
        
        // Create transaction for staging (O(1) memory)
        let cf = self.db.default_column_family();
        let txn = self.db.begin_transaction(&cf)
            .map_err(|e| format!("failed to begin transaction: {:?}", e))?;
        
        let session = AppendSession {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            session_id: session_id.clone(),
            txn,
            event_count: 0,
            total_bytes: 0,
            ingest_metadata,
        };
        
        self.sessions.lock().unwrap().insert(session_id.clone(), session);
        Ok(session_id)
    }
    
    pub fn append_to_session(
        &self,
        session_id: &SessionId,
        event: EventPayload,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions.get_mut(session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;
        
        let event_bytes = event.body.len() + event.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        if session.total_bytes + event_bytes > self.limits.max_batch_bytes {
            return Err(format!(
                "ERR_BATCH_TOO_LARGE: total {} + event {} exceeds max_batch_bytes {}",
                session.total_bytes, event_bytes, self.limits.max_batch_bytes
            ));
        }
        
        // Write to staging transaction (O(1) memory - no heap buffer)
        let staging_key = encode_staging_key(&session.session_id, session.event_count);
        let staging_value = encode_staging_value(&event);
        
        session.txn.put(&staging_key, &staging_value)
            .map_err(|e| format!("staging write failed: {:?}", e))?;
        
        session.total_bytes += event_bytes;
        session.event_count += 1;
        
        Ok(())
    }
    
    pub fn commit_session(
        &self,
        session_id: &SessionId,
        resource_offsets: Vec<u64>,
        area_offsets: Vec<u64>,
        realm_offsets: Vec<u64>,
    ) -> Result<CommitResponse, String> {
        let session = self.sessions.lock().unwrap()
            .remove(session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;
        
        if session.event_count == 0 {
            return Err("ERR_EMPTY_BATCH".to_string());
        }
        
        let batch_size = session.event_count;
        
        if resource_offsets.len() != batch_size || area_offsets.len() != batch_size || realm_offsets.len() != batch_size {
            return Err("ERR_INVALID_OFFSETS".to_string());
        }
        
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let cf = self.db.default_column_family();
        let mut batch = WriteBatch::new();
        
        // Read events from staging transaction and write to final indexes
        for i in 0..batch_size {
            let staging_key = encode_staging_key(&session.session_id, i);
            let staging_value = session.txn.get(&staging_key)
                .map_err(|e| format!("staging read failed: {:?}", e))?
                .ok_or_else(|| format!("staging key {} not found", i))?;
            
            let event = decode_staging_value(&staging_value)?;
            let resource_offset = resource_offsets[i];
            let area_offset = area_offsets[i];
            let realm_offset = realm_offsets[i];
            
            let resource_key = encode_resource_key(&session.realm, &session.area, &session.resource, resource_offset);
            let resource_value = ResourceValue {
                resource_offset,
                area_offset: Some(area_offset),
                realm_offset: Some(realm_offset),
                body: event.body.clone(),
                metadata: event.metadata.clone(),
                created_at,
            };
            if let Some(ttl_secs) = self.ttl.ttl_seconds {
                batch.put_with_ttl(
                    cf.id(),
                    Bytes::from(resource_key),
                    Bytes::from(resource_value.encode()),
                    ttl_secs,
                );
            } else {
                batch.put_cf(
                    cf.id(),
                    Bytes::from(resource_key),
                    Bytes::from(resource_value.encode()),
                );
            }
            
            let area_key = encode_area_key(&session.realm, &session.area, area_offset);
            let area_value = AreaValue {
                realm: session.realm.clone(),
                area: session.area.clone(),
                resource: session.resource.clone(),
                resource_offset,
            };
            if let Some(ttl_secs) = self.ttl.ttl_seconds {
                batch.put_with_ttl(
                    cf.id(),
                    Bytes::from(area_key),
                    Bytes::from(area_value.encode()),
                    ttl_secs,
                );
            } else {
                batch.put_cf(
                    cf.id(),
                    Bytes::from(area_key),
                    Bytes::from(area_value.encode()),
                );
            }
            
            let realm_key = encode_realm_key(&session.realm, realm_offset);
            let realm_value = RealmValue {
                realm: session.realm.clone(),
                area: session.area.clone(),
                area_offset,
            };
            if let Some(ttl_secs) = self.ttl.ttl_seconds {
                batch.put_with_ttl(
                    cf.id(),
                    Bytes::from(realm_key),
                    Bytes::from(realm_value.encode()),
                    ttl_secs,
                );
            } else {
                batch.put_cf(
                    cf.id(),
                    Bytes::from(realm_key),
                    Bytes::from(realm_value.encode()),
                );
            }
        }
        
        self.db.write_batch(&batch)
            .map_err(|e| format!("midge write_batch error: {:?}", e))?;
        
        Ok(CommitResponse {
            first_resource_offset: resource_offsets[0],
            last_resource_offset: resource_offsets[batch_size - 1],
            first_area_offset: area_offsets[0],
            last_area_offset: area_offsets[batch_size - 1],
            first_realm_offset: realm_offsets[0],
            last_realm_offset: realm_offsets[batch_size - 1],
            batch_size,
            ingest_metadata: session.ingest_metadata,
        })
    }
    
    pub fn abort_session(&self, session_id: &SessionId) -> Result<(), String> {
        self.sessions.lock().unwrap()
            .remove(session_id)
            .ok_or_else(|| "ERR_SESSION_NOT_FOUND".to_string())?;
        Ok(())
    }
    
    pub fn session_event_count(&self, session_id: &SessionId) -> Option<usize> {
        self.sessions.lock().unwrap()
            .get(session_id)
            .map(|s| s.event_count)
    }
    
    /// Peek at the last committed record in a resource stream (tail operation)
    pub fn peek_resource(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<StreamRecord>, String> {
        let cf = self.db.default_column_family();
        let watermark = self.get_watermark(realm, area)?;
        
        // If watermark is 0, stream is empty
        if watermark == 0 {
            return Ok(None);
        }
        
        // Read the single record at the watermark offset
        let key = encode_resource_key(realm, area, resource, watermark);
        
        match self.db.get(&cf, &key)
            .map_err(|e| format!("get error: {:?}", e))? {
            Some(value_bytes) => {
                let resource_value = ResourceValue::decode(&value_bytes);
                
                // TTL filtering
                if let Some(ttl_secs) = self.ttl.ttl_seconds {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let age_ms = now.saturating_sub(resource_value.created_at / 1000) * 1000;
                    if age_ms > (ttl_secs * 1000) {
                        return Ok(None);
                    }
                }
                
                Ok(Some(StreamRecord {
                    resource_offset: resource_value.resource_offset,
                    area_offset: resource_value.area_offset,
                    realm_offset: resource_value.realm_offset,
                    body: resource_value.body,
                    metadata: resource_value.metadata,
                    created_at: resource_value.created_at,
                }))
            }
            None => Ok(None),
        }
    }
    
    pub fn read_resource(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        let cf = self.db.default_column_family();
        let watermark = self.get_watermark(realm, area)?;
        
        // Build prefix for this resource
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Resource as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(area.as_bytes());
        prefix_key.push(0);
        prefix_key.extend_from_slice(resource.as_bytes());
        prefix_key.push(0);
        
        let start_key = encode_resource_key(realm, area, resource, from_offset);
        
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(start_key))
            .prefix(Bytes::from(prefix_key))
            .limit(limit as usize);
        
        let results = self.db.scan(&cf, &query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        
        let mut records = Vec::new();
        let mut total_bytes = 0;
        let mut last_offset = from_offset;
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);
        
        for (_, value_bytes) in results {
            let resource_value = ResourceValue::decode(&value_bytes);
            
            // Watermark enforcement: stop if beyond committed data
            if resource_value.resource_offset > watermark {
                break;
            }
            
            // TTL filtering: skip expired records inline, preserve offsets
            if let Some(ttl_secs) = self.ttl.ttl_seconds {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let age_ms = now.saturating_sub(resource_value.created_at / 1000) * 1000;
                if age_ms > (ttl_secs * 1000) {
                    last_offset = resource_value.resource_offset;
                    continue;  // Skip expired, keep scanning
                }
            }
            
            let record_bytes = resource_value.body.len() + 
                resource_value.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            
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
        
        let has_more = records.len() == limit as usize || 
            (last_offset < watermark && total_bytes >= max_bytes_limit);
        
        let cursor = super::protocol::ReadCursor {
            last_resource_offset: last_offset,
            last_area_offset: records.last().and_then(|r| r.area_offset),
            last_realm_offset: records.last().and_then(|r| r.realm_offset),
            has_more,
        };
        
        Ok((records, cursor))
    }
    
    pub fn read_area(
        &self,
        realm: &str,
        area: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        let cf = self.db.default_column_family();
        let watermark = self.get_watermark(realm, area)?;
        
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
        
        let results = self.db.scan(&cf, &query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        
        let mut records = Vec::new();
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
            
            // Fetch actual event from resource index
            let resource_key = encode_resource_key(
                &area_value.realm,
                &area_value.area,
                &area_value.resource,
                area_value.resource_offset,
            );
            
            if let Some(resource_bytes) = self.db.get(&cf, &resource_key)
                .map_err(|e| format!("get error: {:?}", e))? {
                let resource_value = ResourceValue::decode(&resource_bytes);
                
                // TTL filtering inline
                if let Some(ttl_secs) = self.ttl.ttl_seconds {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let age_ms = now.saturating_sub(resource_value.created_at / 1000) * 1000;
                    if age_ms > (ttl_secs * 1000) {
                        last_area_offset = area_offset;
                        continue;
                    }
                }
                
                let record_bytes = resource_value.body.len() + 
                    resource_value.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                
                if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                    break;
                }
                
                last_area_offset = area_offset;
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
        }
        
        let has_more = records.len() == limit as usize || total_bytes >= max_bytes_limit;
        
        let cursor = super::protocol::ReadCursor {
            last_resource_offset: records.last().map(|r| r.resource_offset).unwrap_or(0),
            last_area_offset: Some(last_area_offset),
            last_realm_offset: records.last().and_then(|r| r.realm_offset),
            has_more,
        };
        
        Ok((records, cursor))
    }
    
    pub fn read_realm(
        &self,
        realm: &str,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    ) -> Result<(Vec<StreamRecord>, super::protocol::ReadCursor), String> {
        let cf = self.db.default_column_family();
        
        // Realm index is pre-interleaved by writes - scan sequentially!
        let mut prefix_key = vec![crate::domains::stream::storage::KeyPrefix::Realm as u8];
        prefix_key.extend_from_slice(realm.as_bytes());
        prefix_key.push(0);
        
        let start_key = encode_realm_key(realm, from_offset);
        
        let query = cntryl_midge::Query::new()
            .start_key(Bytes::from(start_key))
            .prefix(Bytes::from(prefix_key))
            .limit(limit as usize);
        
        let results = self.db.scan(&cf, &query)
            .map_err(|e| format!("scan error: {:?}", e))?;
        
        let mut records = Vec::new();
        let mut total_bytes = 0;
        let mut last_realm_offset = from_offset;
        let max_bytes_limit = max_bytes.unwrap_or(usize::MAX);
        
        for (key_bytes, realm_value_bytes) in results {
            let realm_offset = decode_realm_offset_from_key(&key_bytes)?;
            let realm_value = RealmValue::decode(&realm_value_bytes);
            
            // No watermark at realm level - watermarks are per-area
            // Realm reads show globally committed data
            
            // Fetch area record to get resource pointer
            let area_key = encode_area_key(&realm_value.realm, &realm_value.area, realm_value.area_offset);
            
            if let Some(area_bytes) = self.db.get(&cf, &area_key)
                .map_err(|e| format!("get error: {:?}", e))? {
                let area_value = AreaValue::decode(&area_bytes);
                
                // Fetch actual event from resource index
                let resource_key = encode_resource_key(
                    &area_value.realm,
                    &area_value.area,
                    &area_value.resource,
                    area_value.resource_offset,
                );
                
                if let Some(resource_bytes) = self.db.get(&cf, &resource_key)
                    .map_err(|e| format!("get error: {:?}", e))? {
                    let resource_value = ResourceValue::decode(&resource_bytes);
                    
                    // TTL filtering inline
                    if let Some(ttl_secs) = self.ttl.ttl_seconds {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        let age_ms = now.saturating_sub(resource_value.created_at / 1000) * 1000;
                        if age_ms > (ttl_secs * 1000) {
                            last_realm_offset = realm_offset;
                            continue;
                        }
                    }
                    
                    let record_bytes = resource_value.body.len() + 
                        resource_value.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
                    
                    if total_bytes + record_bytes > max_bytes_limit && !records.is_empty() {
                        break;
                    }
                    
                    last_realm_offset = realm_offset;
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
            }
        }
        
        let has_more = records.len() == limit as usize || total_bytes >= max_bytes_limit;
        
        let cursor = super::protocol::ReadCursor {
            last_resource_offset: records.last().map(|r| r.resource_offset).unwrap_or(0),
            last_area_offset: records.last().and_then(|r| r.area_offset),
            last_realm_offset: Some(last_realm_offset),
            has_more,
        };
        
        Ok((records, cursor))
    }
    
    pub fn get_watermark(&self, realm: &str, area: &str) -> Result<u64, String> {
        let cf = self.db.default_column_family();
        let key = encode_watermark_key(realm, area);
        
        match self.db.get(&cf, &key).map_err(|e| format!("midge get error: {:?}", e))? {
            Some(bytes) => {
                let value = WatermarkValue::decode(&bytes);
                Ok(value.watermark)
            },
            None => Ok(0),
        }
    }
    
    pub fn set_watermark(&self, realm: &str, area: &str, watermark: u64) -> Result<(), String> {
        let cf = self.db.default_column_family();
        let key = encode_watermark_key(realm, area);
        let value = WatermarkValue { watermark };
        
        self.db.put(&cf, &key, &value.encode())
            .map_err(|e| format!("midge put error: {:?}", e))
    }
}
