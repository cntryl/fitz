//! In-memory storage backend (simple prototype)

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Record {
    pub id: String,
    pub route: String,
    pub body: Vec<u8>,
    /// optional lease expiry as epoch seconds (approximate). None means not reserved.
    pub lease_expiry: Option<u64>,
    /// which consumer currently holds the lease (optional token or client id)
    pub lease_owner: Option<String>,
    /// number of times this record has been delivered (reserved)
    pub delivery_count: u32,
    /// creation time (epoch seconds) for TTL
    pub created_at: u64,
    /// per-message TTL in seconds (0/None means no per-message TTL)
    pub ttl_secs: Option<u64>,
}

// Stream support (in-memory)
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub resource_seq: u64,     // Client-controlled, 0-indexed monotonic
    pub area_seq: Option<u64>, // Server-assigned at finalization
    pub body: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub created_at: u64,
    pub is_end: bool, // Stream finalization marker
}

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub resource_seq: u64,
    pub area_seq_range: Option<std::ops::Range<u64>>,
}

#[derive(Debug, Clone)]
pub struct AreaReadResponse {
    pub events: Vec<StreamEvent>,
    pub watermark: u64,
}

#[derive(Debug, Clone)]
pub enum StreamError {
    SequenceGap { expected: u64, received: u64 },
    SequenceConflict { seq: u64 },
    StreamClosed,
    WrongExpectedVersion(u64), // carries current head (legacy)
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    Any,
    NoStream,
    StreamExists,
    Exact(u64),
}

// Queue configuration (hierarchical)
#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub dlq_threshold: u32,
    pub default_visibility_secs: u32, // default lease duration when not specified
    pub ttl_secs: u64,                // 0 means no TTL expiry
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            dlq_threshold: 5,
            default_visibility_secs: 30,
            ttl_secs: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueueScope {
    Realm {
        realm: String,
    },
    Area {
        realm: String,
        area: String,
    },
    Resource {
        realm: String,
        area: String,
        resource: String,
    },
}

/// A tiny in-memory store: map of route -> vector of records
#[derive(Debug, Default)]
pub struct MemStore {
    inner: Mutex<HashMap<String, Vec<Record>>>,
    /// per-store HMAC key used to sign delivery tokens
    token_key: Vec<u8>,
    // streams map
    streams: Mutex<HashMap<String, Vec<StreamEvent>>>,
    /// area sequence counter: (realm, area) -> next_area_seq
    area_seq_counter: Mutex<HashMap<(String, String), u64>>,
    /// KV store: route -> (key -> value)
    kv_store: Mutex<HashMap<String, HashMap<String, Vec<u8>>>>,
    // queue configs
    cfg_realm: Mutex<HashMap<String, QueueConfig>>,
    cfg_area: Mutex<HashMap<(String, String), QueueConfig>>, // (realm, area)
    cfg_resource: Mutex<HashMap<(String, String, String), QueueConfig>>, // (realm, area, resource)
}

/// Simple queue statistics returned by store
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub in_flight_count: u32,
}

impl MemStore {
    pub fn new() -> Self {
        // generate random key using uuid v4
        let uuid = Uuid::new_v4();
        let key = uuid.as_bytes().to_vec();
        Self {
            inner: Mutex::new(HashMap::new()),
            token_key: key,
            streams: Mutex::new(HashMap::new()),
            area_seq_counter: Mutex::new(HashMap::new()),
            kv_store: Mutex::new(HashMap::new()),
            cfg_realm: Mutex::new(HashMap::new()),
            cfg_area: Mutex::new(HashMap::new()),
            cfg_resource: Mutex::new(HashMap::new()),
        }
    }

    /// Append a record to a route (async to match call sites)
    /// Applies a simple backpressure guard for RPC routes ("rpc://" prefix):
    /// if total in-memory bytes for RPC records would exceed MAX_RPC_BYTES, returns Err("backpressure").
    pub async fn append(&mut self, route: String, id: String, body: Vec<u8>, ttl_secs: Option<u64>) -> Result<(), String> {
        const MAX_RPC_BYTES: usize = 32 * 1024 * 1024; // 32 MiB cap for all rpc:// routes combined
        let mut guard = self.inner.lock().await;
        if route.starts_with("rpc://") {
            // compute current rpc bytes
            let mut curr: usize = 0;
            for (r, vec) in guard.iter() {
                if r.starts_with("rpc://") {
                    for rec in vec.iter() {
                        curr = curr.saturating_add(rec.body.len());
                    }
                }
            }
            if curr.saturating_add(body.len()) > MAX_RPC_BYTES {
                return Err("backpressure".to_string());
            }
        }
        let v = guard.entry(route.clone()).or_insert_with(Vec::new);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        v.push(Record {
            id,
            route,
            body,
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
            created_at: now,
            ttl_secs,
        });
        Ok(())
    }

    /// Attempt to extend the lease for a specific message id on a route.
    /// `delivery_token` must match the stored `lease_owner` token returned by `reserve_next`.
    /// `add_secs` is number of seconds to extend (relative). Returns new TTL remaining (secs)
    /// or an error string explaining failure.
    pub async fn extend_lease(
        &mut self,
        route: &str,
        id: &str,
        delivery_token: &str,
        add_secs: u32,
    ) -> Result<u32, String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut guard = self.inner.lock().await;
        let v = guard
            .get_mut(route)
            .ok_or_else(|| "route not found".to_string())?;
        if let Some(rec) = v.iter_mut().find(|r| r.id == id) {
            // verify owner
            if let Some(curr_token) = &rec.lease_owner {
                if curr_token != delivery_token {
                    return Err("invalid delivery token".to_string());
                }
            } else {
                return Err("no active lease".to_string());
            }
            // compute new expiry
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "time error".to_string())?
                .as_secs();
            let base = rec.lease_expiry.unwrap_or(now);
            let new_expiry = base.saturating_add(add_secs as u64);
            rec.lease_expiry = Some(new_expiry);
            let remaining = if new_expiry > now {
                (new_expiry - now) as u32
            } else {
                0
            };
            return Ok(remaining);
        }
        Err("id not found".to_string())
    }

    /// Helper to read back for tests (not optimized)
    pub async fn read_all(&self, route: &str) -> Vec<Record> {
        let guard = self.inner.lock().await;
        guard.get(route).cloned().unwrap_or_default()
    }

    /// Peek next available record without reserving; returns id/body if available
    pub async fn peek_next(&self, route: &str) -> Option<(String, Vec<u8>)> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let guard = self.inner.lock().await;
        let v = guard.get(route)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs())
            .unwrap_or(0);
        v.iter()
            .find(|r| r.lease_expiry.map(|e| e <= now).unwrap_or(true))
            .map(|r| (r.id.clone(), r.body.clone()))
    }

    /// Reserve the next available item on the route for `owner` for `lease_secs` seconds.
    /// Returns (id, body) if successful, or Err string if none available.
    /// Reserve the next available item on the route for `owner` for `lease_secs` seconds.
    /// Returns (id, body, delivery_token) if successful.
    pub async fn reserve_next(
        &mut self,
        route: &str,
        lease_secs: u32,
    ) -> Result<(String, Vec<u8>, String), String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut guard = self.inner.lock().await;
        let mut to_dlq: Vec<Record> = Vec::new();
        let mut result: Option<(String, Vec<u8>, String)> = None;
        {
            let v = guard
                .get_mut(route)
                .ok_or_else(|| "route not found".to_string())?;
            // find first item without an active lease (or expired)
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "time error".to_string())?
                .as_secs();
            // DLQ policy: move poison messages to <route>.dlq when delivery_count >= threshold (resolved via config)
            let cfg = self.resolve_queue_cfg_for_route(route).await;
            let dlq_threshold = cfg.dlq_threshold;
            let effective_lease = lease_secs as u64; // honoring explicit 0 for immediate expiry; session can apply defaults
            loop {
                // position of next available item
                // consider TTL
                // remove expired by TTL and continue
                // Enforce per-record TTL first, then queue-level TTL
                let mut i = 0;
                while i < v.len() {
                    let expired = if let Some(rec_ttl) = v[i].ttl_secs {
                        now.saturating_sub(v[i].created_at) >= rec_ttl
                    } else if cfg.ttl_secs > 0 {
                        now.saturating_sub(v[i].created_at) >= cfg.ttl_secs
                    } else {
                        false
                    };
                    if expired {
                        v.remove(i);
                    } else {
                        i += 1;
                    }
                }
                let pos_opt = v
                    .iter()
                    .position(|r| r.lease_expiry.map(|e| e <= now).unwrap_or(true));
                let Some(pos) = pos_opt else {
                    break;
                };
                if v[pos].delivery_count >= dlq_threshold {
                    // move to dlq (defer insert until after we drop v)
                    let mut rec = v.remove(pos);
                    rec.lease_expiry = None;
                    rec.lease_owner = None;
                    to_dlq.push(rec);
                    continue;
                }
                // else reserve this record
                let rec = &mut v[pos];
                let expiry = now.saturating_add(effective_lease);
                // generate a HMAC-SHA256 token over route:id:now and base64-encode it
                type HmacSha256 = Hmac<Sha256>;
                let mut mac = HmacSha256::new_from_slice(&self.token_key)
                    .map_err(|_| "hmac error".to_string())?;
                mac.update(format!("{}:{}:{}", route, rec.id, now).as_bytes());
                let result_mac = mac.finalize();
                let bytes = result_mac.into_bytes();
                let token = general_purpose::STANDARD.encode(bytes);
                rec.lease_expiry = Some(expiry);
                rec.lease_owner = Some(token.clone());
                rec.delivery_count = rec.delivery_count.saturating_add(1);
                result = Some((rec.id.clone(), rec.body.clone(), token));
                break;
            }
        }
        // Now apply DLQ moves after releasing the borrow on v
        if !to_dlq.is_empty() {
            let dlq_key = format!("{}.dlq", route);
            let dlq_vec = guard.entry(dlq_key).or_insert_with(Vec::new);
            dlq_vec.extend(to_dlq.into_iter());
        }
        if let Some(r) = result {
            Ok(r)
        } else {
            Err("no available messages".to_string())
        }
    }

    // Configuration management
    pub async fn set_queue_config(&self, scope: QueueScope, cfg: QueueConfig) {
        match scope {
            QueueScope::Realm { realm } => {
                let mut g = self.cfg_realm.lock().await;
                g.insert(realm, cfg);
            }
            QueueScope::Area { realm, area } => {
                let mut g = self.cfg_area.lock().await;
                g.insert((realm, area), cfg);
            }
            QueueScope::Resource {
                realm,
                area,
                resource,
            } => {
                let mut g = self.cfg_resource.lock().await;
                g.insert((realm, area, resource), cfg);
            }
        }
    }

    async fn resolve_queue_cfg_for_route(&self, route: &str) -> QueueConfig {
        // expected scheme: queue://realm/area/resource (area and resource optional)
        if let Some(rest) = route.strip_prefix("queue://") {
            let parts: Vec<&str> = rest.split('/').collect();
            if parts.is_empty() || parts[0].is_empty() {
                return QueueConfig::default();
            }
            let realm = parts[0].to_string();
            let area_opt = parts.get(1).map(|s| s.to_string());
            let res_opt = parts.get(2).map(|s| s.to_string());

            // resolve in priority: realm -> area -> resource (resource overrides area overrides realm)
            let mut eff = QueueConfig::default();
            if let Some(cfg) = {
                let g = self.cfg_realm.lock().await;
                g.get(&realm).copied()
            } {
                eff = cfg;
            }
            if let Some(area) = &area_opt {
                if let Some(cfg) = {
                    let g = self.cfg_area.lock().await;
                    g.get(&(realm.clone(), area.clone())).copied()
                } {
                    eff = cfg;
                }
            }
            if let (Some(area), Some(res)) = (area_opt.as_ref(), res_opt.as_ref()) {
                if let Some(cfg) = {
                    let g = self.cfg_resource.lock().await;
                    g.get(&(realm.clone(), area.clone(), res.clone())).copied()
                } {
                    eff = cfg;
                }
            }
            eff
        } else {
            QueueConfig::default()
        }
    }

    /// Consume (ack) a reserved message: remove it if the delivery token matches the lease owner.
    pub async fn consume(
        &mut self,
        route: &str,
        id: &str,
        delivery_token: &str,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock().await;
        let v = guard
            .get_mut(route)
            .ok_or_else(|| "route not found".to_string())?;
        if let Some(pos) = v.iter().position(|r| r.id == id) {
            let rec = &v[pos];
            match &rec.lease_owner {
                Some(tok) if tok == delivery_token => {
                    v.remove(pos);
                    Ok(())
                }
                Some(_) => Err("invalid delivery token".to_string()),
                None => Err("no active lease".to_string()),
            }
        } else {
            Err("id not found".to_string())
        }
    }

    /// Get message metadata (cloned Record) if present
    pub async fn get_message_metadata(&self, route: &str, id: &str) -> Result<Option<Record>, String> {
        let guard = self.inner.lock().await;
        Ok(guard.get(route).and_then(|v| v.iter().find(|r| r.id == id).cloned()))
    }

    /// Get simple queue statistics (e.g., in-flight count)
    pub async fn get_queue_stats(&self, route: &str) -> Result<QueueStats, String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let guard = self.inner.lock().await;
        let v = guard.get(route).cloned().unwrap_or_default();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut in_flight: u32 = 0;
        for rec in v.into_iter() {
            if let Some(expiry) = rec.lease_expiry {
                if expiry > now {
                    in_flight = in_flight.saturating_add(1);
                }
            }
        }
        Ok(QueueStats { in_flight_count: in_flight })
    }

    /// Explicitly move a message to the DLQ for the given route. The delivery_token must match
    /// the current lease owner. When moving, choose TTL: preserve remaining message TTL if it's
    /// less than DLQ retention, otherwise apply DLQ TTL.
    pub async fn move_to_dlq(&mut self, route: &str, id: &str, delivery_token: &str) -> Result<(), String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut guard = self.inner.lock().await;
        let v = guard
            .get_mut(route)
            .ok_or_else(|| "route not found".to_string())?;
        if let Some(pos) = v.iter().position(|r| r.id == id) {
            // verify owner
            if let Some(curr_token) = &v[pos].lease_owner {
                if curr_token != delivery_token {
                    return Err("invalid delivery token".to_string());
                }
            } else {
                return Err("no active lease".to_string());
            }

            // remove record
            let mut rec = v.remove(pos);
            // prepare DLQ route and config
            let dlq_route = format!("{}.dlq", route);
            let dlq_cfg = self.resolve_queue_cfg_for_route(&dlq_route).await;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            // compute remaining per-message TTL if present
            let remaining_opt = rec.ttl_secs.map(|orig| {
                if now.saturating_sub(rec.created_at) >= orig {
                    0u64
                } else {
                    orig.saturating_sub(now.saturating_sub(rec.created_at))
                }
            });

            // decide effective TTL to apply in DLQ
            let effective_ttl = if let Some(rem) = remaining_opt {
                if rem == 0 {
                    None
                } else if dlq_cfg.ttl_secs > 0 && rem < dlq_cfg.ttl_secs {
                    Some(rem)
                } else if dlq_cfg.ttl_secs > 0 {
                    Some(dlq_cfg.ttl_secs)
                } else {
                    None
                }
            } else {
                if dlq_cfg.ttl_secs > 0 {
                    Some(dlq_cfg.ttl_secs)
                } else {
                    None
                }
            };

            // reset lease and set created_at to now for DLQ TTL semantics
            rec.lease_owner = None;
            rec.lease_expiry = None;
            rec.created_at = now;
            rec.ttl_secs = effective_ttl;

            let dlq_vec = guard.entry(dlq_route).or_insert_with(Vec::new);
            dlq_vec.push(rec);
            Ok(())
        } else {
            Err("id not found".to_string())
        }
    }
}

// Stream APIs
impl MemStore {
    /// Return current head revision (last seq) or None if stream empty
    pub async fn stream_head(&self, route: &str) -> Option<u64> {
        let g = self.streams.lock().await;
        g.get(route).and_then(|v| v.last().map(|e| e.resource_seq))
    }

    pub async fn stream_append_with_expected(
        &self,
        route: &str,
        _id: Option<String>,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        expected: ExpectedRevision,
    ) -> Result<u64, StreamError> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut g = self.streams.lock().await;
        let v = g.entry(route.to_string()).or_insert_with(Vec::new);
        let head = v.last().map(|e| e.resource_seq);
        let ok = match expected {
            ExpectedRevision::Any => true,
            ExpectedRevision::NoStream => head.is_none(),
            ExpectedRevision::StreamExists => head.is_some(),
            ExpectedRevision::Exact(n) => head.unwrap_or(0) == n,
        };
        if !ok {
            return Err(StreamError::WrongExpectedVersion(head.unwrap_or(0)));
        }
        let next = head.unwrap_or(0).saturating_add(1);
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        v.push(StreamEvent {
            resource_seq: next,
            area_seq: None,
            body,
            metadata,
            created_at,
            is_end: false,
        });
        Ok(next)
    }

    /// New stream append API with client-controlled sequences and gap detection.
    /// Returns AppendResult with resource_seq and optional area_seq_range.
    pub async fn stream_append_new(
        &self,
        route: &str,
        resource_seq: u64,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        is_end: bool,
    ) -> Result<AppendResult, StreamError> {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        // Parse route: stream://realm/area/resource
        let parts: Vec<&str> = route.split('/').collect();
        if parts.len() < 5 || parts[0] != "stream:" {
            return Err(StreamError::Other("invalid stream route".to_string()));
        }
        let realm = parts[2];
        let area = parts[3];
        
        let mut g = self.streams.lock().await;
        let v = g.entry(route.to_string()).or_insert_with(Vec::new);
        
        // Check if stream is already closed (has an event with is_end=true)
        if v.iter().any(|e| e.is_end) {
            return Err(StreamError::StreamClosed);
        }
        
        // Gap detection: if resource_seq > 0, ensure prev exists
        if resource_seq > 0 {
            let has_prev = v.iter().any(|e| e.resource_seq == resource_seq - 1);
            if !has_prev {
                return Err(StreamError::SequenceGap {
                    expected: resource_seq - 1,
                    received: resource_seq,
                });
            }
        }
        
        // Check for duplicate/conflict
        if let Some(existing) = v.iter().find(|e| e.resource_seq == resource_seq) {
            // Idempotent retry: same body is OK
            if existing.body == body {
                // Return existing result (no area_seq assigned yet unless it was finalized)
                return Ok(AppendResult {
                    resource_seq,
                    area_seq_range: existing.area_seq.map(|s| s..s+1),
                });
            } else {
                // Conflict: different body for same seq
                return Err(StreamError::SequenceConflict { seq: resource_seq });
            }
        }
        
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        // Assign area_seq if finalizing
        let (area_seq, area_seq_range) = if is_end {
            let mut counter_g = self.area_seq_counter.lock().await;
            let next_area_seq = counter_g.entry((realm.to_string(), area.to_string()))
                .or_insert(0);
            let start_area_seq = *next_area_seq;
            
            // Count all uncommitted events (those without area_seq) + this one
            let uncommitted_count = v.iter().filter(|e| e.area_seq.is_none()).count() + 1;
            
            // Assign area_seq to ALL uncommitted events in this stream
            for event in v.iter_mut() {
                if event.area_seq.is_none() {
                    event.area_seq = Some(*next_area_seq);
                    *next_area_seq += 1;
                }
            }
            
            // Assign to the new event we're about to push
            let assigned = *next_area_seq;
            *next_area_seq += 1;
            drop(counter_g); // release counter lock
            
            (Some(assigned), Some(start_area_seq..start_area_seq + uncommitted_count as u64))
        } else {
            (None, None)
        };
        
        v.push(StreamEvent {
            resource_seq,
            area_seq,
            body,
            metadata,
            created_at,
            is_end,
        });
        
        Ok(AppendResult {
            resource_seq,
            area_seq_range,
        })
    }

    /// Read events from a specific resource stream (no watermark filtering)
    pub async fn stream_read(&self, route: &str, from_seq: u64, limit: usize) -> Vec<StreamEvent> {
        let g = self.streams.lock().await;
        let v = g.get(route).cloned().unwrap_or_default();
        v.into_iter()
            .filter(|e| e.resource_seq >= from_seq)
            .take(limit)
            .collect()
    }

    /// Read interleaved events from all resources in an area (watermark-controlled)
    pub async fn stream_read_area(
        &self,
        realm: &str,
        area: &str,
        from_seq: u64,
        limit: usize,
    ) -> AreaReadResponse {
        let g = self.streams.lock().await;
        let prefix = format!("stream://{}/{}/", realm, area);
        
        // Collect all finalized events (those with area_seq assigned)
        let mut events: Vec<StreamEvent> = g.iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .flat_map(|(_, v)| v.iter().filter(|e| e.area_seq.is_some()).cloned())
            .collect();
        
        // Sort by area_seq
        events.sort_by_key(|e| e.area_seq.unwrap());
        
        // Calculate watermark: highest contiguous area_seq starting from 0
        let mut watermark = 0u64;
        for e in events.iter() {
            if let Some(seq) = e.area_seq {
                if seq == watermark {
                    watermark += 1;
                } else {
                    break;
                }
            }
        }
        
        // Filter by from_seq and limit
        let filtered: Vec<StreamEvent> = events.into_iter()
            .filter(|e| e.area_seq.unwrap_or(0) >= from_seq)
            .filter(|e| e.area_seq.unwrap_or(0) < watermark) // Only return up to watermark
            .take(limit)
            .collect();
        
        AreaReadResponse {
            events: filtered,
            watermark,
        }
    }

    pub async fn stream_peek(&self, route: &str, from_seq: u64, limit: usize) -> Vec<StreamEvent> {
        let g = self.streams.lock().await;
        let v = g.get(route).cloned().unwrap_or_default();
        let mut out = Vec::new();
        for e in v.into_iter() {
            if e.resource_seq >= from_seq {
                out.push(e);
            }
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    /// Consume hierarchically by prefix (naive merge). Returns (route, seq, body)
    pub async fn stream_consume_prefix(
        &self,
        prefix: &str,
        from_seq: u64,
        limit: usize,
    ) -> Vec<(String, u64, Vec<u8>)> {
        // snapshot keys and vectors under lock to avoid holding it during heap operations
        let snapshot: Vec<(String, Vec<StreamEvent>)> = {
            let g = self.streams.lock().await;
            g.iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        // Build iterators filtered by from_seq
        struct Cursor {
            route: String,
            idx: usize,
            events: Vec<StreamEvent>,
        }
        let mut cursors: Vec<Cursor> = snapshot
            .into_iter()
            .map(|(route, events)| {
                let idx = events
                    .iter()
                    .position(|e| e.resource_seq >= from_seq)
                    .unwrap_or(events.len());
                Cursor { route, idx, events }
            })
            .collect();

        // Min-heap by (ts, route, seq)
        // Min-heap of (ts, route, seq, body, cursor_idx)
        type HeapItem = std::cmp::Reverse<(u64, String, u64, Vec<u8>, usize)>;
        let mut heap: std::collections::BinaryHeap<HeapItem> = std::collections::BinaryHeap::new();

        // Prime the heap
        for (i, c) in cursors.iter_mut().enumerate() {
            if c.idx < c.events.len() {
                let e = &c.events[c.idx];
                heap.push(std::cmp::Reverse((
                    e.created_at,
                    c.route.clone(),
                    e.resource_seq,
                    e.body.clone(),
                    i,
                )));
                c.idx += 1;
            }
        }

        let mut out: Vec<(String, u64, Vec<u8>)> = Vec::new();
        while let Some(std::cmp::Reverse((_ts, route, seq, body, cursor_idx))) = heap.pop() {
            out.push((route.clone(), seq, body.clone()));
            if out.len() >= limit {
                break;
            }
            let c = &mut cursors[cursor_idx];
            if c.idx < c.events.len() {
                let e = &c.events[c.idx];
                heap.push(std::cmp::Reverse((
                    e.created_at,
                    c.route.clone(),
                    e.resource_seq,
                    e.body.clone(),
                    cursor_idx,
                )));
                c.idx += 1;
            }
        }
        out
    }
}

// KV APIs (stubbed for now - storage backend to be implemented later)
impl MemStore {
    /// Put a key-value pair in a route namespace (stubbed).
    pub async fn kv_put(&self, _route: &str, _key: &str, _value: Vec<u8>) -> Result<(), String> {
        // TODO: implement actual KV storage
        Ok(())
    }

    /// Get a value by key (stubbed).
    pub async fn kv_get(&self, _route: &str, _key: &str) -> Result<Option<Vec<u8>>, String> {
        // TODO: implement actual KV storage
        Ok(None)
    }

    /// Delete a key (stubbed).
    pub async fn kv_delete(&self, _route: &str, _key: &str) -> Result<(), String> {
        // TODO: implement actual KV storage
        Ok(())
    }

    /// Scan keys >= start_key up to limit (stubbed).
    pub async fn kv_scan_ge(
        &self,
        _route: &str,
        _start_key: &str,
        _limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, String> {
        // TODO: implement actual KV storage
        Ok(vec![])
    }

    /// Put multiple key-value pairs in a batch (stubbed).
    pub async fn kv_put_batch(
        &self,
        _route: &str,
        _items: Vec<(String, Vec<u8>)>,
    ) -> Result<(), String> {
        // TODO: implement actual KV storage
        Ok(())
    }

    /// Get multiple values by keys in a batch (stubbed).
    pub async fn kv_get_batch(
        &self,
        _route: &str,
        keys: Vec<String>,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>, String> {
        // TODO: implement actual KV storage
        Ok(keys.into_iter().map(|k| (k, None)).collect())
    }

    /// Delete all keys in range [start_key, end_key) (stubbed).
    /// Returns the number of keys deleted.
    pub async fn kv_delete_range(
        &self,
        _route: &str,
        _start_key: &str,
        _end_key: &str,
    ) -> Result<u64, String> {
        // TODO: implement actual KV storage
        Ok(0)
    }
}
