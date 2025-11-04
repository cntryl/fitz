use crate::core::queue::types::{QueueConfig, QueueMessage, QueueScope, QueueStats};
use crate::storage::traits::KvStore;
use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// QueueService owns all queue business logic.
/// Generic over KvStore for durable persistence.
/// Tracks leases in-memory, persists messages to KvStore.
pub struct QueueService<K: KvStore> {
    kv: Arc<K>,
    token_key: Vec<u8>,
    
    // In-memory lease tracking: route -> id -> (expiry_secs, owner_token, delivery_count)
    leases: Arc<Mutex<HashMap<String, HashMap<String, (u64, String, u32)>>>>,
    
    // Hierarchical configuration maps
    cfg_realm: Arc<Mutex<HashMap<String, QueueConfig>>>,
    cfg_area: Arc<Mutex<HashMap<(String, String), QueueConfig>>>,
    cfg_resource: Arc<Mutex<HashMap<(String, String, String), QueueConfig>>>,
}

impl<K: KvStore> QueueService<K> {
    pub fn new(kv: Arc<K>) -> Self {
        // Generate random HMAC key for delivery tokens
        let uuid = Uuid::new_v4();
        let key = uuid.as_bytes().to_vec();
        
        Self {
            kv,
            token_key: key,
            leases: Arc::new(Mutex::new(HashMap::new())),
            cfg_realm: Arc::new(Mutex::new(HashMap::new())),
            cfg_area: Arc::new(Mutex::new(HashMap::new())),
            cfg_resource: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Append a message to a queue route
    pub async fn append(
        &self,
        route: String,
        id: String,
        body: Vec<u8>,
        ttl_secs: Option<u64>,
    ) -> Result<(), String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        let msg = QueueMessage {
            id: id.clone(),
            route: route.clone(),
            body,
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
            created_at: now,
            ttl_secs,
        };
        
        // Serialize to JSON
        let json = serde_json::to_vec(&msg).map_err(|e| format!("serialize error: {}", e))?;
        
        // Key: queue:{route}:{id}
        let key = format!("queue:{}:{}", route, id);
        
        // Store in KvStore
        self.kv
            .put(key.as_bytes(), &json)
            .map_err(|e| format!("kv error: {:?}", e))?;
        
        Ok(())
    }

    /// Reserve a batch of messages (up to batch_size) from a route
    /// Returns Vec<(id, body, delivery_token)>
    /// This is more efficient than calling reserve_next() N times for competing consumers
    pub async fn reserve_batch(
        &self,
        route: &str,
        batch_size: usize,
        lease_secs: u32,
    ) -> Result<Vec<(String, Vec<u8>, String)>, String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        if batch_size == 0 {
            return Ok(Vec::new());
        }
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "time error".to_string())?
            .as_secs();
        
        // Get config for DLQ threshold and TTL
        let cfg = self.resolve_queue_cfg_for_route(route).await;
        let dlq_threshold = cfg.dlq_threshold;
        
        // Scan for messages in this route: queue:{route}: to queue:{route};
        let prefix = format!("queue:{}:", route);
        let end_prefix = format!("queue:{};", route);
        let results = self.kv
            .scan(prefix.as_bytes(), end_prefix.as_bytes())
            .map_err(|e| format!("scan error: {:?}", e))?;
        
        // Acquire lease lock ONCE for entire batch operation
        let mut leases = self.leases.lock().await;
        let route_leases = leases.entry(route.to_string()).or_insert_with(HashMap::new);
        
        let mut reserved = Vec::with_capacity(batch_size);
        
        // Process messages until we have batch_size or run out
        for (key_bytes, val_bytes) in results {
            if reserved.len() >= batch_size {
                break; // Got enough
            }
            
            let msg: QueueMessage = serde_json::from_slice(&val_bytes)
                .map_err(|e| format!("deserialize error: {}", e))?;
            
            // Check TTL expiration (per-message or queue-level)
            let expired = if let Some(rec_ttl) = msg.ttl_secs {
                now.saturating_sub(msg.created_at) >= rec_ttl
            } else if cfg.ttl_secs > 0 {
                now.saturating_sub(msg.created_at) >= cfg.ttl_secs
            } else {
                false
            };
            
            if expired {
                // Delete expired message (defer to avoid holding lock)
                self.kv.delete(&key_bytes).ok();
                continue;
            }
            
            // Check if leased (this prevents conflicts between consumers)
            if let Some((expiry, _, _)) = route_leases.get(&msg.id) {
                if *expiry > now {
                    continue; // Still leased by another consumer
                }
            }
            
            // Check DLQ threshold
            let current_delivery_count = route_leases
                .get(&msg.id)
                .map(|(_, _, count)| *count)
                .unwrap_or(0);
            
            if current_delivery_count >= dlq_threshold {
                // Move to DLQ (release lock temporarily for async call)
                drop(leases);
                self.move_to_dlq_internal(route, &msg.id, &msg).await?;
                leases = self.leases.lock().await;
                let route_leases = leases.entry(route.to_string()).or_insert_with(HashMap::new);
                route_leases.remove(&msg.id);
                continue;
            }
            
            // Reserve this message
            let expiry = now.saturating_add(lease_secs as u64);
            
            // Generate HMAC-SHA256 delivery token
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(&self.token_key)
                .map_err(|_| "hmac error".to_string())?;
            mac.update(format!("{}:{}:{}", route, msg.id, now).as_bytes());
            let result_mac = mac.finalize();
            let token = general_purpose::STANDARD.encode(result_mac.into_bytes());
            
            // Update in-memory lease (prevents other consumers from taking it)
            let new_count = current_delivery_count.saturating_add(1);
            route_leases.insert(msg.id.clone(), (expiry, token.clone(), new_count));
            
            // Add to batch result
            reserved.push((msg.id, msg.body, token));
        }
        
        Ok(reserved)
    }

    /// Reserve the next available message on a route
    /// Returns (id, body, delivery_token)
    /// For single-message use cases. For batches, use reserve_batch() instead.
    pub async fn reserve_next(
        &self,
        route: &str,
        lease_secs: u32,
    ) -> Result<(String, Vec<u8>, String), String> {
        let batch = self.reserve_batch(route, 1, lease_secs).await?;
        batch.into_iter().next()
            .ok_or_else(|| "no available messages".to_string())
    }

    /// Extend lease for a message
    pub async fn extend_lease(
        &self,
        route: &str,
        id: &str,
        delivery_token: &str,
        add_secs: u32,
    ) -> Result<u32, String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "time error".to_string())?
            .as_secs();
        
        let mut leases = self.leases.lock().await;
        let route_leases = leases.get_mut(route)
            .ok_or_else(|| "route not found".to_string())?;
        
        if let Some((expiry, token, _count)) = route_leases.get_mut(id) {
            // Verify token
            if token != delivery_token {
                return Err("invalid delivery token".to_string());
            }
            
            // Extend expiry
            let new_expiry = expiry.saturating_add(add_secs as u64);
            *expiry = new_expiry;
            
            let remaining = if new_expiry > now {
                (new_expiry - now) as u32
            } else {
                0
            };
            
            Ok(remaining)
        } else {
            Err("no active lease".to_string())
        }
    }

    /// Consume (ack) a message - removes it from storage
    pub async fn consume(
        &self,
        route: &str,
        id: &str,
        delivery_token: &str,
    ) -> Result<(), String> {
        // Verify lease
        let mut leases = self.leases.lock().await;
        let route_leases = leases.get_mut(route)
            .ok_or_else(|| "route not found".to_string())?;
        
        if let Some((_, token, _)) = route_leases.get(id) {
            if token != delivery_token {
                return Err("invalid delivery token".to_string());
            }
        } else {
            return Err("no active lease".to_string());
        }
        
        // Remove from storage
        let key = format!("queue:{}:{}", route, id);
        self.kv.delete(key.as_bytes())
            .map_err(|e| format!("delete error: {:?}", e))?;
        
        // Remove lease
        route_leases.remove(id);
        
        Ok(())
    }

    /// Peek next available message without reserving
    pub async fn peek_next(&self, route: &str) -> Option<(String, Vec<u8>)> {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_secs();
        
        let prefix = format!("queue:{}:", route);
        let end_prefix = format!("queue:{};", route);
        let results = self.kv.scan(prefix.as_bytes(), end_prefix.as_bytes()).ok()?;
        
        let leases = self.leases.lock().await;
        let route_leases = leases.get(route);
        
        for (_, val_bytes) in results {
            let msg: QueueMessage = serde_json::from_slice(&val_bytes).ok()?;
            
            // Check if leased
            if let Some(route_leases) = route_leases {
                if let Some((expiry, _, _)) = route_leases.get(&msg.id) {
                    if *expiry > now {
                        continue; // Still leased
                    }
                }
            }
            
            return Some((msg.id, msg.body));
        }
        
        None
    }

    /// Move message to DLQ
    pub async fn move_to_dlq(
        &self,
        route: &str,
        id: &str,
        delivery_token: &str,
    ) -> Result<(), String> {
        // Verify lease
        let mut leases = self.leases.lock().await;
        let route_leases = leases.get_mut(route)
            .ok_or_else(|| "route not found".to_string())?;
        
        if let Some((_, token, _)) = route_leases.get(id) {
            if token != delivery_token {
                return Err("invalid delivery token".to_string());
            }
        } else {
            return Err("no active lease".to_string());
        }
        
        // Get message
        let key = format!("queue:{}:{}", route, id);
        let val_bytes = self.kv.get(key.as_bytes())
            .map_err(|e| format!("get error: {:?}", e))?
            .ok_or_else(|| "message not found".to_string())?;
        
        let msg: QueueMessage = serde_json::from_slice(&val_bytes)
            .map_err(|e| format!("deserialize error: {}", e))?;
        
        // Move to DLQ
        self.move_to_dlq_internal(route, id, &msg).await?;
        
        // Remove original
        self.kv.delete(key.as_bytes()).ok();
        route_leases.remove(id);
        
        Ok(())
    }

    /// Internal helper to move message to DLQ
    async fn move_to_dlq_internal(
        &self,
        route: &str,
        _id: &str,
        msg: &QueueMessage,
    ) -> Result<(), String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        let dlq_route = format!("{}.dlq", route);
        let dlq_cfg = self.resolve_queue_cfg_for_route(&dlq_route).await;
        
        // Compute remaining TTL
        let remaining_opt = msg.ttl_secs.map(|orig| {
            if now.saturating_sub(msg.created_at) >= orig {
                0u64
            } else {
                orig.saturating_sub(now.saturating_sub(msg.created_at))
            }
        });
        
        // Choose TTL: min(remaining, dlq_ttl) or dlq_ttl if no remaining
        let effective_ttl = if let Some(rem) = remaining_opt {
            if rem == 0 {
                None // Expired, but DLQ might have its own TTL
            } else if dlq_cfg.ttl_secs > 0 && rem > dlq_cfg.ttl_secs {
                Some(dlq_cfg.ttl_secs)
            } else {
                Some(rem)
            }
        } else if dlq_cfg.ttl_secs > 0 {
            Some(dlq_cfg.ttl_secs)
        } else {
            None
        };
        
        // Create DLQ message (clear lease info)
        let dlq_msg = QueueMessage {
            id: msg.id.clone(),
            route: dlq_route.clone(),
            body: msg.body.clone(),
            lease_expiry: None,
            lease_owner: None,
            delivery_count: 0,
            created_at: now,
            ttl_secs: effective_ttl,
        };
        
        // Store in DLQ
        let dlq_key = format!("queue:{}:{}", dlq_route, msg.id);
        let dlq_json = serde_json::to_vec(&dlq_msg)
            .map_err(|e| format!("serialize error: {}", e))?;
        
        self.kv.put(dlq_key.as_bytes(), &dlq_json)
            .map_err(|e| format!("kv put error: {:?}", e))?;
        
        Ok(())
    }

    /// Get message metadata
    pub async fn get_message_metadata(
        &self,
        route: &str,
        id: &str,
    ) -> Result<Option<QueueMessage>, String> {
        let key = format!("queue:{}:{}", route, id);
        let val_opt = self.kv.get(key.as_bytes())
            .map_err(|e| format!("get error: {:?}", e))?;
        
        if let Some(val_bytes) = val_opt {
            let msg: QueueMessage = serde_json::from_slice(&val_bytes)
                .map_err(|e| format!("deserialize error: {}", e))?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Get queue statistics
    pub async fn get_queue_stats(&self, route: &str) -> Result<QueueStats, String> {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        let leases = self.leases.lock().await;
        let route_leases = leases.get(route);
        
        let mut in_flight_count = 0u32;
        
        if let Some(route_leases) = route_leases {
            for (expiry, _, _) in route_leases.values() {
                if *expiry > now {
                    in_flight_count = in_flight_count.saturating_add(1);
                }
            }
        }
        
        Ok(QueueStats { in_flight_count })
    }

    /// Set queue configuration at specific scope
    pub async fn set_config(&self, scope: QueueScope, cfg: QueueConfig) {
        match scope {
            QueueScope::Realm { realm } => {
                let mut g = self.cfg_realm.lock().await;
                g.insert(realm, cfg);
            }
            QueueScope::Area { realm, area } => {
                let mut g = self.cfg_area.lock().await;
                g.insert((realm, area), cfg);
            }
            QueueScope::Resource { realm, area, resource } => {
                let mut g = self.cfg_resource.lock().await;
                g.insert((realm, area, resource), cfg);
            }
        }
    }

    /// Resolve hierarchical configuration for a route
    async fn resolve_queue_cfg_for_route(&self, route: &str) -> QueueConfig {
        // Expected: queue://realm/area/resource
        if let Some(rest) = route.strip_prefix("queue://") {
            let parts: Vec<&str> = rest.split('/').collect();
            if parts.is_empty() || parts[0].is_empty() {
                return QueueConfig::default();
            }
            
            let realm = parts[0].to_string();
            let area_opt = parts.get(1).map(|s| s.to_string());
            let res_opt = parts.get(2).map(|s| s.to_string());
            
            // Resolve: realm -> area -> resource (later overrides earlier)
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
}
