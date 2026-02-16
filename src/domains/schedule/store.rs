use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cntryl_midge::WriteOptions;

/// Grace period for schedule TTL (time after fire before key expires)
/// This gives the schedule time to be processed and fanned out before cleanup
const GRACE_PERIOD_SECS: u64 = 3600; // 1 hour

pub struct ScheduleStore {
    db: Arc<cntryl_midge::Engine>,
}

impl ScheduleStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self { db }
    }

    /// Encode storage key: `{next_fire_time_ms:020}:{route}`
    /// Sorted lexicographically by time, enabling range scans
    fn encode_key(next_fire_time_ms: u64, route: &str) -> String {
        format!("{:020}:{}", next_fire_time_ms, route)
    }

    /// Encode value: `{cron_expr}|{payload_bytes}`
    fn encode_value(cron: &str, payload: &Bytes) -> Vec<u8> {
        let mut val = Vec::with_capacity(cron.len() + 1 + payload.len());
        val.extend(cron.as_bytes());
        val.push(b'|');
        val.extend(payload);
        val
    }

    /// Decode value: `{cron_expr}|{payload_bytes}` -> (cron, payload)
    fn decode_value(val: &[u8]) -> Result<(String, Bytes), String> {
        let sep_pos = val.iter().position(|&b| b == b'|')
            .ok_or_else(|| "Invalid value format: missing separator".to_string())?;
        
        let cron = String::from_utf8(val[..sep_pos].to_vec())
            .map_err(|e| format!("Invalid cron encoding: {}", e))?;
        let payload = Bytes::copy_from_slice(&val[sep_pos + 1..]);
        
        Ok((cron, payload))
    }

    /// Convert Instant to milliseconds since UNIX_EPOCH
    fn instant_to_ms(instant: Instant) -> Result<u64, String> {
        let now = SystemTime::now();
        let elapsed = now.duration_since(UNIX_EPOCH)
            .map_err(|e| format!("System time error: {}", e))?;
        let elapsed_secs = elapsed.as_secs();
        
        // Rough approximation: assume Instant started around now
        // In practice, Instant is monotonic but not wall-clock time
        // For schedules, we'll convert using now() as reference
        let duration_since_start = instant.elapsed().as_secs();
        let approx_ms = (elapsed_secs.saturating_sub(duration_since_start)) * 1000;
        Ok(approx_ms)
    }

    /// Insert or update a schedule with TTL
    /// Route is the key; cron and payload are stored in the value
    pub fn insert(
        &self,
        cf_id: u64,
        route: &str,
        cron: &str,
        payload: &Bytes,
        next_fire_time: Instant,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let next_fire_ms = Self::instant_to_ms(next_fire_time)?;
        let key = Self::encode_key(next_fire_ms, route);
        let value = Self::encode_value(cron, payload);
        
        // TTL = time until next fire + grace period
        let now = Instant::now();
        let time_until_fire = if next_fire_time > now {
            next_fire_time - now
        } else {
            Duration::from_secs(0)
        };
        let ttl = time_until_fire + Duration::from_secs(GRACE_PERIOD_SECS);

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        txn.put(key.into_bytes(), value, Some(ttl.as_millis() as u64))
            .map_err(|e| format!("put failed: {:?}", e))?;

        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(())
    }

    /// Delete a schedule by route
    /// Since routes are not directly in the key (they're suffixed by time),
    /// we scan for keys ending with the route and delete them
    pub fn delete(
        &self,
        cf_id: u64,
        route: &str,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        // Scan all keys and find those matching the route
        // In practice, you might want to maintain a separate index
        let query = cntryl_midge::Query::new();
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan failed: {:?}", e))?;
        
        let results = iter.collect_all();
        for (k, _v) in results {
            let key_str = String::from_utf8_lossy(&k);
            // Key format: "TIMESTAMP:ROUTE", check if route matches suffix
            if let Some(colon_pos) = key_str.find(':') {
                let key_route = &key_str[colon_pos + 1..];
                if key_route == route {
                    txn.delete(k)
                        .map_err(|e| format!("delete failed: {:?}", e))?;
                }
            }
        }

        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(())
    }

    /// Load all schedules ready to fire (next_fire_time <= now)
    /// Returns Vec<(route, cron, payload)>
    pub fn load_ready(
        &self,
        cf_id: u64,
        now_ms: u64,
    ) -> Result<Vec<(String, String, Bytes)>, String> {
        let txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let mut ready = Vec::new();
        
        // Range scan: all keys from start (00000000000000000000) up to now (now_ms formatted)
        // Keys are formatted as "TIMESTAMP:ROUTE" so we can sort/compare lexicographically
        let query = cntryl_midge::Query::new();
        
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan ready failed: {:?}", e))?;
        
        let results = iter.collect_all();
        for (k, v) in results {
            let key_str = String::from_utf8_lossy(&k);
            if let Some(colon_pos) = key_str.find(':') {
                let timestamp_part = &key_str[..colon_pos];
                // Only include if timestamp <= now_ms (lexicographically)
                if timestamp_part.len() == 20 {
                    if let Ok(ts) = timestamp_part.parse::<u64>() {
                        if ts <= now_ms {
                            let route = key_str[colon_pos + 1..].to_string();
                            match Self::decode_value(&v) {
                                Ok((cron, payload)) => {
                                    ready.push((route, cron, payload));
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to decode schedule value: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(ready)
    }

    /// Load all schedules (for LIST operation)
    /// Returns Vec<(route, cron, payload)>
    pub fn load_all(
        &self,
        cf_id: u64,
    ) -> Result<Vec<(String, String, Bytes)>, String> {
        let txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let mut all = Vec::new();
        
        let query = cntryl_midge::Query::new();
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan all failed: {:?}", e))?;
        
        let results = iter.collect_all();
        for (k, v) in results {
            let key_str = String::from_utf8_lossy(&k);
            if let Some(colon_pos) = key_str.find(':') {
                let route = key_str[colon_pos + 1..].to_string();
                match Self::decode_value(&v) {
                    Ok((cron, payload)) => {
                        all.push((route, cron, payload));
                    }
                    Err(e) => {
                        tracing::warn!("Failed to decode schedule value: {}", e);
                    }
                }
            }
        }

        Ok(all)
    }
}
