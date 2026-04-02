use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cntryl_midge::WriteOptions;

/// Grace period for schedule TTL (time after fire before key expires)
/// This gives the schedule time to be processed and fanned out before cleanup
const GRACE_PERIOD_SECS: u64 = 3600; // 1 hour

pub type ScheduleBatchInsertItem = (String, String, Bytes, Instant, u64, Option<u64>);

pub struct ScheduleInsert<'a> {
    pub route: &'a str,
    pub cron: &'a str,
    pub payload: &'a Bytes,
    pub next_fire_time: Instant,
    pub next_fire_ms: u64,
    pub previous_fire_ms: Option<u64>,
    pub previous_storage_key: Option<Vec<u8>>,
    pub index_key: Option<Vec<u8>>,
}

pub struct ScheduleStore {
    db: Arc<cntryl_midge::Engine>,
}

impl ScheduleStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self { db }
    }

    /// Index key for O(1) delete by route: `sched:idx:{route}` -> main key bytes
    pub(crate) fn encode_index_key(route: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(10 + route.len());
        key.extend_from_slice(b"sched:idx:");
        key.extend_from_slice(route.as_bytes());
        key
    }

    /// Encode storage key: `sched:m{minute_epoch}/{ms_offset}:{route}`
    /// Prefix format allows rapid scanning of next-fire schedules by minute bucket.
    /// minute_epoch = ms since UNIX_EPOCH / 60_000
    /// ms_offset = ms since last minute boundary (0-59_999), stored as 8-byte BE.
    pub(crate) fn encode_key(next_fire_time_ms: u64, route: &str) -> Vec<u8> {
        let minute_epoch = next_fire_time_ms / 60_000;
        let ms_offset = next_fire_time_ms % 60_000;

        let mut key = Vec::with_capacity(30 + route.len());
        key.extend_from_slice(b"sched:m");
        key.extend_from_slice(minute_epoch.to_be_bytes().as_slice());
        key.push(b'/');
        key.extend_from_slice(ms_offset.to_be_bytes().as_slice());
        key.push(b':');
        key.extend_from_slice(route.as_bytes());

        key
    }

    /// Decode storage key to extract route and timestamp
    fn decode_key(key: &[u8]) -> Result<(u64, String), String> {
        // Format: sched:m{8-byte-minute}/{8-byte-offset}:{route}
        // Older keys may still carry a 6-byte offset, so accept both layouts.
        if !key.starts_with(b"sched:m") {
            return Err("Invalid key prefix".to_string());
        }

        let remaining = &key[7..]; // skip "sched:m"
        if remaining.len() < 16 {
            return Err("Key too short".to_string());
        }

        // Read minute_epoch (8 bytes)
        let minute_bytes = &remaining[0..8];
        let minute_epoch = u64::from_be_bytes([
            minute_bytes[0],
            minute_bytes[1],
            minute_bytes[2],
            minute_bytes[3],
            minute_bytes[4],
            minute_bytes[5],
            minute_bytes[6],
            minute_bytes[7],
        ]);

        // Expect '/' separator at position 8
        if remaining[8] != b'/' {
            return Err("Missing minute/offset separator".to_string());
        }

        let (ms_offset, route_start) = if remaining.len() > 17 && remaining[17] == b':' {
            let offset_bytes = &remaining[9..17];
            (u64::from_be_bytes(offset_bytes.try_into().unwrap()), 18)
        } else if remaining.len() > 15 && remaining[15] == b':' {
            let offset_bytes = &remaining[9..15];
            (
                u64::from_be_bytes([
                    0,
                    0,
                    offset_bytes[0],
                    offset_bytes[1],
                    offset_bytes[2],
                    offset_bytes[3],
                    offset_bytes[4],
                    offset_bytes[5],
                ]),
                16,
            )
        } else {
            return Err("Missing offset/route separator".to_string());
        };

        let timestamp_ms = (minute_epoch * 60_000) + ms_offset;
        let route = String::from_utf8(remaining[route_start..].to_vec())
            .map_err(|e| format!("Invalid route encoding: {}", e))?;

        Ok((timestamp_ms, route))
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
        let sep_pos = val
            .iter()
            .position(|&b| b == b'|')
            .ok_or_else(|| "Invalid value format: missing separator".to_string())?;

        let cron = String::from_utf8(val[..sep_pos].to_vec())
            .map_err(|e| format!("Invalid cron encoding: {}", e))?;
        let payload = Bytes::copy_from_slice(&val[sep_pos + 1..]);

        Ok((cron, payload))
    }

    /// Insert or update a schedule with TTL and keep the hot route->key index current.
    pub fn insert(
        &self,
        cf_id: u64,
        schedule: ScheduleInsert<'_>,
        write_options: WriteOptions,
    ) -> Result<Vec<u8>, String> {
        let key = Self::encode_key(schedule.next_fire_ms, schedule.route);
        let value = Self::encode_value(schedule.cron, schedule.payload);

        // TTL = time until next fire + grace period
        let now = Instant::now();
        let time_until_fire = if schedule.next_fire_time > now {
            schedule.next_fire_time - now
        } else {
            Duration::from_secs(0)
        };
        let ttl = time_until_fire + Duration::from_secs(GRACE_PERIOD_SECS);

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        if let Some(previous_fire_ms) = schedule.previous_fire_ms {
            if previous_fire_ms != schedule.next_fire_ms {
                let old_key = schedule
                    .previous_storage_key
                    .unwrap_or_else(|| Self::encode_key(previous_fire_ms, schedule.route));
                txn.delete(old_key)
                    .map_err(|e| format!("delete previous key failed: {:?}", e))?;
            }
        }

        txn.put(key.clone(), value, Some(ttl.as_millis() as u64))
            .map_err(|e| format!("put failed: {:?}", e))?;
        txn.put(Self::encode_index_key(schedule.route), key.clone(), None)
            .map_err(|e| format!("put index failed: {:?}", e))?;

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(key)
    }

    /// Insert or update multiple schedules in one transaction (batch).
    /// Uses the same key/value/TTL logic as `insert` but one begin_tx, N puts, one commit.
    pub fn insert_batch(
        &self,
        cf_id: u64,
        items: &[ScheduleBatchInsertItem],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for (route, cron, payload, next_fire_time, next_fire_ms, previous_fire_ms) in items {
            let key = Self::encode_key(*next_fire_ms, route);
            let value = Self::encode_value(cron, payload);

            let time_until_fire = if *next_fire_time > now {
                *next_fire_time - now
            } else {
                Duration::from_secs(0)
            };
            let ttl = time_until_fire + Duration::from_secs(GRACE_PERIOD_SECS);

            if let Some(previous_fire_ms) = previous_fire_ms {
                if previous_fire_ms != next_fire_ms {
                    let old_key = Self::encode_key(*previous_fire_ms, route);
                    txn.delete(old_key)
                        .map_err(|e| format!("delete previous key failed: {:?}", e))?;
                }
            }

            txn.put(key.clone(), value, Some(ttl.as_millis() as u64))
                .map_err(|e| format!("put failed: {:?}", e))?;
            txn.put(Self::encode_index_key(route), key.clone(), None)
                .map_err(|e| format!("put index failed: {:?}", e))?;
        }

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(())
    }

    /// Delete a schedule by route (O(1) via route->key index; fallback scan for legacy keys)
    pub fn delete(
        &self,
        cf_id: u64,
        route: &str,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let index_key = Self::encode_index_key(route);
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let main_key_opt = txn
            .get(&index_key)
            .map_err(|e| format!("get index failed: {:?}", e))?;

        if let Some(main_key) = main_key_opt {
            txn.delete(index_key)
                .map_err(|e| format!("delete index failed: {:?}", e))?;
            txn.delete(main_key.to_vec())
                .map_err(|e| format!("delete failed: {:?}", e))?;
        } else {
            // Legacy: no index (e.g. pre-index DB); fallback to one-time scan
            let query = cntryl_midge::Query::new().prefix(Bytes::from("sched:m"));
            let mut iter = txn
                .scan(&query)
                .map_err(|e| format!("scan failed: {:?}", e))?;
            let results = iter.collect_all();
            for (k, _v) in results {
                if let Ok((_timestamp, key_route)) = Self::decode_key(&k) {
                    if key_route == route {
                        txn.delete(k)
                            .map_err(|e| format!("delete failed: {:?}", e))?;
                        break;
                    }
                }
            }
        }

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(())
    }

    /// Delete a schedule when the current fire timestamp is already known by the caller.
    pub fn delete_current(
        &self,
        cf_id: u64,
        route: &str,
        next_fire_ms: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let main_key = Self::encode_key(next_fire_ms, route);
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        txn.delete(main_key)
            .map_err(|e| format!("delete failed: {:?}", e))?;
        txn.delete(Self::encode_index_key(route))
            .map_err(|e| format!("delete index failed: {:?}", e))?;

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(())
    }

    /// Delete a schedule when the caller already holds the current storage keys.
    pub fn delete_prepared(
        &self,
        cf_id: u64,
        index_key: Vec<u8>,
        main_key: Vec<u8>,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        if !index_key.is_empty()
            && txn
                .get(&index_key)
                .map_err(|e| format!("get index failed: {:?}", e))?
                .is_some()
        {
            txn.delete(index_key)
                .map_err(|e| format!("delete index failed: {:?}", e))?;
        }

        txn.delete(main_key)
            .map_err(|e| format!("delete failed: {:?}", e))?;

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;

        Ok(())
    }

    /// Load all schedules ready to fire (next_fire_time <= now)
    /// Returns Vec<(route, cron, payload)>
    ///
    /// Optimization: Only scans keys with sched prefix (reduces full table scans)
    /// and stops early if timestamp has passed the fire time.
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
        let query = cntryl_midge::Query::new().prefix(Bytes::from("sched:m"));
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan ready failed: {:?}", e))?;

        for (k, v) in iter.collect_all() {
            if let Ok((timestamp_ms, route)) = Self::decode_key(&k) {
                if timestamp_ms <= now_ms {
                    match Self::decode_value(&v) {
                        Ok((cron, payload)) => ready.push((route, cron, payload)),
                        Err(e) => tracing::warn!("Failed to decode schedule value: {}", e),
                    }
                }
            }
        }

        Ok(ready)
    }

    /// Load all schedules (for actor startup / LIST cache hydration)
    /// Returns Vec<(route, cron, payload, next_fire_ms)>
    pub fn load_all(&self, cf_id: u64) -> Result<Vec<(String, String, Bytes, u64)>, String> {
        let txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let mut all = Vec::new();
        let query = cntryl_midge::Query::new().prefix(Bytes::from("sched:m"));
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan all failed: {:?}", e))?;

        for (key, value) in iter.collect_all() {
            if let Ok((timestamp_ms, route)) = Self::decode_key(&key) {
                match Self::decode_value(&value) {
                    Ok((cron, payload)) => all.push((route, cron, payload, timestamp_ms)),
                    Err(e) => tracing::warn!("Failed to decode schedule value: {}", e),
                }
            }
        }

        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::create_test_engine_with_cfs;

    fn make_store() -> (ScheduleStore, Arc<cntryl_midge::Engine>) {
        let db = create_test_engine_with_cfs(vec![1]);
        (ScheduleStore::new(db.clone()), db)
    }

    fn read_raw_value(db: &Arc<cntryl_midge::Engine>, cf_id: u64, key: &[u8]) -> Option<Vec<u8>> {
        let txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");

        txn.get(key)
            .expect("read raw key")
            .map(|value| value.to_vec())
    }

    #[test]
    fn should_write_route_index_when_inserting_schedule() {
        // Arrange
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/backup/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_time = Instant::now() + Duration::from_secs(300);
        let next_fire_ms = 1_700_000_001_000_u64;
        let expected_main_key = ScheduleStore::encode_key(next_fire_ms, route);

        // Act
        let stored_key = store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_time,
                    next_fire_ms,
                    previous_fire_ms: None,
                    previous_storage_key: None,
                    index_key: None,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");

        // Assert
        let index_key = ScheduleStore::encode_index_key(route);
        let index_value = read_raw_value(&db, 1, &index_key);
        assert_eq!(stored_key, expected_main_key);
        assert_eq!(index_value.as_deref(), Some(expected_main_key.as_slice()));
    }

    #[test]
    fn should_write_route_indexes_when_inserting_schedule_batch() {
        // Arrange
        let (store, db) = make_store();
        let first_route = "schedule://acme/jobs/backup/run".to_string();
        let second_route = "schedule://acme/jobs/report/run".to_string();
        let first_next_fire_time = Instant::now() + Duration::from_secs(300);
        let second_next_fire_time = Instant::now() + Duration::from_secs(600);
        let first_next_fire_ms = 1_700_000_002_000_u64;
        let second_next_fire_ms = 1_700_000_003_000_u64;
        let items = vec![
            (
                first_route.clone(),
                "* * * * *".to_string(),
                Bytes::from_static(b"first"),
                first_next_fire_time,
                first_next_fire_ms,
                None,
            ),
            (
                second_route.clone(),
                "*/5 * * * *".to_string(),
                Bytes::from_static(b"second"),
                second_next_fire_time,
                second_next_fire_ms,
                None,
            ),
        ];

        // Act
        store
            .insert_batch(1, &items, WriteOptions::buffered())
            .expect("insert schedule batch");

        // Assert
        let actual_indexes = vec![
            read_raw_value(&db, 1, &ScheduleStore::encode_index_key(&first_route)),
            read_raw_value(&db, 1, &ScheduleStore::encode_index_key(&second_route)),
        ];
        let expected_indexes = vec![
            Some(ScheduleStore::encode_key(first_next_fire_ms, &first_route)),
            Some(ScheduleStore::encode_key(
                second_next_fire_ms,
                &second_route,
            )),
        ];
        assert_eq!(actual_indexes, expected_indexes);
    }
}
