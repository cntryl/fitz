use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::sync::Arc;

use cntryl_midge::WriteOptions;

const BUCKET_SIZE_SECS: i64 = 10; // Time buckets for index

pub struct ScheduleStore {
    db: Arc<cntryl_midge::Engine>,
}

impl ScheduleStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self { db }
    }

    /// schedule_def key: "family:{family}:def:{id:016x}"
    fn encode_def_key(family: u64, id: u64) -> Vec<u8> {
        format!("family:{}:def:{:016x}", family, id).into_bytes()
    }

    /// schedule_idx key: "family:{family}:idx:{bucket:016x}/{id:016x}"
    /// Enables range scans by bucket (time window)
    fn encode_idx_key(family: u64, bucket: u64, id: u64) -> Vec<u8> {
        format!("family:{}:idx:{:016x}/{:016x}", family, bucket, id).into_bytes()
    }

    /// Compute time bucket from DateTime
    fn time_to_bucket(dt: DateTime<Utc>) -> u64 {
        let ts = dt.timestamp();
        (ts.max(0) as u64) / (BUCKET_SIZE_SECS as u64)
    }

    /// Persist schedule definition + index entry.
    /// Value format: [4 bytes BE route_len][route bytes][payload bytes]
    pub fn insert(
        &self,
        family: u64,
        id: u64,
        route: &[u8],
        payload: Bytes,
        next_fire: DateTime<Utc>,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        // Encode and store definition
        let mut val = Vec::with_capacity(4 + route.len() + payload.len());
        let route_len = (route.len() as u32).to_be_bytes();
        val.extend(&route_len);
        val.extend(route);
        val.extend(payload);

        txn.put(Self::encode_def_key(family, id), val, None)
            .map_err(|e| format!("put def failed: {:?}", e))?;

        // Index entry: just marks presence in time bucket
        let bucket = Self::time_to_bucket(next_fire);
        txn.put(Self::encode_idx_key(family, bucket, id), vec![], None)
            .map_err(|e| format!("put idx failed: {:?}", e))?;

        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;
        Ok(())
    }

    pub fn delete(&self, family: u64, id: u64, write_options: WriteOptions) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;
        txn.delete(Self::encode_def_key(family, id))
            .map_err(|e| format!("delete def failed: {:?}", e))?;

        // Also delete from all index buckets (could be in multiple if not yet fired)
        // For now, we'll rely on a lazy cleanup or schedule update
        // A more robust approach: iterate all buckets and remove

        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("commit failed: {:?}", e))?;
        Ok(())
    }

    /// Scan schedules whose next_fire_time falls within [window_start, window_end]
    /// Returns list of schedule IDs due in the window
    pub fn scan_window(
        &self,
        family: u64,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<Vec<u64>, String> {
        let txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let start_bucket = Self::time_to_bucket(window_start);
        let end_bucket = Self::time_to_bucket(window_end);

        let mut due_ids = Vec::new();

        // Scan each bucket in the window
        for bucket in start_bucket..=end_bucket {
            let prefix = format!("family:{}:idx:{:016x}/", family, bucket);
            let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix.clone().into_bytes()));

            let mut iter = txn
                .scan(&query)
                .map_err(|e| format!("scan window failed: {:?}", e))?;
            let results = iter.collect_all();

            for (k, _v) in results {
                // Parse schedule_id from key: "family:N:idx:BUCKET/ID"
                let keystr = String::from_utf8_lossy(&k);
                if let Some(slash_pos) = keystr.rfind('/') {
                    let id_hex = &keystr[slash_pos + 1..];
                    if let Ok(id) = u64::from_str_radix(id_hex, 16) {
                        due_ids.push(id);
                    }
                }
            }
        }

        Ok(due_ids)
    }

    /// Batch update index entries after schedules fire
    /// This atomically:
    /// 1. Removes old index entries (current bucket)
    /// 2. Adds new index entries (next fire bucket)
    pub fn batch_update_index(
        &self,
        family: u64,
        updates: Vec<(u64, DateTime<Utc>, DateTime<Utc>)>, // (id, old_next_fire, new_next_fire)
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|e| format!("batch begin_tx failed: {:?}", e))?;

        for (id, old_fire, new_fire) in updates {
            let old_bucket = Self::time_to_bucket(old_fire);
            let new_bucket = Self::time_to_bucket(new_fire);

            // Delete old index entry
            txn.delete(Self::encode_idx_key(family, old_bucket, id))
                .map_err(|e| format!("batch delete idx failed: {:?}", e))?;

            // Insert new index entry
            txn.put(Self::encode_idx_key(family, new_bucket, id), vec![], None)
                .map_err(|e| format!("batch put idx failed: {:?}", e))?;
        }

        self.db
            .commit(txn, write_options)
            .map_err(|e| format!("batch commit failed: {:?}", e))?;
        Ok(())
    }

    /// Load all schedule definitions from store (used on startup)
    /// Scans schedule_def CF
    pub fn list(&self, family: u64) -> Result<Vec<(u64, Bytes, Bytes)>, String> {
        let txn = self
            .db
            .begin_tx(
                cntryl_midge::ColumnFamilyId(family as u32),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let prefix = format!("family:{}:def:", family);
        let mut out = Vec::new();
        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix.into_bytes()));
        let mut iter = txn
            .scan(&query)
            .map_err(|e| format!("scan failed: {:?}", e))?;
        let results = iter.collect_all();

        for (k, v) in results {
            let keystr = String::from_utf8_lossy(&k);
            if let Some(pos) = keystr.rfind(":def:") {
                let id_hex = &keystr[pos + 5..];
                if let Ok(id) = u64::from_str_radix(id_hex, 16) {
                    if v.len() >= 4 {
                        let route_len = u32::from_be_bytes([v[0], v[1], v[2], v[3]]) as usize;
                        if v.len() >= 4 + route_len {
                            let route_bytes = Bytes::copy_from_slice(&v[4..4 + route_len]);
                            let payload = Bytes::copy_from_slice(&v[4 + route_len..]);
                            out.push((id, route_bytes, payload));
                        }
                    }
                }
            }
        }

        Ok(out)
    }
}
