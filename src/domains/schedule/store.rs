use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

use cntryl_midge::WriteOptions;

const DEFINITION_VALUE_VERSION: u8 = 1;
const DEFINITION_PREFIX: &[u8] = b"sched:def:";
const DUE_PREFIX: &[u8] = b"sched:due:";
const DUE_INDEX_VALUE: &[u8] = &[1];
const LEGACY_PREFIX: &[u8] = b"sched:m";
const LEGACY_INDEX_PREFIX: &[u8] = b"sched:idx:";

pub type ScheduleBatchInsertItem = (String, String, Bytes, u64, Option<u64>);

pub struct ScheduleInsert<'a> {
    pub route: &'a str,
    pub cron: &'a str,
    pub payload: &'a Bytes,
    pub next_fire_ms: u64,
    pub previous_fire_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedSchedule {
    pub route: String,
    pub cron: String,
    pub payload: Bytes,
    pub next_fire_ms: u64,
}

pub struct ScheduleStore {
    db: Arc<cntryl_midge::Engine>,
    #[cfg(test)]
    fail_next_commit: Arc<std::sync::atomic::AtomicBool>,
}

impl ScheduleStore {
    pub fn new(db: Arc<cntryl_midge::Engine>) -> Self {
        Self {
            db,
            #[cfg(test)]
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(crate) fn encode_definition_key(route: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(DEFINITION_PREFIX.len() + route.len());
        key.extend_from_slice(DEFINITION_PREFIX);
        key.extend_from_slice(route.as_bytes());
        key
    }

    pub(crate) fn encode_due_key(next_fire_ms: u64, route: &str) -> Vec<u8> {
        let minute_epoch = next_fire_ms / 60_000;
        let ms_offset = next_fire_ms % 60_000;

        let mut key = Vec::with_capacity(DUE_PREFIX.len() + 18 + route.len());
        key.extend_from_slice(DUE_PREFIX);
        key.extend_from_slice(minute_epoch.to_be_bytes().as_slice());
        key.push(b'/');
        key.extend_from_slice(ms_offset.to_be_bytes().as_slice());
        key.push(b':');
        key.extend_from_slice(route.as_bytes());
        key
    }

    fn decode_definition_key(key: &[u8]) -> Result<String, String> {
        if !key.starts_with(DEFINITION_PREFIX) {
            return Err("Invalid schedule definition key prefix".to_string());
        }

        String::from_utf8(key[DEFINITION_PREFIX.len()..].to_vec())
            .map_err(|e| format!("Invalid schedule route encoding: {}", e))
    }

    fn decode_due_key_with_prefix(key: &[u8], prefix: &[u8]) -> Result<(u64, String), String> {
        if !key.starts_with(prefix) {
            return Err("Invalid schedule due key prefix".to_string());
        }

        let remaining = &key[prefix.len()..];
        if remaining.len() < 18 {
            return Err("Schedule due key too short".to_string());
        }

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

        if remaining[8] != b'/' {
            return Err("Missing minute/offset separator".to_string());
        }

        let ms_offset = if remaining.len() > 17 && remaining[17] == b':' {
            let offset_bytes = &remaining[9..17];
            u64::from_be_bytes(offset_bytes.try_into().unwrap())
        } else if remaining.len() > 15 && remaining[15] == b':' {
            let offset_bytes = &remaining[9..15];
            u64::from_be_bytes([
                0,
                0,
                offset_bytes[0],
                offset_bytes[1],
                offset_bytes[2],
                offset_bytes[3],
                offset_bytes[4],
                offset_bytes[5],
            ])
        } else {
            return Err("Missing offset/route separator".to_string());
        };

        let route_start = if remaining.len() > 17 && remaining[17] == b':' {
            18
        } else {
            16
        };

        let route = String::from_utf8(remaining[route_start..].to_vec())
            .map_err(|e| format!("Invalid route encoding: {}", e))?;

        Ok(((minute_epoch * 60_000) + ms_offset, route))
    }

    #[allow(dead_code)]
    pub(crate) fn decode_due_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, DUE_PREFIX)
    }

    fn decode_legacy_key(key: &[u8]) -> Result<(u64, String), String> {
        Self::decode_due_key_with_prefix(key, LEGACY_PREFIX)
    }

    fn encode_definition_value(next_fire_ms: u64, cron: &str, payload: &Bytes) -> Vec<u8> {
        let mut value = Vec::with_capacity(1 + 8 + 4 + cron.len() + 4 + payload.len());
        value.push(DEFINITION_VALUE_VERSION);
        value.extend_from_slice(&next_fire_ms.to_be_bytes());
        value.extend_from_slice(&(cron.len() as u32).to_be_bytes());
        value.extend_from_slice(cron.as_bytes());
        value.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        value.extend_from_slice(payload);
        value
    }

    fn decode_definition_value(value: &[u8]) -> Result<(u64, String, Bytes), String> {
        if value.len() < 17 {
            return Err("Schedule definition value too short".to_string());
        }
        if value[0] != DEFINITION_VALUE_VERSION {
            return Err(format!(
                "Unsupported schedule definition value version: {}",
                value[0]
            ));
        }

        let next_fire_ms = u64::from_be_bytes(value[1..9].try_into().unwrap());
        let cron_len = u32::from_be_bytes(value[9..13].try_into().unwrap()) as usize;
        let cron_start = 13;
        let cron_end = cron_start + cron_len;
        if value.len() < cron_end + 4 {
            return Err("Schedule definition value truncated before cron".to_string());
        }

        let cron = String::from_utf8(value[cron_start..cron_end].to_vec())
            .map_err(|e| format!("Invalid cron encoding: {}", e))?;
        let payload_len =
            u32::from_be_bytes(value[cron_end..cron_end + 4].try_into().unwrap()) as usize;
        let payload_start = cron_end + 4;
        let payload_end = payload_start + payload_len;
        if value.len() != payload_end {
            return Err("Schedule definition value has invalid payload length".to_string());
        }

        Ok((
            next_fire_ms,
            cron,
            Bytes::copy_from_slice(&value[payload_start..payload_end]),
        ))
    }

    fn decode_legacy_value(value: &[u8]) -> Result<(String, Bytes), String> {
        let sep_pos = value
            .iter()
            .position(|&b| b == b'|')
            .ok_or_else(|| "Invalid legacy schedule value format".to_string())?;

        let cron = String::from_utf8(value[..sep_pos].to_vec())
            .map_err(|e| format!("Invalid cron encoding: {}", e))?;
        let payload = Bytes::copy_from_slice(&value[sep_pos + 1..]);
        Ok((cron, payload))
    }

    pub fn insert(
        &self,
        cf_id: u64,
        schedule: ScheduleInsert<'_>,
        write_options: WriteOptions,
    ) -> Result<Vec<u8>, String> {
        let definition_key = Self::encode_definition_key(schedule.route);
        let due_key = Self::encode_due_key(schedule.next_fire_ms, schedule.route);
        let definition_value =
            Self::encode_definition_value(schedule.next_fire_ms, schedule.cron, schedule.payload);

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        txn.put(definition_key, definition_value, None)
            .map_err(|e| format!("put schedule definition failed: {:?}", e))?;

        if let Some(previous_fire_ms) = schedule.previous_fire_ms {
            if previous_fire_ms != schedule.next_fire_ms {
                txn.delete(Self::encode_due_key(previous_fire_ms, schedule.route))
                    .map_err(|e| format!("delete previous due key failed: {:?}", e))?;
            }
        }

        txn.put(due_key.clone(), DUE_INDEX_VALUE.to_vec(), None)
            .map_err(|e| format!("put due index failed: {:?}", e))?;

        self.commit_or_inject(txn, write_options)?;
        Ok(due_key)
    }

    pub fn insert_batch(
        &self,
        cf_id: u64,
        items: &[ScheduleBatchInsertItem],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        for (route, cron, payload, next_fire_ms, previous_fire_ms) in items {
            let definition_key = Self::encode_definition_key(route);
            let definition_value = Self::encode_definition_value(*next_fire_ms, cron, payload);
            let due_key = Self::encode_due_key(*next_fire_ms, route);

            txn.put(definition_key, definition_value, None)
                .map_err(|e| format!("put schedule definition failed: {:?}", e))?;

            if let Some(previous_fire_ms) = previous_fire_ms {
                if previous_fire_ms != next_fire_ms {
                    txn.delete(Self::encode_due_key(*previous_fire_ms, route))
                        .map_err(|e| format!("delete previous due key failed: {:?}", e))?;
                }
            }

            txn.put(due_key, DUE_INDEX_VALUE.to_vec(), None)
                .map_err(|e| format!("put due index failed: {:?}", e))?;
        }

        self.commit_or_inject(txn, write_options)
    }

    pub fn delete_current(
        &self,
        cf_id: u64,
        route: &str,
        next_fire_ms: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        txn.delete(Self::encode_definition_key(route))
            .map_err(|e| format!("delete schedule definition failed: {:?}", e))?;
        txn.delete(Self::encode_due_key(next_fire_ms, route))
            .map_err(|e| format!("delete schedule due index failed: {:?}", e))?;

        self.commit_or_inject(txn, write_options)
    }

    /// Load authoritative durable schedule definitions, migrate any legacy TTL-backed
    /// rows that are still present, rebuild the full due index, and delete stale
    /// legacy rows and indexes for the current family.
    pub fn load_all(
        &self,
        cf_id: u64,
        write_options: WriteOptions,
    ) -> Result<Vec<PersistedSchedule>, String> {
        let read_tx = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;

        let mut schedules = BTreeMap::<String, PersistedSchedule>::new();
        let mut imported_definitions = Vec::<PersistedSchedule>::new();

        let definition_rows = read_tx
            .scan(&cntryl_midge::Query::new().prefix(Bytes::from_static(DEFINITION_PREFIX)))
            .map_err(|e| format!("scan definitions failed: {:?}", e))?
            .collect_all();
        for (key, value) in definition_rows {
            match (
                Self::decode_definition_key(&key),
                Self::decode_definition_value(&value),
            ) {
                (Ok(route), Ok((next_fire_ms, cron, payload))) => {
                    schedules.insert(
                        route.clone(),
                        PersistedSchedule {
                            route,
                            cron,
                            payload,
                            next_fire_ms,
                        },
                    );
                }
                (Err(error), _) | (_, Err(error)) => {
                    tracing::warn!("Failed to decode persisted schedule definition: {}", error);
                }
            }
        }

        let legacy_rows = read_tx
            .scan(&cntryl_midge::Query::new().prefix(Bytes::from_static(LEGACY_PREFIX)))
            .map_err(|e| format!("scan legacy schedule rows failed: {:?}", e))?
            .collect_all();

        for (key, value) in &legacy_rows {
            match (
                Self::decode_legacy_key(key),
                Self::decode_legacy_value(value),
            ) {
                (Ok((next_fire_ms, route)), Ok((cron, payload))) => {
                    if schedules.contains_key(&route) {
                        continue;
                    }

                    let persisted = PersistedSchedule {
                        route: route.clone(),
                        cron,
                        payload,
                        next_fire_ms,
                    };
                    imported_definitions.push(persisted.clone());
                    schedules.insert(route.clone(), persisted);
                }
                (Err(error), _) | (_, Err(error)) => {
                    tracing::warn!("Failed to decode legacy schedule row: {}", error);
                }
            }
        }

        let due_rows = read_tx
            .scan(&cntryl_midge::Query::new().prefix(Bytes::from_static(DUE_PREFIX)))
            .map_err(|e| format!("scan due index failed: {:?}", e))?
            .collect_all();
        let legacy_index_rows = read_tx
            .scan(&cntryl_midge::Query::new().prefix(Bytes::from_static(LEGACY_INDEX_PREFIX)))
            .map_err(|e| format!("scan legacy schedule index failed: {:?}", e))?
            .collect_all();
        drop(read_tx);

        let mut write_tx = self
            .db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin migration tx failed: {:?}", e))?;

        for schedule in &imported_definitions {
            write_tx
                .put(
                    Self::encode_definition_key(&schedule.route),
                    Self::encode_definition_value(
                        schedule.next_fire_ms,
                        &schedule.cron,
                        &schedule.payload,
                    ),
                    None,
                )
                .map_err(|e| format!("import legacy schedule definition failed: {:?}", e))?;
        }

        for (key, _) in due_rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("delete stale due index failed: {:?}", e))?;
        }

        for schedule in schedules.values() {
            write_tx
                .put(
                    Self::encode_due_key(schedule.next_fire_ms, &schedule.route),
                    DUE_INDEX_VALUE.to_vec(),
                    None,
                )
                .map_err(|e| format!("rebuild due index failed: {:?}", e))?;
        }

        for (key, _) in legacy_index_rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("delete legacy schedule index failed: {:?}", e))?;
        }

        for (key, _) in legacy_rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("delete legacy schedule row failed: {:?}", e))?;
        }

        self.commit_or_inject(write_tx, write_options)?;
        Ok(schedules.into_values().collect())
    }

    #[cfg(test)]
    pub fn fail_next_commit_for_tests(&self) {
        self.fail_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected schedule store commit failure".to_string());
        }

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))
    }

    #[cfg(not(test))]
    fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {:?}", e))
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

    fn put_raw(
        db: &Arc<cntryl_midge::Engine>,
        cf_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), String> {
        let mut txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {:?}", e))?;
        txn.put(key, value, None)
            .map_err(|e| format!("put failed: {:?}", e))?;
        txn.commit(WriteOptions::buffered())
            .map_err(|e| format!("commit failed: {:?}", e))
    }

    fn read_raw_value(db: &Arc<cntryl_midge::Engine>, cf_id: u64, key: &[u8]) -> Option<Vec<u8>> {
        let txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");

        txn.get(key)
            .expect("read raw key")
            .map(|value| value.to_vec())
    }

    fn count_prefix(db: &Arc<cntryl_midge::Engine>, cf_id: u64, prefix: &'static [u8]) -> usize {
        let txn = db
            .begin_tx(cf_id as u32, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        txn.scan(&cntryl_midge::Query::new().prefix(Bytes::from_static(prefix)))
            .expect("scan prefix")
            .collect_all()
            .len()
    }

    #[test]
    fn should_write_durable_definition_and_due_index_when_inserting_schedule() {
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/backup/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_ms = 1_700_000_001_000_u64;
        let expected_due_key = ScheduleStore::encode_due_key(next_fire_ms, route);

        let stored_due_key = store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "* * * * *",
                    payload: &payload,
                    next_fire_ms,
                    previous_fire_ms: None,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");

        let definition_key = ScheduleStore::encode_definition_key(route);
        let definition_value = read_raw_value(&db, 1, &definition_key).expect("definition row");

        assert_eq!(stored_due_key, expected_due_key);
        assert_eq!(
            ScheduleStore::decode_definition_value(&definition_value).unwrap(),
            (
                next_fire_ms,
                "* * * * *".to_string(),
                Bytes::from_static(b"payload")
            )
        );
        assert_eq!(
            read_raw_value(&db, 1, &expected_due_key),
            Some(DUE_INDEX_VALUE.to_vec())
        );
    }

    #[test]
    fn should_import_legacy_rows_and_rebuild_due_index_from_definitions() {
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/legacy/run";
        let legacy_due_key = {
            let minute_epoch = 1_700_000_002_000_u64 / 60_000;
            let ms_offset = 1_700_000_002_000_u64 % 60_000;
            let mut key = Vec::new();
            key.extend_from_slice(LEGACY_PREFIX);
            key.extend_from_slice(minute_epoch.to_be_bytes().as_slice());
            key.push(b'/');
            key.extend_from_slice(ms_offset.to_be_bytes().as_slice());
            key.push(b':');
            key.extend_from_slice(route.as_bytes());
            key
        };
        put_raw(&db, 1, legacy_due_key, b"*/5 * * * *|legacy".to_vec()).expect("write legacy row");
        put_raw(
            &db,
            1,
            ScheduleStore::encode_due_key(9_999_999_999_999, "schedule://stale/index/only/run"),
            DUE_INDEX_VALUE.to_vec(),
        )
        .expect("write stale due row");

        let loaded = store
            .load_all(1, WriteOptions::buffered())
            .expect("load schedules");

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].route, route);
        assert_eq!(loaded[0].cron, "*/5 * * * *");
        assert_eq!(loaded[0].payload, Bytes::from_static(b"legacy"));
        assert_eq!(count_prefix(&db, 1, LEGACY_PREFIX), 0);
        assert_eq!(count_prefix(&db, 1, LEGACY_INDEX_PREFIX), 0);
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_due_key(loaded[0].next_fire_ms, route),
            )
            .is_some(),
            "rebuilt due index should exist for the imported schedule"
        );
        assert!(
            read_raw_value(
                &db,
                1,
                &ScheduleStore::encode_due_key(
                    9_999_999_999_999,
                    "schedule://stale/index/only/run",
                ),
            )
            .is_none(),
            "stale due index rows should be removed during rebuild"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_some(),
            "definition row should exist after legacy import"
        );
    }

    #[test]
    fn should_delete_definition_and_due_index_when_canceling_schedule() {
        let (store, db) = make_store();
        let route = "schedule://acme/jobs/delete/run";
        let payload = Bytes::from_static(b"payload");
        let next_fire_ms = 1_700_000_010_000_u64;
        store
            .insert(
                1,
                ScheduleInsert {
                    route,
                    cron: "0 * * * *",
                    payload: &payload,
                    next_fire_ms,
                    previous_fire_ms: None,
                },
                WriteOptions::buffered(),
            )
            .expect("insert schedule");

        store
            .delete_current(1, route, next_fire_ms, WriteOptions::buffered())
            .expect("delete schedule");

        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_definition_key(route)).is_none(),
            "definition row should be deleted"
        );
        assert!(
            read_raw_value(&db, 1, &ScheduleStore::encode_due_key(next_fire_ms, route)).is_none(),
            "due index should be deleted"
        );
    }
}
