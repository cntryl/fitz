//! Queue recovery persistence with cached keys and a single read snapshot.

use super::{
    FastMap, IndexMetaSnapshot, IndexRecoveryAttempt, MessageId, QueueActor, QueueKey, QueueRecord,
    ReadyRange,
};
use bytes::Bytes;
use cntryl_midge::Query;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::domains::WritePolicy;

#[derive(Clone)]
pub struct QueueStore {
    engine: Arc<cntryl_midge::Engine>,
}

pub(crate) struct QueueTransaction(cntryl_midge::Transaction);

#[derive(Clone, Copy)]
pub(crate) enum QueueTransactionMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug)]
pub(crate) struct QueueStoreError {
    message: String,
    missing_snapshot: bool,
}

impl std::fmt::Display for QueueStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl QueueStoreError {
    pub(super) fn is_missing_snapshot(&self) -> bool {
        self.missing_snapshot
    }

    #[allow(clippy::needless_pass_by_value)]
    fn from_midge(error: cntryl_midge::MidgeError) -> Self {
        let missing_snapshot = Self::is_missing_midge(&error);
        Self {
            message: error.to_string(),
            missing_snapshot,
        }
    }

    fn is_missing_midge(error: &cntryl_midge::MidgeError) -> bool {
        matches!(
            error,
            cntryl_midge::MidgeError::InvalidArgument(message)
                if message.contains("read snapshot not available")
        )
    }
}

impl QueueStore {
    pub(crate) fn new(engine: Arc<cntryl_midge::Engine>) -> Self {
        Self { engine }
    }

    pub(crate) fn begin(
        &self,
        family: u32,
        mode: QueueTransactionMode,
    ) -> Result<QueueTransaction, QueueStoreError> {
        let mode = match mode {
            QueueTransactionMode::ReadOnly => cntryl_midge::TransactionMode::ReadOnly,
            QueueTransactionMode::ReadWrite => cntryl_midge::TransactionMode::ReadWrite,
        };
        self.engine
            .begin_tx(family, mode)
            .map(QueueTransaction)
            .map_err(QueueStoreError::from_midge)
    }

    pub(crate) fn family_ids(&self) -> Result<Vec<u32>, QueueStoreError> {
        self.engine
            .list_column_families()
            .map(|families| families.into_iter().map(|family| family.id()).collect())
            .map_err(QueueStoreError::from_midge)
    }

    pub(crate) fn flush_family(&self, family_id: u32) -> Result<bool, QueueStoreError> {
        let families = self
            .engine
            .list_column_families()
            .map_err(QueueStoreError::from_midge)?;
        let Some(family) = families.iter().find(|family| family.id() == family_id) else {
            return Ok(false);
        };
        self.engine
            .flush_cf(family)
            .map(|()| true)
            .map_err(QueueStoreError::from_midge)
    }
}

impl From<Arc<cntryl_midge::Engine>> for QueueStore {
    fn from(engine: Arc<cntryl_midge::Engine>) -> Self {
        Self::new(engine)
    }
}

impl From<crate::storage::FitzStorageEngine> for QueueStore {
    fn from(engine: crate::storage::FitzStorageEngine) -> Self {
        Self::new(engine.clone_inner())
    }
}

impl QueueTransaction {
    pub(super) fn get(&self, key: &[u8]) -> Result<Option<Bytes>, QueueStoreError> {
        self.0.get(key).map_err(QueueStoreError::from_midge)
    }

    pub(super) fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> Result<(), QueueStoreError> {
        self.0
            .put(key, value, ttl)
            .map_err(QueueStoreError::from_midge)
    }

    pub(super) fn delete(&mut self, key: Vec<u8>) -> Result<(), QueueStoreError> {
        self.0.delete(key).map_err(QueueStoreError::from_midge)
    }

    pub(super) fn commit(self, policy: WritePolicy) -> Result<(), QueueStoreError> {
        self.0
            .commit(policy.into())
            .map_err(QueueStoreError::from_midge)
    }

    pub(crate) fn scan_all(&self) -> Result<Vec<(Bytes, Bytes)>, QueueStoreError> {
        self.0
            .scan(&cntryl_midge::Query::new())
            .map_err(QueueStoreError::from_midge)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(QueueStoreError::from_midge)
    }

    fn scan_prefix(&self, prefix: Bytes) -> Result<Vec<(Bytes, Bytes)>, QueueStoreError> {
        self.0
            .scan(&Query::new().prefix(prefix))
            .map_err(QueueStoreError::from_midge)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(QueueStoreError::from_midge)
    }
}

pub(super) struct QueuePersistence {
    pub store: QueueStore,
    pub recovery: Arc<QueueRecoveryStore>,
    pub body_key_prefix: Vec<u8>,
    write_policy: WritePolicy,
}

impl QueuePersistence {
    pub(super) fn new(
        engine: impl Into<QueueStore>,
        key: &QueueKey,
        write_policy: WritePolicy,
    ) -> Self {
        let store = engine.into();
        Self {
            recovery: Arc::new(QueueRecoveryStore::new(store.clone(), key.clone())),
            body_key_prefix: QueueActor::body_key_prefix(key),
            store,
            write_policy,
        }
    }

    pub(super) fn write_options(&self) -> WritePolicy {
        self.write_policy
    }

    pub(super) fn write_policy(&self) -> WritePolicy {
        self.write_policy
    }
}

pub(super) struct QueueRecoveryStore {
    store: QueueStore,
    key: QueueKey,
    pub meta_key: Vec<u8>,
    pub index_meta_key: Vec<u8>,
    pub header_key_prefix: Bytes,
    pub ready_index_prefix: Bytes,
    pub delayed_index_prefix: Bytes,
    pub dlq_index_prefix: Bytes,
}

pub(super) struct QueueRecoverySnapshot(QueueTransaction);

pub(super) struct QueueIndexRebuild<'a> {
    pub meta: IndexMetaSnapshot,
    pub ready: &'a [VecDeque<ReadyRange>],
    pub delayed: &'a FastMap<MessageId, u64>,
    pub dlq: &'a FastMap<MessageId, u64>,
}

impl QueueRecoveryStore {
    pub(super) fn new(store: QueueStore, key: QueueKey) -> Self {
        Self {
            meta_key: QueueActor::meta_key(&key),
            index_meta_key: QueueActor::index_meta_key(&key),
            header_key_prefix: QueueActor::header_key_prefix(&key).into(),
            ready_index_prefix: QueueActor::ready_index_prefix(&key).into(),
            delayed_index_prefix: QueueActor::delayed_index_prefix(&key).into(),
            dlq_index_prefix: QueueActor::dlq_index_prefix(&key).into(),
            store,
            key,
        }
    }

    pub(super) fn snapshot(&self) -> Result<QueueRecoverySnapshot, String> {
        self.store
            .begin(self.key.family.id(), QueueTransactionMode::ReadOnly)
            .map(QueueRecoverySnapshot)
            .map_err(|error| format!("Failed to begin queue recovery snapshot: {error:?}"))
    }

    pub(super) fn read_index(
        &self,
        snapshot: &QueueRecoverySnapshot,
    ) -> Result<IndexMetaSnapshot, IndexRecoveryAttempt> {
        let index_meta = match snapshot.0.get(&self.index_meta_key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(IndexRecoveryAttempt::Missing {
                    next_id: self.next_id(snapshot),
                })
            }
            Err(error) if error.is_missing_snapshot() => {
                return Err(IndexRecoveryAttempt::Missing {
                    next_id: self.next_id(snapshot),
                });
            }
            Err(error) => {
                return Err(IndexRecoveryAttempt::Error {
                    next_id: self.next_id(snapshot),
                    reason: format!("Failed to read queue index meta: {error:?}"),
                })
            }
        };
        QueueActor::decode_index_meta(&index_meta).map_err(|reason| IndexRecoveryAttempt::Invalid {
            next_id: self.next_id(snapshot),
            reason,
        })
    }

    pub(super) fn ready_ranges<'a>(
        &'a self,
        snapshot: &'a QueueRecoverySnapshot,
    ) -> Result<impl Iterator<Item = Result<ReadyRange, String>> + 'a, String> {
        let rows = snapshot
            .0
            .scan_prefix(self.ready_index_prefix.clone())
            .map_err(|error| format!("Failed to scan queue ready index: {error:?}"))?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| decode_ready(&key, &value, &self.ready_index_prefix)))
    }

    pub(super) fn delayed_entries<'a>(
        &'a self,
        snapshot: &'a QueueRecoverySnapshot,
    ) -> Result<impl Iterator<Item = Result<(u64, MessageId), String>> + 'a, String> {
        let rows = snapshot
            .0
            .scan_prefix(self.delayed_index_prefix.clone())
            .map_err(|error| format!("Failed to scan queue delayed index: {error:?}"))?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| decode_delayed(&key, &value, &self.delayed_index_prefix)))
    }

    pub(super) fn dead_letters<'a>(
        &'a self,
        snapshot: &'a QueueRecoverySnapshot,
    ) -> Result<impl Iterator<Item = Result<(u64, MessageId), String>> + 'a, String> {
        let rows = snapshot
            .0
            .scan_prefix(self.dlq_index_prefix.clone())
            .map_err(|error| format!("Failed to scan queue DLQ index: {error:?}"))?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| decode_dlq(&key, &value, &self.dlq_index_prefix)))
    }

    pub(super) fn headers<'a>(
        &'a self,
        snapshot: &'a QueueRecoverySnapshot,
    ) -> Result<impl Iterator<Item = Result<(MessageId, QueueRecord), String>> + 'a, String> {
        let rows = match snapshot.0.scan_prefix(self.header_key_prefix.clone()) {
            Ok(rows) => Some(rows),
            Err(error) if error.is_missing_snapshot() => None,
            Err(error) => {
                return Err(format!(
                    "Failed to scan queue headers for recovery: {error:?}"
                ))
            }
        };
        Ok(rows
            .into_iter()
            .flatten()
            .map(|(key, value)| decode_header(&key, &value, &self.header_key_prefix)))
    }

    pub(super) fn replace_index(
        &self,
        state: &QueueIndexRebuild<'_>,
        write_policy: WritePolicy,
    ) -> Result<(), String> {
        let mut transaction = self
            .store
            .begin(self.key.family.id(), QueueTransactionMode::ReadWrite)
            .map_err(|error| format!("Failed to begin queue index rebuild tx: {error:?}"))?;
        let ready_prefix = &self.ready_index_prefix;
        let delayed_prefix = &self.delayed_index_prefix;
        let dlq_prefix = &self.dlq_index_prefix;
        // Read every old key before mutation; the replacement is one atomic commit.
        let mut stale_keys = Vec::new();
        for (prefix, label) in [
            (ready_prefix, "ready"),
            (delayed_prefix, "delayed"),
            (dlq_prefix, "DLQ"),
        ] {
            let rows = transaction
                .scan_prefix(prefix.clone())
                .map_err(|error| format!("Failed to scan {label} index for rebuild: {error:?}"))?;
            for (key, _) in rows {
                stale_keys.push(key);
            }
        }
        for key in stale_keys {
            transaction
                .delete(key.to_vec())
                .map_err(|error| format!("Failed to delete stale queue index key: {error:?}"))?;
        }
        for (shard, ranges) in state.ready.iter().enumerate() {
            for range in ranges {
                transaction
                    .put(
                        QueueActor::ready_range_key_with_prefix(ready_prefix, shard, range.next),
                        QueueActor::encode_ready_range_value(*range),
                        None,
                    )
                    .map_err(|error| format!("Failed to write queue ready index: {error:?}"))?;
            }
        }
        for (rows, prefix, label) in [
            (state.delayed, delayed_prefix, "delayed"),
            (state.dlq, dlq_prefix, "DLQ"),
        ] {
            for (&id, &timestamp) in rows {
                transaction
                    .put(
                        QueueActor::delayed_index_key_with_prefix(prefix, timestamp, id),
                        Vec::new(),
                        None,
                    )
                    .map_err(|error| format!("Failed to write queue {label} index: {error:?}"))?;
            }
        }
        transaction
            .put(
                self.index_meta_key.clone(),
                QueueActor::encode_index_meta(
                    state.meta.next_id,
                    state.meta.ready_count,
                    state.meta.delayed_count,
                    state.meta.next_delayed_visibility_ms,
                ),
                None,
            )
            .map_err(|error| format!("Failed to write queue index meta: {error:?}"))?;
        transaction
            .commit(write_policy)
            .map_err(|error| format!("Failed to commit queue index rebuild: {error:?}"))
    }

    pub(super) fn next_id(&self, snapshot: &QueueRecoverySnapshot) -> u64 {
        // The reservation row, not potentially corrupt index metadata, owns the
        // fallback ID floor. Read it from the same snapshot as the index/headers.
        match snapshot.0.get(&self.meta_key) {
            Ok(Some(bytes)) => QueueActor::decode_next_id(Some(&bytes)),
            Ok(None) => 1,
            Err(error) if error.is_missing_snapshot() => 1,
            Err(error) => {
                tracing::warn!(queue = ?self.key, route_family = self.key.family.as_u64(), ?error,
                    "Failed to recover queue next_id; starting from 1");
                1
            }
        }
    }
}

fn decode_ready(key: &[u8], value: &[u8], prefix: &[u8]) -> Result<ReadyRange, String> {
    let (shard, start) =
        QueueActor::parse_ready_range_key(key, prefix).ok_or("Malformed queue ready index key")?;
    let range =
        QueueActor::decode_ready_range(start, value).ok_or("Malformed queue ready index value")?;
    if shard != QueueActor::ready_shard_index(range.next) {
        return Err("Queue ready index shard does not match message ID".to_string());
    }
    Ok(range)
}

fn decode_delayed(key: &[u8], _: &[u8], prefix: &[u8]) -> Result<(u64, MessageId), String> {
    QueueActor::parse_delayed_index_key(key, prefix)
        .ok_or_else(|| "Malformed queue delayed index key".to_string())
}

fn decode_dlq(key: &[u8], _: &[u8], prefix: &[u8]) -> Result<(u64, MessageId), String> {
    QueueActor::parse_dlq_index_key(key, prefix)
        .ok_or_else(|| "Malformed queue DLQ index key".to_string())
}

fn decode_header(
    key: &[u8],
    value: &[u8],
    prefix: &[u8],
) -> Result<(MessageId, QueueRecord), String> {
    let id = QueueActor::parse_message_id_from_key(key, prefix)
        .ok_or("Malformed authoritative queue header key")?;
    let record = QueueActor::decode_record_header(value)
        .map_err(|_| format!("Malformed authoritative queue header record for message {id}"))?;
    Ok((id, record))
}

#[cfg(test)]
mod tests;
