//! Queue recovery persistence. Transactions, encoded rows, and index replacement
//! stay here; the actor decides recovery fallback and reconstructs live state.

use super::{
    FastMap, IndexMetaSnapshot, IndexRecoveryAttempt, MessageId, QueueActor, QueueKey, QueueRecord,
    ReadyRange,
};
use bytes::Bytes;
use cntryl_midge::{Engine, Query, ScanIterator, Transaction, TransactionMode, WriteOptions};
use std::collections::VecDeque;
use std::sync::Arc;

pub(super) struct QueueRecoveryStore {
    engine: Arc<Engine>,
    key: QueueKey,
}

pub(super) struct QueueRecoveryIndex {
    pub meta: IndexMetaSnapshot,
    key: QueueKey,
    transaction: Transaction,
}

pub(super) struct QueueRecoveryRows<'a> {
    pub ready: RecoveryRows<'a, ReadyRange>,
    pub delayed: RecoveryRows<'a, (u64, MessageId)>,
    pub dlq: RecoveryRows<'a, (u64, MessageId)>,
}

impl QueueRecoveryIndex {
    pub(super) fn rows(&self) -> Result<QueueRecoveryRows<'_>, String> {
        let ready = QueueRecoveryStore::rows(
            &self.transaction,
            QueueActor::ready_index_prefix(&self.key),
            "ready index",
            decode_ready,
        )?;
        let delayed = QueueRecoveryStore::rows(
            &self.transaction,
            QueueActor::delayed_index_prefix(&self.key),
            "delayed index",
            decode_delayed,
        )?;
        let dlq = QueueRecoveryStore::rows(
            &self.transaction,
            QueueActor::dlq_index_prefix(&self.key),
            "DLQ index",
            decode_dlq,
        )?;
        Ok(QueueRecoveryRows {
            ready,
            delayed,
            dlq,
        })
    }
}

pub(super) struct QueueIndexRebuild<'a> {
    pub meta: IndexMetaSnapshot,
    pub ready: &'a [VecDeque<ReadyRange>],
    pub delayed: &'a FastMap<MessageId, u64>,
    pub dlq: &'a FastMap<MessageId, u64>,
}

type RowDecoder<T> = fn(&[u8], &[u8], &[u8]) -> Result<T, String>;

pub(super) struct RecoveryRows<'a, T> {
    scan: ScanIterator<'a>,
    prefix: Vec<u8>,
    label: &'static str,
    decode: RowDecoder<T>,
}

impl<T> Iterator for RecoveryRows<'_, T> {
    type Item = Result<T, String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next().map(|row| {
            let (key, value) =
                row.map_err(|error| format!("Failed to read queue {}: {error:?}", self.label))?;
            (self.decode)(&key, &value, &self.prefix)
        })
    }
}

impl QueueRecoveryStore {
    pub(super) fn new(engine: Arc<Engine>, key: QueueKey) -> Self {
        Self { engine, key }
    }

    pub(super) fn read_index(&self) -> Result<QueueRecoveryIndex, IndexRecoveryAttempt> {
        let transaction = self
            .engine
            .begin_tx(self.key.family.id(), TransactionMode::ReadOnly)
            .map_err(|error| IndexRecoveryAttempt::Error {
                next_id: 1,
                reason: format!("Failed to begin index recovery tx: {error:?}"),
            })?;
        let index_meta = match transaction.get(&QueueActor::index_meta_key(&self.key)) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(IndexRecoveryAttempt::Missing {
                    next_id: self.next_id(),
                })
            }
            Err(error) if QueueActor::is_missing_read_snapshot_error(&error) => {
                return Err(IndexRecoveryAttempt::Missing {
                    next_id: self.next_id(),
                });
            }
            Err(error) => {
                return Err(IndexRecoveryAttempt::Error {
                    next_id: self.next_id(),
                    reason: format!("Failed to read queue index meta: {error:?}"),
                })
            }
        };
        let meta = QueueActor::decode_index_meta(&index_meta).map_err(|reason| {
            IndexRecoveryAttempt::Invalid {
                next_id: self.next_id(),
                reason,
            }
        })?;
        Ok(QueueRecoveryIndex {
            meta,
            key: self.key.clone(),
            transaction,
        })
    }

    fn rows<'a, T>(
        transaction: &'a Transaction,
        prefix: Vec<u8>,
        label: &'static str,
        decode: RowDecoder<T>,
    ) -> Result<RecoveryRows<'a, T>, String> {
        let scan = transaction
            .scan(&Query::new().prefix(Bytes::copy_from_slice(&prefix)))
            .map_err(|error| format!("Failed to scan queue {label}: {error:?}"))?;
        Ok(RecoveryRows {
            scan,
            prefix,
            label,
            decode,
        })
    }

    pub(super) fn read_headers(&self) -> Result<Vec<(MessageId, QueueRecord)>, String> {
        let transaction = self
            .engine
            .begin_tx(self.key.family.id(), TransactionMode::ReadOnly)
            .map_err(|error| format!("Failed to begin recovery scan tx: {error:?}"))?;
        let prefix = QueueActor::header_key_prefix(&self.key);
        let scan = match transaction.scan(&Query::new().prefix(Bytes::copy_from_slice(&prefix))) {
            Ok(scan) => scan,
            Err(error) if QueueActor::is_missing_read_snapshot_error(&error) => {
                return Ok(Vec::new())
            }
            Err(error) => {
                return Err(format!(
                    "Failed to scan queue headers for recovery: {error:?}"
                ))
            }
        };
        RecoveryRows {
            scan,
            prefix,
            label: "recovery scan",
            decode: decode_header,
        }
        .collect()
    }

    pub(super) fn replace_index(
        &self,
        state: &QueueIndexRebuild<'_>,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let mut transaction = self
            .engine
            .begin_tx(self.key.family.id(), TransactionMode::ReadWrite)
            .map_err(|error| format!("Failed to begin queue index rebuild tx: {error:?}"))?;
        let ready_prefix = QueueActor::ready_index_prefix(&self.key);
        let delayed_prefix = QueueActor::delayed_index_prefix(&self.key);
        let dlq_prefix = QueueActor::dlq_index_prefix(&self.key);
        // Read every old key before mutation; the replacement is one atomic commit.
        let mut stale_keys = Vec::new();
        for (prefix, label) in [
            (&ready_prefix, "ready"),
            (&delayed_prefix, "delayed"),
            (&dlq_prefix, "DLQ"),
        ] {
            let rows = transaction
                .scan(&Query::new().prefix(Bytes::copy_from_slice(prefix)))
                .map_err(|error| format!("Failed to scan {label} index for rebuild: {error:?}"))?;
            for row in rows {
                let (key, _) = row.map_err(|error| {
                    format!("Failed to scan {label} index for rebuild: {error:?}")
                })?;
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
                        QueueActor::ready_range_key_with_prefix(&ready_prefix, shard, range.next),
                        QueueActor::encode_ready_range_value(*range),
                        None,
                    )
                    .map_err(|error| format!("Failed to write queue ready index: {error:?}"))?;
            }
        }
        for (rows, prefix, label) in [
            (state.delayed, &delayed_prefix, "delayed"),
            (state.dlq, &dlq_prefix, "DLQ"),
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
                QueueActor::index_meta_key(&self.key),
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
            .commit(write_options)
            .map_err(|error| format!("Failed to commit queue index rebuild: {error:?}"))
    }

    pub(super) fn next_id(&self) -> u64 {
        let transaction = match self
            .engine
            .begin_tx(self.key.family.id(), TransactionMode::ReadOnly)
        {
            Ok(transaction) => transaction,
            Err(error) => {
                tracing::warn!(queue = ?self.key, route_family = self.key.family.as_u64(), ?error,
                    "Failed to begin queue meta recovery transaction; starting from 1");
                return 1;
            }
        };
        match transaction.get(&QueueActor::meta_key(&self.key)) {
            Ok(Some(bytes)) => QueueActor::decode_next_id(Some(&bytes)),
            Ok(None) => 1,
            Err(error) if QueueActor::is_missing_read_snapshot_error(&error) => 1,
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
