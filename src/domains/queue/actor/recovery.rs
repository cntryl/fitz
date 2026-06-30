use super::{
    DecodedIndexMeta, DelayedMessage, IndexRecoveryAttempt, MessageId, QueueActor, QueueState,
    RecoveryPath,
};
use crate::observability as obs;
use bytes::Bytes;
use std::cmp::Reverse;
use std::time::{Duration, Instant};

struct IndexScanStats {
    max_id: Option<u64>,
    scanned_ready_count: u64,
    scanned_delayed_count: u64,
    scanned_next_delayed: Option<u64>,
    matured_delayed_ids: Vec<MessageId>,
}

impl IndexScanStats {
    fn new() -> Self {
        Self {
            max_id: None,
            scanned_ready_count: 0,
            scanned_delayed_count: 0,
            scanned_next_delayed: None,
            matured_delayed_ids: Vec::new(),
        }
    }
}

impl QueueActor {
    pub(super) fn try_recover_from_index(&mut self) -> IndexRecoveryAttempt {
        let start = Instant::now();
        let txn = match self.begin_index_recovery_tx() {
            Ok(txn) => txn,
            Err(error) => return error,
        };
        let meta_snapshot = match self.load_index_meta_snapshot(&txn) {
            Ok(snapshot) => snapshot,
            Err(error) => return error,
        };

        self.index_meta_written = true;
        self.reset_recovery_state();

        let ready_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.ready_index_prefix));
        let delayed_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.delayed_index_prefix));
        let dlq_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.dlq_index_prefix));

        let mut ready_iter = match txn.scan(&ready_query) {
            Ok(iter) => iter,
            Err(e) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: format!("Failed to scan queue ready index: {e:?}"),
                };
            }
        };
        let mut delayed_iter = match txn.scan(&delayed_query) {
            Ok(iter) => iter,
            Err(e) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: format!("Failed to scan queue delayed index: {e:?}"),
                };
            }
        };
        let mut dlq_iter = match txn.scan(&dlq_query) {
            Ok(iter) => iter,
            Err(e) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: format!("Failed to scan queue DLQ index: {e:?}"),
                };
            }
        };

        let stats = match self.scan_index_entries(
            &mut ready_iter,
            &mut delayed_iter,
            &mut dlq_iter,
            meta_snapshot.next_id,
        ) {
            Ok(stats) => stats,
            Err(error) => return error,
        };

        self.populate_live_ready_from_persisted(&stats.matured_delayed_ids);

        if meta_snapshot.ready_count != stats.scanned_ready_count
            || meta_snapshot.delayed_count != stats.scanned_delayed_count
            || meta_snapshot.next_delayed_visibility_ms != stats.scanned_next_delayed
        {
            self.index_meta_written = false;
            Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
            return IndexRecoveryAttempt::Invalid {
                next_id: self.load_next_id_from_meta_key(),
                reason: format!(
                    "Queue index meta counters mismatch (meta ready={}, scanned ready={}, meta delayed={}, scanned delayed={})",
                    meta_snapshot.ready_count,
                    stats.scanned_ready_count,
                    meta_snapshot.delayed_count,
                    stats.scanned_delayed_count
                ),
            };
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECOVERY_INDEX_LOAD_LATENCY, start);
        Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_HITS);
        IndexRecoveryAttempt::Hit {
            next_id: meta_snapshot.next_id,
            max_id: stats.max_id,
        }
    }

    pub(super) fn rewrite_index_from_memory(&mut self, next_id: u64) -> Result<(), String> {
        let cf_id = self.queue_key.family.id();
        let mut txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("Failed to begin queue index rebuild tx: {e:?}"))?;

        let ready_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.ready_index_prefix));
        let delayed_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.delayed_index_prefix));
        let dlq_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.dlq_index_prefix));

        let mut ready_iter = txn
            .scan(&ready_query)
            .map_err(|e| format!("Failed to scan ready index for rebuild: {e:?}"))?;
        let mut delayed_iter = txn
            .scan(&delayed_query)
            .map_err(|e| format!("Failed to scan delayed index for rebuild: {e:?}"))?;
        let mut dlq_iter = txn
            .scan(&dlq_query)
            .map_err(|e| format!("Failed to scan DLQ index for rebuild: {e:?}"))?;
        let mut ready_keys = Vec::new();
        let mut delayed_keys = Vec::new();
        let mut dlq_keys = Vec::new();

        while let Some((key, _)) = ready_iter.next() {
            ready_keys.push(key.clone());
        }
        while let Some((key, _)) = delayed_iter.next() {
            delayed_keys.push(key.clone());
        }
        while let Some((key, _)) = dlq_iter.next() {
            dlq_keys.push(key.clone());
        }

        for key in ready_keys.into_iter().chain(delayed_keys).chain(dlq_keys) {
            txn.delete(key)
                .map_err(|e| format!("Failed to delete stale queue index key: {e:?}"))?;
        }

        for (shard, ranges) in self.persisted_ready_shards.iter().enumerate() {
            for range in ranges {
                txn.put(
                    self.ready_range_key(shard, range.next),
                    Self::encode_ready_range_value(*range),
                    None,
                )
                .map_err(|e| format!("Failed to write queue ready index: {e:?}"))?;
            }
        }

        for (&id, &visible_at_ms) in &self.persisted_delayed {
            txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
                .map_err(|e| format!("Failed to write queue delayed index: {e:?}"))?;
        }

        for (&id, &dead_lettered_at_ms) in &self.persisted_dlq {
            txn.put(
                self.dlq_index_key(dead_lettered_at_ms, id),
                Vec::new(),
                None,
            )
            .map_err(|e| format!("Failed to write queue DLQ index: {e:?}"))?;
        }

        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                next_id,
                self.persisted_ready_count as u64,
                self.persisted_delayed.len() as u64,
                self.min_persisted_delayed_visibility_ms(),
            ),
            None,
        )
        .map_err(|e| format!("Failed to write queue index meta: {e:?}"))?;

        txn.commit(self.commit_write_options)
            .map_err(|e| format!("Failed to commit queue index rebuild: {e:?}"))?;
        self.index_meta_written = true;
        Ok(())
    }

    pub(super) fn recover_from_scan_and_rebuild_index(
        &mut self,
        fallback_next_id: u64,
    ) -> Result<Option<u64>, String> {
        let start = Instant::now();
        let txn = self
            .store
            .begin_tx(
                self.queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| format!("Failed to begin recovery scan tx: {e:?}"))?;

        self.reset_recovery_state();

        let header_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.header_key_prefix));
        let legacy_query = cntryl_midge::Query::new()
            .prefix(Bytes::copy_from_slice(&self.legacy_message_key_prefix));

        let mut header_iter = match txn.scan(&header_query) {
            Ok(iter) => iter,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => return Ok(None),
            Err(e) => {
                return Err(format!("Failed to scan queue headers for recovery: {e:?}"));
            }
        };
        let mut legacy_iter = match txn.scan(&legacy_query) {
            Ok(iter) => iter,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => return Ok(None),
            Err(e) => {
                return Err(format!(
                    "Failed to scan legacy queue records for recovery: {e:?}"
                ));
            }
        };

        if header_iter.remaining() == 0 && legacy_iter.remaining() == 0 {
            return Ok(None);
        }

        let recovered_count = header_iter.remaining() + legacy_iter.remaining();
        let header_entries = Self::collect_scan_entries(&mut header_iter);
        let legacy_entries = Self::collect_scan_entries(&mut legacy_iter);
        let per_shard = recovered_count / Self::READY_SHARDS + 1;
        for shard in &mut self.ready_shards {
            shard.reserve(per_shard);
        }
        for shard in &mut self.persisted_ready_shards {
            shard.reserve(per_shard);
        }
        self.delayed.reserve(recovered_count);

        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut max_id = None::<u64>;
        let mut recovered_ready_ids = Vec::with_capacity(recovered_count);

        self.recover_header_entries(
            &header_entries,
            now_epoch_ms,
            now_instant,
            &mut recovered_ready_ids,
            &mut max_id,
        )?;
        self.recover_legacy_entries(
            &legacy_entries,
            now_epoch_ms,
            now_instant,
            &mut recovered_ready_ids,
            &mut max_id,
        )?;

        if self.delayed.is_empty() {
            self.next_delayed_deadline = now_instant + Duration::from_hours(1);
        }

        recovered_ready_ids.sort_unstable_by_key(MessageId::as_u64);
        for id in recovered_ready_ids {
            self.push_ready(id);
            self.push_persisted_ready(id);
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECOVERY_FALLBACK_SCAN_LATENCY, start);
        Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_FALLBACKS);

        let rebuild_next_id = max_id
            .map_or(fallback_next_id, |value| value.saturating_add(1))
            .max(fallback_next_id);

        self.rewrite_index_from_memory(rebuild_next_id)?;
        Ok(max_id)
    }

    pub(super) fn recover_from_store(&mut self) -> Result<(), String> {
        let (mut next_id, max_id) = match self.try_recover_from_index() {
            IndexRecoveryAttempt::Hit { next_id, max_id } => {
                self.recovery_path = RecoveryPath::IndexHit;
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Missing { next_id } => {
                self.recovery_path = RecoveryPath::IndexMissingFallback;
                let max_id = self.recover_from_scan_and_rebuild_index(next_id)?;
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Invalid { next_id, reason } => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    recovery_reason = %reason,
                    "Queue index recovery found invalid state; falling back to full scan"
                );
                self.recovery_path = RecoveryPath::IndexInvalidFallback;
                let max_id = self.recover_from_scan_and_rebuild_index(next_id)?;
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Error { next_id, reason } => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    recovery_reason = %reason,
                    "Queue index recovery failed; falling back to full scan"
                );
                self.recovery_path = RecoveryPath::IndexErrorFallback;
                let max_id = self.recover_from_scan_and_rebuild_index(next_id)?;
                (next_id, max_id)
            }
        };

        if let Some(max_id) = max_id {
            next_id = next_id.max(max_id.saturating_add(1));
        }

        self.next_id = next_id;
        self.next_id_limit = next_id;
        if self.ready_len() == 0
            && self.delayed.is_empty()
            && self.persisted_dlq.is_empty()
            && self.recovery_path == RecoveryPath::IndexHit
        {
            self.recovery_path = RecoveryPath::Empty;
        }
        Ok(())
    }

    fn begin_index_recovery_tx(
        &mut self,
    ) -> Result<cntryl_midge::Transaction, IndexRecoveryAttempt> {
        self.store
            .begin_tx(
                self.queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .map_err(|e| {
                self.index_meta_written = false;
                IndexRecoveryAttempt::Error {
                    next_id: 1,
                    reason: format!("Failed to begin index recovery tx: {e:?}"),
                }
            })
    }

    fn load_index_meta_snapshot(
        &mut self,
        txn: &cntryl_midge::Transaction,
    ) -> Result<super::IndexMetaSnapshot, IndexRecoveryAttempt> {
        let index_meta = match txn.get(&self.index_meta_key) {
            Ok(value) => value,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => {
                self.index_meta_written = false;
                Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_MISSING);
                return Err(IndexRecoveryAttempt::Missing {
                    next_id: self.load_next_id_from_meta_key(),
                });
            }
            Err(e) => {
                self.index_meta_written = false;
                return Err(IndexRecoveryAttempt::Error {
                    next_id: self.load_next_id_from_meta_key(),
                    reason: format!("Failed to read queue index meta: {e:?}"),
                });
            }
        };
        let Some(index_meta) = index_meta else {
            self.index_meta_written = false;
            Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_MISSING);
            return Err(IndexRecoveryAttempt::Missing {
                next_id: self.load_next_id_from_meta_key(),
            });
        };

        let meta_snapshot = match Self::decode_index_meta(&index_meta) {
            Ok(DecodedIndexMeta::V2(snapshot)) => snapshot,
            Ok(DecodedIndexMeta::LegacyV1) => {
                self.index_meta_written = false;
                Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
                return Err(IndexRecoveryAttempt::Invalid {
                    next_id: self.load_next_id_from_meta_key(),
                    reason: "Queue index meta is legacy v1 and missing authoritative counters"
                        .to_string(),
                });
            }
            Err(reason) => {
                self.index_meta_written = false;
                Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
                return Err(IndexRecoveryAttempt::Invalid {
                    next_id: self.load_next_id_from_meta_key(),
                    reason,
                });
            }
        };

        if !Self::index_meta_is_valid(&index_meta) {
            self.index_meta_written = false;
            Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
            return Err(IndexRecoveryAttempt::Invalid {
                next_id: self.load_next_id_from_meta_key(),
                reason: "Queue index meta is missing version or validity marker".to_string(),
            });
        }

        Ok(meta_snapshot)
    }

    fn scan_index_entries(
        &mut self,
        ready_iter: &mut cntryl_midge::ScanIterator,
        delayed_iter: &mut cntryl_midge::ScanIterator,
        dlq_iter: &mut cntryl_midge::ScanIterator,
        next_id: u64,
    ) -> Result<IndexScanStats, IndexRecoveryAttempt> {
        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut stats = IndexScanStats::new();

        self.scan_ready_ranges(ready_iter, next_id, &mut stats)?;
        self.scan_delayed_entries(delayed_iter, next_id, now_epoch_ms, now_instant, &mut stats)?;
        self.scan_dlq_entries(dlq_iter, next_id, &mut stats)?;

        if self.delayed.is_empty() {
            self.next_delayed_deadline = now_instant + Duration::from_hours(1);
        }

        Ok(stats)
    }

    fn scan_ready_ranges(
        &mut self,
        ready_iter: &mut cntryl_midge::ScanIterator,
        next_id: u64,
        stats: &mut IndexScanStats,
    ) -> Result<(), IndexRecoveryAttempt> {
        while let Some((key_bytes, value_bytes)) = ready_iter.next() {
            let Some((shard, start_id)) =
                Self::parse_ready_range_key(&key_bytes, &self.ready_index_prefix)
            else {
                return Err(IndexRecoveryAttempt::Error {
                    next_id,
                    reason: "Malformed queue ready index key".to_string(),
                });
            };
            let Some(range) = Self::decode_ready_range(start_id, &value_bytes) else {
                return Err(IndexRecoveryAttempt::Error {
                    next_id,
                    reason: "Malformed queue ready index value".to_string(),
                });
            };
            if shard != Self::ready_shard_index(range.next) {
                return Err(IndexRecoveryAttempt::Error {
                    next_id,
                    reason: "Queue ready index shard does not match message ID".to_string(),
                });
            }

            self.push_persisted_ready_range(range);
            stats.scanned_ready_count += Self::usize_to_u64(Self::range_len(range));
            stats.max_id = Some(
                stats
                    .max_id
                    .map_or(range.end, |max_id| max_id.max(range.end)),
            );
        }

        Ok(())
    }

    fn scan_delayed_entries(
        &mut self,
        delayed_iter: &mut cntryl_midge::ScanIterator,
        next_id: u64,
        now_epoch_ms: u64,
        now_instant: Instant,
        stats: &mut IndexScanStats,
    ) -> Result<(), IndexRecoveryAttempt> {
        while let Some((key_bytes, _value_bytes)) = delayed_iter.next() {
            let Some((visible_at_ms, id)) =
                Self::parse_delayed_index_key(&key_bytes, &self.delayed_index_prefix)
            else {
                return Err(IndexRecoveryAttempt::Error {
                    next_id,
                    reason: "Malformed queue delayed index key".to_string(),
                });
            };

            self.insert_persisted_delayed(id, visible_at_ms);
            stats.scanned_delayed_count += 1;
            stats.scanned_next_delayed = Some(
                stats
                    .scanned_next_delayed
                    .map_or(visible_at_ms, |current| current.min(visible_at_ms)),
            );
            stats.max_id = Some(
                stats
                    .max_id
                    .map_or(id.as_u64(), |max_id| max_id.max(id.as_u64())),
            );

            if visible_at_ms <= now_epoch_ms {
                stats.matured_delayed_ids.push(id);
            } else {
                self.push_delayed_message(
                    id,
                    id.as_u64(),
                    visible_at_ms,
                    now_epoch_ms,
                    now_instant,
                );
            }
        }

        Ok(())
    }

    fn scan_dlq_entries(
        &mut self,
        dlq_iter: &mut cntryl_midge::ScanIterator,
        next_id: u64,
        stats: &mut IndexScanStats,
    ) -> Result<(), IndexRecoveryAttempt> {
        while let Some((key_bytes, _value_bytes)) = dlq_iter.next() {
            let Some((dead_lettered_at_ms, id)) =
                Self::parse_dlq_index_key(&key_bytes, &self.dlq_index_prefix)
            else {
                return Err(IndexRecoveryAttempt::Error {
                    next_id,
                    reason: "Malformed queue DLQ index key".to_string(),
                });
            };

            self.insert_persisted_dlq(id, dead_lettered_at_ms);
            stats.max_id = Some(
                stats
                    .max_id
                    .map_or(id.as_u64(), |max_id| max_id.max(id.as_u64())),
            );
        }

        Ok(())
    }

    fn collect_scan_entries(iter: &mut cntryl_midge::ScanIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();
        while let Some(entry) = iter.next() {
            entries.push(entry);
        }
        entries
    }

    fn recover_header_entries(
        &mut self,
        entries: &[(Vec<u8>, Vec<u8>)],
        now_epoch_ms: u64,
        now_instant: Instant,
        recovered_ready_ids: &mut Vec<MessageId>,
        max_id: &mut Option<u64>,
    ) -> Result<(), String> {
        for (key_bytes, value_bytes) in entries {
            let Some(id) = Self::parse_message_id_from_key(key_bytes, &self.header_key_prefix)
            else {
                return Err("Malformed authoritative queue header key".to_string());
            };
            let record = if let Ok(record) = Self::decode_record_header(value_bytes) {
                record
            } else if let Ok(record) = Self::decode_legacy_record(value_bytes.clone()) {
                record.metadata_only_from()
            } else {
                return Err(format!(
                    "Malformed authoritative queue header record for message {id}"
                ));
            };

            self.recover_record(
                id,
                &record,
                now_epoch_ms,
                now_instant,
                recovered_ready_ids,
                max_id,
            );
        }

        Ok(())
    }

    fn recover_legacy_entries(
        &mut self,
        entries: &[(Vec<u8>, Vec<u8>)],
        now_epoch_ms: u64,
        now_instant: Instant,
        recovered_ready_ids: &mut Vec<MessageId>,
        max_id: &mut Option<u64>,
    ) -> Result<(), String> {
        for (key_bytes, value_bytes) in entries {
            let Some(id) =
                Self::parse_message_id_from_key(key_bytes, &self.legacy_message_key_prefix)
            else {
                return Err("Malformed authoritative legacy queue message key".to_string());
            };
            let record = Self::decode_legacy_record(value_bytes.clone()).map_err(|error| {
                format!("Malformed authoritative legacy queue message {id}: {error}")
            })?;

            self.recover_record(
                id,
                &record,
                now_epoch_ms,
                now_instant,
                recovered_ready_ids,
                max_id,
            );
        }

        Ok(())
    }

    fn recover_record(
        &mut self,
        id: MessageId,
        record: &super::QueueRecord,
        now_epoch_ms: u64,
        now_instant: Instant,
        recovered_ready_ids: &mut Vec<MessageId>,
        max_id: &mut Option<u64>,
    ) {
        *max_id = Some(max_id.map_or(id.as_u64(), |current| current.max(id.as_u64())));
        if matches!(record.state, QueueState::Dlq) {
            self.insert_persisted_dlq(
                id,
                record
                    .dead_lettered_at_ms
                    .unwrap_or(record.first_enqueued_at_ms),
            );
            return;
        }

        if record.visible_at_ms <= now_epoch_ms {
            recovered_ready_ids.push(id);
            return;
        }

        let enqueue_seq = if record.enqueue_seq != 0 {
            record.enqueue_seq
        } else {
            id.as_u64()
        };
        self.push_delayed_message(
            id,
            enqueue_seq,
            record.visible_at_ms,
            now_epoch_ms,
            now_instant,
        );
        self.insert_persisted_delayed(id, record.visible_at_ms);
    }

    fn push_delayed_message(
        &mut self,
        id: MessageId,
        enqueue_seq: u64,
        visible_at_ms: u64,
        now_epoch_ms: u64,
        now_instant: Instant,
    ) {
        let delay_ms = visible_at_ms.saturating_sub(now_epoch_ms);
        let visible_at = now_instant + Duration::from_millis(delay_ms);
        self.delayed.push(Reverse(DelayedMessage {
            id,
            enqueue_seq,
            visible_at,
            visible_at_ms,
        }));
        if visible_at < self.next_delayed_deadline {
            self.next_delayed_deadline = visible_at;
        }
    }
}
