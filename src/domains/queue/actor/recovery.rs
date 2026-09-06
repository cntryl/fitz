use super::recovery_store::{QueueRecoverySnapshot, QueueRecoveryStore};
use super::{
    DelayedMessage, IndexRecoveryAttempt, MessageId, QueueActor, QueueState, RecoveryPath,
    QUEUE_IDLE_HORIZON,
};
use crate::observability as obs;
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
    fn try_recover_from_index(
        &mut self,
        store: &QueueRecoveryStore,
        snapshot: &QueueRecoverySnapshot,
    ) -> IndexRecoveryAttempt {
        let start = Instant::now();
        let meta_snapshot = match store.read_index(snapshot) {
            Ok(index) => index,
            Err(error) => {
                self.index_meta_written = false;
                match &error {
                    IndexRecoveryAttempt::Missing { .. } => {
                        Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_MISSING);
                    }
                    IndexRecoveryAttempt::Invalid { .. } => {
                        Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
                    }
                    _ => {}
                }
                return error;
            }
        };
        self.index_meta_written = true;
        self.reset_recovery_state();
        let scan = || -> Result<_, String> {
            Ok((
                store.ready_ranges(snapshot)?,
                store.delayed_entries(snapshot)?,
                store.dead_letters(snapshot)?,
            ))
        };
        let (mut ready, mut delayed, mut dlq) = match scan() {
            Ok(rows) => rows,
            Err(reason) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason,
                }
            }
        };
        let stats = match self.scan_index_entries(
            &mut ready,
            &mut delayed,
            &mut dlq,
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
                next_id: store.next_id(snapshot),
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
        self.recovery_store.replace_index(
            &super::recovery_store::QueueIndexRebuild {
                meta: super::IndexMetaSnapshot {
                    next_id,
                    ready_count: Self::usize_to_u64(self.persisted_ready_count),
                    delayed_count: Self::usize_to_u64(self.persisted_delayed.len()),
                    next_delayed_visibility_ms: self.min_persisted_delayed_visibility_ms(),
                },
                ready: &self.persisted_ready_shards,
                delayed: &self.persisted_delayed,
                dlq: &self.persisted_dlq,
            },
            self.commit_write_options,
        )?;
        self.index_meta_written = true;
        Ok(())
    }

    pub(super) fn recover_from_scan_and_rebuild_index(
        &mut self,
        fallback_next_id: u64,
        store: &QueueRecoveryStore,
        snapshot: &QueueRecoverySnapshot,
    ) -> Result<Option<u64>, String> {
        let start = Instant::now();
        self.reset_recovery_state();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut max_id = None::<u64>;
        let mut recovered_ready_ids = Vec::new();
        for entry in store.headers(snapshot)? {
            let (id, record) = entry?;
            self.recover_record(
                id,
                &record,
                now_epoch_ms,
                now_instant,
                &mut recovered_ready_ids,
                &mut max_id,
            );
        }
        if max_id.is_none() {
            return Ok(None);
        }
        if self.delayed.is_empty() {
            self.next_delayed_deadline = now_instant + QUEUE_IDLE_HORIZON;
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
        let store = self.recovery_store.clone();
        let snapshot = store.snapshot()?;
        let (mut next_id, max_id) = match self.try_recover_from_index(&store, &snapshot) {
            IndexRecoveryAttempt::Hit { next_id, max_id } => {
                self.recovery_path = RecoveryPath::IndexHit;
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Missing { next_id } => {
                self.recovery_path = RecoveryPath::IndexMissingFallback;
                let max_id =
                    self.recover_from_scan_and_rebuild_index(next_id, &store, &snapshot)?;
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
                let max_id =
                    self.recover_from_scan_and_rebuild_index(next_id, &store, &snapshot)?;
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
                let max_id =
                    self.recover_from_scan_and_rebuild_index(next_id, &store, &snapshot)?;
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

    fn scan_index_entries(
        &mut self,
        ready_iter: &mut impl Iterator<Item = Result<super::ReadyRange, String>>,
        delayed_iter: &mut impl Iterator<Item = Result<(u64, MessageId), String>>,
        dlq_iter: &mut impl Iterator<Item = Result<(u64, MessageId), String>>,
        next_id: u64,
    ) -> Result<IndexScanStats, IndexRecoveryAttempt> {
        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut stats = IndexScanStats::new();

        self.scan_ready_ranges(ready_iter, next_id, &mut stats)?;
        self.scan_delayed_entries(delayed_iter, next_id, now_epoch_ms, now_instant, &mut stats)?;
        self.scan_dlq_entries(dlq_iter, next_id, &mut stats)?;

        if self.delayed.is_empty() {
            self.next_delayed_deadline = now_instant + QUEUE_IDLE_HORIZON;
        }

        Ok(stats)
    }

    fn scan_ready_ranges(
        &mut self,
        ready_iter: &mut impl Iterator<Item = Result<super::ReadyRange, String>>,
        next_id: u64,
        stats: &mut IndexScanStats,
    ) -> Result<(), IndexRecoveryAttempt> {
        for entry in ready_iter.by_ref() {
            let range = entry.map_err(|reason| IndexRecoveryAttempt::Error { next_id, reason })?;

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
        delayed_iter: &mut impl Iterator<Item = Result<(u64, MessageId), String>>,
        next_id: u64,
        now_epoch_ms: u64,
        now_instant: Instant,
        stats: &mut IndexScanStats,
    ) -> Result<(), IndexRecoveryAttempt> {
        for entry in delayed_iter.by_ref() {
            let (visible_at_ms, id) =
                entry.map_err(|reason| IndexRecoveryAttempt::Error { next_id, reason })?;

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
        dlq_iter: &mut impl Iterator<Item = Result<(u64, MessageId), String>>,
        next_id: u64,
        stats: &mut IndexScanStats,
    ) -> Result<(), IndexRecoveryAttempt> {
        for entry in dlq_iter.by_ref() {
            let (dead_lettered_at_ms, id) =
                entry.map_err(|reason| IndexRecoveryAttempt::Error { next_id, reason })?;

            self.insert_persisted_dlq(id, dead_lettered_at_ms);
            stats.max_id = Some(
                stats
                    .max_id
                    .map_or(id.as_u64(), |max_id| max_id.max(id.as_u64())),
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
