//! Inflight-expiration and redelivery timer processing.

use super::{
    dlq::{decide_redelivery, RedeliveryOutcome},
    Inflight, InflightExpiry, Instant, MessageId, QueueActor, QueueCommit, QueueRecord, Reverse,
    QUEUE_IDLE_HORIZON, QUEUE_STORAGE_RETRY_BACKOFF,
};
use crate::observability as obs;

impl QueueActor {
    pub(super) fn handle_inflight_expired(&mut self, id: MessageId) -> bool {
        let Some(inflight) = self.load_expired_inflight(id) else {
            return false;
        };
        let now_epoch_ms = self.clock.now_epoch_ms();
        let Some(mut record) = self.load_redelivery_record(id, &inflight) else {
            return false;
        };

        let Some(next_attempts) = record.attempts.checked_add(1) else {
            tracing::error!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                "Queue delivery attempt counter exhausted during redelivery"
            );
            self.schedule_inflight_retry(id, &inflight);
            return false;
        };
        record.attempts = next_attempts;
        let outcome = decide_redelivery(self.max_attempts, &record, &inflight);
        let Some(txn) = self.begin_redelivery_transaction(id, &inflight) else {
            return false;
        };
        if outcome == RedeliveryOutcome::DeadLetter {
            return self.persist_dlq_transition(id, &inflight, &mut record, txn, now_epoch_ms);
        }

        if !self.persist_redelivery_attempt(id, &inflight, &record, txn) {
            return false;
        }

        Self::increment_counter(obs::METRIC_QUEUE_REDELIVERIES);
        self.inflight.remove(&id);
        self.finish_redelivery_retry(id, &inflight, &record);
        true
    }

    /// # Panics
    ///
    /// Panics only if the timer heap is internally inconsistent after a successful `peek`.
    pub fn process_expired_timers(&mut self) -> bool {
        let now = self.clock.now_instant();
        let mut mutated = false;
        let mut processed = 0;

        while processed < super::MAX_DUE_ITEMS_PER_PASS {
            let Some(Reverse(expiry)) = self.timers.peek() else {
                break;
            };
            if expiry.expires_at > now {
                self.next_expiration_deadline = expiry.expires_at;
                break;
            }

            let expiry = self.timers.pop().unwrap().0;
            processed += 1;

            if let Some(inflight) = self.inflight.get(&expiry.id) {
                if inflight.inflight_epoch != expiry.inflight_epoch {
                    continue;
                }
            }

            mutated |= self.handle_inflight_expired(expiry.id);
        }

        if self.timers.is_empty() {
            self.next_expiration_deadline = now + QUEUE_IDLE_HORIZON;
        }
        mutated
    }

    pub(super) fn schedule_inflight_retry(&mut self, id: MessageId, inflight: &Inflight) {
        let retry_at = self.clock.now_instant() + QUEUE_STORAGE_RETRY_BACKOFF;
        self.timers.push(Reverse(InflightExpiry {
            id,
            inflight_epoch: inflight.inflight_epoch,
            expires_at: retry_at,
            expires_at_ms: inflight.expires_at_epoch_ms,
        }));
        if retry_at < self.next_expiration_deadline {
            self.next_expiration_deadline = retry_at;
        }
    }

    fn load_expired_inflight(&self, id: MessageId) -> Option<Inflight> {
        let inflight = self.inflight.get(&id)?.clone();
        if inflight.expires_at > self.clock.now_instant() {
            return None;
        }

        Some(inflight)
    }

    fn load_redelivery_record(&self, id: MessageId, inflight: &Inflight) -> Option<QueueRecord> {
        if let Some(cached) = self.records.get(&id) {
            return Some(cached.clone());
        }

        match self.load_record_metadata_from_store(id) {
            Ok(record) => Some(record),
            Err(error) => {
                self.warn_redelivery_load_error(id, error.as_str());
                let _ = inflight;
                None
            }
        }
    }

    fn warn_redelivery_load_error(&self, id: MessageId, error: &str) {
        tracing::warn!(
            queue = ?self.queue_key,
            route_family = self.queue_key.family.as_u64(),
            message_id = id.as_u64(),
            error_reason = %error,
            "Failed to load queue message during redelivery"
        );
    }

    fn begin_redelivery_transaction(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
    ) -> Option<cntryl_midge::Transaction> {
        match self.store.begin_tx(
            self.queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadWrite,
        ) {
            Ok(txn) => Some(txn),
            Err(error) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    message_id = id.as_u64(),
                    error = ?error,
                    "Failed to begin queue redelivery transaction"
                );
                self.schedule_inflight_retry(id, inflight);
                None
            }
        }
    }

    fn persist_redelivery_attempt(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &QueueRecord,
        mut txn: cntryl_midge::Transaction,
    ) -> bool {
        let write_result = self.persist_split_redelivery_attempt(id, record, &mut txn);

        if let Err(error) = write_result {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to persist queue redelivery attempt update"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        let update_start = Instant::now();
        if let Err(error) =
            Self::commit_transaction(txn, self.commit_write_options, QueueCommit::Redelivery)
        {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to commit queue redelivery retry transaction"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY, update_start);
        true
    }

    fn persist_split_redelivery_attempt(
        &self,
        id: MessageId,
        record: &QueueRecord,
        txn: &mut cntryl_midge::Transaction,
    ) -> Result<(), String> {
        let header_key = self.cached_header_key(id);
        match txn.get(&header_key) {
            Ok(Some(_)) => txn
                .put(header_key, Self::encode_record_header(record), None)
                .map_err(|error| format!("persist split redelivery failed: {error:?}")),
            Ok(None) => Err("Queue message disappeared during redelivery".to_string()),
            Err(error) => Err(format!(
                "Failed to read queue message during redelivery: {error:?}"
            )),
        }
    }

    fn finish_redelivery_retry(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &QueueRecord,
    ) {
        self.cache_record(
            id,
            QueueRecord::metadata_only(record.attempts, record.visible_at_ms),
        );
        self.update_cached_inflight_metadata(
            id,
            inflight.inflight_epoch,
            None,
            None,
            record.last_inflight_at_ms,
        );
        self.push_ready(id);
    }
}
