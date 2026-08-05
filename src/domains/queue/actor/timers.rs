use super::{
    DlqReason, Duration, Inflight, InflightExpiry, Instant, MessageId, QueueActor, QueueRecord,
    QueueState, Reverse,
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
        let Some(txn) = self.begin_redelivery_transaction(id, &inflight) else {
            return false;
        };
        if self.should_move_to_dlq(record.attempts) {
            return self.handle_redelivery_dlq(id, &inflight, &mut record, txn, now_epoch_ms);
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

        while let Some(Reverse(expiry)) = self.timers.peek() {
            if expiry.expires_at > now {
                self.next_expiration_deadline = expiry.expires_at;
                break;
            }

            let expiry = self.timers.pop().unwrap().0;

            if let Some(inflight) = self.inflight.get(&expiry.id) {
                if inflight.inflight_epoch != expiry.inflight_epoch {
                    continue;
                }
            }

            mutated |= self.handle_inflight_expired(expiry.id);
        }

        if self.timers.is_empty() {
            self.next_expiration_deadline = now + Duration::from_hours(1);
        }
        mutated
    }

    pub(super) fn schedule_inflight_retry(&mut self, id: MessageId, inflight: &Inflight) {
        let retry_at = self.clock.now_instant() + Duration::from_secs(1);
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

    fn should_move_to_dlq(&self, attempts: u32) -> bool {
        self.max_attempts
            .is_some_and(|max_attempts| attempts >= max_attempts)
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

    fn handle_redelivery_dlq(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &mut QueueRecord,
        mut txn: cntryl_midge::Transaction,
        now_epoch_ms: u64,
    ) -> bool {
        let dead_lettered_at_ms = now_epoch_ms;
        let index_plan = self.plan_index_mutation_for_unavailable_message(id);
        Self::prepare_dlq_record(record, dead_lettered_at_ms);

        if !self.persist_dlq_record(id, inflight, record, &mut txn) {
            return false;
        }

        if let Err(error) =
            self.write_index_mutation_plan(&mut txn, id, index_plan, Some(dead_lettered_at_ms))
        {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error_reason = %error,
                "Failed to update queue indexes during DLQ transition"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        let update_start = Instant::now();
        if let Err(error) = Self::commit_redelivery_transaction(txn, self.commit_write_options) {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to commit queue DLQ transition"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY, update_start);
        self.inflight.remove(&id);
        self.update_cached_inflight_metadata(
            id,
            inflight.inflight_epoch,
            None,
            None,
            record.last_inflight_at_ms,
        );
        self.apply_index_mutation_plan(id, index_plan, Some(dead_lettered_at_ms));
        self.cache_record(id, record.metadata_only_from());
        self.evict_cached_body(id);

        tracing::info!(
            queue = ?self.queue_key,
            route_family = self.queue_key.family.as_u64(),
            message_id = id.as_u64(),
            attempts = record.attempts,
            "Message moved to queue dead letter state"
        );
        Self::increment_counter(obs::METRIC_QUEUE_DLQ_TRANSITIONS);
        true
    }

    fn prepare_dlq_record(record: &mut QueueRecord, dead_lettered_at_ms: u64) {
        record.state = QueueState::Dlq;
        record.ready_seq = None;
        record.visible_at_ms = 0;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        record.dead_lettered_at_ms = Some(dead_lettered_at_ms);
        record.dlq_reason = Some(DlqReason::MaxAttemptsExceeded);
    }

    fn persist_dlq_record(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &mut QueueRecord,
        txn: &mut cntryl_midge::Transaction,
    ) -> bool {
        let _ = inflight;
        let write_result = txn
            .put(
                self.cached_header_key(id),
                Self::encode_record_header(record),
                None,
            )
            .map_err(|error| format!("Failed to write DLQ header: {error:?}"));

        if let Err(error) = write_result {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to persist queue DLQ record"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        true
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
        if let Err(error) = Self::commit_redelivery_transaction(txn, self.commit_write_options) {
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
