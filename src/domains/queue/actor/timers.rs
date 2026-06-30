use super::{
    DlqReason, Duration, Inflight, InflightExpiry, Instant, MessageId, QueueActor, QueueRecord,
    QueueState, Reverse, StoredRecordLayout,
};
use crate::observability as obs;

#[derive(Clone, Copy)]
struct DlqTransitionContext {
    record_layout: StoredRecordLayout,
    now_epoch_ms: u64,
    has_body_key: bool,
}

impl QueueActor {
    pub(super) fn handle_inflight_expired(&mut self, id: MessageId) {
        let Some(inflight) = self.load_expired_inflight(id) else {
            return;
        };
        let now_epoch_ms = self.clock.now_epoch_ms();
        let Some((mut record, record_layout)) = self.load_redelivery_record(id, &inflight) else {
            return;
        };

        record.attempts += 1;
        let Some(txn) = self.begin_redelivery_transaction(id, &inflight) else {
            return;
        };
        let Some((has_split_record, has_body_key)) =
            self.inspect_redelivery_layout(&txn, id, &inflight)
        else {
            return;
        };

        if self.should_move_to_dlq(record.attempts) {
            self.handle_redelivery_dlq(
                id,
                &inflight,
                &mut record,
                txn,
                DlqTransitionContext {
                    record_layout,
                    now_epoch_ms,
                    has_body_key,
                },
            );
            return;
        }

        if !self.persist_redelivery_attempt(
            id,
            &inflight,
            &record,
            txn,
            has_split_record,
            has_body_key,
        ) {
            return;
        }

        Self::increment_counter(obs::METRIC_QUEUE_REDELIVERIES);
        self.inflight.remove(&id);
        self.finish_redelivery_retry(id, &inflight, &record, record_layout);
    }

    /// # Panics
    ///
    /// Panics only if the timer heap is internally inconsistent after a successful `peek`.
    pub fn process_expired_timers(&mut self) {
        let now = self.clock.now_instant();

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

            self.handle_inflight_expired(expiry.id);
        }

        if self.timers.is_empty() {
            self.next_expiration_deadline = now + Duration::from_hours(1);
        }
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

    fn load_redelivery_record(
        &self,
        id: MessageId,
        inflight: &Inflight,
    ) -> Option<(QueueRecord, StoredRecordLayout)> {
        if let Some(cached) = self.records.get(&id) {
            return Some((
                cached.clone(),
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            ));
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

    fn inspect_redelivery_layout(
        &mut self,
        txn: &cntryl_midge::Transaction,
        id: MessageId,
        inflight: &Inflight,
    ) -> Option<(bool, bool)> {
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let has_split_record = match txn.get(&header_key) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(error) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    message_id = id.as_u64(),
                    error = ?error,
                    "Failed to inspect queue storage layout during redelivery"
                );
                self.schedule_inflight_retry(id, inflight);
                return None;
            }
        };

        let has_body_key = if has_split_record {
            match txn.get(&body_key) {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(error) => {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error = ?error,
                        "Failed to inspect queue body storage layout during redelivery"
                    );
                    false
                }
            }
        } else {
            false
        };

        Some((has_split_record, has_body_key))
    }

    fn handle_redelivery_dlq(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &mut QueueRecord,
        mut txn: cntryl_midge::Transaction,
        context: DlqTransitionContext,
    ) {
        let dead_lettered_at_ms = context.now_epoch_ms;
        let index_plan = self.plan_index_mutation_for_unavailable_message(id);
        Self::prepare_dlq_record(record, dead_lettered_at_ms);

        if !self.persist_dlq_record(
            id,
            inflight,
            record,
            context.record_layout,
            &mut txn,
            context.has_body_key,
        ) {
            return;
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
            return;
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
            return;
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
        self.cache_record(
            id,
            record.metadata_only_from(),
            StoredRecordLayout::SplitHeaderBody,
        );
        self.evict_cached_body(id);

        tracing::info!(
            queue = ?self.queue_key,
            route_family = self.queue_key.family.as_u64(),
            message_id = id.as_u64(),
            attempts = record.attempts,
            "Message moved to queue dead letter state"
        );
        Self::increment_counter(obs::METRIC_QUEUE_DLQ_TRANSITIONS);
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
        record_layout: StoredRecordLayout,
        txn: &mut cntryl_midge::Transaction,
        has_body_key: bool,
    ) -> bool {
        let header_key = self.cached_header_key(id);
        let write_result =
            if matches!(record_layout, StoredRecordLayout::SplitHeaderBody) && has_body_key {
                txn.put(header_key, Self::encode_record_header(record), None)
                    .map_err(|error| format!("Failed to write DLQ header: {error:?}"))
            } else {
                if record.body.is_none() && !self.load_body_into_record(id, record, inflight) {
                    return false;
                }
                self.write_record_as_split(txn, id, record, Some(record_layout))
            };

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

    fn load_body_into_record(
        &mut self,
        id: MessageId,
        record: &mut QueueRecord,
        inflight: &Inflight,
    ) -> bool {
        match self.load_body_from_store(id) {
            Ok(body) => {
                record.body = Some(body);
                true
            }
            Err(error) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    message_id = id.as_u64(),
                    error_reason = %error,
                    "Failed to load queue body for DLQ transition"
                );
                self.schedule_inflight_retry(id, inflight);
                false
            }
        }
    }

    fn persist_redelivery_attempt(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &QueueRecord,
        mut txn: cntryl_midge::Transaction,
        has_split_record: bool,
        has_body_key: bool,
    ) -> bool {
        let write_result = if has_split_record {
            self.persist_split_redelivery_attempt(id, record, &mut txn, has_body_key)
        } else {
            self.persist_legacy_redelivery_attempt(id, record, &mut txn)
        };

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
        has_body_key: bool,
    ) -> Result<(), String> {
        let header_key = self.cached_header_key(id);
        match txn.get(&header_key) {
            Ok(Some(bytes)) if !has_body_key && bytes.len() >= 16 => {
                match Self::decode_legacy_record(bytes) {
                    Ok(mut embedded_record) => {
                        embedded_record.attempts = record.attempts;
                        embedded_record.visible_at_ms = record.visible_at_ms;
                        let value = Self::encode_legacy_record(&embedded_record);
                        txn.put(header_key, value, None).map_err(|error| {
                            format!("persist embedded redelivery failed: {error:?}")
                        })
                    }
                    Err(error) => Err(format!(
                        "Failed to decode embedded queue message during redelivery: {error}"
                    )),
                }
            }
            Ok(Some(_)) => txn
                .put(header_key, Self::encode_record_header(record), None)
                .map_err(|error| format!("persist split redelivery failed: {error:?}")),
            Ok(None) => Err("Queue message disappeared during redelivery".to_string()),
            Err(error) => Err(format!(
                "Failed to read queue message during redelivery: {error:?}"
            )),
        }
    }

    fn persist_legacy_redelivery_attempt(
        &self,
        id: MessageId,
        record: &QueueRecord,
        txn: &mut cntryl_midge::Transaction,
    ) -> Result<(), String> {
        let legacy_key = self.cached_legacy_message_key(id);
        match txn.get(&legacy_key) {
            Ok(Some(bytes)) => match Self::decode_legacy_record(bytes) {
                Ok(mut legacy_record) => {
                    legacy_record.attempts = record.attempts;
                    legacy_record.visible_at_ms = record.visible_at_ms;
                    let value = Self::encode_legacy_record(&legacy_record);
                    txn.put(legacy_key, value, None)
                        .map_err(|error| format!("persist legacy redelivery failed: {error:?}"))
                }
                Err(error) => Err(format!(
                    "Failed to decode legacy queue message during redelivery: {error}"
                )),
            },
            Ok(None) => Err("Legacy queue message disappeared during redelivery".to_string()),
            Err(error) => Err(format!(
                "Failed to read legacy queue message during redelivery: {error:?}"
            )),
        }
    }

    fn finish_redelivery_retry(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &QueueRecord,
        record_layout: StoredRecordLayout,
    ) {
        self.cache_record(
            id,
            QueueRecord::metadata_only(record.attempts, record.visible_at_ms),
            record_layout,
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
