//! Dead-letter administration and delayed-message promotion.

use super::{
    DelayedMessage, MessageId, QueueActor, QueueState, Reverse, QUEUE_IDLE_HORIZON,
    QUEUE_STORAGE_RETRY_BACKOFF,
};

impl QueueActor {
    /// # Errors
    ///
    /// Returns an error when the dead-letter replay transaction cannot be persisted.
    pub fn replay_dead_letter(&mut self, id: MessageId) -> Result<bool, String> {
        self.process_due_work();

        let mut record = match self.load_full_record_for_admin_mutation(id) {
            Ok(value) => value,
            Err(error) if error == format!("Message {id} disappeared from storage") => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };

        if !matches!(record.state, QueueState::Dlq) {
            return Ok(false);
        }

        let dead_lettered_at_ms = record
            .dead_lettered_at_ms
            .or_else(|| self.persisted_dlq.get(&id).copied())
            .unwrap_or(record.first_enqueued_at_ms);
        let ready_seq = self.next_ready_seq;
        let (ready_shard, ready_range) = Self::prepare_persisted_ready_append(
            self.persisted_ready_shards[Self::shard_for_id(id)]
                .back()
                .copied(),
            id,
        );

        record.state = QueueState::Ready;
        record.ready_seq = Some(ready_seq);
        record.attempts = 0;
        record.visible_at_ms = 0;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        record.dead_lettered_at_ms = None;
        record.dlq_reason = None;

        let cf_id = self.queue_key.family.id();
        let mut txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("Failed to begin replay tx for message {id}: {e:?}"))?;

        self.write_record_as_split(&mut txn, id, &record)?;
        txn.delete(self.dlq_index_key(dead_lettered_at_ms, id))
            .map_err(|e| format!("Failed to delete queue DLQ index for message {id}: {e:?}"))?;
        txn.put(
            self.ready_range_key(ready_shard, ready_range.next),
            Self::encode_ready_range_value(ready_range),
            None,
        )
        .map_err(|e| format!("Failed to write queue ready index for message {id}: {e:?}"))?;
        txn.put(
            self.recovery_store.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                Self::usize_to_u64(self.persisted_ready_count.saturating_add(1)),
                Self::usize_to_u64(self.persisted_delayed.len()),
                self.min_persisted_delayed_visibility_ms(),
            ),
            None,
        )
        .map_err(|e| format!("Failed to update queue index meta for message {id}: {e:?}"))?;
        txn.commit(self.commit_write_options)
            .map_err(|e| format!("Failed to commit replay tx for message {id}: {e:?}"))?;

        self.remove_persisted_dlq(id);
        self.cache_record_state(id, &record);
        self.push_ready_entry(ready_seq, id);
        self.next_ready_seq = self.next_ready_seq.saturating_add(1);
        self.push_persisted_ready(id);

        Ok(true)
    }

    /// # Errors
    ///
    /// Returns an error when the dead-letter purge transaction cannot be persisted.
    pub fn purge_dead_letter(&mut self, id: MessageId) -> Result<bool, String> {
        self.process_due_work();

        let record = if let Some(record) = self.records.get(&id).cloned() {
            record
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok(record) => record,
                Err(error) if error == format!("Message {id} disappeared from storage") => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        };

        if !matches!(record.state, QueueState::Dlq) {
            return Ok(false);
        }

        let dead_lettered_at_ms = record
            .dead_lettered_at_ms
            .or_else(|| self.persisted_dlq.get(&id).copied())
            .unwrap_or(record.first_enqueued_at_ms);
        let cf_id = self.queue_key.family.id();
        let mut txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("Failed to begin purge tx for message {id}: {e:?}"))?;

        Self::delete_record(
            &mut txn,
            self.cached_header_key(id),
            self.cached_body_key(id),
        )
        .map_err(|e| format!("Failed to delete message {id} in purge tx: {e:?}"))?;
        txn.delete(self.dlq_index_key(dead_lettered_at_ms, id))
            .map_err(|e| format!("Failed to delete queue DLQ index for message {id}: {e:?}"))?;
        txn.put(
            self.recovery_store.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                Self::usize_to_u64(self.persisted_ready_count),
                Self::usize_to_u64(self.persisted_delayed.len()),
                self.min_persisted_delayed_visibility_ms(),
            ),
            None,
        )
        .map_err(|e| format!("Failed to update queue index meta for message {id}: {e:?}"))?;
        txn.commit(self.commit_write_options)
            .map_err(|e| format!("Failed to commit purge tx for message {id}: {e:?}"))?;

        self.remove_persisted_dlq(id);
        self.evict_cached_record(id);
        self.evict_cached_body(id);

        Ok(true)
    }

    fn promote_delayed_message(&mut self, delayed: DelayedMessage) -> bool {
        let id = delayed.id;
        let visible_at_ms = self
            .persisted_delayed
            .get(&id)
            .copied()
            .unwrap_or(delayed.visible_at_ms);
        let ready_seq = self.next_ready_seq;
        let (ready_shard, ready_range) = Self::prepare_persisted_ready_append(
            self.persisted_ready_shards[Self::shard_for_id(id)]
                .back()
                .copied(),
            id,
        );
        let staged_delayed_count = self
            .persisted_delayed
            .len()
            .saturating_sub(usize::from(self.persisted_delayed.contains_key(&id)));
        let staged_next_delayed_visibility = if self.persisted_delayed.contains_key(&id) {
            self.min_persisted_delayed_visibility_ms_excluding(id)
        } else {
            self.min_persisted_delayed_visibility_ms()
        };
        let cf_id = self.queue_key.family.id();
        let mut txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(txn) => txn,
            Err(error) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    message_id = id.as_u64(),
                    error = ?error,
                    "Failed to begin queue delayed promotion transaction"
                );
                return false;
            }
        };

        if let Err(error) = txn.delete(self.delayed_index_key(visible_at_ms, id)) {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to delete queue delayed index during promotion"
            );
            return false;
        }

        if let Err(error) = txn.put(
            self.ready_range_key(ready_shard, ready_range.next),
            Self::encode_ready_range_value(ready_range),
            None,
        ) {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to write queue ready index during delayed promotion"
            );
            return false;
        }

        if let Err(error) = txn.put(
            self.recovery_store.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                Self::usize_to_u64(self.persisted_ready_count.saturating_add(1)),
                Self::usize_to_u64(staged_delayed_count),
                staged_next_delayed_visibility,
            ),
            None,
        ) {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to update queue index meta during delayed promotion"
            );
            return false;
        }

        if let Err(error) = txn.commit(self.commit_write_options) {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to commit queue delayed promotion"
            );
            return false;
        }

        self.remove_persisted_delayed(id);
        self.push_persisted_ready(id);
        self.push_ready_entry(ready_seq, id);
        self.next_ready_seq = self.next_ready_seq.saturating_add(1);
        self.index_meta_written = true;
        true
    }

    /// Process delayed messages that are now visible and update the next delayed deadline.
    ///
    /// # Panics
    ///
    /// Panics only if the delayed heap is internally inconsistent after a successful `peek`.
    pub fn process_delayed_messages(&mut self) -> bool {
        let now = self.clock.now_instant();
        let was_empty = self.ready_count == 0;
        let mut mutated = false;
        let mut processed = 0;

        while processed < super::MAX_DUE_ITEMS_PER_PASS {
            let Some(Reverse(delayed)) = self.delayed.peek() else {
                break;
            };
            if delayed.visible_at > now {
                // Found next delayed message deadline, cache it
                self.next_delayed_deadline = delayed.visible_at;
                break;
            }

            // Pop now-visible message
            let delayed = self.delayed.pop().unwrap().0;
            processed += 1;

            if !self.promote_delayed_message(delayed) {
                self.delayed.push(Reverse(delayed));
                self.next_delayed_deadline = now + QUEUE_STORAGE_RETRY_BACKOFF;
                break;
            }
            mutated = true;
        }

        // If no more delayed messages, set deadline to far future
        if self.delayed.is_empty() {
            self.next_delayed_deadline = now + QUEUE_IDLE_HORIZON;
        }

        if was_empty && self.ready_count > 0 {
            self.needs_wake_waiters = true;
        }
        mutated
    }

    /// Process only the timer and delayed-message work that is actually due.
    pub fn process_due_work(&mut self) -> bool {
        let now = self.clock.now_instant();
        let mut mutated = false;

        if now >= self.next_expiration_deadline {
            mutated |= self.process_expired_timers();
        }

        if now >= self.next_delayed_deadline {
            mutated |= self.process_delayed_messages();
        }

        mutated
    }
}
