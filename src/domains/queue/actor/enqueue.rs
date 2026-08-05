use super::{
    obs, Bytes, DelayedMessage, Duration, Instant, MessageId, QueueActor, QueueRecord,
    QueueResponse, ReadyRange, Reverse,
};

struct SendTiming {
    delay_ms: u64,
    visible_at_ms: u64,
    visible_at: Instant,
}

struct BatchSendPlan {
    ids: Vec<MessageId>,
    post_commit: Vec<(MessageId, QueueRecord, Bytes, Instant)>,
    staged_ready_ids: Vec<MessageId>,
    staged_delayed: Vec<(MessageId, u64)>,
    staged_ready_add: usize,
    staged_next_delayed_visibility: Option<u64>,
    next_id: u64,
    reserved_limit: Option<u64>,
}

impl QueueActor {
    /// Handle send operation
    pub fn handle_send(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse {
        if !self.has_message_id_capacity(1) {
            return QueueResponse::Error {
                message: "queue message id space exhausted".to_string(),
            };
        }

        // Track empty state before send for notification
        let was_empty = self.ready_count == 0;

        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let timing = match Self::send_timing(delay_seconds, now_instant, now_epoch_ms) {
            Ok(timing) => timing,
            Err(response) => return response,
        };
        let mut txn = match self.begin_enqueue_tx() {
            Ok(txn) => txn,
            Err(response) => return response,
        };

        // Allocate message ID
        let id = MessageId::new(self.next_id);
        let cached_body = body;
        let reserved_limit = self.reserved_id_limit_for(1);
        let staged_next_id = reserved_limit.unwrap_or(self.next_id_limit);
        let is_ready = timing.visible_at <= now_instant;
        let record = QueueRecord::enqueued(id, timing.visible_at_ms, is_ready, now_epoch_ms);
        let staged_ready_count = self
            .persisted_ready_count
            .saturating_add(usize::from(is_ready));
        let staged_delayed_count = self
            .persisted_delayed
            .len()
            .saturating_add(usize::from(!is_ready));
        let staged_next_delayed_visibility = if is_ready {
            self.min_persisted_delayed_visibility_ms()
        } else {
            Some(
                self.min_persisted_delayed_visibility_ms()
                    .map_or(timing.visible_at_ms, |current| {
                        current.min(timing.visible_at_ms)
                    }),
            )
        };
        let ready_index_write = if is_ready {
            let tail = self.persisted_ready_shards[Self::shard_for_id(id)]
                .back()
                .copied();
            Some(Self::prepare_persisted_ready_append(tail, id))
        } else {
            None
        };

        if let Err(response) = self.write_send_record(
            &mut txn,
            id,
            &cached_body,
            &record,
            ready_index_write,
            timing.visible_at_ms,
        ) {
            return response;
        }
        if let Err(response) = self.write_enqueue_meta(
            &mut txn,
            reserved_limit,
            staged_next_id,
            staged_ready_count,
            staged_delayed_count,
            staged_next_delayed_visibility,
        ) {
            return response;
        }

        // Commit with buffered mode for high throughput
        // The store will sync periodically, maintaining durability without per-operation cost
        let commit_start = Instant::now();
        if let Err(e) = txn.commit(self.commit_write_options) {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {e:?}"),
            };
        }
        Self::observe_elapsed_us(obs::METRIC_QUEUE_ENQUEUE_COMMIT_LATENCY, commit_start);

        self.apply_send_post_commit(
            id,
            record,
            cached_body,
            timing.visible_at,
            reserved_limit,
            is_ready,
        );

        // Mark queue-local waiters for wakeup if the queue transitioned from empty to non-empty
        // (only for immediately visible messages, not delayed ones).
        if was_empty && is_ready && self.ready_count > 0 {
            self.needs_wake_waiters = true;
        }

        self.enqueue_success_window.record(now_epoch_ms, 1);

        QueueResponse::Sent { id }
    }

    /// Send multiple messages in one transaction (batch).
    /// Same semantics as `N×handle_send`; use for throughput when the caller has many messages.
    pub fn handle_send_batch(&mut self, items: &[(Bytes, Option<u64>)]) -> QueueResponse {
        if items.is_empty() {
            return QueueResponse::SentBatch { ids: vec![] };
        }
        if !self.has_message_id_capacity(Self::usize_to_u64(items.len())) {
            return QueueResponse::Error {
                message: "queue message id space exhausted".to_string(),
            };
        }

        let was_empty = self.ready_count == 0;
        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let mut txn = match self.begin_enqueue_tx() {
            Ok(txn) => txn,
            Err(response) => return response,
        };

        let mut staged_ready_tails: Vec<Option<ReadyRange>> = self
            .persisted_ready_shards
            .iter()
            .map(|ranges| ranges.back().copied())
            .collect();
        let plan = match self.stage_batch_send(
            &mut txn,
            items,
            now_instant,
            now_epoch_ms,
            &mut staged_ready_tails,
        ) {
            Ok(plan) => plan,
            Err(response) => return response,
        };
        let staged_next_id = plan.reserved_limit.unwrap_or(self.next_id_limit);
        if let Err(response) = self.write_enqueue_meta(
            &mut txn,
            plan.reserved_limit,
            staged_next_id,
            self.persisted_ready_count
                .saturating_add(plan.staged_ready_add),
            self.persisted_delayed
                .len()
                .saturating_add(plan.staged_delayed.len()),
            plan.staged_next_delayed_visibility,
        ) {
            return response;
        }

        let commit_start = Instant::now();
        if let Err(e) = txn.commit(self.commit_write_options) {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {e:?}"),
            };
        }
        Self::observe_elapsed_us(obs::METRIC_QUEUE_ENQUEUE_COMMIT_LATENCY, commit_start);

        self.next_id = plan.next_id;
        if let Some(limit) = plan.reserved_limit {
            self.next_id_limit = limit;
        }
        for (id, record, cached_body, visible_at) in plan.post_commit {
            let visible_at_ms = record.visible_at_ms;
            self.cache_record(id, record);
            self.cache_body(id, cached_body);
            if visible_at <= now_instant {
                self.push_ready(id);
            } else {
                self.delayed.push(Reverse(DelayedMessage {
                    id,
                    enqueue_seq: id.as_u64(),
                    visible_at,
                    visible_at_ms,
                }));
                if visible_at < self.next_delayed_deadline {
                    self.next_delayed_deadline = visible_at;
                }
            }
        }
        for id in plan.staged_ready_ids {
            self.push_persisted_ready(id);
        }
        for (id, visible_at_ms) in plan.staged_delayed {
            self.insert_persisted_delayed(id, visible_at_ms);
        }
        if !self.index_meta_written {
            self.index_meta_written = true;
        }
        if was_empty && plan.staged_ready_add > 0 && self.ready_count > 0 {
            self.needs_wake_waiters = true;
        }

        self.enqueue_success_window
            .record(now_epoch_ms, Self::usize_to_u64(plan.ids.len()));

        QueueResponse::SentBatch { ids: plan.ids }
    }

    fn begin_enqueue_tx(&self) -> Result<cntryl_midge::Transaction, QueueResponse> {
        self.store
            .begin_tx(
                self.queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| QueueResponse::Error {
                message: format!("Failed to begin transaction: {error:?}"),
            })
    }

    fn send_timing(
        delay_seconds: Option<u64>,
        now_instant: Instant,
        now_epoch_ms: u64,
    ) -> Result<SendTiming, QueueResponse> {
        let Some(delay_ms) = delay_seconds.unwrap_or(0).checked_mul(1_000) else {
            return Err(QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            });
        };
        let Some(visible_at_ms) = now_epoch_ms.checked_add(delay_ms) else {
            return Err(QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            });
        };
        let Some(visible_at) = now_instant.checked_add(Duration::from_millis(delay_ms)) else {
            return Err(QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            });
        };

        Ok(SendTiming {
            delay_ms,
            visible_at_ms,
            visible_at,
        })
    }

    fn write_send_record(
        &self,
        txn: &mut cntryl_midge::Transaction,
        id: MessageId,
        body: &Bytes,
        record: &QueueRecord,
        ready_index_write: Option<(usize, ReadyRange)>,
        visible_at_ms: u64,
    ) -> Result<(), QueueResponse> {
        txn.put(
            self.cached_header_key(id),
            Self::encode_record_header(record),
            None,
        )
        .map_err(|error| QueueResponse::Error {
            message: format!("Failed to add message header to transaction: {error:?}"),
        })?;
        txn.put(self.cached_body_key(id), body.to_vec(), None)
            .map_err(|error| QueueResponse::Error {
                message: format!("Failed to add message body to transaction: {error:?}"),
            })?;

        if let Some((shard, range)) = ready_index_write {
            txn.put(
                self.ready_range_key(shard, range.next),
                Self::encode_ready_range_value(range),
                None,
            )
            .map_err(|error| QueueResponse::Error {
                message: format!("Failed to update queue ready index: {error:?}"),
            })?;
        } else {
            txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
                .map_err(|error| QueueResponse::Error {
                    message: format!("Failed to update queue delayed index: {error:?}"),
                })?;
        }

        Ok(())
    }

    fn write_enqueue_meta(
        &self,
        txn: &mut cntryl_midge::Transaction,
        reserved_limit: Option<u64>,
        staged_next_id: u64,
        staged_ready_count: usize,
        staged_delayed_count: usize,
        staged_next_delayed_visibility: Option<u64>,
    ) -> Result<(), QueueResponse> {
        if let Some(limit) = reserved_limit {
            txn.put(self.meta_key.clone(), limit.to_le_bytes().to_vec(), None)
                .map_err(|error| QueueResponse::Error {
                    message: format!("Failed to update queue meta: {error:?}"),
                })?;
        }

        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                staged_next_id,
                Self::usize_to_u64(staged_ready_count),
                Self::usize_to_u64(staged_delayed_count),
                staged_next_delayed_visibility,
            ),
            None,
        )
        .map_err(|error| QueueResponse::Error {
            message: format!("Failed to update queue index meta: {error:?}"),
        })?;

        Ok(())
    }

    fn apply_send_post_commit(
        &mut self,
        id: MessageId,
        record: QueueRecord,
        cached_body: Bytes,
        visible_at: Instant,
        reserved_limit: Option<u64>,
        is_ready: bool,
    ) {
        self.next_id = self.next_id.saturating_add(1);
        if let Some(limit) = reserved_limit {
            self.next_id_limit = limit;
        }
        self.cache_record(id, record);
        self.cache_body(id, cached_body);

        if is_ready {
            self.push_ready(id);
            self.push_persisted_ready(id);
        } else {
            let visible_at_ms = self
                .records
                .get(&id)
                .map_or(0, |stored_record| stored_record.visible_at_ms);
            self.delayed.push(Reverse(DelayedMessage {
                id,
                enqueue_seq: id.as_u64(),
                visible_at,
                visible_at_ms,
            }));
            if visible_at < self.next_delayed_deadline {
                self.next_delayed_deadline = visible_at;
            }
            self.insert_persisted_delayed(id, visible_at_ms);
        }
        if !self.index_meta_written {
            self.index_meta_written = true;
        }
    }

    fn stage_batch_send(
        &self,
        txn: &mut cntryl_midge::Transaction,
        items: &[(Bytes, Option<u64>)],
        now_instant: Instant,
        now_epoch_ms: u64,
        staged_ready_tails: &mut [Option<ReadyRange>],
    ) -> Result<BatchSendPlan, QueueResponse> {
        let mut ids = Vec::with_capacity(items.len());
        let mut post_commit = Vec::with_capacity(items.len());
        let mut staged_ready_ids = Vec::with_capacity(items.len());
        let mut staged_delayed = Vec::with_capacity(items.len());
        let mut staged_ready_add = 0usize;
        let mut staged_next_delayed_visibility = self.min_persisted_delayed_visibility_ms();
        let mut next_id = self.next_id;
        let reserved_limit = self.reserved_id_limit_for(Self::usize_to_u64(items.len()));

        for (body, delay_seconds) in items {
            let timing = Self::send_timing(*delay_seconds, now_instant, now_epoch_ms)?;
            let id = MessageId::new(next_id);
            let record =
                QueueRecord::enqueued(id, timing.visible_at_ms, timing.delay_ms == 0, now_epoch_ms);
            let ready_index_write = if timing.delay_ms == 0 {
                let (shard, range) = Self::prepare_persisted_ready_append(
                    staged_ready_tails[Self::shard_for_id(id)],
                    id,
                );
                staged_ready_tails[shard] = Some(range);
                staged_ready_ids.push(id);
                staged_ready_add += 1;
                Some((shard, range))
            } else {
                staged_delayed.push((id, timing.visible_at_ms));
                staged_next_delayed_visibility = Some(
                    staged_next_delayed_visibility.map_or(timing.visible_at_ms, |current| {
                        current.min(timing.visible_at_ms)
                    }),
                );
                None
            };

            self.write_send_record(
                txn,
                id,
                body,
                &record,
                ready_index_write,
                timing.visible_at_ms,
            )?;
            ids.push(id);
            post_commit.push((id, record, body.clone(), timing.visible_at));
            next_id = next_id.saturating_add(1);
        }

        Ok(BatchSendPlan {
            ids,
            post_commit,
            staged_ready_ids,
            staged_delayed,
            staged_ready_add,
            staged_next_delayed_visibility,
            next_id,
            reserved_limit,
        })
    }
}
