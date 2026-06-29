use super::*;

impl QueueActor {
    /// Handle send operation
    pub fn handle_send(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse {
        // Track empty state before send for notification
        let was_empty = self.ready_count == 0;

        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let Some(delay_ms) = delay_seconds.unwrap_or(0).checked_mul(1_000) else {
            return QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            };
        };

        // Start transaction
        let cf_id = self.queue_key.family.id();
        let mut txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(t) => t,
            Err(e) => {
                return QueueResponse::Error {
                    message: format!("Failed to begin transaction: {e:?}"),
                };
            }
        };

        // Allocate message ID
        let id = MessageId::new(self.next_id);
        let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
        let Some(visible_at) = now_instant.checked_add(Duration::from_millis(delay_ms)) else {
            return QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            };
        };

        let record = QueueRecord::metadata_only(0, visible_at_ms);
        let cached_body = body.clone();
        let reserved_limit = self.reserved_id_limit_for(1);
        let staged_next_id = reserved_limit.unwrap_or(self.next_id_limit);
        let staged_ready_count =
            self.persisted_ready_count + usize::from(visible_at <= now_instant);
        let staged_delayed_count =
            self.persisted_delayed.len() + usize::from(visible_at > now_instant);
        let staged_next_delayed_visibility = if visible_at > now_instant {
            Some(
                self.min_persisted_delayed_visibility_ms()
                    .map_or(visible_at_ms, |current| current.min(visible_at_ms)),
            )
        } else {
            self.min_persisted_delayed_visibility_ms()
        };
        let ready_index_write = if visible_at <= now_instant {
            let tail = self.persisted_ready_shards[Self::shard_for_id(id)]
                .back()
                .copied();
            Some(Self::prepare_persisted_ready_append(tail, id))
        } else {
            None
        };

        // Write message header + body to one durable transaction.
        let header_key = self.cached_header_key(id);
        let header_value = Self::encode_legacy_record(&QueueRecord::loaded(
            body.clone(),
            record.attempts,
            record.visible_at_ms,
        ));
        if let Err(e) = txn.put(header_key, header_value, None) {
            return QueueResponse::Error {
                message: format!("Failed to add message header to transaction: {e:?}"),
            };
        }

        if let Some((shard, range)) = ready_index_write {
            if let Err(e) = txn.put(
                self.ready_range_key(shard, range.next),
                Self::encode_ready_range_value(range),
                None,
            ) {
                return QueueResponse::Error {
                    message: format!("Failed to update queue ready index: {e:?}"),
                };
            }
        } else if let Err(e) = txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
        {
            return QueueResponse::Error {
                message: format!("Failed to update queue delayed index: {e:?}"),
            };
        }

        if let Some(limit) = reserved_limit {
            if let Err(e) = txn.put(self.meta_key.clone(), limit.to_le_bytes().to_vec(), None) {
                return QueueResponse::Error {
                    message: format!("Failed to update queue meta: {e:?}"),
                };
            }
        }

        if let Err(e) = txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                staged_next_id,
                staged_ready_count as u64,
                staged_delayed_count as u64,
                staged_next_delayed_visibility,
            ),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue index meta: {e:?}"),
            };
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

        // Commit succeeded; advance in-memory ID state.
        self.next_id = self.next_id.saturating_add(1);
        if let Some(limit) = reserved_limit {
            self.next_id_limit = limit;
        }

        // Cache record in memory for fast reserve path
        self.cache_record(id, record, StoredRecordLayout::EmbeddedHeader);
        self.cache_body(id, cached_body);

        // Update in-memory queues
        if visible_at <= now_instant {
            self.push_ready(id);
            self.push_persisted_ready(id);
        } else {
            self.delayed.push(Reverse(DelayedMessage {
                id,
                enqueue_seq: id.as_u64(),
                visible_at,
                visible_at_ms,
            }));
            self.insert_persisted_delayed(id, visible_at_ms);
        }
        if !self.index_meta_written {
            self.index_meta_written = true;
        }

        // Mark queue-local waiters for wakeup if the queue transitioned from empty to non-empty
        // (only for immediately visible messages, not delayed ones).
        if was_empty && visible_at <= now_instant && self.ready_count > 0 {
            self.needs_wake_waiters = true;
        }

        self.enqueue_success_window.record(now_epoch_ms, 1);

        QueueResponse::Sent { id }
    }

    /// Send multiple messages in one transaction (batch).
    /// Same semantics as N×handle_send; use for throughput when the caller has many messages.
    pub fn handle_send_batch(&mut self, items: &[(Bytes, Option<u64>)]) -> QueueResponse {
        if items.is_empty() {
            return QueueResponse::SentBatch { ids: vec![] };
        }

        let was_empty = self.ready_count == 0;
        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let cf_id = self.queue_key.family.id();

        let mut txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(t) => t,
            Err(e) => {
                return QueueResponse::Error {
                    message: format!("Failed to begin transaction: {e:?}"),
                };
            }
        };

        let mut ids = Vec::with_capacity(items.len());
        let mut post_commit: Vec<(MessageId, QueueRecord, Bytes, std::time::Instant)> =
            Vec::with_capacity(items.len());
        let mut staged_ready_tails: Vec<Option<ReadyRange>> = self
            .persisted_ready_shards
            .iter()
            .map(|ranges| ranges.back().copied())
            .collect();
        let mut staged_ready_ids = Vec::new();
        let mut staged_delayed = Vec::new();
        let mut staged_ready_add = 0usize;
        let mut staged_delayed_add = 0usize;
        let mut staged_next_delayed_visibility = self.min_persisted_delayed_visibility_ms();
        let mut next_id = self.next_id;
        let reserved_limit = self.reserved_id_limit_for(items.len() as u64);

        for (body, delay_seconds) in items {
            let Some(delay_ms) = delay_seconds.unwrap_or(0).checked_mul(1_000) else {
                return QueueResponse::BadRequest {
                    reason: "delay_seconds is too large".to_string(),
                };
            };
            let id = MessageId::new(next_id);
            let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
            let Some(visible_at) = now_instant.checked_add(Duration::from_millis(delay_ms)) else {
                return QueueResponse::BadRequest {
                    reason: "delay_seconds is too large".to_string(),
                };
            };

            let record = QueueRecord::metadata_only(0, visible_at_ms);

            let header_key = self.cached_header_key(id);
            let header_value = Self::encode_legacy_record(&QueueRecord::loaded(
                body.clone(),
                record.attempts,
                record.visible_at_ms,
            ));
            if let Err(e) = txn.put(header_key, header_value, None) {
                return QueueResponse::Error {
                    message: format!("Failed to add message header to transaction: {e:?}"),
                };
            }

            if visible_at <= now_instant {
                let (shard, range) = Self::prepare_persisted_ready_append(
                    staged_ready_tails[Self::shard_for_id(id)],
                    id,
                );
                staged_ready_tails[shard] = Some(range);
                if let Err(e) = txn.put(
                    self.ready_range_key(shard, range.next),
                    Self::encode_ready_range_value(range),
                    None,
                ) {
                    return QueueResponse::Error {
                        message: format!("Failed to update queue ready index: {e:?}"),
                    };
                }
                staged_ready_ids.push(id);
                staged_ready_add += 1;
            } else if let Err(e) =
                txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
            {
                return QueueResponse::Error {
                    message: format!("Failed to update queue delayed index: {e:?}"),
                };
            }

            ids.push(id);
            post_commit.push((id, record, body.clone(), visible_at));
            if visible_at > now_instant {
                staged_delayed.push((id, visible_at_ms));
                staged_delayed_add += 1;
                staged_next_delayed_visibility = Some(
                    staged_next_delayed_visibility
                        .map_or(visible_at_ms, |current| current.min(visible_at_ms)),
                );
            }
            next_id += 1;
        }

        if let Some(limit) = reserved_limit {
            if let Err(e) = txn.put(self.meta_key.clone(), limit.to_le_bytes().to_vec(), None) {
                return QueueResponse::Error {
                    message: format!("Failed to update queue meta: {e:?}"),
                };
            }
        }

        let staged_next_id = reserved_limit.unwrap_or(self.next_id_limit);
        if let Err(e) = txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                staged_next_id,
                (self.persisted_ready_count + staged_ready_add) as u64,
                (self.persisted_delayed.len() + staged_delayed_add) as u64,
                staged_next_delayed_visibility,
            ),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue index meta: {e:?}"),
            };
        }

        let commit_start = Instant::now();
        if let Err(e) = txn.commit(self.commit_write_options) {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {e:?}"),
            };
        }
        Self::observe_elapsed_us(obs::METRIC_QUEUE_ENQUEUE_COMMIT_LATENCY, commit_start);

        self.next_id = next_id;
        if let Some(limit) = reserved_limit {
            self.next_id_limit = limit;
        }
        for (id, record, cached_body, visible_at) in post_commit {
            let visible_at_ms = record.visible_at_ms;
            self.cache_record(id, record, StoredRecordLayout::EmbeddedHeader);
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
            }
        }
        for id in staged_ready_ids {
            self.push_persisted_ready(id);
        }
        for (id, visible_at_ms) in staged_delayed {
            self.insert_persisted_delayed(id, visible_at_ms);
        }
        if !self.index_meta_written {
            self.index_meta_written = true;
        }
        if was_empty && staged_ready_add > 0 && self.ready_count > 0 {
            self.needs_wake_waiters = true;
        }

        self.enqueue_success_window
            .record(now_epoch_ms, ids.len() as u64);

        QueueResponse::SentBatch { ids }
    }
}
