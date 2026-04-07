use super::*;

impl QueueActor {
    pub(super) fn shard_for_id(id: MessageId) -> usize {
        id.as_u64() as usize & (Self::READY_SHARDS - 1)
    }

    pub(super) fn range_len(range: ReadyRange) -> usize {
        (((range.end - range.next) / Self::READY_SHARDS as u64) + 1) as usize
    }

    pub(super) fn reset_live_ready_state(&mut self) {
        self.ready.clear();
        for shard in &mut self.ready_shards {
            shard.clear();
        }
        self.ready_count = 0;
        self.next_ready_shard = 0;
    }

    pub(super) fn reset_persisted_index_state(&mut self) {
        for shard in &mut self.persisted_ready_shards {
            shard.clear();
        }
        self.persisted_ready_count = 0;
        self.clear_persisted_delayed();
        self.clear_persisted_dlq();
    }

    pub(super) fn push_range_into(
        shards: &mut [VecDeque<ReadyRange>],
        shard: usize,
        range: ReadyRange,
    ) {
        let step = Self::READY_SHARDS as u64;

        if let Some(existing) = shards[shard].back_mut() {
            if range.next == existing.end.saturating_add(step) {
                existing.end = range.end;
                return;
            }
        }

        shards[shard].push_back(range);
    }

    pub(super) fn prepare_persisted_ready_append(
        tail: Option<ReadyRange>,
        id: MessageId,
    ) -> (usize, ReadyRange) {
        let shard = Self::shard_for_id(id);
        let id_u64 = id.as_u64();
        let range = match tail {
            Some(existing) if id_u64 == existing.end.saturating_add(Self::READY_SHARDS as u64) => {
                ReadyRange {
                    next: existing.next,
                    end: id_u64,
                }
            }
            _ => ReadyRange {
                next: id_u64,
                end: id_u64,
            },
        };

        (shard, range)
    }

    #[cfg(test)]
    pub(super) fn stage_persisted_ready_append(
        shards: &mut [VecDeque<ReadyRange>],
        id: MessageId,
    ) -> (usize, ReadyRange) {
        let shard = Self::shard_for_id(id);
        let (_, range) = Self::prepare_persisted_ready_append(shards[shard].back().copied(), id);

        if let Some(existing) = shards[shard].back_mut() {
            if range.next == existing.next {
                existing.end = range.end;
                return (shard, range);
            }
        }

        shards[shard].push_back(range);
        (shard, range)
    }

    pub(super) fn push_ready(&mut self, id: MessageId) {
        self.push_ready_entry(self.next_ready_seq, id);
        self.next_ready_seq = self.next_ready_seq.saturating_add(1);
    }

    pub(super) fn push_ready_entry(&mut self, ready_seq: u64, id: MessageId) {
        self.ready.push_back(ReadyEntry { ready_seq, id });
        self.ready_count = self.ready.len();
        if let Some(record) = self.records.get(&id) {
            let enqueue_ms = if record.first_enqueued_at_ms != 0 {
                record.first_enqueued_at_ms
            } else {
                self.clock.now_epoch_ms()
            };
            self.oldest_ready_enqueued_at_ms = Some(
                self.oldest_ready_enqueued_at_ms
                    .map(|current| current.min(enqueue_ms))
                    .unwrap_or(enqueue_ms),
            );
        }
    }

    pub(super) fn push_persisted_ready(&mut self, id: MessageId) {
        let shard = Self::shard_for_id(id);
        let range = ReadyRange {
            next: id.as_u64(),
            end: id.as_u64(),
        };
        Self::push_range_into(&mut self.persisted_ready_shards, shard, range);
        self.persisted_ready_count += 1;
    }

    pub(super) fn push_persisted_ready_range(&mut self, range: ReadyRange) {
        let shard = range.next as usize & (Self::READY_SHARDS - 1);
        Self::push_range_into(&mut self.persisted_ready_shards, shard, range);
        self.persisted_ready_count += Self::range_len(range);
    }

    pub(super) fn recompute_oldest_ready_enqueued_at_ms(&mut self) {
        self.oldest_ready_enqueued_at_ms = self.ready.front().and_then(|entry| {
            self.records.get(&entry.id).and_then(|record| {
                (record.first_enqueued_at_ms != 0).then_some(record.first_enqueued_at_ms)
            })
        });
    }

    pub(super) fn pop_ready_entry(&mut self) -> Option<ReadyEntry> {
        let entry = self.ready.pop_front()?;
        self.ready_count = self.ready.len();
        if self.ready.is_empty() {
            self.oldest_ready_enqueued_at_ms = None;
        } else {
            self.recompute_oldest_ready_enqueued_at_ms();
        }
        Some(entry)
    }

    pub(super) fn plan_ready_index_mutation(
        shards: &[VecDeque<ReadyRange>],
        id: MessageId,
    ) -> Option<(usize, PersistedReadyMutation)> {
        let shard = Self::shard_for_id(id);
        let id_u64 = id.as_u64();
        let step = Self::READY_SHARDS as u64;
        let ranges = &shards[shard];

        for range in ranges.iter().copied() {
            if id_u64 < range.next
                || id_u64 > range.end
                || !(id_u64 - range.next).is_multiple_of(step)
            {
                continue;
            }

            let mutation = if range.next == range.end {
                PersistedReadyMutation::Delete { removed: range }
            } else if id_u64 == range.next {
                let inserted = ReadyRange {
                    next: range.next.saturating_add(step),
                    end: range.end,
                };
                PersistedReadyMutation::Replace {
                    removed: range,
                    inserted,
                }
            } else if id_u64 == range.end {
                let inserted = ReadyRange {
                    next: range.next,
                    end: range.end.saturating_sub(step),
                };
                PersistedReadyMutation::Replace {
                    removed: range,
                    inserted,
                }
            } else {
                let left = ReadyRange {
                    next: range.next,
                    end: id_u64.saturating_sub(step),
                };
                let right = ReadyRange {
                    next: id_u64.saturating_add(step),
                    end: range.end,
                };
                PersistedReadyMutation::Split {
                    removed: range,
                    left,
                    right,
                }
            };

            return Some((shard, mutation));
        }

        None
    }

    pub(super) fn apply_ready_index_mutation(
        &mut self,
        shard: usize,
        mutation: PersistedReadyMutation,
    ) {
        Self::apply_ready_index_mutation_to_shards(
            &mut self.persisted_ready_shards,
            shard,
            mutation,
        );

        let removed_len = match mutation {
            PersistedReadyMutation::Delete { removed }
            | PersistedReadyMutation::Replace { removed, .. }
            | PersistedReadyMutation::Split { removed, .. } => Self::range_len(removed),
        };
        let inserted_len = match mutation {
            PersistedReadyMutation::Delete { .. } => 0,
            PersistedReadyMutation::Replace { inserted, .. } => Self::range_len(inserted),
            PersistedReadyMutation::Split { left, right, .. } => {
                Self::range_len(left) + Self::range_len(right)
            }
        };

        self.persisted_ready_count = self
            .persisted_ready_count
            .saturating_sub(removed_len)
            .saturating_add(inserted_len);
    }

    pub(super) fn apply_ready_index_mutation_to_shards(
        shards: &mut [VecDeque<ReadyRange>],
        shard: usize,
        mutation: PersistedReadyMutation,
    ) {
        let ranges = &mut shards[shard];
        let removed = match mutation {
            PersistedReadyMutation::Delete { removed }
            | PersistedReadyMutation::Replace { removed, .. }
            | PersistedReadyMutation::Split { removed, .. } => removed,
        };

        let Some(idx) = ranges.iter().position(|range| *range == removed) else {
            return;
        };

        match mutation {
            PersistedReadyMutation::Delete { .. } => {
                ranges.remove(idx);
            }
            PersistedReadyMutation::Replace { inserted, .. } => {
                ranges[idx] = inserted;
            }
            PersistedReadyMutation::Split { left, right, .. } => {
                ranges[idx] = left;
                ranges.insert(idx + 1, right);
            }
        }
    }

    pub(super) fn cache_record(
        &mut self,
        id: MessageId,
        record: QueueRecord,
        layout: StoredRecordLayout,
    ) {
        if self.records.insert(id, record).is_none() {
            self.record_cache_fifo.push_back(id);
        }
        self.record_layouts.insert(id, layout);

        self.compact_record_cache_fifo_if_needed();

        while self.records.len() > Self::RECORD_CACHE_LIMIT {
            let Some(evicted_id) = self.record_cache_fifo.pop_front() else {
                break;
            };
            self.records.remove(&evicted_id);
            self.record_layouts.remove(&evicted_id);
        }
    }

    pub(super) fn evict_cached_record(&mut self, id: MessageId) {
        self.records.remove(&id);
        self.record_layouts.remove(&id);
        self.compact_record_cache_fifo_if_needed();
    }

    pub(super) fn compact_record_cache_fifo_if_needed(&mut self) {
        let max_fifo_len = Self::RECORD_CACHE_LIMIT * Self::RECORD_CACHE_FIFO_SLACK_MULTIPLIER
            + self.records.len();
        if self.record_cache_fifo.len() <= max_fifo_len {
            return;
        }

        self.record_cache_fifo
            .retain(|id| self.records.contains_key(id));
    }

    pub(super) fn cache_body(&mut self, id: MessageId, body: Bytes) {
        if self.body_cache.contains_key(&id) {
            return;
        }

        let body_len = body.len();
        if body_len > Self::BODY_CACHE_LIMIT_BYTES {
            return;
        }

        self.body_cache.insert(id, body);
        self.body_cache_fifo.push_back(id);
        self.body_cache_bytes = self.body_cache_bytes.saturating_add(body_len);
        self.compact_body_cache_fifo_if_needed();

        while self.body_cache.len() > Self::BODY_CACHE_LIMIT
            || self.body_cache_bytes > Self::BODY_CACHE_LIMIT_BYTES
        {
            let Some(evicted_id) = self.body_cache_fifo.pop_front() else {
                break;
            };
            self.evict_cached_body(evicted_id);
        }
    }

    pub(super) fn evict_cached_body(&mut self, id: MessageId) {
        if let Some(body) = self.body_cache.remove(&id) {
            self.body_cache_bytes = self.body_cache_bytes.saturating_sub(body.len());
        }
        self.compact_body_cache_fifo_if_needed();
    }

    pub(super) fn compact_body_cache_fifo_if_needed(&mut self) {
        let max_fifo_len =
            Self::BODY_CACHE_LIMIT * Self::BODY_CACHE_FIFO_SLACK_MULTIPLIER + self.body_cache.len();
        if self.body_cache_fifo.len() <= max_fifo_len {
            return;
        }

        self.body_cache_fifo
            .retain(|id| self.body_cache.contains_key(id));
    }

    pub(super) fn pop_ready(&mut self) -> Option<MessageId> {
        self.pop_ready_entry().map(|entry| entry.id)
    }
}
