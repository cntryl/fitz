use super::super::*;

impl QueueActor {
    pub(in crate::domains::queue::actor) fn delete_record_for_layout(
        txn: &mut cntryl_midge::Transaction,
        layout: StoredRecordLayout,
        header_key: Vec<u8>,
        body_key: Vec<u8>,
        legacy_key: Vec<u8>,
    ) -> cntryl_midge::MidgeResult<()> {
        match layout {
            StoredRecordLayout::EmbeddedHeader => txn.delete(header_key),
            StoredRecordLayout::SplitHeaderBody => {
                txn.delete(header_key).and_then(|_| txn.delete(body_key))
            }
            StoredRecordLayout::LegacyKey => txn.delete(legacy_key),
        }
    }

    pub(in crate::domains::queue::actor) fn staged_ready_count_after_mutation(
        &self,
        mutation: Option<(usize, PersistedReadyMutation)>,
    ) -> usize {
        let mut count = self.persisted_ready_count;
        if let Some((_shard, mutation)) = mutation {
            let removed_len = match mutation {
                PersistedReadyMutation::Delete { removed }
                | PersistedReadyMutation::Replace { removed, .. }
                | PersistedReadyMutation::Split { removed, .. } => Self::range_len(removed),
            };
            count = count.saturating_sub(removed_len);
            count += match mutation {
                PersistedReadyMutation::Delete { .. } => 0,
                PersistedReadyMutation::Replace { inserted, .. } => Self::range_len(inserted),
                PersistedReadyMutation::Split { left, right, .. } => {
                    Self::range_len(left) + Self::range_len(right)
                }
            };
        }
        count
    }

    pub(in crate::domains::queue::actor) fn plan_index_mutation_for_unavailable_message(
        &self,
        id: MessageId,
    ) -> PersistedIndexMutationPlan {
        let ready_mutation = Self::plan_ready_index_mutation(&self.persisted_ready_shards, id);
        let delayed_index_delete = self.persisted_delayed.get(&id).copied();

        PersistedIndexMutationPlan {
            ready_mutation,
            delayed_index_delete,
            staged_ready_count: self.staged_ready_count_after_mutation(ready_mutation),
            staged_delayed_count: self
                .persisted_delayed
                .len()
                .saturating_sub(usize::from(delayed_index_delete.is_some())),
            staged_next_delayed_visibility: if delayed_index_delete.is_some() {
                self.min_persisted_delayed_visibility_ms_excluding(id)
            } else {
                self.min_persisted_delayed_visibility_ms()
            },
        }
    }

    pub(in crate::domains::queue::actor) fn write_persisted_ready_mutation(
        &self,
        txn: &mut cntryl_midge::Transaction,
        shard: usize,
        mutation: PersistedReadyMutation,
    ) -> Result<(), String> {
        match mutation {
            PersistedReadyMutation::Delete { removed } => txn
                .delete(self.ready_range_key(shard, removed.next))
                .map_err(|e| format!("Failed to delete queue ready index: {:?}", e)),
            PersistedReadyMutation::Replace { removed, inserted } => txn
                .delete(self.ready_range_key(shard, removed.next))
                .and_then(|_| {
                    txn.put(
                        self.ready_range_key(shard, inserted.next),
                        Self::encode_ready_range_value(inserted),
                        None,
                    )
                })
                .map_err(|e| format!("Failed to replace queue ready index: {:?}", e)),
            PersistedReadyMutation::Split {
                removed,
                left,
                right,
            } => txn
                .delete(self.ready_range_key(shard, removed.next))
                .and_then(|_| {
                    txn.put(
                        self.ready_range_key(shard, left.next),
                        Self::encode_ready_range_value(left),
                        None,
                    )
                })
                .and_then(|_| {
                    txn.put(
                        self.ready_range_key(shard, right.next),
                        Self::encode_ready_range_value(right),
                        None,
                    )
                })
                .map_err(|e| format!("Failed to split queue ready index: {:?}", e)),
        }
    }

    pub(in crate::domains::queue::actor) fn write_index_mutation_plan(
        &self,
        txn: &mut cntryl_midge::Transaction,
        id: MessageId,
        plan: PersistedIndexMutationPlan,
        dead_lettered_at_ms: Option<u64>,
    ) -> Result<(), String> {
        if let Some((shard, mutation)) = plan.ready_mutation {
            self.write_persisted_ready_mutation(txn, shard, mutation)
                .map_err(|error| {
                    format!("Failed to update ready index for message {}: {}", id, error)
                })?;
        }

        if let Some(visible_at_ms) = plan.delayed_index_delete {
            txn.delete(self.delayed_index_key(visible_at_ms, id))
                .map_err(|e| {
                    format!("Failed to update delayed index for message {}: {:?}", id, e)
                })?;
        }

        if let Some(dead_lettered_at_ms) = dead_lettered_at_ms {
            txn.put(
                self.dlq_index_key(dead_lettered_at_ms, id),
                Vec::new(),
                None,
            )
            .map_err(|e| format!("Failed to write DLQ index for message {}: {:?}", id, e))?;
        }

        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                plan.staged_ready_count as u64,
                plan.staged_delayed_count as u64,
                plan.staged_next_delayed_visibility,
            ),
            None,
        )
        .map_err(|e| {
            format!(
                "Failed to update queue index meta for message {}: {:?}",
                id, e
            )
        })
    }

    pub(in crate::domains::queue::actor) fn apply_index_mutation_plan(
        &mut self,
        id: MessageId,
        plan: PersistedIndexMutationPlan,
        dead_lettered_at_ms: Option<u64>,
    ) {
        if let Some((shard, mutation)) = plan.ready_mutation {
            self.apply_ready_index_mutation(shard, mutation);
        }

        if plan.delayed_index_delete.is_some() {
            self.remove_persisted_delayed(id);
        }

        if let Some(dead_lettered_at_ms) = dead_lettered_at_ms {
            self.insert_persisted_dlq(id, dead_lettered_at_ms);
        }
    }
}
