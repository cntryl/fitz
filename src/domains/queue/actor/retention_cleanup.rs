//! Bounded bookkeeping cleanup. Midge independently expires message data.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::mutation_storage::QueueMutationError;
use super::{FastMap, MessageId, QueueActor, QueueCommit, QueueRecord, MAX_DUE_ITEMS_PER_PASS};

#[derive(Default)]
pub(super) struct RetentionSchedule {
    deadlines: FastMap<u64, u64>,
    events: BinaryHeap<Reverse<(u64, u64, u64)>>,
}

impl RetentionSchedule {
    pub(super) fn schedule(&mut self, id: MessageId, deadline: Option<u64>) {
        let id = id.as_u64();
        if let Some(deadline) = deadline {
            if self.deadlines.insert(id, deadline) != Some(deadline) {
                self.events.push(Reverse((deadline, id, deadline)));
            }
        } else {
            self.deadlines.remove(&id);
        }
        // Repeated DLQ/replay cycles and completes cannot grow stale events forever.
        if self.events.len() > self.deadlines.len().saturating_mul(2).saturating_add(64) {
            self.events
                .retain(|Reverse((_, id, deadline))| self.deadlines.get(id) == Some(deadline));
        }
    }
}

impl QueueActor {
    pub(super) fn current_record(&self, id: MessageId) -> Result<Option<QueueRecord>, String> {
        if let Some(record) = self.records.get(&id) {
            return Ok(Some(record.clone()));
        }
        match self.load_record_metadata_from_store(id) {
            Ok(record) => Ok(Some(record)),
            Err(error) if error == format!("Message {id} disappeared from storage") => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) fn remove_unavailable_message(
        &mut self,
        id: MessageId,
    ) -> Result<(), QueueMutationError> {
        self.remove_unavailable_message_inner(id, true)
    }

    pub(super) fn remove_expired_prepared_message(
        &mut self,
        id: MessageId,
    ) -> Result<(), QueueMutationError> {
        self.remove_unavailable_message_inner(id, false)
    }

    fn remove_unavailable_message_inner(
        &mut self,
        id: MessageId,
        may_be_ready: bool,
    ) -> Result<(), QueueMutationError> {
        let plan = self.plan_index_mutation_for_unavailable_message(id);
        let was_ready = may_be_ready
            && plan.ready_mutation.is_some()
            && !self.inflight.contains_key(&id)
            && !self.persisted_delayed.contains_key(&id)
            && !self.persisted_dlq.contains_key(&id);
        let mut txn = self.persistence.store.begin(
            self.queue_key.family.id(),
            super::recovery_store::QueueTransactionMode::ReadWrite,
        )?;
        Self::delete_record(
            &mut txn,
            self.cached_header_key(id),
            self.cached_body_key(id),
        )?;
        self.write_index_mutation_plan(&mut txn, id, plan, None)?;
        if let Some(dead_lettered_at) = self.persisted_dlq.get(&id) {
            txn.delete(self.dlq_index_key(*dead_lettered_at, id))?;
        }
        Self::commit_transaction(txn, QueueCommit::Redelivery)?;
        self.apply_index_mutation_plan(id, plan, None);
        self.remove_persisted_dlq(id);
        self.inflight.remove(&id);
        if may_be_ready && self.ready.front().is_some_and(|entry| entry.id == id) {
            self.pop_ready_entry();
        } else if was_ready {
            self.expire_ready_entry(id);
        }
        self.evict_cached_record(id);
        self.evict_cached_body(id);
        self.retention_schedule.schedule(id, None);
        Ok(())
    }

    pub(super) fn message_is_live(&mut self, id: MessageId) -> Result<bool, QueueMutationError> {
        match self.current_record(id)? {
            Some(record) if !self.record_has_expired(&record) => Ok(true),
            _ => {
                self.remove_unavailable_message(id)?;
                Ok(false)
            }
        }
    }

    pub(super) fn process_retention_expirations(&mut self) -> bool {
        let now_ms = self.clock.now_epoch_ms();
        let mut mutated = false;
        for _ in 0..MAX_DUE_ITEMS_PER_PASS {
            let Some(&Reverse((check_at, id, deadline))) = self.retention_schedule.events.peek()
            else {
                break;
            };
            if check_at > now_ms {
                break;
            }
            self.retention_schedule.events.pop();
            if self.retention_schedule.deadlines.get(&id) != Some(&deadline) {
                continue;
            }
            if let Err(error) = self.remove_unavailable_message(MessageId::new(id)) {
                tracing::warn!(message_id = id, error = %error, "Failed to clean expired queue indexes");
                self.retention_schedule.events.push(Reverse((
                    now_ms.saturating_add(1_000),
                    id,
                    deadline,
                )));
                break;
            }
            mutated = true;
        }
        mutated
    }
}
