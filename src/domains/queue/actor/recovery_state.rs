use super::{MessageId, QueueActor, QUEUE_IDLE_HORIZON};

impl QueueActor {
    pub(in crate::domains::queue::actor) fn reset_recovery_state(&mut self) {
        self.reset_live_ready_state();
        self.reset_persisted_index_state();
        self.delayed.clear();
        let now = self.clock.now_instant();
        self.next_delayed_deadline = now + QUEUE_IDLE_HORIZON;
    }

    pub(in crate::domains::queue::actor) fn populate_live_ready_from_persisted(
        &mut self,
        matured_delayed_ids: &[MessageId],
    ) {
        self.reset_live_ready_state();

        let mut ready_ids =
            Vec::with_capacity(self.persisted_ready_count + matured_delayed_ids.len());
        for ranges in &self.persisted_ready_shards {
            for range in ranges {
                let mut id = range.next;
                while id <= range.end {
                    ready_ids.push(MessageId::new(id));
                    let Some(next_id) = id.checked_add(Self::ready_shards_u64()) else {
                        break;
                    };
                    id = next_id;
                }
            }
        }
        ready_ids.extend_from_slice(matured_delayed_ids);
        ready_ids.sort_unstable_by_key(MessageId::as_u64);
        ready_ids.dedup_by_key(|id| id.as_u64());

        for id in ready_ids {
            self.push_ready(id);
        }
    }
}
