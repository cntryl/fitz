use super::super::*;

impl QueueActor {
    pub(in crate::domains::queue::actor) fn min_persisted_delayed_visibility_ms(
        &self,
    ) -> Option<u64> {
        self.persisted_next_delayed_visibility_ms
    }

    pub(in crate::domains::queue::actor) fn min_persisted_delayed_visibility_ms_excluding(
        &self,
        excluded: MessageId,
    ) -> Option<u64> {
        if self.persisted_delayed.get(&excluded).copied()
            != self.persisted_next_delayed_visibility_ms
        {
            return self.persisted_next_delayed_visibility_ms;
        }

        self.persisted_delayed
            .iter()
            .filter_map(|(&id, &visible_at_ms)| (id != excluded).then_some(visible_at_ms))
            .min()
    }

    pub(in crate::domains::queue::actor) fn recompute_persisted_delayed_visibility_ms(&mut self) {
        self.persisted_next_delayed_visibility_ms = self.persisted_delayed.values().copied().min();
    }

    pub(in crate::domains::queue::actor) fn insert_persisted_delayed(
        &mut self,
        id: MessageId,
        visible_at_ms: u64,
    ) {
        self.persisted_delayed.insert(id, visible_at_ms);
        self.persisted_next_delayed_visibility_ms = Some(
            self.persisted_next_delayed_visibility_ms
                .map_or(visible_at_ms, |current| current.min(visible_at_ms)),
        );
    }

    pub(in crate::domains::queue::actor) fn remove_persisted_delayed(
        &mut self,
        id: MessageId,
    ) -> Option<u64> {
        let removed = self.persisted_delayed.remove(&id);
        if removed == self.persisted_next_delayed_visibility_ms {
            self.recompute_persisted_delayed_visibility_ms();
        }
        removed
    }

    pub(in crate::domains::queue::actor) fn clear_persisted_delayed(&mut self) {
        self.persisted_delayed.clear();
        self.persisted_next_delayed_visibility_ms = None;
    }

    pub(in crate::domains::queue::actor) fn insert_persisted_dlq(
        &mut self,
        id: MessageId,
        dead_lettered_at_ms: u64,
    ) {
        self.persisted_dlq.insert(id, dead_lettered_at_ms);
        self.dlq_count = self.persisted_dlq.len();
    }

    pub(in crate::domains::queue::actor) fn remove_persisted_dlq(
        &mut self,
        id: MessageId,
    ) -> Option<u64> {
        let removed = self.persisted_dlq.remove(&id);
        self.dlq_count = self.persisted_dlq.len();
        removed
    }

    pub(in crate::domains::queue::actor) fn clear_persisted_dlq(&mut self) {
        self.persisted_dlq.clear();
        self.dlq_count = 0;
    }
}
