use super::super::{MessageId, QueueActor};

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

        let mut visibilities = self.persisted_delayed_visibilities.iter();
        let (&first, &count) = visibilities.next()?;
        if count > 1 {
            Some(first)
        } else {
            visibilities.next().map(|(&timestamp, _)| timestamp)
        }
    }

    pub(in crate::domains::queue::actor) fn insert_persisted_delayed(
        &mut self,
        id: MessageId,
        visible_at_ms: u64,
    ) {
        let previous = self.persisted_delayed.insert(id, visible_at_ms);
        if previous != Some(visible_at_ms) {
            if let Some(previous) = previous {
                self.remove_delayed_visibility(previous);
            }
            *self
                .persisted_delayed_visibilities
                .entry(visible_at_ms)
                .or_default() += 1;
            self.refresh_delayed_visibility();
        }
    }

    pub(in crate::domains::queue::actor) fn remove_persisted_delayed(
        &mut self,
        id: MessageId,
    ) -> Option<u64> {
        let removed = self.persisted_delayed.remove(&id);
        if let Some(timestamp) = removed {
            self.remove_delayed_visibility(timestamp);
            self.refresh_delayed_visibility();
            // TTL may remove long-delay messages years before their timer is due.
            // Compact stale timers occasionally, with cost amortized over removals.
            if self.delayed.len()
                > self
                    .persisted_delayed
                    .len()
                    .saturating_mul(2)
                    .saturating_add(64)
            {
                self.delayed.retain(|entry| {
                    self.persisted_delayed.get(&entry.0.id) == Some(&entry.0.visible_at_ms)
                });
                self.next_delayed_deadline = self.delayed.peek().map_or_else(
                    || self.clock.now_instant() + super::super::QUEUE_IDLE_HORIZON,
                    |entry| entry.0.visible_at,
                );
            }
        }
        removed
    }

    fn remove_delayed_visibility(&mut self, timestamp: u64) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) =
            self.persisted_delayed_visibilities.entry(timestamp)
        {
            if *entry.get() == 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }

    fn refresh_delayed_visibility(&mut self) {
        self.persisted_next_delayed_visibility_ms = self
            .persisted_delayed_visibilities
            .first_key_value()
            .map(|(&timestamp, _)| timestamp);
    }

    pub(in crate::domains::queue::actor) fn clear_persisted_delayed(&mut self) {
        self.persisted_delayed.clear();
        self.persisted_delayed_visibilities.clear();
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
