//! Wildcard Queue inventory, fairness and broker-local waiting reservations.

use super::model::PendingQueueReserve;
use crate::domains::queue::QueueKey;
use crate::runtime::routing::RouteFamily;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct ReservationBook {
    known_queue_keys: HashSet<QueueKey>,
    inventory_error: Option<String>,
    wildcard_reserve_sequence: AtomicU64,
    pending_reserves: VecDeque<PendingQueueReserve>,
}

impl ReservationBook {
    pub(super) fn new(
        known_queue_keys: HashSet<QueueKey>,
        inventory_error: Option<String>,
    ) -> Self {
        Self {
            known_queue_keys,
            inventory_error,
            wildcard_reserve_sequence: AtomicU64::new(0),
            pending_reserves: VecDeque::new(),
        }
    }

    pub(super) fn matching_keys(
        &self,
        family: RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> Vec<QueueKey> {
        let mut keys = self
            .known_queue_keys
            .iter()
            .filter(|key| key.family == family)
            .filter(|key| pattern.matches(&super::model::QueueFamilyState::queue_ready_route(key)))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
            ))
        });
        keys
    }

    pub(super) fn matching_key_count(
        &self,
        family: RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> usize {
        self.known_queue_keys
            .iter()
            .filter(|key| key.family == family)
            .filter(|key| pattern.matches(&super::model::QueueFamilyState::queue_ready_route(key)))
            .count()
    }

    pub(super) fn next_wildcard_start(&self, key_count: usize) -> usize {
        usize::try_from(
            self.wildcard_reserve_sequence
                .fetch_add(1, Ordering::Relaxed),
        )
        .unwrap_or(0)
            % key_count
    }

    pub(super) fn inventory_error(&self) -> Option<&str> {
        self.inventory_error.as_deref()
    }

    #[cfg(test)]
    pub(super) fn set_inventory_error(&mut self, error: String) {
        self.inventory_error = Some(error);
    }

    pub(super) fn insert_key(&mut self, key: QueueKey) {
        self.known_queue_keys.insert(key);
    }

    pub(super) fn remove_key(&mut self, key: &QueueKey) {
        self.known_queue_keys.remove(key);
    }

    #[cfg(test)]
    pub(super) fn known_key_count(&self) -> usize {
        self.known_queue_keys.len()
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &QueueKey) -> bool {
        self.known_queue_keys.contains(key)
    }

    pub(super) fn enqueue(&mut self, pending: PendingQueueReserve) {
        self.pending_reserves.push_back(pending);
    }

    pub(super) fn take_pending(&mut self) -> VecDeque<PendingQueueReserve> {
        std::mem::take(&mut self.pending_reserves)
    }

    pub(super) fn restore_pending(&mut self, pending: &mut VecDeque<PendingQueueReserve>) {
        self.pending_reserves.append(pending);
    }

    pub(super) fn remove_session(&mut self, session_id: u64) {
        self.pending_reserves
            .retain(|pending| pending.meta.session_id != session_id);
    }

    #[cfg(test)]
    pub(super) fn pending_count(&self) -> usize {
        self.pending_reserves.len()
    }
}

#[cfg(test)]
mod tests {
    use super::ReservationBook;
    use std::collections::HashSet;

    #[test]
    fn should_rotate_wildcard_reserve_start_across_known_keys() {
        // Arrange
        let book = ReservationBook::new(HashSet::new(), None);

        // Act
        let starts = (0..4)
            .map(|_| book.next_wildcard_start(3))
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(starts, vec![0, 1, 2, 0]);
    }
}
