use super::{HashSet, RpcRegistrationId, VecDeque};

pub(in crate::domains::rpc::sink) struct RpcRouteState {
    pub(in crate::domains::rpc::sink) queued: VecDeque<uuid::Uuid>,
    pub(in crate::domains::rpc::sink) first_seen_sequence: u64,
    registrations: RegistrationRotor,
    ready: bool,
}

struct RegistrationRotor {
    ids: Vec<RpcRegistrationId>,
    next_index: usize,
}

impl RegistrationRotor {
    fn new(ids: Vec<RpcRegistrationId>) -> Self {
        Self { ids, next_index: 0 }
    }

    fn advance_after(&mut self, index: usize) {
        self.next_index = if self.ids.is_empty() {
            0
        } else {
            (index + 1) % self.ids.len()
        };
    }
}

impl RpcRouteState {
    pub(in crate::domains::rpc::sink) fn new(
        first_seen_sequence: u64,
        registration_ids: Vec<RpcRegistrationId>,
    ) -> Self {
        Self {
            queued: VecDeque::new(),
            first_seen_sequence,
            registrations: RegistrationRotor::new(registration_ids),
            ready: false,
        }
    }

    pub(in crate::domains::rpc::sink) fn has_registrations(&self) -> bool {
        !self.registrations.ids.is_empty()
    }

    pub(in crate::domains::rpc::sink) fn registration_ids(&self) -> &[RpcRegistrationId] {
        &self.registrations.ids
    }

    pub(in crate::domains::rpc::sink) fn next_registration_index(&self) -> usize {
        self.registrations.next_index
    }

    pub(in crate::domains::rpc::sink) fn advance_registration_cursor(&mut self, index: usize) {
        self.registrations.advance_after(index);
    }

    pub(in crate::domains::rpc::sink) fn add_registration(
        &mut self,
        registration_id: RpcRegistrationId,
    ) {
        match self.registrations.ids.binary_search(&registration_id) {
            Ok(_) => {}
            Err(index) => {
                self.registrations.ids.insert(index, registration_id);
                if index < self.registrations.next_index {
                    self.registrations.next_index = self.registrations.next_index.saturating_add(1);
                }
            }
        }
    }

    pub(in crate::domains::rpc::sink) fn remove_registration(
        &mut self,
        registration_id: RpcRegistrationId,
    ) {
        let Ok(index) = self.registrations.ids.binary_search(&registration_id) else {
            return;
        };
        self.registrations.ids.remove(index);
        if index < self.registrations.next_index {
            self.registrations.next_index = self.registrations.next_index.saturating_sub(1);
        }
        if self.registrations.next_index >= self.registrations.ids.len() {
            self.registrations.next_index = 0;
        }
    }

    pub(in crate::domains::rpc::sink) fn remove_registrations(
        &mut self,
        registration_ids: &HashSet<RpcRegistrationId>,
    ) {
        let next_registration_id = self
            .registrations
            .ids
            .get(self.registrations.next_index)
            .copied();
        self.registrations
            .ids
            .retain(|registration_id| !registration_ids.contains(registration_id));
        self.registrations.next_index = next_registration_id.map_or(0, |registration_id| {
            self.registrations
                .ids
                .binary_search(&registration_id)
                .unwrap_or_else(|index| {
                    if index == self.registrations.ids.len() {
                        0
                    } else {
                        index
                    }
                })
        });
    }

    pub(in crate::domains::rpc::sink) fn has_queued_requests(&self) -> bool {
        !self.queued.is_empty()
    }

    pub(in crate::domains::rpc::sink) fn queued_len(&self) -> usize {
        self.queued.len()
    }

    pub(in crate::domains::rpc::sink) fn enqueue_request(&mut self, correlation_id: uuid::Uuid) {
        self.queued.push_back(correlation_id);
    }

    pub(in crate::domains::rpc::sink) fn pop_queued_request(&mut self) -> Option<uuid::Uuid> {
        self.queued.pop_front()
    }

    pub(in crate::domains::rpc::sink) fn remove_queued_request(
        &mut self,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        let before = self.queued.len();
        self.queued.retain(|queued_id| queued_id != correlation_id);
        before != self.queued.len()
    }

    pub(in crate::domains::rpc::sink) fn mark_ready(&mut self) -> bool {
        if self.ready {
            return false;
        }
        self.ready = true;
        true
    }

    pub(in crate::domains::rpc::sink) fn clear_ready(&mut self) {
        self.ready = false;
    }
}

#[cfg(test)]
mod tests {
    use super::RegistrationRotor;

    #[test]
    fn should_rotate_registrations_independent_of_route_queue_state() {
        // Arrange
        let mut rotor = RegistrationRotor::new(vec![11, 22, 33]);

        // Act
        rotor.advance_after(2);

        // Assert
        assert_eq!(rotor.next_index, 0);
    }
}
