use super::{HashSet, RpcRegistrationId, VecDeque};

pub(in crate::domains::rpc::sink) struct RpcRouteState {
    pub(in crate::domains::rpc::sink) queued: VecDeque<uuid::Uuid>,
    pub(in crate::domains::rpc::sink) first_seen_sequence: u64,
    registration_ids: Vec<RpcRegistrationId>,
    next_registration_index: usize,
    ready: bool,
}

impl RpcRouteState {
    pub(in crate::domains::rpc::sink) fn new(
        first_seen_sequence: u64,
        registration_ids: Vec<RpcRegistrationId>,
    ) -> Self {
        Self {
            queued: VecDeque::new(),
            first_seen_sequence,
            registration_ids,
            next_registration_index: 0,
            ready: false,
        }
    }

    pub(in crate::domains::rpc::sink) fn has_registrations(&self) -> bool {
        !self.registration_ids.is_empty()
    }

    pub(in crate::domains::rpc::sink) fn registration_ids(&self) -> &[RpcRegistrationId] {
        &self.registration_ids
    }

    pub(in crate::domains::rpc::sink) fn next_registration_index(&self) -> usize {
        self.next_registration_index
    }

    pub(in crate::domains::rpc::sink) fn advance_registration_cursor(&mut self, index: usize) {
        self.next_registration_index = if self.registration_ids.is_empty() {
            0
        } else {
            (index + 1) % self.registration_ids.len()
        };
    }

    pub(in crate::domains::rpc::sink) fn add_registration(
        &mut self,
        registration_id: RpcRegistrationId,
    ) {
        match self.registration_ids.binary_search(&registration_id) {
            Ok(_) => {}
            Err(index) => {
                self.registration_ids.insert(index, registration_id);
                if index < self.next_registration_index {
                    self.next_registration_index = self.next_registration_index.saturating_add(1);
                }
            }
        }
    }

    pub(in crate::domains::rpc::sink) fn remove_registration(
        &mut self,
        registration_id: RpcRegistrationId,
    ) {
        let Ok(index) = self.registration_ids.binary_search(&registration_id) else {
            return;
        };
        self.registration_ids.remove(index);
        if index < self.next_registration_index {
            self.next_registration_index = self.next_registration_index.saturating_sub(1);
        }
        if self.next_registration_index >= self.registration_ids.len() {
            self.next_registration_index = 0;
        }
    }

    pub(in crate::domains::rpc::sink) fn remove_registrations(
        &mut self,
        registration_ids: &HashSet<RpcRegistrationId>,
    ) {
        let next_registration_id = self
            .registration_ids
            .get(self.next_registration_index)
            .copied();
        self.registration_ids
            .retain(|registration_id| !registration_ids.contains(registration_id));
        self.next_registration_index = next_registration_id.map_or(0, |registration_id| {
            self.registration_ids
                .binary_search(&registration_id)
                .unwrap_or_else(|index| {
                    if index == self.registration_ids.len() {
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
