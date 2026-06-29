use super::*;

pub(in crate::domains::rpc::sink) struct RpcRouteState {
    pub(in crate::domains::rpc::sink) workers: Vec<RpcWorker>,
    pub(in crate::domains::rpc::sink) ready_queue: VecDeque<usize>,
    pub(in crate::domains::rpc::sink) queued: VecDeque<uuid::Uuid>,
}

impl RpcRouteState {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            workers: Vec::new(),
            ready_queue: VecDeque::new(),
            queued: VecDeque::new(),
        }
    }

    pub(in crate::domains::rpc::sink) fn register_worker(&mut self, worker: RpcWorker) {
        if self.workers.iter().any(|existing| {
            existing.addr == worker.addr && existing.session_id == worker.session_id
        }) {
            return;
        }

        let index = self.workers.len();
        self.workers.push(worker);
        if self.workers[index].is_available() {
            self.ready_queue.push_back(index);
        }
    }

    pub(in crate::domains::rpc::sink) fn unregister_worker(
        &mut self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) {
        if let Some(index) = self
            .workers
            .iter()
            .position(|worker| worker.addr == *worker_addr && worker.session_id == session_id)
        {
            self.workers.remove(index);
            self.ready_queue.retain(|ready_index| *ready_index != index);
            for ready_index in &mut self.ready_queue {
                if *ready_index > index {
                    *ready_index -= 1;
                }
            }
        }
    }

    pub(in crate::domains::rpc::sink) fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub(in crate::domains::rpc::sink) fn unregister_session(&mut self, session_id: u64) -> usize {
        let before = self.workers.len();

        while let Some(index) = self
            .workers
            .iter()
            .position(|worker| worker.session_id == session_id)
        {
            self.workers.remove(index);
            self.ready_queue.retain(|ready_index| *ready_index != index);
            for ready_index in &mut self.ready_queue {
                if *ready_index > index {
                    *ready_index -= 1;
                }
            }
        }

        before.saturating_sub(self.workers.len())
    }

    pub(in crate::domains::rpc::sink) fn has_available_worker(&self) -> bool {
        !self.ready_queue.is_empty()
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

    pub(in crate::domains::rpc::sink) fn claim_worker(&mut self) -> Option<RpcWorker> {
        let index = self.ready_queue.pop_front()?;
        let worker = self.workers.get_mut(index)?;
        worker.claim_slot();
        if worker.is_available() {
            self.ready_queue.push_back(index);
        }

        Some(worker.clone())
    }

    pub(in crate::domains::rpc::sink) fn release_worker(
        &mut self,
        worker_addr: &RouteAddress,
        session_id: u64,
        latency_us: Option<u64>,
    ) -> bool {
        let Some((index, worker)) = self
            .workers
            .iter_mut()
            .enumerate()
            .find(|(_, worker)| worker.addr == *worker_addr && worker.session_id == session_id)
        else {
            return false;
        };

        let was_available = worker.is_available();
        if let Some(latency_us) = latency_us {
            worker.record_completion(latency_us);
        }
        worker.release_slot();
        if !was_available && worker.is_available() {
            self.ready_queue.push_back(index);
        }

        true
    }
}
