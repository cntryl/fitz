use super::{
    FxBuildHasher, HashMap, RouteAddress, RpcFastMap, RpcWorker, RpcWorkerDispatch, RpcWorkerKey,
    VecDeque,
};

pub(in crate::domains::rpc::sink) struct RpcRouteState {
    pub(in crate::domains::rpc::sink) workers: Vec<Option<RpcWorker>>,
    worker_index: RpcFastMap<RpcWorkerKey, usize>,
    pub(in crate::domains::rpc::sink) ready_queue: VecDeque<usize>,
    pub(in crate::domains::rpc::sink) queued: VecDeque<uuid::Uuid>,
    live_workers: usize,
}

impl RpcRouteState {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            workers: Vec::new(),
            worker_index: HashMap::with_capacity_and_hasher(16, FxBuildHasher),
            ready_queue: VecDeque::new(),
            queued: VecDeque::new(),
            live_workers: 0,
        }
    }

    pub(in crate::domains::rpc::sink) fn register_worker(&mut self, worker: RpcWorker) {
        let key = RpcWorkerKey::from_parts(&worker.addr, worker.session_id);
        if self.worker_index.contains_key(&key) {
            return;
        }

        let index = self
            .workers
            .iter()
            .position(Option::is_none)
            .unwrap_or_else(|| {
                self.workers.push(None);
                self.workers.len() - 1
            });
        let is_available = worker.is_available();
        self.workers[index] = Some(worker);
        self.worker_index.insert(key, index);
        self.live_workers = self.live_workers.saturating_add(1);
        if is_available {
            self.ready_queue.push_back(index);
        }
    }

    pub(in crate::domains::rpc::sink) fn unregister_worker(
        &mut self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) {
        let key = RpcWorkerKey::from_parts(worker_addr, session_id);
        self.unregister_worker_key(&key);
    }

    pub(in crate::domains::rpc::sink) fn worker_count(&self) -> usize {
        self.live_workers
    }

    pub(in crate::domains::rpc::sink) fn unregister_session(&mut self, session_id: u64) -> usize {
        let keys: Vec<RpcWorkerKey> = self
            .workers
            .iter()
            .filter_map(|worker| {
                let worker = worker.as_ref()?;
                (worker.session_id == session_id)
                    .then(|| RpcWorkerKey::new(worker.addr.clone(), worker.session_id))
            })
            .collect();

        let removed = keys.len();
        for key in keys {
            self.unregister_worker_key(&key);
        }
        removed
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

    pub(in crate::domains::rpc::sink) fn claim_worker(&mut self) -> Option<RpcWorkerDispatch> {
        while let Some(index) = self.ready_queue.pop_front() {
            let Some(Some(worker)) = self.workers.get_mut(index) else {
                continue;
            };
            if !worker.is_available() {
                continue;
            }

            worker.claim_slot();
            let dispatch = worker.dispatch_view(index);
            if worker.is_available() {
                self.ready_queue.push_front(index);
            }

            return Some(dispatch);
        }

        None
    }

    pub(in crate::domains::rpc::sink) fn release_worker_slot(
        &mut self,
        worker_slot: usize,
        latency_us: Option<u64>,
    ) -> bool {
        let Some(Some(worker)) = self.workers.get_mut(worker_slot) else {
            return false;
        };
        let was_available = worker.is_available();
        if let Some(latency_us) = latency_us {
            worker.record_completion(latency_us);
        }
        worker.release_slot();
        if !was_available && worker.is_available() {
            if worker.max_concurrent > 1 {
                self.ready_queue.push_front(worker_slot);
            } else {
                self.ready_queue.push_back(worker_slot);
            }
        }

        true
    }

    fn unregister_worker_key(&mut self, key: &RpcWorkerKey) -> bool {
        let Some(index) = self.worker_index.remove(key) else {
            return false;
        };

        if self.workers.get_mut(index).and_then(Option::take).is_some() {
            self.live_workers = self.live_workers.saturating_sub(1);
            self.ready_queue.retain(|ready_index| *ready_index != index);
            return true;
        }

        false
    }
}
