use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, Instant, Route, RouteAddress,
    RpcFastMap, RpcPendingAckDisposition, RpcPendingDispatchInfo, RpcPendingErrorDelivery,
    RpcPendingRequest, RpcPendingRequestInit, RpcPendingTable, RpcPendingTimeoutResult,
    RpcQueuedDispatch, RpcQueuedRequest, RpcRequestDispatch, RpcRouteState,
    RpcSessionCleanupResult, RpcWorkerCleanupResult,
};

pub(in crate::domains::rpc::sink) struct RpcState {
    pub(in crate::domains::rpc::sink) routes: RpcFastMap<Route, RpcRouteState>,
    pub(in crate::domains::rpc::sink) pending: RpcPendingTable,
    pub(in crate::domains::rpc::sink) queued: RpcFastMap<uuid::Uuid, RpcQueuedRequest>,
    pub(in crate::domains::rpc::sink) queued_expirations: BinaryHeap<ExpiringPendingRequest>,
}

enum DispatchAction {
    Queue,
    Dispatch(super::RpcWorkerDispatch),
}

impl RpcState {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            routes: HashMap::with_capacity_and_hasher(64, FxBuildHasher::default()),
            pending: RpcPendingTable::new(),
            queued: HashMap::with_capacity_and_hasher(256, FxBuildHasher::default()),
            queued_expirations: BinaryHeap::with_capacity(256),
        }
    }

    pub(in crate::domains::rpc::sink) fn ensure_route_state(
        &mut self,
        route: &Route,
    ) -> &mut RpcRouteState {
        self.routes
            .entry(route.clone())
            .or_insert_with(RpcRouteState::new)
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn route_state(
        &mut self,
        route: &Route,
    ) -> Option<&mut RpcRouteState> {
        self.routes.get_mut(route)
    }

    pub(in crate::domains::rpc::sink) fn prune_route_if_empty(&mut self, route: &Route) {
        let should_remove = self.routes.get(route).is_some_and(|route_state| {
            route_state.worker_count() == 0 && !route_state.has_queued_requests()
        });

        if should_remove {
            self.routes.remove(route);
        }
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcSessionCleanupResult {
        let mut removed_workers = 0;
        let mut empty_routes = Vec::new();

        for (route, route_state) in &mut self.routes {
            removed_workers += route_state.unregister_session(session_id);
            if route_state.worker_count() == 0 && !route_state.has_queued_requests() {
                empty_routes.push(route.clone());
            }
        }

        for route in empty_routes {
            self.routes.remove(&route);
        }

        let pending_cleanup = self.pending.cleanup_session(session_id);
        let queued_removed = self.cleanup_queued_session(session_id);

        RpcSessionCleanupResult {
            removed_workers,
            detached_callers: pending_cleanup.detached_callers,
            removed_pending: pending_cleanup.removed_pending + queued_removed,
            pending_len: self.live_request_count(),
            disconnect_deliveries: pending_cleanup.disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn unregister_worker(
        &mut self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let removed_workers = {
            let mut removed = 0;
            let mut remove_route = false;

            if let Some(route_state) = self.routes.get_mut(worker_addr.route()) {
                let before = route_state.worker_count();
                route_state.unregister_worker(worker_addr, session_id);
                removed = before.saturating_sub(route_state.worker_count());
                remove_route =
                    route_state.worker_count() == 0 && !route_state.has_queued_requests();
            }

            if remove_route {
                self.routes.remove(worker_addr.route());
            }

            removed
        };

        let pending_cleanup = self.pending.cleanup_worker(worker_addr, session_id);

        RpcWorkerCleanupResult {
            removed_workers,
            removed_pending: pending_cleanup.removed_pending,
            pending_len: self.live_request_count(),
            disconnect_deliveries: pending_cleanup.disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn contains_correlation(
        &self,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        self.pending.contains_correlation(correlation_id)
            || self.queued.contains_key(correlation_id)
    }

    pub(in crate::domains::rpc::sink) fn live_request_count(&self) -> usize {
        self.pending.len() + self.queued.len()
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn queue_request(
        &mut self,
        correlation_id: uuid::Uuid,
        request: RpcQueuedRequest,
    ) {
        let route = request.request.route.clone();
        self.ensure_route_state(&route)
            .enqueue_request(correlation_id);
        self.queued_expirations.push(ExpiringPendingRequest {
            expires_at: request.expires_at,
            correlation_id,
        });
        self.queued.insert(correlation_id, request);
    }

    pub(in crate::domains::rpc::sink) fn dispatch_or_queue_request(
        &mut self,
        request: crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        request_timeout: std::time::Duration,
        route_pending_capacity: usize,
        global_pending_capacity: usize,
    ) -> RpcRequestDispatch {
        if self.contains_correlation(&request.correlation_id) {
            return RpcRequestDispatch::Duplicate { request };
        }

        let live_request_count = self.live_request_count();
        let route = request.route.clone();
        let Some(route_state) = self.routes.get_mut(&route) else {
            return RpcRequestDispatch::NoWorkers { request };
        };

        if route_state.worker_count() == 0 {
            return RpcRequestDispatch::NoWorkers { request };
        }

        if live_request_count >= global_pending_capacity {
            return RpcRequestDispatch::GlobalCapacityFull { request };
        }

        let correlation_id = request.correlation_id;
        let action = if route_state.has_queued_requests() || !route_state.has_available_worker() {
            if route_state.queued_len() >= route_pending_capacity {
                return RpcRequestDispatch::RouteCapacityFull { request };
            }
            route_state.enqueue_request(correlation_id);
            DispatchAction::Queue
        } else {
            let Some(worker) = route_state.claim_worker() else {
                return RpcRequestDispatch::NoWorkers { request };
            };
            DispatchAction::Dispatch(worker)
        };

        let expires_at = Instant::now() + request_timeout;
        match action {
            DispatchAction::Queue => {
                let queued = RpcQueuedRequest::from_request(
                    request,
                    caller_session_id,
                    caller_inbox_addr,
                    expires_at,
                );
                self.queued_expirations.push(ExpiringPendingRequest {
                    expires_at,
                    correlation_id,
                });
                self.queued.insert(correlation_id, queued);
                RpcRequestDispatch::Queued {
                    route,
                    correlation_id,
                    live_request_count: self.live_request_count(),
                }
            }
            DispatchAction::Dispatch(worker) => {
                self.pending.track_pending(
                    correlation_id,
                    RpcPendingRequest::from_dispatch(
                        &request,
                        caller_session_id,
                        caller_inbox_addr,
                        &worker,
                        expires_at,
                    ),
                );
                RpcRequestDispatch::Immediate {
                    request,
                    worker,
                    live_request_count: self.live_request_count(),
                }
            }
        }
    }

    pub(in crate::domains::rpc::sink) fn remove_queued_request(
        &mut self,
        correlation_id: &uuid::Uuid,
    ) -> Option<RpcQueuedRequest> {
        let queued = self.queued.remove(correlation_id)?;
        if let Some(route_state) = self.routes.get_mut(&queued.request.route) {
            route_state.remove_queued_request(correlation_id);
        }
        self.prune_route_if_empty(&queued.request.route);
        Some(queued)
    }

    pub(in crate::domains::rpc::sink) fn next_queued_dispatch(
        &mut self,
        route: &Route,
    ) -> Option<RpcQueuedDispatch> {
        let (worker, correlation_id) = {
            let route_state = self.routes.get_mut(route)?;
            if !route_state.has_available_worker() || !route_state.has_queued_requests() {
                return None;
            }
            let worker = route_state.claim_worker()?;
            let correlation_id = route_state
                .pop_queued_request()
                .expect("queued correlation id for dispatch");
            (worker, correlation_id)
        };

        let queued = self
            .queued
            .remove(&correlation_id)
            .expect("queued request for dispatch");
        let RpcQueuedRequest {
            request,
            caller_session_id,
            caller_inbox_addr,
            submitted_at,
            submitted_at_instant,
            expires_at,
        } = queued;
        let route = request.route.clone();
        let pending = RpcPendingRequest::new(RpcPendingRequestInit {
            route,
            caller_session_id,
            caller_inbox_addr,
            worker_addr: worker.addr.clone(),
            worker_session_id: worker.session_id,
            worker_slot: worker.slot,
            submitted_at,
            submitted_at_instant,
            expires_at,
        });
        self.pending.track_pending(correlation_id, pending);

        Some(RpcQueuedDispatch {
            request,
            worker,
            live_request_count: self.live_request_count(),
        })
    }

    pub(in crate::domains::rpc::sink) fn remove_pending_request(
        &mut self,
        correlation_id: &uuid::Uuid,
    ) -> Option<(RpcPendingRequest, usize)> {
        let pending = self.pending.pending.remove(correlation_id)?;
        if let Some(route_state) = self.routes.get_mut(&pending.route) {
            route_state.release_worker_slot(pending.worker_slot, None);
        }
        self.prune_route_if_empty(&pending.route);
        Some((pending, self.live_request_count()))
    }

    pub(in crate::domains::rpc::sink) fn release_worker_for_pending(
        &mut self,
        pending: &RpcPendingRequest,
        latency_us: Option<u64>,
    ) {
        if let Some(route_state) = self.routes.get_mut(&pending.route) {
            route_state.release_worker_slot(pending.worker_slot, latency_us);
        }
    }

    pub(in crate::domains::rpc::sink) fn release_worker_for_dispatch_info(
        &mut self,
        pending: &RpcPendingDispatchInfo,
        latency_us: Option<u64>,
    ) {
        if let Some(route_state) = self.routes.get_mut(&pending.route) {
            route_state.release_worker_slot(pending.worker_slot, latency_us);
        }
    }

    pub(in crate::domains::rpc::sink) fn remove_pending_for_ack(
        &mut self,
        correlation_id: &uuid::Uuid,
        worker_session_id: u64,
    ) -> (RpcPendingAckDisposition, usize) {
        let disposition = self
            .pending
            .remove_for_ack(correlation_id, worker_session_id);
        if let RpcPendingAckDisposition::Removed(pending) = &disposition {
            if let Some(route_state) = self.routes.get_mut(&pending.route) {
                route_state.release_worker_slot(pending.worker_slot, None);
            }
            self.prune_route_if_empty(&pending.route);
        }

        (disposition, self.live_request_count())
    }

    pub(in crate::domains::rpc::sink) fn cleanup_queued_session(
        &mut self,
        session_id: u64,
    ) -> usize {
        let queued_to_remove: Vec<uuid::Uuid> = self
            .queued
            .iter()
            .filter(|(_, queued)| queued.caller_session_id == session_id)
            .map(|(correlation_id, _)| *correlation_id)
            .collect();

        for correlation_id in &queued_to_remove {
            self.remove_queued_request(correlation_id);
        }

        queued_to_remove.len()
    }

    pub(in crate::domains::rpc::sink) fn expire_timed_out(
        &mut self,
        now: Instant,
    ) -> RpcPendingTimeoutResult {
        let mut timeout_deliveries = Vec::new();
        let mut removed_pending = 0usize;

        while let Some(expiring) = self.pending.expirations.peek() {
            if expiring.expires_at > now {
                break;
            }

            let expiring = self
                .pending
                .expirations
                .pop()
                .expect("pending expiration entry");
            let Some(pending) = self.pending.pending.get(&expiring.correlation_id) else {
                continue;
            };

            if pending.expires_at != expiring.expires_at {
                continue;
            }

            let pending = self
                .pending
                .pending
                .remove(&expiring.correlation_id)
                .expect("tracked pending request");
            self.release_worker_for_pending(&pending, None);
            self.prune_route_if_empty(&pending.route);
            removed_pending = removed_pending.saturating_add(1);

            if let Some(caller_inbox_addr) = pending.caller_inbox_addr {
                timeout_deliveries.push(RpcPendingErrorDelivery {
                    correlation_id: expiring.correlation_id,
                    caller_session_id: pending.caller_session_id,
                    caller_inbox_addr,
                });
            }
        }

        while let Some(expiring) = self.queued_expirations.peek() {
            if expiring.expires_at > now {
                break;
            }

            let expiring = self
                .queued_expirations
                .pop()
                .expect("queued expiration entry");
            let Some(queued) = self.queued.get(&expiring.correlation_id) else {
                continue;
            };

            if queued.expires_at != expiring.expires_at {
                continue;
            }

            let queued = self
                .remove_queued_request(&expiring.correlation_id)
                .expect("tracked queued request");
            removed_pending = removed_pending.saturating_add(1);
            timeout_deliveries.push(RpcPendingErrorDelivery {
                correlation_id: expiring.correlation_id,
                caller_session_id: queued.caller_session_id,
                caller_inbox_addr: queued.caller_inbox_addr,
            });
        }

        RpcPendingTimeoutResult {
            removed_pending,
            pending_len: self.live_request_count(),
            timeout_deliveries,
        }
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn route_count(&self) -> usize {
        self.routes.len()
    }
}
