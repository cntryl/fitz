use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, Instant, Route, RouteAddress,
    RouteFamily, RpcCorrelationKey, RpcFastMap, RpcPendingDispatchInfo, RpcPendingErrorDelivery,
    RpcPendingRequest, RpcPendingRequestInit, RpcPendingTable, RpcPendingTimeoutResult,
    RpcQueuedDispatch, RpcQueuedRequest, RpcRequestDispatch, RpcRouteState,
    RpcSessionCleanupResult, RpcWorkerCleanupResult,
};

pub(in crate::domains::rpc::sink) struct RpcState {
    pub(in crate::domains::rpc::sink) routes: RpcFastMap<(RouteFamily, Route), RpcRouteState>,
    pub(in crate::domains::rpc::sink) pending: RpcPendingTable,
    pub(in crate::domains::rpc::sink) queued: RpcFastMap<RpcCorrelationKey, RpcQueuedRequest>,
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

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn ensure_route_state(
        &mut self,
        route: &Route,
    ) -> &mut RpcRouteState {
        self.routes
            .entry((RouteFamily::new(1), route.clone()))
            .or_insert_with(RpcRouteState::new)
    }

    pub(in crate::domains::rpc::sink) fn ensure_route_state_for_family(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> &mut RpcRouteState {
        self.routes
            .entry((family, route.clone()))
            .or_insert_with(RpcRouteState::new)
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn route_state(
        &mut self,
        route: &Route,
    ) -> Option<&mut RpcRouteState> {
        self.routes.get_mut(&(RouteFamily::new(1), route.clone()))
    }

    fn prune_route_if_empty_for_family(&mut self, family: RouteFamily, route: &Route) {
        let key = (family, route.clone());
        let should_remove = self.routes.get(&key).is_some_and(|route_state| {
            route_state.worker_count() == 0 && !route_state.has_queued_requests()
        });

        if should_remove {
            self.routes.remove(&key);
        }
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcSessionCleanupResult {
        let mut removed_workers = 0;
        let mut empty_routes = Vec::new();

        for (key, route_state) in &mut self.routes {
            removed_workers += route_state.unregister_session(session_id);
            if route_state.worker_count() == 0 && !route_state.has_queued_requests() {
                empty_routes.push(key.clone());
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

            let key = (*worker_addr.family(), worker_addr.route().clone());
            if let Some(route_state) = self.routes.get_mut(&key) {
                let before = route_state.worker_count();
                route_state.unregister_worker(worker_addr, session_id);
                removed = before.saturating_sub(route_state.worker_count());
                remove_route =
                    route_state.worker_count() == 0 && !route_state.has_queued_requests();
            }

            if remove_route {
                self.routes.remove(&key);
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

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn contains_correlation(
        &self,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        self.contains_correlation_in_family(RouteFamily::new(1), correlation_id)
    }

    fn contains_correlation_in_family(
        &self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        self.pending
            .contains_correlation_in_family(family, correlation_id)
            || self.queued.contains_key(&RpcCorrelationKey {
                family,
                correlation_id: *correlation_id,
            })
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
        self.ensure_route_state_for_family(*request.caller_inbox_addr.family(), &route)
            .enqueue_request(correlation_id);
        let key = RpcCorrelationKey {
            family: *request.caller_inbox_addr.family(),
            correlation_id,
        };
        self.queued_expirations.push(ExpiringPendingRequest {
            expires_at: request.expires_at,
            key,
        });
        self.queued.insert(key, request);
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
        let family = *caller_inbox_addr.family();
        if self.contains_correlation_in_family(family, &request.correlation_id) {
            return RpcRequestDispatch::Duplicate { request };
        }

        let live_request_count = self.live_request_count();
        let route = request.route.clone();
        let Some(route_state) = self.routes.get_mut(&(family, route.clone())) else {
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
                let key = RpcCorrelationKey {
                    family,
                    correlation_id,
                };
                self.queued_expirations
                    .push(ExpiringPendingRequest { expires_at, key });
                self.queued.insert(key, queued);
                RpcRequestDispatch::Queued {
                    route,
                    correlation_id,
                    live_request_count: self.live_request_count(),
                }
            }
            DispatchAction::Dispatch(worker) => {
                self.pending.track_pending_for_family(
                    family,
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

    pub(in crate::domains::rpc::sink) fn remove_queued_request_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<RpcQueuedRequest> {
        let key = RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        };
        let queued = self.queued.remove(&key)?;
        let family = *queued.caller_inbox_addr.family();
        if let Some(route_state) = self.routes.get_mut(&(family, queued.request.route.clone())) {
            route_state.remove_queued_request(correlation_id);
        }
        self.prune_route_if_empty_for_family(family, &queued.request.route);
        Some(queued)
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn next_queued_dispatch(
        &mut self,
        route: &Route,
    ) -> Option<RpcQueuedDispatch> {
        self.next_queued_dispatch_for_family(route, RouteFamily::new(1))
    }

    pub(in crate::domains::rpc::sink) fn next_queued_dispatch_for_family(
        &mut self,
        route: &Route,
        family: RouteFamily,
    ) -> Option<RpcQueuedDispatch> {
        let (worker, correlation_id) = {
            let route_state = self.routes.get_mut(&(family, route.clone()))?;
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
            .remove(&RpcCorrelationKey {
                family,
                correlation_id,
            })
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
        self.pending
            .track_pending_for_family(family, correlation_id, pending);

        Some(RpcQueuedDispatch {
            request,
            worker,
            live_request_count: self.live_request_count(),
        })
    }

    pub(in crate::domains::rpc::sink) fn remove_pending_request_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<(RpcPendingRequest, usize)> {
        let pending = self.pending.pending.remove(&RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        })?;
        if let Some(caller) = pending.caller_inbox_addr.as_ref() {
            if let Some(route_state) = self
                .routes
                .get_mut(&(*caller.family(), pending.route.clone()))
            {
                route_state.release_worker_slot(pending.worker_slot, None);
            }
        }
        if let Some(caller) = pending.caller_inbox_addr.as_ref() {
            self.prune_route_if_empty_for_family(*caller.family(), &pending.route);
        }
        Some((pending, self.live_request_count()))
    }

    pub(in crate::domains::rpc::sink) fn release_worker_for_pending(
        &mut self,
        pending: &RpcPendingRequest,
        latency_us: Option<u64>,
    ) {
        if let Some(caller) = pending.caller_inbox_addr.as_ref() {
            if let Some(route_state) = self
                .routes
                .get_mut(&(*caller.family(), pending.route.clone()))
            {
                route_state.release_worker_slot(pending.worker_slot, latency_us);
            }
        }
    }

    pub(in crate::domains::rpc::sink) fn release_worker_for_dispatch_info(
        &mut self,
        pending: &RpcPendingDispatchInfo,
        latency_us: Option<u64>,
    ) {
        if let Some(caller) = pending.caller_inbox_addr.as_ref() {
            if let Some(route_state) = self
                .routes
                .get_mut(&(*caller.family(), pending.route.clone()))
            {
                route_state.release_worker_slot(pending.worker_slot, latency_us);
            }
        }
    }

    pub(in crate::domains::rpc::sink) fn cleanup_queued_session(
        &mut self,
        session_id: u64,
    ) -> usize {
        let queued_to_remove: Vec<(RouteFamily, uuid::Uuid)> = self
            .queued
            .iter()
            .filter(|(_, queued)| queued.caller_session_id == session_id)
            .map(|(key, _)| (key.family, key.correlation_id))
            .collect();

        for (family, correlation_id) in &queued_to_remove {
            self.remove_queued_request_for_family(*family, correlation_id);
        }

        queued_to_remove.len()
    }

    pub(in crate::domains::rpc::sink) fn expire_timed_out(
        &mut self,
        now: Instant,
    ) -> RpcPendingTimeoutResult {
        let mut timeout_deliveries = Vec::new();
        let mut removed_pending = 0usize;
        let mut closed_caller_drops = 0usize;

        while let Some(expiring) = self.pending.expirations.peek() {
            if expiring.expires_at > now {
                break;
            }

            let expiring = self
                .pending
                .expirations
                .pop()
                .expect("pending expiration entry");
            let Some(pending) = self.pending.pending.get(&expiring.key) else {
                continue;
            };

            if pending.expires_at != expiring.expires_at {
                continue;
            }

            let pending = self
                .pending
                .pending
                .remove(&expiring.key)
                .expect("tracked pending request");
            self.release_worker_for_pending(&pending, None);
            if let Some(caller) = pending.caller_inbox_addr.as_ref() {
                self.prune_route_if_empty_for_family(*caller.family(), &pending.route);
            }
            removed_pending = removed_pending.saturating_add(1);

            if let Some(caller_inbox_addr) = pending.caller_inbox_addr {
                timeout_deliveries.push(RpcPendingErrorDelivery {
                    correlation_id: expiring.key.correlation_id,
                    caller_session_id: pending.caller_session_id,
                    caller_inbox_addr,
                });
            } else {
                closed_caller_drops = closed_caller_drops.saturating_add(1);
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
            let Some(queued) = self.queued.get(&expiring.key) else {
                continue;
            };

            if queued.expires_at != expiring.expires_at {
                continue;
            }

            let queued = self
                .remove_queued_request_for_family(expiring.key.family, &expiring.key.correlation_id)
                .expect("tracked queued request");
            removed_pending = removed_pending.saturating_add(1);
            timeout_deliveries.push(RpcPendingErrorDelivery {
                correlation_id: expiring.key.correlation_id,
                caller_session_id: queued.caller_session_id,
                caller_inbox_addr: queued.caller_inbox_addr,
            });
        }

        RpcPendingTimeoutResult {
            removed_pending,
            pending_len: self.live_request_count(),
            closed_caller_drops,
            timeout_deliveries,
        }
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn route_count(&self) -> usize {
        self.routes.len()
    }
}
