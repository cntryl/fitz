use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, HashSet, Instant,
    RegistrationTable, Route, RouteAddress, RouteFamily, RouteReadyQueue, RpcCorrelationKey,
    RpcFastMap, RpcPendingDispatchInfo, RpcPendingErrorDelivery, RpcPendingRequest,
    RpcPendingRequestInit, RpcPendingTable, RpcPendingTimeoutResult, RpcQueuedDispatch,
    RpcQueuedRequest, RpcRegistrationId, RpcRequestDispatch, RpcRequestRejection, RpcRouteState,
    RpcSessionCleanupResult, RpcWorker, RpcWorkerCleanupResult, RpcWorkerDispatch, RpcWorkerKey,
};

/// Coordinates registration, route fairness, and pending-request collaborators.
pub(in crate::domains::rpc::sink) struct RpcState {
    pub(in crate::domains::rpc::sink) routes: RpcFastMap<(RouteFamily, Route), RpcRouteState>,
    pub(in crate::domains::rpc::sink) registrations: RegistrationTable,
    ready_routes: RouteReadyQueue,
    next_route_sequence: u64,
    pub(in crate::domains::rpc::sink) pending: RpcPendingTable,
    pub(in crate::domains::rpc::sink) queued: RpcFastMap<RpcCorrelationKey, RpcQueuedRequest>,
    pub(in crate::domains::rpc::sink) queued_expirations: BinaryHeap<ExpiringPendingRequest>,
}

pub(in crate::domains::rpc::sink) enum RpcWorkerRegistration {
    Registered,
    Existing,
    WildcardLimit,
}

enum DispatchAction {
    Queue,
    Dispatch(RpcWorkerDispatch),
}

pub(in crate::domains::rpc::sink) trait RpcDispatchState {
    fn claim_registration_for_route(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> Option<RpcWorkerDispatch>;
}

pub(in crate::domains::rpc::sink) trait RpcRequestState {
    fn register(&mut self, registration: RpcWorker) -> RpcWorkerRegistration;

    #[allow(clippy::too_many_arguments)]
    fn dispatch_or_queue(
        &mut self,
        request: crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        request_timeout: std::time::Duration,
        route_pending_capacity: usize,
        global_pending_capacity: usize,
        global_pending_count: Option<&std::sync::atomic::AtomicUsize>,
    ) -> RpcRequestDispatch;
}

pub(in crate::domains::rpc::sink) trait RpcResponseState {
    fn pending_for_response(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        registration_session_id: u64,
        response_seq: u64,
        stream_end: bool,
    ) -> super::RpcPendingResponseDisposition;

    fn live_count(&self) -> usize;

    fn release_dispatch(&mut self, pending: &RpcPendingDispatchInfo, latency_us: Option<u64>);

    /// Advance past a chunk the caller has actually received; a `stream_end`
    /// chunk completes and drops the request.
    fn commit_response_delivery(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        stream_end: bool,
    ) -> bool;

    /// Record a failed delivery attempt, returning the consecutive count.
    fn record_delivery_failure(&mut self, family: RouteFamily, correlation_id: &uuid::Uuid) -> u32;

    /// Drop a live pending request without delivering anything further on it.
    ///
    /// Used when a response chunk could not be handed to the caller: the
    /// stream has a hole, so it must end rather than continue with later
    /// chunks that would silently present as contiguous.
    fn abandon_pending(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<RpcPendingDispatchInfo>;
}

impl RpcDispatchState for RpcState {
    fn claim_registration_for_route(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> Option<RpcWorkerDispatch> {
        self.claim_registration(family, route)
    }
}

impl RpcRequestState for RpcState {
    fn register(&mut self, registration: RpcWorker) -> RpcWorkerRegistration {
        self.register_registration(registration)
    }

    fn dispatch_or_queue(
        &mut self,
        request: crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        request_timeout: std::time::Duration,
        route_pending_capacity: usize,
        global_pending_capacity: usize,
        global_pending_count: Option<&std::sync::atomic::AtomicUsize>,
    ) -> RpcRequestDispatch {
        self.dispatch_or_queue_request_with_global_count(
            request,
            caller_session_id,
            caller_inbox_addr,
            request_timeout,
            route_pending_capacity,
            global_pending_capacity,
            global_pending_count,
        )
    }
}

impl RpcResponseState for RpcState {
    fn pending_for_response(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        registration_session_id: u64,
        response_seq: u64,
        stream_end: bool,
    ) -> super::RpcPendingResponseDisposition {
        self.pending.pending_for_response_in_family(
            family,
            correlation_id,
            registration_session_id,
            response_seq,
            stream_end,
        )
    }

    fn live_count(&self) -> usize {
        self.live_request_count()
    }

    fn release_dispatch(&mut self, pending: &RpcPendingDispatchInfo, latency_us: Option<u64>) {
        self.release_registration_for_dispatch_info(pending, latency_us);
    }

    fn commit_response_delivery(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        stream_end: bool,
    ) -> bool {
        self.pending
            .commit_response_delivery(family, correlation_id, stream_end)
    }

    fn record_delivery_failure(&mut self, family: RouteFamily, correlation_id: &uuid::Uuid) -> u32 {
        self.pending.record_delivery_failure(family, correlation_id)
    }

    fn abandon_pending(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<RpcPendingDispatchInfo> {
        self.pending
            .remove(&super::RpcCorrelationKey {
                family,
                correlation_id: *correlation_id,
            })
            .map(super::RpcPendingRequest::into_dispatch_info)
    }
}

impl RpcState {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            routes: HashMap::with_capacity_and_hasher(64, FxBuildHasher),
            registrations: RegistrationTable::new(),
            ready_routes: RouteReadyQueue::new(),
            next_route_sequence: 1,
            pending: RpcPendingTable::new(),
            queued: HashMap::with_capacity_and_hasher(256, FxBuildHasher),
            queued_expirations: BinaryHeap::with_capacity(256),
        }
    }

    pub(in crate::domains::rpc::sink) fn register_registration(
        &mut self,
        registration: RpcWorker,
    ) -> RpcWorkerRegistration {
        let key = RpcWorkerKey::from_parts(&registration.addr, registration.session_id);
        if self.registrations.contains_registration(&key) {
            return RpcWorkerRegistration::Existing;
        }

        if let Some(violation) = self.registration_policy_violation(&registration) {
            return violation;
        }

        let family = *registration.addr.family();
        let matching_routes = self
            .routes
            .keys()
            .filter(|(route_family, route)| {
                *route_family == family && registration.matches(family, route)
            })
            .cloned()
            .collect::<Vec<_>>();
        let registration_id = self.registrations.insert(key, registration);
        for route_key in matching_routes {
            if let Some(route_state) = self.routes.get_mut(&route_key) {
                route_state.add_registration(registration_id);
            }
        }
        self.enqueue_eligible_routes_for_family(family);
        RpcWorkerRegistration::Registered
    }

    fn registration_policy_violation(
        &self,
        registration: &RpcWorker,
    ) -> Option<RpcWorkerRegistration> {
        let session_wildcard_count = self
            .registrations
            .values()
            .filter(|existing| existing.session_id == registration.session_id)
            .filter(|existing| existing.is_wildcard())
            .count();
        if crate::domains::subscription_state::wildcard_registration_limit_reached(
            registration.pattern(),
            session_wildcard_count,
        ) {
            return Some(RpcWorkerRegistration::WildcardLimit);
        }
        None
    }

    pub(in crate::domains::rpc::sink) fn registration_count(&self) -> usize {
        self.registrations.len()
    }

    pub(in crate::domains::rpc::sink) fn ensure_route_state_for_family(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> &mut RpcRouteState {
        let key = (family, route.clone());
        if !self.routes.contains_key(&key) {
            let first_seen_sequence = self.next_route_sequence;
            self.next_route_sequence = self.next_route_sequence.wrapping_add(1).max(1);
            let registration_ids = self
                .registrations
                .iter()
                .filter_map(|(registration_id, registration)| {
                    registration
                        .matches(family, route)
                        .then_some(*registration_id)
                })
                .collect();
            self.routes.insert(
                key.clone(),
                RpcRouteState::new(first_seen_sequence, registration_ids),
            );
        }
        self.routes.get_mut(&key).expect("ensured RPC route state")
    }

    fn has_matching_registration(&self, family: RouteFamily, route: &Route) -> bool {
        self.routes
            .get(&(family, route.clone()))
            .is_some_and(RpcRouteState::has_registrations)
    }

    fn has_available_registration(&self, family: RouteFamily, route: &Route) -> bool {
        self.available_registration(family, route).is_some()
    }

    /// Selects the next available registration using the route's rotation cursor.
    fn available_registration(
        &self,
        family: RouteFamily,
        route: &Route,
    ) -> Option<(usize, RpcRegistrationId)> {
        let route_state = self.routes.get(&(family, route.clone()))?;
        let registration_ids = route_state.registration_ids();
        let registration_count = registration_ids.len();
        if registration_count == 0 {
            return None;
        }
        let start = route_state.next_registration_index() % registration_count;
        (0..registration_count).find_map(|offset| {
            let index = (start + offset) % registration_count;
            let registration_id = registration_ids[index];
            self.registrations
                .get(registration_id)
                .is_some_and(RpcWorker::is_available)
                .then_some((index, registration_id))
        })
    }

    /// Claims one registration credit and advances fairness only after selection.
    fn claim_registration(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> Option<RpcWorkerDispatch> {
        self.ensure_route_state_for_family(family, route);
        let (registration_index, registration_id) = self.available_registration(family, route)?;

        let registration = self
            .registrations
            .get_mut(registration_id)
            .expect("selected RPC registration");
        registration.claim_slot();
        let dispatch = registration.dispatch_view();
        self.ensure_route_state_for_family(family, route)
            .advance_registration_cursor(registration_index);
        Some(dispatch)
    }

    fn remove_registration_from_routes(&mut self, registration_id: RpcRegistrationId) {
        for route_state in self.routes.values_mut() {
            route_state.remove_registration(registration_id);
        }
    }

    fn remove_registrations_from_routes(&mut self, registration_ids: &HashSet<RpcRegistrationId>) {
        for route_state in self.routes.values_mut() {
            route_state.remove_registrations(registration_ids);
        }
    }

    fn release_slot(
        &mut self,
        registration_id: RpcRegistrationId,
        latency_us: Option<u64>,
    ) -> Option<RouteFamily> {
        let registration = self.registrations.get_mut(registration_id)?;
        let was_available = registration.is_available();
        if let Some(latency_us) = latency_us {
            registration.record_completion(latency_us);
        }
        registration.release_slot();
        (!was_available && registration.is_available()).then_some(*registration.addr.family())
    }

    fn mark_route_ready_if_eligible(&mut self, family: RouteFamily, route: &Route) {
        let eligible = self
            .routes
            .get(&(family, route.clone()))
            .is_some_and(RpcRouteState::has_queued_requests)
            && self.has_available_registration(family, route);
        if !eligible {
            return;
        }

        if self
            .routes
            .get_mut(&(family, route.clone()))
            .is_some_and(RpcRouteState::mark_ready)
        {
            self.ready_routes.push(family, route.clone());
        }
    }

    fn refresh_ready_routes_for_family(&mut self, family: RouteFamily) {
        // Registration and cleanup are control-plane churn, so rebuilding the family's sorted
        // route set is intentionally simple. Benchmark high-churn, high-cardinality families
        // before replacing this O(routes log routes) path with incremental bookkeeping.
        self.ready_routes.clear(family);
        let mut routes: Vec<(u64, Route)> = self
            .routes
            .iter_mut()
            .filter_map(|((route_family, route), route_state)| {
                if *route_family != family {
                    return None;
                }
                route_state.clear_ready();
                route_state
                    .has_queued_requests()
                    .then(|| (route_state.first_seen_sequence, route.clone()))
            })
            .collect();
        routes.sort_by_key(|(first_seen_sequence, _)| *first_seen_sequence);
        for (_, route) in routes {
            self.mark_route_ready_if_eligible(family, &route);
        }
    }

    fn enqueue_eligible_routes_for_family(&mut self, family: RouteFamily) {
        let mut routes: Vec<(u64, Route)> = self
            .routes
            .iter()
            .filter(|((route_family, _), route_state)| {
                *route_family == family && route_state.has_queued_requests()
            })
            .map(|((_, route), route_state)| (route_state.first_seen_sequence, route.clone()))
            .collect();
        routes.sort_by_key(|(first_seen_sequence, _)| *first_seen_sequence);
        for (_, route) in routes {
            self.mark_route_ready_if_eligible(family, &route);
        }
    }

    fn remove_ready_route(&mut self, family: RouteFamily, route: &Route) {
        if let Some(route_state) = self.routes.get_mut(&(family, route.clone())) {
            route_state.clear_ready();
        }
        self.ready_routes.remove(family, route);
    }

    fn prune_route_if_unused(&mut self, family: RouteFamily, route: &Route) {
        let key = (family, route.clone());
        let should_remove = self
            .routes
            .get(&key)
            .is_some_and(|route_state| !route_state.has_queued_requests())
            && !self.pending.has_pending_for_route(family, route);
        if should_remove {
            self.remove_ready_route(family, route);
            self.routes.remove(&key);
        }
    }

    fn prune_unused_routes_for_family(&mut self, family: RouteFamily) {
        let routes: Vec<Route> = self
            .routes
            .keys()
            .filter(|(route_family, _)| *route_family == family)
            .map(|(_, route)| route.clone())
            .collect();
        for route in routes {
            self.prune_route_if_unused(family, &route);
        }
    }

    fn remove_registrations_for_session(&mut self, session_id: u64) -> Vec<RpcRegistrationId> {
        let registration_ids = self
            .registrations
            .remove_session(session_id)
            .into_iter()
            .map(|(registration_id, _)| registration_id)
            .collect::<Vec<_>>();
        let registration_ids_set: HashSet<RpcRegistrationId> =
            registration_ids.iter().copied().collect();
        if !registration_ids_set.is_empty() {
            self.remove_registrations_from_routes(&registration_ids_set);
        }
        registration_ids
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcSessionCleanupResult {
        let mut affected_families: Vec<RouteFamily> = self
            .registrations
            .values()
            .filter_map(|registration| {
                (registration.session_id == session_id).then_some(*registration.addr.family())
            })
            .collect();
        affected_families.sort_by_key(RouteFamily::id);
        affected_families.dedup();
        let removed_registration_ids = self.remove_registrations_for_session(session_id);
        let pending_cleanup = self.pending.cleanup_session(session_id);
        let queued_removed = self.cleanup_queued_session(session_id);

        for family in affected_families {
            self.refresh_ready_routes_for_family(family);
            self.prune_unused_routes_for_family(family);
        }

        RpcSessionCleanupResult {
            removed_registrations: removed_registration_ids.len(),
            detached_callers: pending_cleanup.detached_callers,
            removed_pending: pending_cleanup.removed_pending + queued_removed,
            pending_len: self.live_request_count(),
            disconnect_deliveries: pending_cleanup.disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn unregister_registration(
        &mut self,
        registration_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let key = RpcWorkerKey::from_parts(registration_addr, session_id);
        let Some((registration_id, registration)) = self.registrations.remove_by_key(&key) else {
            return RpcWorkerCleanupResult {
                pending_len: self.live_request_count(),
                ..RpcWorkerCleanupResult::default()
            };
        };
        self.remove_registration_from_routes(registration_id);
        let family = *registration.addr.family();
        let pending_cleanup = self
            .pending
            .cleanup_registrations(&HashSet::from([registration_id]));
        self.refresh_ready_routes_for_family(family);
        self.prune_unused_routes_for_family(family);

        RpcWorkerCleanupResult {
            removed_registrations: 1,
            removed_pending: pending_cleanup.removed_pending,
            pending_len: self.live_request_count(),
            disconnect_deliveries: pending_cleanup.disconnect_deliveries,
        }
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

    fn check_duplicate(&self, family: RouteFamily, correlation_id: &uuid::Uuid) -> bool {
        self.contains_correlation_in_family(family, correlation_id)
    }

    fn check_capacity(
        &self,
        family: RouteFamily,
        route: &Route,
        route_pending_capacity: usize,
    ) -> bool {
        self.routes
            .get(&(family, route.clone()))
            .is_some_and(|state| {
                state.has_queued_requests() && state.queued_len() >= route_pending_capacity
            })
    }

    /// Reserves one unit of global pending capacity without oversubscribing the shared limit.
    fn reserve_global_capacity(
        global_pending_count: Option<&std::sync::atomic::AtomicUsize>,
        local_live_request_count: usize,
        global_pending_capacity: usize,
    ) -> Option<bool> {
        let Some(global_pending_count) = global_pending_count else {
            return (local_live_request_count < global_pending_capacity).then_some(false);
        };
        let mut current = global_pending_count.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if current >= global_pending_capacity {
                return None;
            }
            match global_pending_count.compare_exchange_weak(
                current,
                current.saturating_add(1),
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => return Some(true),
                Err(observed) => current = observed,
            }
        }
    }

    fn select_action(
        &mut self,
        family: RouteFamily,
        route: &Route,
        correlation_id: uuid::Uuid,
    ) -> Option<DispatchAction> {
        let should_queue = self
            .routes
            .get(&(family, route.clone()))
            .is_some_and(RpcRouteState::has_queued_requests)
            || !self.has_available_registration(family, route);
        if should_queue {
            self.ensure_route_state_for_family(family, route)
                .enqueue_request(correlation_id);
            Some(DispatchAction::Queue)
        } else {
            self.claim_registration_for_route(family, route)
                .map(DispatchAction::Dispatch)
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Coordinates duplicate, capacity, fairness, and tracking policy for one request.
    pub(in crate::domains::rpc::sink) fn dispatch_or_queue_request_with_global_count(
        &mut self,
        request: crate::domains::rpc::protocol::RpcRequest,
        caller_session_id: u64,
        caller_inbox_addr: RouteAddress,
        request_timeout: std::time::Duration,
        route_pending_capacity: usize,
        global_pending_capacity: usize,
        global_pending_count: Option<&std::sync::atomic::AtomicUsize>,
    ) -> RpcRequestDispatch {
        let family = *caller_inbox_addr.family();
        if self.check_duplicate(family, &request.correlation_id) {
            return RpcRequestDispatch::Rejected {
                request,
                reason: RpcRequestRejection::Duplicate,
            };
        }
        let route = request.route.clone();
        self.ensure_route_state_for_family(family, &route);
        if !self.has_matching_registration(family, &route) {
            self.prune_route_if_unused(family, &route);
            return RpcRequestDispatch::Rejected {
                request,
                reason: RpcRequestRejection::NoWorkers,
            };
        }

        let local_live_request_count = self.live_request_count();
        if self.check_capacity(family, &route, route_pending_capacity) {
            return RpcRequestDispatch::Rejected {
                request,
                reason: RpcRequestRejection::RouteCapacityFull,
            };
        }

        let Some(reserved_global_capacity) = Self::reserve_global_capacity(
            global_pending_count,
            local_live_request_count,
            global_pending_capacity,
        ) else {
            self.prune_route_if_unused(family, &route);
            return RpcRequestDispatch::Rejected {
                request,
                reason: RpcRequestRejection::GlobalCapacityFull,
            };
        };

        let correlation_id = request.correlation_id;
        let Some(action) = self.select_action(family, &route, correlation_id) else {
            if reserved_global_capacity {
                if let Some(global_pending_count) = global_pending_count {
                    global_pending_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
            }
            self.prune_route_if_unused(family, &route);
            return RpcRequestDispatch::Rejected {
                request,
                reason: RpcRequestRejection::NoWorkers,
            };
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
                self.mark_route_ready_if_eligible(family, &route);
                RpcRequestDispatch::Queued {
                    route,
                    correlation_id,
                    live_request_count: global_pending_count
                        .map_or(local_live_request_count.saturating_add(1), |count| {
                            count.load(std::sync::atomic::Ordering::Acquire)
                        }),
                }
            }
            DispatchAction::Dispatch(registration) => {
                self.pending.track_pending_for_family(
                    family,
                    correlation_id,
                    RpcPendingRequest::from_dispatch(
                        &request,
                        caller_session_id,
                        caller_inbox_addr,
                        &registration,
                        expires_at,
                    ),
                );
                RpcRequestDispatch::Immediate {
                    request,
                    registration,
                    live_request_count: global_pending_count
                        .map_or(local_live_request_count.saturating_add(1), |count| {
                            count.load(std::sync::atomic::Ordering::Acquire)
                        }),
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
        let route = queued.request.route.clone();
        if let Some(route_state) = self.routes.get_mut(&(family, route.clone())) {
            route_state.remove_queued_request(correlation_id);
        }
        self.remove_ready_route(family, &route);
        self.mark_route_ready_if_eligible(family, &route);
        self.prune_route_if_unused(family, &route);
        Some(queued)
    }

    pub(in crate::domains::rpc::sink) fn next_ready_dispatch_for_family(
        &mut self,
        family: RouteFamily,
    ) -> Option<RpcQueuedDispatch> {
        loop {
            let route = self.ready_routes.pop(family)?;
            if let Some(route_state) = self.routes.get_mut(&(family, route.clone())) {
                route_state.clear_ready();
            }
            if let Some(dispatch) = self.dispatch_queued_route(family, &route) {
                return Some(dispatch);
            }
        }
    }

    fn dispatch_queued_route(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> Option<RpcQueuedDispatch> {
        if !self
            .routes
            .get(&(family, route.clone()))?
            .has_queued_requests()
        {
            return None;
        }
        let registration = self.claim_registration(family, route)?;
        let correlation_id = self
            .routes
            .get_mut(&(family, route.clone()))?
            .pop_queued_request()
            .expect("queued RPC correlation id for dispatch");
        let queued = self
            .queued
            .remove(&RpcCorrelationKey {
                family,
                correlation_id,
            })
            .expect("queued RPC request for dispatch");
        let RpcQueuedRequest {
            request,
            caller_session_id,
            caller_inbox_addr,
            submitted_at,
            submitted_at_instant,
            expires_at,
        } = queued;
        let pending = RpcPendingRequest::new(RpcPendingRequestInit {
            route: request.route.clone(),
            caller_session_id,
            caller_inbox_addr,
            registration_addr: registration.addr.clone(),
            registration_session_id: registration.session_id,
            registration_id: registration.registration_id,
            submitted_at,
            submitted_at_instant,
            expires_at,
        });
        self.pending
            .track_pending_for_family(family, correlation_id, pending);
        self.mark_route_ready_if_eligible(family, route);

        Some(RpcQueuedDispatch {
            request,
            registration,
            live_request_count: self.live_request_count(),
        })
    }

    pub(in crate::domains::rpc::sink) fn remove_pending_request_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<(RpcPendingRequest, usize)> {
        let pending = self.pending.remove(&RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        })?;
        if let Some(family) = self.release_slot(pending.dispatch_info.registration_id, None) {
            self.enqueue_eligible_routes_for_family(family);
        }
        self.prune_route_if_unused(pending.dispatch_info.family, &pending.dispatch_info.route);
        Some((pending, self.live_request_count()))
    }

    pub(in crate::domains::rpc::sink) fn release_registration_for_pending(
        &mut self,
        pending: &RpcPendingRequest,
        latency_us: Option<u64>,
    ) {
        if let Some(family) = self.release_slot(pending.dispatch_info.registration_id, latency_us) {
            self.enqueue_eligible_routes_for_family(family);
        }
        self.prune_route_if_unused(pending.dispatch_info.family, &pending.dispatch_info.route);
    }

    pub(in crate::domains::rpc::sink) fn release_registration_for_dispatch_info(
        &mut self,
        pending: &RpcPendingDispatchInfo,
        latency_us: Option<u64>,
    ) {
        if let Some(family) = self.release_slot(pending.registration_id, latency_us) {
            self.enqueue_eligible_routes_for_family(family);
        }
        self.prune_route_if_unused(pending.family, &pending.route);
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
                .remove(&expiring.key)
                .expect("tracked pending request");
            self.release_registration_for_pending(&pending, None);
            removed_pending = removed_pending.saturating_add(1);
            if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                timeout_deliveries.push(RpcPendingErrorDelivery {
                    correlation_id: expiring.key.correlation_id,
                    caller_session_id: pending.dispatch_info.caller_session_id,
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
}

#[cfg(test)]
mod test_api;
