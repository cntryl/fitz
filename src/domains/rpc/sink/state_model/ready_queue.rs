use super::{
    FxBuildHasher, HashMap, Route, RouteFamily, RpcCorrelationKey, RpcFastMap, RpcPendingRequest,
    RpcPendingRequestInit, RpcQueuedDispatch, RpcQueuedRequest, RpcRouteState, RpcState, VecDeque,
};

/// Owns fair ready-route ordering independently from route queue storage.
pub(in crate::domains::rpc::sink) struct RouteReadyQueue {
    by_family: RpcFastMap<RouteFamily, VecDeque<Route>>,
}

impl RouteReadyQueue {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            by_family: HashMap::with_capacity_and_hasher(8, FxBuildHasher),
        }
    }

    pub(in crate::domains::rpc::sink) fn push(&mut self, family: RouteFamily, route: Route) {
        self.by_family.entry(family).or_default().push_back(route);
    }

    pub(in crate::domains::rpc::sink) fn push_front(&mut self, family: RouteFamily, route: Route) {
        self.by_family.entry(family).or_default().push_front(route);
    }

    pub(in crate::domains::rpc::sink) fn clear(&mut self, family: RouteFamily) {
        if let Some(routes) = self.by_family.get_mut(&family) {
            routes.clear();
        }
    }

    pub(in crate::domains::rpc::sink) fn remove(&mut self, family: RouteFamily, route: &Route) {
        if let Some(routes) = self.by_family.get_mut(&family) {
            routes.retain(|ready_route| ready_route != route);
        }
    }

    pub(in crate::domains::rpc::sink) fn pop(&mut self, family: RouteFamily) -> Option<Route> {
        self.by_family.get_mut(&family)?.pop_front()
    }
}

impl RpcState {
    pub(in crate::domains::rpc::sink) fn next_ready_dispatch_for_family(
        &mut self,
        family: RouteFamily,
    ) -> Option<RpcQueuedDispatch> {
        // A queued route whose shared credit is exhausted keeps its place and
        // its ready mark. Dropping it would let routes re-marked later in
        // first-seen order take every freed credit and starve it.
        let mut blocked_routes = Vec::new();
        let dispatch = loop {
            let Some(route) = self.ready_routes.pop(family) else {
                break None;
            };
            let has_queued_requests = self
                .routes
                .get(&(family, route.clone()))
                .is_some_and(RpcRouteState::has_queued_requests);
            if !has_queued_requests {
                if let Some(route_state) = self.routes.get_mut(&(family, route.clone())) {
                    route_state.clear_ready();
                }
                continue;
            }
            if let Some(dispatch) = self.dispatch_queued_route(family, &route) {
                break Some(dispatch);
            }
            blocked_routes.push(route);
        };
        for route in blocked_routes.into_iter().rev() {
            self.ready_routes.push_front(family, route);
        }
        dispatch
    }

    /// Dispatches the oldest queued request on a ready route that has queued
    /// work, or returns `None` when the route is empty or no registration has credit.
    pub(super) fn dispatch_queued_route(
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
        if let Some(route_state) = self.routes.get_mut(&(family, route.clone())) {
            route_state.clear_ready();
        }
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
}
