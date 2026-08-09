use super::*;

impl RpcState {
    pub(in crate::domains::rpc::sink) fn registration_count_for_family(
        &self,
        family: RouteFamily,
    ) -> usize {
        self.registrations
            .values()
            .filter(|registration| *registration.addr.family() == family)
            .count()
    }

    pub(in crate::domains::rpc::sink) fn registration_id_for(
        &self,
        addr: &RouteAddress,
        session_id: u64,
    ) -> Option<RpcRegistrationId> {
        self.registrations
            .id_for(&RpcWorkerKey::from_parts(addr, session_id))
    }

    pub(in crate::domains::rpc::sink) fn claim_registration_for_tests(
        &mut self,
        family: RouteFamily,
        route: &Route,
    ) -> Option<RpcWorkerDispatch> {
        self.claim_registration(family, route)
    }

    pub(in crate::domains::rpc::sink) fn release_registration_for_tests(
        &mut self,
        registration_id: RpcRegistrationId,
    ) {
        if let Some(family) = self.release_slot(registration_id, None) {
            self.enqueue_eligible_routes_for_family(family);
        }
    }

    pub(in crate::domains::rpc::sink) fn release_registration_with_latency_for_tests(
        &mut self,
        registration_id: RpcRegistrationId,
        latency_us: u64,
    ) {
        if let Some(family) = self.release_slot(registration_id, Some(latency_us)) {
            self.enqueue_eligible_routes_for_family(family);
        }
    }

    pub(in crate::domains::rpc::sink) fn ensure_route_state(
        &mut self,
        route: &Route,
    ) -> &mut RpcRouteState {
        self.ensure_route_state_for_family(RouteFamily::new(1), route)
    }

    pub(in crate::domains::rpc::sink) fn route_state(
        &mut self,
        route: &Route,
    ) -> Option<&mut RpcRouteState> {
        self.routes.get_mut(&(RouteFamily::new(1), route.clone()))
    }

    pub(in crate::domains::rpc::sink) fn contains_correlation(
        &self,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        self.contains_correlation_in_family(RouteFamily::new(1), correlation_id)
    }

    pub(in crate::domains::rpc::sink) fn queue_request(
        &mut self,
        correlation_id: uuid::Uuid,
        request: RpcQueuedRequest,
    ) {
        let route = request.request.route.clone();
        let family = *request.caller_inbox_addr.family();
        self.ensure_route_state_for_family(family, &route)
            .enqueue_request(correlation_id);
        let key = RpcCorrelationKey {
            family,
            correlation_id,
        };
        self.queued_expirations.push(ExpiringPendingRequest {
            expires_at: request.expires_at,
            key,
        });
        self.queued.insert(key, request);
        self.mark_route_ready_if_eligible(family, &route);
    }

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
        self.remove_ready_route(family, route);
        self.dispatch_queued_route(family, route)
    }

    pub(in crate::domains::rpc::sink) fn route_count(&self) -> usize {
        self.routes.len()
    }
}
