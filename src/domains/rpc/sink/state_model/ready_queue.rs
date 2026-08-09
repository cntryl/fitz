use super::{FxBuildHasher, HashMap, Route, RouteFamily, RpcFastMap, VecDeque};

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
