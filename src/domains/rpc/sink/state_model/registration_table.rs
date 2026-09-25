use super::{
    BTreeMap, FxBuildHasher, HashMap, Route, RouteFamily, RpcFastMap, RpcRegistrationId, RpcWorker,
    RpcWorkerKey, RpcWorkerRegistration,
};

/// Owns registration identity, lookup, and lifecycle bookkeeping.
pub(in crate::domains::rpc::sink) struct RegistrationTable {
    by_id: BTreeMap<RpcRegistrationId, RpcWorker>,
    by_registration: RpcFastMap<RpcWorkerKey, RpcRegistrationId>,
    next_id: RpcRegistrationId,
}

impl RegistrationTable {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            by_id: BTreeMap::new(),
            by_registration: HashMap::with_capacity_and_hasher(64, FxBuildHasher),
            next_id: 1,
        }
    }

    pub(in crate::domains::rpc::sink) fn contains_registration(&self, key: &RpcWorkerKey) -> bool {
        self.by_registration.contains_key(key)
    }

    pub(in crate::domains::rpc::sink) fn registration_policy_violation(
        &self,
        key: &RpcWorkerKey,
        registration: &RpcWorker,
    ) -> Option<RpcWorkerRegistration> {
        if self.contains_registration(key) {
            return Some(RpcWorkerRegistration::Existing);
        }
        let session_wildcard_count = self
            .by_id
            .values()
            .filter(|existing| existing.session_id == registration.session_id)
            .filter(|existing| existing.is_wildcard())
            .count();
        crate::domains::subscription_state::wildcard_registration_limit_reached(
            registration.pattern(),
            session_wildcard_count,
        )
        .then_some(RpcWorkerRegistration::WildcardLimit)
    }

    pub(in crate::domains::rpc::sink) fn matching_ids(
        &self,
        family: RouteFamily,
        route: &Route,
    ) -> Vec<RpcRegistrationId> {
        self.by_id
            .iter()
            .filter_map(|(registration_id, registration)| {
                registration
                    .matches(family, route)
                    .then_some(*registration_id)
            })
            .collect()
    }

    pub(in crate::domains::rpc::sink) fn families_for_session(
        &self,
        session_id: u64,
    ) -> Vec<RouteFamily> {
        let mut families = self
            .by_id
            .values()
            .filter_map(|registration| {
                (registration.session_id == session_id).then_some(*registration.addr.family())
            })
            .collect::<Vec<_>>();
        families.sort_by_key(RouteFamily::id);
        families.dedup();
        families
    }

    pub(in crate::domains::rpc::sink) fn release_slot(
        &mut self,
        registration_id: RpcRegistrationId,
        latency_us: Option<u64>,
    ) -> Option<RouteFamily> {
        let registration = self.get_mut(registration_id)?;
        let was_available = registration.is_available();
        if let Some(latency_us) = latency_us {
            registration.record_completion(latency_us);
        }
        registration.release_slot();
        (!was_available && registration.is_available()).then_some(*registration.addr.family())
    }

    pub(in crate::domains::rpc::sink) fn insert(
        &mut self,
        key: RpcWorkerKey,
        mut registration: RpcWorker,
    ) -> RpcRegistrationId {
        let registration_id = self.allocate_id();
        registration.assign_registration_id(registration_id);
        self.by_registration.insert(key, registration_id);
        self.by_id.insert(registration_id, registration);
        registration_id
    }

    fn allocate_id(&mut self) -> RpcRegistrationId {
        loop {
            let candidate = self.next_id;
            self.next_id = self.next_id.wrapping_add(1).max(1);
            if !self.by_id.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn id_for(
        &self,
        key: &RpcWorkerKey,
    ) -> Option<RpcRegistrationId> {
        self.by_registration.get(key).copied()
    }

    pub(in crate::domains::rpc::sink) fn get(
        &self,
        registration_id: RpcRegistrationId,
    ) -> Option<&RpcWorker> {
        self.by_id.get(&registration_id)
    }

    pub(in crate::domains::rpc::sink) fn get_mut(
        &mut self,
        registration_id: RpcRegistrationId,
    ) -> Option<&mut RpcWorker> {
        self.by_id.get_mut(&registration_id)
    }

    pub(in crate::domains::rpc::sink) fn values(&self) -> impl Iterator<Item = &RpcWorker> {
        self.by_id.values()
    }

    pub(in crate::domains::rpc::sink) fn len(&self) -> usize {
        self.by_id.len()
    }

    pub(in crate::domains::rpc::sink) fn remove_by_key(
        &mut self,
        key: &RpcWorkerKey,
    ) -> Option<(RpcRegistrationId, RpcWorker)> {
        let registration_id = self.by_registration.remove(key)?;
        let registration = self
            .by_id
            .remove(&registration_id)
            .expect("indexed RPC registration");
        Some((registration_id, registration))
    }

    pub(in crate::domains::rpc::sink) fn remove_session(
        &mut self,
        session_id: u64,
    ) -> Vec<(RpcRegistrationId, RpcWorker)> {
        let keys = self
            .by_registration
            .keys()
            .filter(|key| key.session_id == session_id)
            .cloned()
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| self.remove_by_key(&key))
            .collect()
    }
}
