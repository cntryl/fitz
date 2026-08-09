use super::{
    BTreeMap, FxBuildHasher, HashMap, RpcFastMap, RpcRegistrationId, RpcWorker, RpcWorkerKey,
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

    pub(in crate::domains::rpc::sink) fn iter(
        &self,
    ) -> impl Iterator<Item = (&RpcRegistrationId, &RpcWorker)> {
        self.by_id.iter()
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
