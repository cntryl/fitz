use super::{
    BTreeMap, FxBuildHasher, HashMap, HashSet, Route, RouteFamily, RpcFastMap, RpcRegistrationId,
    RpcWorker, RpcWorkerKey, RpcWorkerRegistration,
};

/// Owns registration identity, lookup, and lifecycle bookkeeping.
pub(in crate::domains::rpc::sink) struct RegistrationTable {
    by_id: BTreeMap<RpcRegistrationId, RpcWorker>,
    by_registration: RpcFastMap<RpcWorkerKey, RpcRegistrationId>,
    by_session: HashMap<u64, HashSet<RpcWorkerKey>>,
    session_counts: HashMap<u64, SessionRegistrationCount>,
    next_id: RpcRegistrationId,
}

#[derive(Default)]
struct SessionRegistrationCount {
    total: usize,
    wildcard: usize,
}

impl RegistrationTable {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            by_id: BTreeMap::new(),
            by_registration: HashMap::with_capacity_and_hasher(64, FxBuildHasher),
            by_session: HashMap::new(),
            session_counts: HashMap::new(),
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
        let (session_total_count, session_wildcard_count) = self
            .session_counts
            .get(&registration.session_id)
            .map_or((0, 0), |counts| (counts.total, counts.wildcard));
        match crate::domains::subscription_state::registration_limit_for_counts(
            session_total_count,
            registration.pattern(),
            session_wildcard_count,
        ) {
            Some(crate::domains::subscription_state::RegistrationLimit::Total) => {
                Some(RpcWorkerRegistration::TotalLimit)
            }
            Some(crate::domains::subscription_state::RegistrationLimit::Wildcard) => {
                Some(RpcWorkerRegistration::WildcardLimit)
            }
            None => None,
        }
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
            .by_session
            .get(&session_id)
            .into_iter()
            .flatten()
            .map(|key| *key.addr.family())
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
        let counts = self
            .session_counts
            .entry(registration.session_id)
            .or_default();
        counts.total = counts.total.saturating_add(1);
        if registration.is_wildcard() {
            counts.wildcard = counts.wildcard.saturating_add(1);
        }
        self.by_registration.insert(key.clone(), registration_id);
        self.by_session
            .entry(registration.session_id)
            .or_default()
            .insert(key);
        self.by_id.insert(registration_id, registration);
        crate::domains::subscription_state::record_registration_delta("rpc", true);
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
        let session_id = registration.session_id;
        if let Some(keys) = self.by_session.get_mut(&session_id) {
            keys.remove(key);
            if keys.is_empty() {
                self.by_session.remove(&session_id);
            }
        }
        if let Some(counts) = self.session_counts.get_mut(&session_id) {
            counts.total = counts.total.saturating_sub(1);
            if registration.is_wildcard() {
                counts.wildcard = counts.wildcard.saturating_sub(1);
            }
            if counts.total == 0 {
                self.session_counts.remove(&session_id);
            }
        }
        crate::domains::subscription_state::record_registration_delta("rpc", false);
        Some((registration_id, registration))
    }

    pub(in crate::domains::rpc::sink) fn remove_session(
        &mut self,
        session_id: u64,
    ) -> Vec<(RpcRegistrationId, RpcWorker)> {
        let keys = self
            .by_session
            .get(&session_id)
            .cloned()
            .unwrap_or_default();
        keys.into_iter()
            .filter_map(|key| self.remove_by_key(&key))
            .collect()
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn registration_count_for_session(
        &self,
        session_id: u64,
    ) -> usize {
        self.session_counts
            .get(&session_id)
            .map_or(0, |counts| counts.total)
    }
}

impl Drop for RegistrationTable {
    fn drop(&mut self) {
        for _ in 0..self.by_id.len() {
            crate::domains::subscription_state::record_registration_delta("rpc", false);
        }
    }
}
