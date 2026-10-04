//! Family-indexed projections. Family selection precedes cloning or scan admission.
use super::super::{
    KvTransaction, NoticeRouteInfo, NoticeSubscription, QueueDeadLetter, QueueInflight, QueueInfo,
    RpcPendingRequest, RpcWorker, SessionInfo, StreamInfo,
};
use crate::runtime::routing::{route_quad, route_triplet};
use std::collections::{BTreeMap, HashMap};
pub(super) type ResourceIdentity = (u64, String, String, String);

pub(super) trait FamilyRow {
    fn family(&self) -> u64;
    fn inventory_identity(&self) -> Option<ResourceIdentity>;
}

macro_rules! resource_row {
    ($field:ident; $($ty:ty),+) => { $(impl FamilyRow for $ty {
        fn family(&self) -> u64 { self.$field }
        fn inventory_identity(&self) -> Option<ResourceIdentity> { Some((self.$field, self.realm.clone(), self.area.clone(), self.resource.clone())) }
    })+ };
}
macro_rules! route_row {
    ($field:ident; $($ty:ty),+) => { $(impl FamilyRow for $ty {
        fn family(&self) -> u64 { self.route_family }
        fn inventory_identity(&self) -> Option<ResourceIdentity> {
            let route = &self.$field;
            route_quad(route).map(|parts| (self.route_family, parts.realm.to_string(), parts.area.to_string(), parts.resource.to_string()))
                .or_else(|| route_triplet(route).map(|parts| (self.route_family, parts.realm.to_string(), parts.area.to_string(), parts.resource.to_string())))
        }
    })+ };
}
resource_row!(route_family; KvTransaction, StreamInfo);
resource_row!(family; QueueInfo, QueueInflight, QueueDeadLetter);
route_row!(route; NoticeRouteInfo, RpcWorker, RpcPendingRequest);
route_row!(pattern; NoticeSubscription);

pub(super) struct FamilyRows<T> {
    families: BTreeMap<u64, Vec<T>>,
    resources: BTreeMap<ResourceIdentity, usize>,
}

impl<T> Default for FamilyRows<T> {
    fn default() -> Self {
        Self {
            families: BTreeMap::new(),
            resources: BTreeMap::new(),
        }
    }
}

impl<T: FamilyRow> From<Vec<T>> for FamilyRows<T> {
    fn from(items: Vec<T>) -> Self {
        let mut rows = Self::default();
        rows.extend(items);
        rows
    }
}

impl<T: FamilyRow> FamilyRows<T> {
    pub(super) fn iter(&self) -> impl Iterator<Item = &T> {
        self.families.values().flatten()
    }

    pub(super) fn replace_matching(&mut self, item: T, matches: impl Fn(&T) -> bool) {
        self.retain(|existing| !matches(existing));
        self.push(item);
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.families.values().map(Vec::len).sum()
    }

    pub(super) fn push(&mut self, item: T) {
        if let Some(identity) = item.inventory_identity() {
            *self.resources.entry(identity).or_default() += 1;
        }
        self.families.entry(item.family()).or_default().push(item);
    }

    pub(super) fn extend(&mut self, items: impl IntoIterator<Item = T>) {
        for item in items {
            self.push(item);
        }
    }

    pub(super) fn retain(&mut self, mut include: impl FnMut(&T) -> bool) {
        self.families.retain(|_, items| {
            items.retain(|item| {
                if include(item) {
                    return true;
                }
                if let Some(identity) = item.inventory_identity() {
                    if let Some(count) = self.resources.get_mut(&identity) {
                        *count -= 1;
                        if *count == 0 {
                            self.resources.remove(&identity);
                        }
                    }
                }
                false
            });
            !items.is_empty()
        });
    }

    pub(super) fn resource_page(
        &self,
        scope: super::inventory::InventoryScope<'_>,
        after: Option<&ResourceIdentity>,
        limit: usize,
    ) -> Vec<ResourceIdentity> {
        let start = after.cloned().unwrap_or_else(|| scope.start());
        self.resources
            .range(start..)
            .take_while(|(key, _)| scope.contains(key))
            .take(limit)
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub(super) fn scoped(&self, family: Option<u64>) -> impl Iterator<Item = &T> {
        let (start, end) = family.map_or((0, u64::MAX), |value| (value, value));
        self.families
            .range(start..=end)
            .flat_map(|(_, items)| items)
    }
}

/// Sessions retain one record, indexed by family and session ID.
#[derive(Default)]
pub(super) struct SessionRows {
    records: BTreeMap<(u64, u64), SessionInfo>,
    families: HashMap<u64, u64>,
}

impl SessionRows {
    pub(super) fn insert(&mut self, id: u64, info: SessionInfo) {
        self.remove(id);
        self.families.insert(id, info.route_family);
        self.records.insert((info.route_family, id), info);
    }

    pub(super) fn get(&self, id: u64) -> Option<&SessionInfo> {
        self.families
            .get(&id)
            .and_then(|family| self.records.get(&(*family, id)))
    }

    pub(super) fn remove(&mut self, id: u64) {
        if let Some(family) = self.families.remove(&id) {
            self.records.remove(&(family, id));
        }
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &SessionInfo> {
        self.records.values()
    }

    pub(super) fn scoped(&self, family: Option<u64>) -> impl Iterator<Item = &SessionInfo> {
        let (start, end) = family.map_or(((0, 0), (u64::MAX, u64::MAX)), |value| {
            ((value, 0), (value, u64::MAX))
        });
        self.records.range(start..=end).map(|(_, info)| info)
    }
}
