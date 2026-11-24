use super::model::LeaseInfo;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::BTreeMap;
use std::sync::Arc;
type LeaseIdentity = (u64, String, String, String);
fn lease_identity_key(route_family: u64, realm: &str, area: &str, resource: &str) -> LeaseIdentity {
    (
        route_family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
    )
}

fn lease_identity_for(info: &LeaseInfo) -> LeaseIdentity {
    lease_identity_key(info.route_family, &info.realm, &info.area, &info.resource)
}

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

#[derive(Default)]
pub struct AdminReadModel {
    leases: RwLock<BTreeMap<LeaseIdentity, LeaseInfo>>,
    lease_waiter_counts: RwLock<BTreeMap<u64, usize>>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_leases(&self, leases: Vec<LeaseInfo>) {
        *self.leases.write() = leases
            .into_iter()
            .map(|lease| (lease_identity_for(&lease), lease))
            .collect();
    }

    pub fn set_lease_family_waiter_count(&self, route_family: u64, count: usize) -> usize {
        let mut counts = self.lease_waiter_counts.write();
        counts.insert(route_family, count);
        counts.values().sum()
    }

    pub fn lease_count(&self) -> usize {
        self.leases.read().len()
    }

    pub fn upsert_lease(&self, lease: LeaseInfo) {
        self.leases
            .write()
            .insert(lease_identity_for(&lease), lease);
    }

    pub fn remove_lease(&self, route_family: u64, realm: &str, area: &str, resource: &str) {
        self.leases
            .write()
            .remove(&lease_identity_key(route_family, realm, area, resource));
    }

    pub fn leases(&self, realm: Option<&str>) -> Vec<LeaseInfo> {
        let leases = self.leases.read();
        leases
            .values()
            .filter(|item| matches_realm(realm, &item.realm))
            .cloned()
            .collect()
    }

    pub fn leases_for_route_family(&self, route_family: u64) -> Vec<LeaseInfo> {
        let leases = self.leases.read();
        leases
            .values()
            .filter(|item| item.route_family == route_family)
            .cloned()
            .collect()
    }

    #[doc(hidden)]
    pub fn leases_rows(&self) -> RwLockReadGuard<'_, BTreeMap<LeaseIdentity, LeaseInfo>> {
        self.leases.read()
    }
}
