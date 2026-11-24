use super::model::{RpcPendingRequest, RpcWorker};
use fitz_core::projection::{FamilyRow, FamilyRows};
use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;

fn matches_rpc_route_realm(realm: Option<&str>, route: &str) -> bool {
    realm.is_none_or(|needle| {
        parse_pattern_segments(route)
            .first()
            .is_some_and(|segment| match segment {
                PatternSegment::Literal(value) => value == needle,
                PatternSegment::Star | PatternSegment::DoubleStar => true,
            })
    })
}

fn collect_slice_matches<T: Clone + FamilyRow>(
    items: &FamilyRows<T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}
use fitz_core::runtime::matcher::{parse_pattern_segments, PatternSegment};

#[derive(Default)]
pub struct AdminReadModel {
    rpc_workers: RwLock<FamilyRows<RpcWorker>>,
    rpc_pending: RwLock<FamilyRows<RpcPendingRequest>>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_rpc_workers(&self, workers: Vec<RpcWorker>) {
        *self.rpc_workers.write() = workers.into();
    }

    pub fn replace_rpc_family_workers(&self, route_family: u64, workers: Vec<RpcWorker>) {
        let mut current = self.rpc_workers.write();
        current.retain(|worker| worker.route_family != route_family);
        current.extend(workers);
    }

    pub fn rpc_workers(&self, realm: Option<&str>) -> Vec<RpcWorker> {
        let workers = self.rpc_workers.read();
        collect_slice_matches(&workers, |item| matches_rpc_route_realm(realm, &item.route))
    }

    pub fn replace_rpc_pending(&self, requests: Vec<RpcPendingRequest>) {
        *self.rpc_pending.write() = requests.into();
    }

    pub fn replace_rpc_family_pending(&self, route_family: u64, requests: Vec<RpcPendingRequest>) {
        let mut current = self.rpc_pending.write();
        current.retain(|request| request.route_family != route_family);
        current.extend(requests);
    }

    pub fn rpc_pending(&self, realm: Option<&str>) -> Vec<RpcPendingRequest> {
        let pending = self.rpc_pending.read();
        collect_slice_matches(&pending, |item| matches_rpc_route_realm(realm, &item.route))
    }

    #[doc(hidden)]
    pub fn rpc_workers_rows(&self) -> RwLockReadGuard<'_, FamilyRows<RpcWorker>> {
        self.rpc_workers.read()
    }

    #[doc(hidden)]
    pub fn rpc_pending_rows(&self) -> RwLockReadGuard<'_, FamilyRows<RpcPendingRequest>> {
        self.rpc_pending.read()
    }
}
