use super::super::SessionInfo;
use std::collections::{BTreeMap, HashMap};

pub(super) use fitz_core::projection::ResourceIdentity;

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
