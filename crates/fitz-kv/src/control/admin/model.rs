use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTransaction {
    pub route_family: u64,
    pub tx_id: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub mode: String,
    pub started_at: String,
    pub operations_count: usize,
    pub idle_seconds: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct KvLatencySnapshot {
    pub avg_ms: f64,
    pub p95_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResourceInventoryEntry {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub estimated_record_count: u64,
    pub estimated_storage_bytes: u64,
    pub estimate_complete: bool,
    pub read_latency_avg_ms: f64,
    pub read_latency_p95_ms: f64,
    pub write_latency_avg_ms: f64,
    pub write_latency_p95_ms: f64,
    pub transactions_active: usize,
}

impl KvTransaction {
    #[must_use]
    pub fn snapshot(
        route_family: u64,
        tx_id: u64,
        session_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        started_at: &str,
    ) -> Self {
        Self {
            route_family,
            tx_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            mode: format!("session:{session_id}:readwrite"),
            started_at: started_at.to_string(),
            operations_count: 0,
            idle_seconds: 0,
        }
    }
}

use fitz_core::projection::{FamilyRow, ResourceIdentity};

macro_rules! resource_row {
    ($field:ident; $($ty:ty),+) => { $(impl FamilyRow for $ty {
        fn family(&self) -> u64 { self.$field }
        fn inventory_identity(&self) -> Option<ResourceIdentity> { Some((self.$field, self.realm.clone(), self.area.clone(), self.resource.clone())) }
    })+ };
}
resource_row!(route_family; KvTransaction);
