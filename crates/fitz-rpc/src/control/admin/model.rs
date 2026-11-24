use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RpcWorker {
    pub route_family: u64,
    pub session_id: String,
    pub realm: String,
    pub route: String,
    pub registered_at: String,
    pub requests_handled: u64,
    pub average_latency_ms: f64,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct RpcLatencyBuckets {
    pub under_5ms: usize,
    pub under_25ms: usize,
    pub under_100ms: usize,
    pub over_100ms: usize,
}

impl RpcLatencyBuckets {
    pub fn record_latency_ms(&mut self, latency_ms: f64) {
        if latency_ms < 5.0 {
            self.under_5ms += 1;
        } else if latency_ms < 25.0 {
            self.under_25ms += 1;
        } else if latency_ms < 100.0 {
            self.under_100ms += 1;
        } else {
            self.over_100ms += 1;
        }
    }

    pub fn merge(&mut self, other: RpcLatencyBuckets) {
        self.under_5ms += other.under_5ms;
        self.under_25ms += other.under_25ms;
        self.under_100ms += other.under_100ms;
        self.over_100ms += other.over_100ms;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingRequest {
    pub route_family: u64,
    pub correlation_id: String,
    pub route: String,
    pub submitted_at: String,
    pub age_seconds: u64,
    pub worker_session_id: Option<String>,
}

impl RpcWorker {
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    #[must_use]
    pub fn snapshot(
        route_family: u64,
        session_id: u64,
        realm: &str,
        route: &str,
        registered_at: &str,
        requests_handled: u64,
        average_latency_ms: f64,
    ) -> Self {
        Self {
            route_family,
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            route: route.to_string(),
            registered_at: registered_at.to_string(),
            requests_handled,
            average_latency_ms,
        }
    }
}

impl RpcPendingRequest {
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub fn snapshot(
        route_family: u64,
        correlation_id: &impl ToString,
        route: &str,
        submitted_at: &str,
        age_seconds: u64,
        worker_session_id: Option<String>,
    ) -> Self {
        Self {
            route_family,
            correlation_id: correlation_id.to_string(),
            route: route.to_string(),
            submitted_at: submitted_at.to_string(),
            age_seconds,
            worker_session_id,
        }
    }
}

use fitz_core::projection::{FamilyRow, ResourceIdentity};
use fitz_core::runtime::routing::{route_quad, route_triplet};

macro_rules! route_row {
    ($field:ident; $($ty:ty),+) => { $(impl FamilyRow for $ty {
        fn family(&self) -> u64 { self.route_family }
        fn inventory_identity(&self) -> Option<ResourceIdentity> {
            let route = &self.$field;
            route_quad(route).map(|parts| (self.route_family, parts.realm.to_string(), parts.area.to_string(), parts.resource.to_string()))
                .or_else(|| route_triplet(route).map(|parts| (self.route_family, parts.realm.to_string(), parts.area.to_string(), parts.resource.to_string())))
        }
        fn matches_resource(&self, realm: &str, area: &str, resource: &str) -> bool {
            fitz_core::runtime::matcher::matches_resource_registration(&self.$field, realm, area, resource)
        }
    })+ };
}
route_row!(route; RpcWorker, RpcPendingRequest);
