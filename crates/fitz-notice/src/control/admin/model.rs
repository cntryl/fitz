use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscription {
    pub route_family: u64,
    pub subscription_id: u64,
    pub session_id: String,
    pub realm: String,
    pub pattern: String,
    pub created_at: String,
    pub notifications_received: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRouteInfo {
    pub route_family: u64,
    pub route: String,
    pub subscribers: usize,
    pub publishes_total: u64,
    pub publishes_per_minute: f64,
}

impl NoticeSubscription {
    #[must_use]
    pub fn snapshot(
        route_family: u64,
        subscription_id: u64,
        session_id: u64,
        realm: &str,
        pattern: String,
        created_at: &str,
    ) -> Self {
        Self {
            route_family,
            subscription_id,
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            pattern,
            created_at: created_at.to_string(),
            notifications_received: 0,
        }
    }
}

impl NoticeRouteInfo {
    #[must_use]
    pub fn snapshot(route_family: u64, route: String, subscribers: usize) -> Self {
        Self {
            route_family,
            route,
            subscribers,
            publishes_total: 0,
            publishes_per_minute: 0.0,
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
route_row!(route; NoticeRouteInfo);
route_row!(pattern; NoticeSubscription);
