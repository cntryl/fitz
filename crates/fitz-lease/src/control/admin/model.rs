use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub owner_session_id: String,
    pub acquired_at: String,
    pub expires_at: String,
    pub renewals: usize,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseWaiterInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub owner_id: String,
    pub session_id: String,
    pub queued_token: u64,
    pub expires_at: String,
}

impl LeaseInfo {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn snapshot(
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        owner_session_id: &str,
        acquired_at: &str,
        expires_at: String,
        renewals: usize,
        fencing_token: u64,
    ) -> Self {
        Self {
            route_family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            owner_session_id: owner_session_id.to_string(),
            acquired_at: acquired_at.to_string(),
            expires_at,
            renewals,
            fencing_token,
        }
    }
}
