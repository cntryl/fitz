//! Public admin façade and crate-internal storage-backed implementations.

use super::state::{KvDomain, KvFamilyRuntime};
use crate::runtime::routing::RouteFamily;

mod inventory;
mod scans;
mod values;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminKvCommittedPair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminKvPrefixScanResult {
    pub items: Vec<AdminKvCommittedPair>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminKvRowsResult {
    pub items: Vec<AdminKvCommittedPair>,
    pub next_cursor: Option<Vec<u8>>,
    pub has_more: bool,
}

pub struct AdminKvRowsRequest<'a> {
    pub route_family: RouteFamily,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub starts_with: &'a [u8],
    pub cursor: Option<&'a [u8]>,
    pub limit: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum AdminKvRowsError {
    InvalidCursorPrefix,
    Backend(String),
}

impl std::fmt::Display for AdminKvRowsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCursorPrefix => {
                formatter.write_str("cursor must start with starts_with prefix")
            }
            Self::Backend(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for AdminKvRowsError {}

impl KvDomain {
    /// Restore a validated KV snapshot, replacing selected resources one at a time.
    ///
    /// # Errors
    /// Returns an error when decoding, validation, family dispatch, or storage fails.
    pub fn restore_kv_snapshot(&self, bytes: &[u8]) -> Result<Vec<String>, String> {
        let artifact = crate::snapshot::SnapshotArtifact::from_bytes(bytes)?;
        if artifact.domain() != crate::snapshot::SnapshotDomain::Kv {
            return Err("KV snapshot restore requires a KV artifact".to_string());
        }
        let family = RouteFamily::try_from(artifact.route_family())
            .map_err(|_| "snapshot route family is invalid".to_string())?;
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            super::commands::KvDomainCommand::RestoreSnapshot(artifact, reply_tx),
        )
        .map_err(|error| error.to_string())?;
        reply_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .map_err(|error| format!("KV snapshot restore did not complete: {error}"))?
    }

    #[cfg(test)]
    /// Read one family directly for storage-backed admin regression tests.
    pub(super) fn admin_inventory_for_family_for_tests(
        &self,
        family_id: u64,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        let mut core = self.admin_core();
        KvFamilyRuntime { core: &mut core }.admin_inventory_for_family(family_id)
    }

    /// Build an admin inventory snapshot for the requested route family scope.
    ///
    /// # Errors
    /// Returns an error when the underlying storage inventory scan fails.
    pub fn admin_inventory(
        &self,
        family: Option<RouteFamily>,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        let mut core = self.admin_core();
        KvFamilyRuntime { core: &mut core }.admin_inventory(family)
    }

    /// Read one admin inventory entry for a specific KV resource.
    ///
    /// # Errors
    /// Returns an error when storage reads, estimate refreshes, or estimate decoding fails.
    pub fn admin_inventory_resource(
        &self,
        route_family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<crate::control::admin::KvResourceInventoryEntry>, String> {
        let mut core = self.admin_core();
        KvFamilyRuntime { core: &mut core }.admin_inventory_resource(
            route_family,
            realm,
            area,
            resource,
        )
    }

    /// Read one committed KV value directly from storage for admin inspection.
    ///
    /// # Errors
    /// Returns an error when the storage transaction or read fails.
    pub fn admin_get_committed_value(
        &self,
        route_family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        let mut core = self.admin_core();
        KvFamilyRuntime { core: &mut core }.admin_get_committed_value(
            route_family,
            realm,
            area,
            resource,
            key,
        )
    }

    /// Scan a committed KV prefix directly from storage for admin inspection.
    ///
    /// # Errors
    /// Returns an error when the storage transaction or scan fails.
    pub fn admin_scan_committed_prefix(
        &self,
        route_family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<AdminKvPrefixScanResult, String> {
        let mut core = self.admin_core();
        KvFamilyRuntime { core: &mut core }.admin_scan_committed_prefix(
            route_family,
            realm,
            area,
            resource,
            key_prefix,
            limit,
        )
    }

    /// Scan committed KV rows with an optional pagination cursor.
    ///
    /// # Errors
    /// Returns an error when cursor validation fails or the storage scan fails.
    pub fn admin_scan_committed_rows(
        &self,
        request: &AdminKvRowsRequest<'_>,
    ) -> Result<AdminKvRowsResult, AdminKvRowsError> {
        let mut core = self.admin_core();
        KvFamilyRuntime { core: &mut core }.admin_scan_committed_rows(request)
    }

    pub(crate) fn capture_kv_snapshot(
        &self,
        selector: &crate::snapshot::SnapshotSelector,
    ) -> Result<crate::snapshot::SnapshotArtifact, String> {
        if selector.domain() != crate::snapshot::SnapshotDomain::Kv {
            return Err("KV snapshot capture requires a KV selector".to_string());
        }
        let mut core = self.admin_core();
        let mut resources =
            KvFamilyRuntime { core: &mut core }.admin_snapshot_committed_resources(selector)?;
        if selector.is_exact_resource() && resources.is_empty() {
            let route = selector.route_pattern();
            let parts = crate::runtime::routing::route_exact_triplet(route).ok_or_else(|| {
                "KV snapshot selector must name one concrete resource".to_string()
            })?;
            if [parts.realm, parts.area, parts.resource]
                .iter()
                .any(|part| part.is_empty() || part.contains('*'))
            {
                return Err("KV snapshot selector must name one concrete resource".to_string());
            }
            resources.push(crate::snapshot::SnapshotKvResource {
                route: route.to_string(),
                entries: Vec::new(),
            });
        }
        crate::snapshot::SnapshotArtifact::from_kv_resources(selector, resources)
    }
}
