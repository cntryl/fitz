//! Public admin façade and crate-internal storage-backed implementations.

use super::state::KvDomainSink;
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

impl KvDomainSink {
    #[cfg(test)]
    /// Read one family directly for storage-backed admin regression tests.
    pub(super) fn admin_inventory_for_family_for_tests(
        &self,
        family_id: u64,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        self.state.runtime().admin_inventory_for_family(family_id)
    }

    /// Build an admin inventory snapshot for the requested route family scope.
    ///
    /// # Errors
    /// Returns an error when the underlying storage inventory scan fails.
    pub fn admin_inventory(
        &self,
        family: Option<RouteFamily>,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        self.state.runtime().admin_inventory(family)
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
        self.state
            .runtime()
            .admin_inventory_resource(route_family, realm, area, resource)
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
        self.state
            .runtime()
            .admin_get_committed_value(route_family, realm, area, resource, key)
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
        self.state.runtime().admin_scan_committed_prefix(
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
    ) -> Result<AdminKvRowsResult, String> {
        self.state.runtime().admin_scan_committed_rows(request)
    }
}
