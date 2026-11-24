use serde::{Deserialize, Serialize};

pub use fitz_core::snapshot::{SnapshotDomain, SnapshotSelector};

/// One raw key/value pair in a KV snapshot resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotKvEntry {
    /// Application key bytes.
    pub key: Vec<u8>,
    /// Committed value bytes.
    pub value: Vec<u8>,
}

/// Captured committed values for one concrete KV resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotKvResource {
    /// Canonical concrete route for this resource.
    pub route: String,
    /// Committed key/value entries.
    pub entries: Vec<SnapshotKvEntry>,
}

/// Validated capture or restore data for one Kv family.
#[derive(Debug, Clone)]
pub struct SnapshotArtifact {
    selector: SnapshotSelector,
    resources: Vec<SnapshotKvResource>,
}

impl SnapshotArtifact {
    /// Validate resource routes before sending any work to a family actor.
    ///
    /// # Errors
    /// Rejects an invalid selector or mismatched resource.
    pub fn from_kv_resources(
        selector: &SnapshotSelector,
        resources: Vec<SnapshotKvResource>,
    ) -> Result<Self, String> {
        if selector.domain() != SnapshotDomain::Kv {
            return Err("Kv snapshot requires a Kv selector".to_string());
        }
        if selector.is_exact_resource()
            && (resources.len() != 1 || resources[0].route != selector.route_pattern())
        {
            return Err("exact Kv snapshot must include its selected resource".to_string());
        }
        let mut routes = std::collections::BTreeSet::new();
        for resource in &resources {
            let concrete = SnapshotSelector::new(
                SnapshotDomain::Kv,
                selector.route_family(),
                &resource.route,
            )?;
            if !concrete.is_exact_resource() {
                return Err("snapshot resource route must be concrete".to_string());
            }
            if !selector.matches(SnapshotDomain::Kv, selector.route_family(), &resource.route) {
                return Err("Kv resource does not match the snapshot selector".to_string());
            }
            if !routes.insert(&resource.route) {
                return Err("snapshot artifact contains a duplicate resource".to_string());
            }
        }
        Ok(Self {
            selector: selector.clone(),
            resources,
        })
    }

    #[must_use]
    pub const fn domain(&self) -> SnapshotDomain {
        self.selector.domain()
    }

    #[must_use]
    pub fn route_family(&self) -> u64 {
        self.selector.route_family().as_u64()
    }

    #[must_use]
    pub fn selector(&self) -> &str {
        self.selector.route_pattern()
    }

    #[must_use]
    pub fn kv_resources(&self) -> &[SnapshotKvResource] {
        &self.resources
    }

    #[cfg(test)]
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.resources
            .iter()
            .map(|resource| resource.entries.len())
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn with_resource_route(&self, index: usize, route: &str) -> Result<Self, String> {
        let mut resources = self.resources.clone();
        resources[index].route = route.to_string();
        Self::from_kv_resources(&self.selector, resources)
    }

    /// # Errors
    /// Returns an error if the concrete resource is outside the selector.
    #[cfg(test)]
    pub fn from_kv_resource(
        selector: &SnapshotSelector,
        route: &str,
        entries: Vec<SnapshotKvEntry>,
    ) -> Result<Self, String> {
        Self::from_kv_resources(
            selector,
            vec![SnapshotKvResource {
                route: route.to_string(),
                entries,
            }],
        )
    }

    #[must_use]
    pub fn into_resources(self) -> Vec<SnapshotKvResource> {
        self.resources
    }
}
