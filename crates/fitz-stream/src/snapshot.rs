use serde::{Deserialize, Serialize};

pub use fitz_core::snapshot::{SnapshotDomain, SnapshotSelector};

/// One readable committed Stream event, in resource order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotStreamRecord {
    /// Event body bytes.
    pub body: Vec<u8>,
    /// Optional event metadata bytes.
    pub metadata: Option<Vec<u8>>,
}

/// Readable committed history for one concrete Stream resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotStreamResource {
    /// Canonical concrete route for this resource.
    pub route: String,
    /// Exclusive resource read watermark captured with the records.
    pub captured_watermark: u64,
    /// Records ordered as read from the source resource.
    pub records: Vec<SnapshotStreamRecord>,
}

/// Validated capture or restore data for one Stream family.
#[derive(Debug, Clone)]
pub struct SnapshotArtifact {
    selector: SnapshotSelector,
    resources: Vec<SnapshotStreamResource>,
}

impl SnapshotArtifact {
    /// Validate resource routes before sending any work to a family actor.
    ///
    /// # Errors
    /// Rejects an invalid selector or mismatched resource.
    pub fn from_stream_resources(
        selector: &SnapshotSelector,
        resources: Vec<SnapshotStreamResource>,
    ) -> Result<Self, String> {
        if selector.domain() != SnapshotDomain::Stream {
            return Err("Stream snapshot requires a Stream selector".to_string());
        }
        crate::domain::route_grammar::validate_snapshot_route(selector.route_pattern())?;
        if selector.is_exact_resource()
            && (resources.len() != 1 || resources[0].route != selector.route_pattern())
        {
            return Err("exact Stream snapshot must include its selected resource".to_string());
        }
        let mut routes = std::collections::BTreeSet::new();
        for resource in &resources {
            let concrete = SnapshotSelector::new(
                SnapshotDomain::Stream,
                selector.route_family(),
                &resource.route,
            )?;
            crate::domain::route_grammar::validate_snapshot_route(&resource.route)?;
            if !concrete.is_exact_resource() {
                return Err("snapshot resource route must be concrete".to_string());
            }
            if !selector.matches(
                SnapshotDomain::Stream,
                selector.route_family(),
                &resource.route,
            ) {
                return Err("Stream resource does not match the snapshot selector".to_string());
            }
            if resource.captured_watermark
                < u64::try_from(resource.records.len()).unwrap_or(u64::MAX)
            {
                return Err("Stream snapshot watermark is below its record count".to_string());
            }
            if resource.records.iter().any(|record| {
                record
                    .body
                    .len()
                    .saturating_add(record.metadata.as_ref().map_or(0, Vec::len))
                    > crate::domain::protocol::MAX_EVENT_SIZE
            }) {
                return Err("Stream snapshot contains an event too large to replay".to_string());
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
    pub fn stream_resources(&self) -> &[SnapshotStreamResource] {
        &self.resources
    }

    #[cfg(test)]
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.resources
            .iter()
            .map(|resource| resource.records.len())
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn with_resource_route(&self, index: usize, route: &str) -> Result<Self, String> {
        let mut resources = self.resources.clone();
        resources[index].route = route.to_string();
        Self::from_stream_resources(&self.selector, resources)
    }

    #[must_use]
    pub fn into_resources(self) -> Vec<SnapshotStreamResource> {
        self.resources
    }
}
