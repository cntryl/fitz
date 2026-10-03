//! Typed route selectors used by Fitz domain snapshot operations.

use crate::runtime::matcher::Pattern;
use crate::runtime::routing::RouteFamily;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// A durable domain supported by route-pattern snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotDomain {
    /// Current authoritative values.
    Kv,
    /// Committed append history.
    Stream,
}

/// A route pattern bound to one explicit domain and route family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotSelector {
    domain: SnapshotDomain,
    route_family: RouteFamily,
    pattern: Pattern,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotArtifactPayload {
    format_version: u32,
    domain: SnapshotDomain,
    route_family: u64,
    selector: String,
    record_count: u64,
    resources: Vec<SnapshotKvResource>,
    stream_resources: Vec<SnapshotStreamResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotArtifactEnvelope {
    payload: SnapshotArtifactPayload,
    checksum_sha256: [u8; 32],
}

/// A complete versioned snapshot artifact with an integrity checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotArtifact {
    envelope: SnapshotArtifactEnvelope,
}

impl SnapshotArtifact {
    /// Build a KV artifact from one concrete resource selected by `selector`.
    ///
    /// # Errors
    ///
    /// Returns an error when the selector is not for KV, the route is outside
    /// its match set, a resource route is non-concrete, or the record count cannot
    /// be represented.
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

    /// Build a KV artifact from all concrete resources selected by `selector`.
    ///
    /// # Errors
    ///
    /// Returns an error when the selector is not for KV, a resource is outside
    /// its match set, a resource route is non-concrete, or the record count cannot
    /// be represented.
    pub fn from_kv_resources(
        selector: &SnapshotSelector,
        resources: Vec<SnapshotKvResource>,
    ) -> Result<Self, String> {
        if selector.domain != SnapshotDomain::Kv {
            return Err("KV snapshot requires a KV selector".to_string());
        }
        if selector.is_exact_resource()
            && (resources.len() != 1 || resources[0].route != selector.route_pattern())
        {
            return Err("exact KV snapshot must include its selected resource".to_string());
        }
        let mut record_count = 0_u64;
        for resource in &resources {
            if !selector.matches(SnapshotDomain::Kv, selector.route_family, &resource.route) {
                return Err("KV resource does not match the snapshot selector".to_string());
            }
            record_count = record_count
                .checked_add(
                    u64::try_from(resource.entries.len())
                        .map_err(|_| "snapshot record count exceeds u64".to_string())?,
                )
                .ok_or_else(|| "snapshot record count exceeds u64".to_string())?;
        }
        let payload = SnapshotArtifactPayload {
            format_version: SNAPSHOT_FORMAT_VERSION,
            domain: SnapshotDomain::Kv,
            route_family: selector.route_family.as_u64(),
            selector: selector.pattern.route().to_string(),
            record_count,
            resources,
            stream_resources: Vec::new(),
        };
        Self::from_payload(payload)
    }

    /// Build a Stream artifact from all selected concrete resources.
    ///
    /// # Errors
    /// Returns an error when the selector is not for Stream, a resource is
    /// outside its match set, a resource route is non-concrete, or the record count
    /// cannot be represented.
    pub fn from_stream_resources(
        selector: &SnapshotSelector,
        resources: Vec<SnapshotStreamResource>,
    ) -> Result<Self, String> {
        if selector.domain != SnapshotDomain::Stream {
            return Err("Stream snapshot requires a Stream selector".to_string());
        }
        if selector.is_exact_resource()
            && (resources.len() != 1 || resources[0].route != selector.route_pattern())
        {
            return Err("exact Stream snapshot must include its selected resource".to_string());
        }
        let record_count = resources.iter().try_fold(0_u64, |count, resource| {
            if !selector.matches(
                SnapshotDomain::Stream,
                selector.route_family,
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
                    > crate::domains::stream::protocol::MAX_EVENT_SIZE
            }) {
                return Err("Stream snapshot contains an event too large to replay".to_string());
            }
            count
                .checked_add(
                    u64::try_from(resource.records.len())
                        .map_err(|_| "snapshot record count exceeds u64".to_string())?,
                )
                .ok_or_else(|| "snapshot record count exceeds u64".to_string())
        })?;
        Self::from_payload(SnapshotArtifactPayload {
            format_version: SNAPSHOT_FORMAT_VERSION,
            domain: SnapshotDomain::Stream,
            route_family: selector.route_family.as_u64(),
            selector: selector.pattern.route().to_string(),
            record_count,
            resources: Vec::new(),
            stream_resources: resources,
        })
    }

    /// Read the domain recorded in this artifact.
    #[must_use]
    pub const fn domain(&self) -> SnapshotDomain {
        self.envelope.payload.domain
    }

    /// Read the explicit route family recorded in this artifact.
    #[must_use]
    pub const fn route_family(&self) -> u64 {
        self.envelope.payload.route_family
    }

    /// Read the selector recorded in this artifact.
    #[must_use]
    pub fn selector(&self) -> &str {
        &self.envelope.payload.selector
    }

    /// Read the captured record count.
    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.envelope.payload.record_count
    }

    /// Read the captured KV resources.
    #[must_use]
    pub fn kv_resources(&self) -> &[SnapshotKvResource] {
        &self.envelope.payload.resources
    }

    /// Read the captured Stream resources.
    #[must_use]
    pub fn stream_resources(&self) -> &[SnapshotStreamResource] {
        &self.envelope.payload.stream_resources
    }

    /// Encode the complete artifact as JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is invalid or cannot be serialized.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        serde_json::to_vec(&self.envelope)
            .map_err(|error| format!("failed to encode snapshot artifact: {error}"))
    }

    /// Decode and validate an artifact before exposing its contents.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, incompatible, or checksum-invalid data.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let envelope: SnapshotArtifactEnvelope = serde_json::from_slice(bytes)
            .map_err(|error| format!("invalid snapshot artifact: {error}"))?;
        let artifact = Self { envelope };
        artifact.validate()?;
        Ok(artifact)
    }

    fn from_payload(payload: SnapshotArtifactPayload) -> Result<Self, String> {
        let checksum_sha256 = checksum_payload(&payload)?;
        let artifact = Self {
            envelope: SnapshotArtifactEnvelope {
                payload,
                checksum_sha256,
            },
        };
        artifact.validate()?;
        Ok(artifact)
    }

    fn validate(&self) -> Result<(), String> {
        let payload = &self.envelope.payload;
        if payload.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(format!(
                "unsupported snapshot format version {}",
                payload.format_version
            ));
        }
        let route_family = RouteFamily::try_from(payload.route_family)
            .map_err(|_| "snapshot route family is invalid".to_string())?;
        let selector = SnapshotSelector::new(payload.domain, route_family, &payload.selector)?;
        let mut record_count = 0_u64;
        let mut routes = std::collections::BTreeSet::new();
        match payload.domain {
            SnapshotDomain::Kv => {
                if !payload.stream_resources.is_empty() {
                    return Err("KV artifact contains Stream resources".to_string());
                }
                if selector.is_exact_resource()
                    && (payload.resources.len() != 1
                        || payload.resources[0].route != selector.route_pattern())
                {
                    return Err("exact KV snapshot must include its selected resource".to_string());
                }
                for resource in &payload.resources {
                    selector.validate_resource_route(&resource.route)?;
                    if !selector.matches(payload.domain, route_family, &resource.route) {
                        return Err("snapshot resource is outside its selector".to_string());
                    }
                    if !routes.insert(&resource.route) {
                        return Err("snapshot artifact contains a duplicate resource".to_string());
                    }
                    let mut keys = std::collections::HashSet::new();
                    for entry in &resource.entries {
                        if !keys.insert(&entry.key) {
                            return Err("snapshot resource contains a duplicate key".to_string());
                        }
                    }
                    record_count = record_count
                        .checked_add(u64::try_from(resource.entries.len()).map_err(|_| {
                            "snapshot resource record count exceeds u64".to_string()
                        })?)
                        .ok_or_else(|| "snapshot record count exceeds u64".to_string())?;
                }
            }
            SnapshotDomain::Stream => {
                if !payload.resources.is_empty() {
                    return Err("Stream artifact contains KV resources".to_string());
                }
                if selector.is_exact_resource()
                    && (payload.stream_resources.len() != 1
                        || payload.stream_resources[0].route != selector.route_pattern())
                {
                    return Err(
                        "exact Stream snapshot must include its selected resource".to_string()
                    );
                }
                for resource in &payload.stream_resources {
                    selector.validate_resource_route(&resource.route)?;
                    if !selector.matches(payload.domain, route_family, &resource.route) {
                        return Err("Stream resource is outside its selector".to_string());
                    }
                    if !routes.insert(&resource.route) {
                        return Err("snapshot artifact contains a duplicate resource".to_string());
                    }
                    if resource.captured_watermark
                        < u64::try_from(resource.records.len()).unwrap_or(u64::MAX)
                    {
                        return Err(
                            "Stream snapshot watermark is below its record count".to_string()
                        );
                    }
                    if resource.records.iter().any(|record| {
                        record
                            .body
                            .len()
                            .saturating_add(record.metadata.as_ref().map_or(0, Vec::len))
                            > crate::domains::stream::protocol::MAX_EVENT_SIZE
                    }) {
                        return Err(
                            "Stream snapshot contains an event too large to replay".to_string()
                        );
                    }
                    record_count = record_count
                        .checked_add(u64::try_from(resource.records.len()).map_err(|_| {
                            "snapshot resource record count exceeds u64".to_string()
                        })?)
                        .ok_or_else(|| "snapshot record count exceeds u64".to_string())?;
                }
            }
        }
        if record_count != payload.record_count {
            return Err("snapshot record count does not match its data".to_string());
        }
        if checksum_payload(payload)? != self.envelope.checksum_sha256 {
            return Err("snapshot checksum does not match its data".to_string());
        }
        Ok(())
    }
}

fn checksum_payload(payload: &SnapshotArtifactPayload) -> Result<[u8; 32], String> {
    serde_json::to_vec(payload)
        .map(|bytes| Sha256::digest(bytes).into())
        .map_err(|error| format!("failed to encode snapshot checksum input: {error}"))
}

impl SnapshotSelector {
    /// Compile a snapshot selector using the selected domain's route grammar.
    ///
    /// `route_family` is required independently from the route pattern. It is
    /// never inferred from the realm or any part of the selector.
    ///
    /// # Errors
    ///
    /// Returns an error for the default route family, invalid domain pattern,
    /// or Stream selector outside its narrower read grammar.
    pub fn new(
        domain: SnapshotDomain,
        route_family: RouteFamily,
        route_pattern: &str,
    ) -> Result<Self, String> {
        crate::storage::cf_validation::validate_route_family(route_family)?;
        let kind = match domain {
            SnapshotDomain::Kv => crate::runtime::DomainKind::Kv,
            SnapshotDomain::Stream => crate::runtime::DomainKind::Stream,
        };
        let pattern = kind
            .descriptor()
            .compile_registration_pattern(route_pattern)?;
        if domain == SnapshotDomain::Stream {
            crate::domains::stream::route_grammar::classify_stream_route_shape(route_pattern)?;
        }

        Ok(Self {
            domain,
            route_family,
            pattern,
        })
    }

    fn validate_resource_route(&self, route: &str) -> Result<(), String> {
        let resource = Self::new(self.domain, self.route_family, route)
            .map_err(|error| format!("invalid snapshot resource route: {error}"))?;
        if !resource.is_exact_resource() {
            return Err("snapshot resource route must be concrete".to_string());
        }
        Ok(())
    }

    /// Return whether a concrete domain route belongs to this selector.
    #[must_use]
    pub fn matches(&self, domain: SnapshotDomain, route_family: RouteFamily, route: &str) -> bool {
        self.domain == domain
            && self.route_family == route_family
            && self.pattern.matches_str(route)
    }

    /// Read the selected domain.
    #[must_use]
    pub const fn domain(&self) -> SnapshotDomain {
        self.domain
    }

    /// Read the explicit route family.
    #[must_use]
    pub const fn route_family(&self) -> RouteFamily {
        self.route_family
    }

    /// Read the validated route pattern.
    #[must_use]
    pub fn route_pattern(&self) -> &str {
        self.pattern.route()
    }

    /// Return whether this selector names one exact resource.
    #[must_use]
    pub fn is_exact_resource(&self) -> bool {
        !self.pattern.is_wildcard()
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests {
    mod invalid_routes;
    use super::{SnapshotArtifact, SnapshotDomain, SnapshotKvEntry, SnapshotSelector};
    use crate::runtime::routing::RouteFamily;

    #[test]
    fn should_match_only_selected_domain_and_route_family() {
        // Arrange
        let selector =
            SnapshotSelector::new(SnapshotDomain::Kv, RouteFamily::new(7), "kv://acme/**")
                .expect("valid KV snapshot selector");

        // Act
        let selected = selector.matches(
            SnapshotDomain::Kv,
            RouteFamily::new(7),
            "kv://acme/jobs/orders",
        );
        let other_family = selector.matches(
            SnapshotDomain::Kv,
            RouteFamily::new(8),
            "kv://acme/jobs/orders",
        );
        let other_domain = selector.matches(
            SnapshotDomain::Stream,
            RouteFamily::new(7),
            "stream://acme/jobs/orders",
        );

        // Assert
        assert!(selected);
        assert!(!other_family);
        assert!(!other_domain);
    }

    #[test]
    fn should_reject_snapshot_pattern_with_wrong_domain_scheme() {
        // Arrange
        // Act
        let result =
            SnapshotSelector::new(SnapshotDomain::Kv, RouteFamily::new(7), "stream://acme/**");

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_snapshot_selector_with_default_route_family() {
        // Arrange
        // Act
        let result = SnapshotSelector::new(SnapshotDomain::Kv, RouteFamily::new(0), "kv://acme/**");

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_kv_artifact_when_a_value_changes_after_capture() {
        // Arrange
        let selector = SnapshotSelector::new(
            SnapshotDomain::Kv,
            RouteFamily::new(7),
            "kv://acme/jobs/orders",
        )
        .expect("valid exact-resource selector");
        let artifact = SnapshotArtifact::from_kv_resource(
            &selector,
            "kv://acme/jobs/orders",
            vec![SnapshotKvEntry {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }],
        )
        .expect("valid snapshot artifact");
        let mut encoded: serde_json::Value =
            serde_json::from_slice(&artifact.to_bytes().expect("encode artifact"))
                .expect("artifact JSON");
        encoded["payload"]["resources"][0]["entries"][0]["value"][0] = 86.into();

        // Act
        let decoded = SnapshotArtifact::from_bytes(
            &serde_json::to_vec(&encoded).expect("encode modified artifact"),
        );

        // Assert
        assert!(decoded.is_err());
    }

    #[test]
    fn should_require_exact_snapshot_to_include_its_empty_resource() {
        // Arrange
        let selector = SnapshotSelector::new(
            SnapshotDomain::Kv,
            RouteFamily::new(7),
            "kv://acme/jobs/orders",
        )
        .expect("valid exact-resource selector");

        // Act
        let artifact = SnapshotArtifact::from_kv_resources(&selector, Vec::new());

        // Assert
        assert!(artifact.is_err());
    }

    #[test]
    fn should_round_trip_stream_records_and_captured_watermark() {
        // Arrange
        let selector = SnapshotSelector::new(
            SnapshotDomain::Stream,
            RouteFamily::new(7),
            "stream://acme/jobs/orders",
        )
        .expect("valid exact-resource selector");
        let artifact = SnapshotArtifact::from_stream_resources(
            &selector,
            vec![super::SnapshotStreamResource {
                route: "stream://acme/jobs/orders".to_string(),
                captured_watermark: 2,
                records: vec![
                    super::SnapshotStreamRecord {
                        body: b"first".to_vec(),
                        metadata: None,
                    },
                    super::SnapshotStreamRecord {
                        body: b"second".to_vec(),
                        metadata: Some(b"meta".to_vec()),
                    },
                ],
            }],
        )
        .expect("valid Stream snapshot artifact");

        // Act
        let decoded = SnapshotArtifact::from_bytes(&artifact.to_bytes().expect("encode artifact"))
            .expect("decode Stream artifact");

        // Assert
        assert_eq!(decoded.domain(), SnapshotDomain::Stream);
        assert_eq!(decoded.record_count(), 2);
        assert_eq!(decoded.stream_resources()[0].captured_watermark, 2);
        assert_eq!(decoded.stream_resources()[0].records[0].body, b"first");
        assert_eq!(decoded.stream_resources()[0].records[1].body, b"second");
        assert_eq!(
            decoded.stream_resources()[0].records[1].metadata.as_deref(),
            Some(&b"meta"[..])
        );
    }

    #[test]
    fn should_reject_wildcard_route_in_stream_snapshot_resource() {
        // Arrange
        let selector = SnapshotSelector::new(
            SnapshotDomain::Stream,
            RouteFamily::new(7),
            "stream://acme/**",
        )
        .expect("valid Stream selector");
        let resource = super::SnapshotStreamResource {
            route: "stream://acme/jobs/*".to_string(),
            captured_watermark: 0,
            records: Vec::new(),
        };

        // Act
        let artifact = SnapshotArtifact::from_stream_resources(&selector, vec![resource]);

        // Assert
        assert!(artifact.is_err(), "resource routes must be concrete");
    }

    #[test]
    fn should_reject_stream_event_that_cannot_be_replayed_before_restore() {
        // Arrange
        let selector = SnapshotSelector::new(
            SnapshotDomain::Stream,
            RouteFamily::new(7),
            "stream://acme/jobs/orders",
        )
        .expect("valid exact-resource selector");
        let artifact = SnapshotArtifact::from_stream_resources(
            &selector,
            vec![super::SnapshotStreamResource {
                route: "stream://acme/jobs/orders".to_string(),
                captured_watermark: 1,
                records: vec![super::SnapshotStreamRecord {
                    body: vec![0; crate::domains::stream::protocol::MAX_EVENT_SIZE + 1],
                    metadata: None,
                }],
            }],
        );

        // Act
        let result = artifact.and_then(|artifact| artifact.to_bytes());

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_truncated_snapshot_artifact() {
        // Arrange
        let selector = SnapshotSelector::new(
            SnapshotDomain::Kv,
            RouteFamily::new(7),
            "kv://acme/jobs/orders",
        )
        .expect("valid exact-resource selector");
        let artifact =
            SnapshotArtifact::from_kv_resource(&selector, "kv://acme/jobs/orders", Vec::new())
                .expect("valid empty exact snapshot");
        let mut bytes = artifact.to_bytes().expect("encode artifact");
        bytes.truncate(bytes.len() - 1);

        // Act
        let decoded = SnapshotArtifact::from_bytes(&bytes);

        // Assert
        assert!(decoded.is_err());
    }
}
