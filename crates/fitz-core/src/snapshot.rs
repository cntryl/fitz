//! A family-scoped selection for durable-domain snapshot operations.

use crate::runtime::matcher::Pattern;
use crate::runtime::routing::RouteFamily;
use serde::{Deserialize, Serialize};

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
        if route_family.id() == 0 {
            return Err("RouteFamily with id=0 would map to default column family. RouteFamily IDs MUST be non-zero.".to_string());
        }
        let kind = match domain {
            SnapshotDomain::Kv => crate::runtime::DomainKind::Kv,
            SnapshotDomain::Stream => crate::runtime::DomainKind::Stream,
        };
        let pattern = kind
            .descriptor()
            .compile_registration_pattern(route_pattern)?;

        Ok(Self {
            domain,
            route_family,
            pattern,
        })
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
