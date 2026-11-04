//! Explicit whole-resource, area, and realm deletion scopes.

use super::KvResourceScope;
use crate::runtime::routing::RouteFamily;

/// The resources selected by one standalone KV DROP operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvDropTarget {
    Realm,
    Area { area: String },
    Resource { area: String, resource: String },
}

/// Application-visible deletion scope within one broker route family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvDropScope {
    pub route_family: RouteFamily,
    pub realm: String,
    pub target: KvDropTarget,
}

impl KvDropScope {
    /// Parse only `realm/*`, `realm/area/*`, or `realm/area/resource`.
    ///
    /// # Errors
    /// Returns an error for a foreign scheme, invalid segment, or unsupported selector.
    pub fn parse(route_family: RouteFamily, route: &str) -> Result<Self, String> {
        crate::utils::route_shape::validate_route_shape(route)?;
        let path = route.strip_prefix("kv://").unwrap_or(route);
        let parts = path.split('/').collect::<Vec<_>>();
        let (realm, target) = match parts.as_slice() {
            [realm, "*"] => (*realm, KvDropTarget::Realm),
            [realm, area, "*"] if !area.contains('*') => (
                *realm,
                KvDropTarget::Area {
                    area: (*area).to_string(),
                },
            ),
            [realm, area, resource] if !area.contains('*') && !resource.contains('*') => (
                *realm,
                KvDropTarget::Resource {
                    area: (*area).to_string(),
                    resource: (*resource).to_string(),
                },
            ),
            _ => {
                return Err(
                    "DROP requires realm/*, realm/area/*, or realm/area/resource".to_string(),
                )
            }
        };
        let scope = Self {
            route_family,
            realm: realm.to_string(),
            target,
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Validate a typed scope constructed without the wire parser.
    ///
    /// # Errors
    /// Returns an error when the realm or literal route components are invalid.
    pub fn validate(&self) -> Result<(), String> {
        crate::auth::validate_realm_format(&self.realm).map_err(|error| error.to_string())?;
        let literal = |segment: &str| !segment.is_empty() && !segment.contains(['*', '/']);
        let valid_target = match &self.target {
            KvDropTarget::Realm => true,
            KvDropTarget::Area { area } => literal(area),
            KvDropTarget::Resource { area, resource } => literal(area) && literal(resource),
        };
        if !valid_target {
            return Err("DROP area and resource must be literal non-empty segments".to_string());
        }
        crate::runtime::DomainKind::Kv
            .descriptor()
            .compile_registration_pattern(&self.authorization_route())?;
        Ok(())
    }

    /// Express the complete selected set at KV's concrete route depth.
    #[must_use]
    pub fn authorization_route(&self) -> String {
        let realm = &self.realm;
        match &self.target {
            KvDropTarget::Realm => format!("kv://{realm}/*/*"),
            KvDropTarget::Area { area } => format!("kv://{realm}/{area}/*"),
            KvDropTarget::Resource { area, resource } => format!("kv://{realm}/{area}/{resource}"),
        }
    }

    #[must_use]
    pub fn matches_resource(&self, scope: &KvResourceScope) -> bool {
        self.matches_parts(
            scope.route_family,
            &scope.realm,
            &scope.area,
            &scope.resource,
        )
    }

    pub(crate) fn matches_parts(
        &self,
        route_family: RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> bool {
        self.route_family == route_family
            && self.realm == realm
            && match &self.target {
                KvDropTarget::Realm => true,
                KvDropTarget::Area { area: expected } => expected == area,
                KvDropTarget::Resource {
                    area: expected_area,
                    resource: expected_resource,
                } => expected_area == area && expected_resource == resource,
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_invalid_literal_components_in_typed_drop_scopes() {
        // Arrange
        let targets = [
            KvDropTarget::Area { area: "*".into() },
            KvDropTarget::Area {
                area: String::new(),
            },
            KvDropTarget::Resource {
                area: "app".into(),
                resource: "one/two".into(),
            },
            KvDropTarget::Resource {
                area: "app".into(),
                resource: "\0".into(),
            },
        ];

        // Act
        let results = targets.map(|target| {
            KvDropScope {
                route_family: RouteFamily::new(41),
                realm: "acme".into(),
                target,
            }
            .validate()
        });

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_match_resources_only_inside_the_drop_namespace() {
        // Arrange
        let scope = KvDropScope::parse(RouteFamily::new(41), "kv://acme/app/*").unwrap();
        let resources = [
            (
                KvResourceScope::new(RouteFamily::new(41), "acme", "app", "one"),
                true,
            ),
            (
                KvResourceScope::new(RouteFamily::new(42), "acme", "app", "one"),
                false,
            ),
            (
                KvResourceScope::new(RouteFamily::new(41), "other", "app", "one"),
                false,
            ),
            (
                KvResourceScope::new(RouteFamily::new(41), "acme", "apps", "one"),
                false,
            ),
        ];

        // Act
        let results =
            resources.map(|(resource, expected)| (scope.matches_resource(&resource), expected));

        // Assert
        for (matched, expected) in results {
            assert_eq!(matched, expected);
        }
    }
}
