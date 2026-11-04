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
        let (realm, target) = parse_selector(route)?;
        Ok(Self {
            route_family,
            realm,
            target,
        })
    }

    /// Canonicalize a deletion selector without assigning it to a route family.
    ///
    /// # Errors
    /// Returns an error for an invalid deletion selector.
    pub fn canonical_authorization_route(route: &str) -> Result<String, String> {
        let (realm, target) = parse_selector(route)?;
        Ok(authorization_route(&realm, &target))
    }

    /// Validate a typed scope constructed without the wire parser.
    ///
    /// # Errors
    /// Returns an error when the realm or literal route components are invalid.
    pub fn validate(&self) -> Result<(), String> {
        validate_selector(&self.realm, &self.target)
    }

    /// Express the complete selected set at KV's concrete route depth.
    #[must_use]
    pub fn authorization_route(&self) -> String {
        authorization_route(&self.realm, &self.target)
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

fn parse_selector(route: &str) -> Result<(String, KvDropTarget), String> {
    crate::utils::route_shape::validate_route_shape(route)?;
    let path = route.strip_prefix("kv://").unwrap_or(route);
    let mut parts = path.split('/');
    let (realm, target) = match [parts.next(), parts.next(), parts.next(), parts.next()] {
        [Some(realm), Some("*"), None, None] => (realm, KvDropTarget::Realm),
        [Some(realm), Some(area), Some("*"), None] if !area.contains('*') => (
            realm,
            KvDropTarget::Area {
                area: area.to_string(),
            },
        ),
        [Some(realm), Some(area), Some(resource), None]
            if !area.contains('*') && !resource.contains('*') =>
        {
            (
                realm,
                KvDropTarget::Resource {
                    area: area.to_string(),
                    resource: resource.to_string(),
                },
            )
        }
        _ => return Err("DROP requires realm/*, realm/area/*, or realm/area/resource".to_string()),
    };
    validate_selector(realm, &target)?;
    Ok((realm.to_string(), target))
}

fn validate_selector(realm: &str, target: &KvDropTarget) -> Result<(), String> {
    crate::auth::validate_realm_format(realm).map_err(|error| error.to_string())?;
    let literal = |segment: &str| !segment.is_empty() && !segment.contains(['*', '/']);
    let valid_target = match target {
        KvDropTarget::Realm => true,
        KvDropTarget::Area { area } => literal(area),
        KvDropTarget::Resource { area, resource } => literal(area) && literal(resource),
    };
    if !valid_target {
        return Err("DROP area and resource must be literal non-empty segments".to_string());
    }
    let (area, resource) = match target {
        KvDropTarget::Realm => ("*", "*"),
        KvDropTarget::Area { area } => (area.as_str(), "*"),
        KvDropTarget::Resource { area, resource } => (area.as_str(), resource.as_str()),
    };
    if crate::utils::route_shape::contains_control_character(area)
        || crate::utils::route_shape::contains_control_character(resource)
    {
        return Err("route must not contain control characters".to_string());
    }
    // The selector's three authorization segments already enforce KV's depth
    // and whole-segment wildcard grammar. Check its rendered byte bound without
    // allocating a route and a compiled pattern that would both be discarded.
    let route_bytes = "kv://".len() + realm.len() + 1 + area.len() + 1 + resource.len();
    let maximum = crate::utils::route_shape::MAX_ROUTE_BYTES;
    if route_bytes > maximum {
        return Err(format!("route exceeds maximum length of {maximum} bytes"));
    }
    Ok(())
}

fn authorization_route(realm: &str, target: &KvDropTarget) -> String {
    match target {
        KvDropTarget::Realm => format!("kv://{realm}/*/*"),
        KvDropTarget::Area { area } => format!("kv://{realm}/{area}/*"),
        KvDropTarget::Resource { area, resource } => format!("kv://{realm}/{area}/{resource}"),
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
            KvDropTarget::Area {
                area: "app\u{85}".into(),
            },
            KvDropTarget::Resource {
                area: "app".into(),
                resource: "rows\n".into(),
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
    fn should_preserve_literal_drop_selector_grammar() {
        // Arrange
        let routes = [
            ("kv://acme/*", true),
            ("acme/app/*", true),
            ("kv://Åcme_2/my area/café?raw#1", true),
            ("queue://acme/app/rows", false),
            ("kv://acme/app/rows/extra", false),
            ("kv://acme/*/rows", false),
            ("kv://acme/**", false),
            ("kv://acme/app/row*", false),
            ("kv://acme//rows", false),
            ("kv://acme/app/", false),
            ("kv://acme/a\0b/rows", false),
        ];

        // Act
        let results = routes.map(|(route, expected)| {
            (
                route,
                KvDropScope::parse(RouteFamily::new(41), route),
                KvDropScope::canonical_authorization_route(route),
                expected,
            )
        });

        // Assert
        for (route, parsed, canonical, expected) in results {
            assert_eq!(parsed.is_ok(), expected, "{route:?}");
            assert_eq!(canonical.is_ok(), expected, "{route:?}");
        }
    }

    #[test]
    fn should_bound_drop_selectors_by_canonical_route_bytes() {
        // Arrange
        let maximum = crate::utils::route_shape::MAX_ROUTE_BYTES;
        let resource = "r".repeat(maximum - "kv://acme/app/".len());
        let realm = "r".repeat(maximum - "kv:///*/*".len());
        let routes = [
            (format!("kv://acme/app/{resource}"), true),
            (format!("acme/app/{resource}"), true),
            (format!("kv://acme/app/{resource}x"), false),
            (format!("acme/app/{resource}x"), false),
            (format!("kv://{realm}/*"), true),
            (format!("kv://{realm}x/*"), false),
        ];

        // Act
        let results = routes.map(|(route, expected)| {
            (
                KvDropScope::parse(RouteFamily::new(41), &route).is_ok(),
                KvDropScope::canonical_authorization_route(&route).is_ok(),
                expected,
            )
        });

        // Assert
        for (parsed, canonical, expected) in results {
            assert_eq!(parsed, expected);
            assert_eq!(canonical, expected);
        }
    }

    #[test]
    fn should_bound_typed_drop_scopes_by_canonical_route_bytes() {
        // Arrange
        let maximum = crate::utils::route_shape::MAX_ROUTE_BYTES;
        let mut scope = KvDropScope {
            route_family: RouteFamily::new(41),
            realm: "acme".into(),
            target: KvDropTarget::Resource {
                area: "app".into(),
                resource: "r".repeat(maximum - "kv://acme/app/".len()),
            },
        };

        // Act
        let at_limit = scope.validate();
        let KvDropTarget::Resource { resource, .. } = &mut scope.target else {
            unreachable!()
        };
        resource.push('x');
        let over_limit = scope.validate();

        // Assert
        assert!(at_limit.is_ok());
        assert!(over_limit.is_err());
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
