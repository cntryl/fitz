//! Explicit destructive deletion scopes, separate from replay selectors.

use crate::runtime::routing::RouteFamily;

/// The resources selected by one standalone Stream DROP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamDropTarget {
    Realm,
    Area { area: String },
    Resource { area: String, resource: String },
}

/// Application-visible deletion scope within one broker route family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamDropScope {
    pub route_family: RouteFamily,
    pub realm: String,
    pub target: StreamDropTarget,
}

impl StreamDropScope {
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

    /// Express a deletion selector at Stream's concrete authorization depth.
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
    /// Returns an error for invalid literal components or excessive route length.
    pub fn validate(&self) -> Result<(), String> {
        validate_selector(&self.realm, &self.target)
    }

    #[must_use]
    pub fn authorization_route(&self) -> String {
        authorization_route(&self.realm, &self.target)
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
                StreamDropTarget::Realm => true,
                StreamDropTarget::Area { area: expected } => expected == area,
                StreamDropTarget::Resource {
                    area: expected_area,
                    resource: expected_resource,
                } => expected_area == area && expected_resource == resource,
            }
    }
}

fn parse_selector(route: &str) -> Result<(String, StreamDropTarget), String> {
    crate::utils::route_shape::validate_route_shape(route)?;
    let path = route.strip_prefix("stream://").unwrap_or(route);
    let mut parts = path.split('/');
    let (realm, target) = match [parts.next(), parts.next(), parts.next(), parts.next()] {
        [Some(realm), Some("*"), None, None] => (realm, StreamDropTarget::Realm),
        [Some(realm), Some(area), Some("*"), None] if !area.contains('*') => (
            realm,
            StreamDropTarget::Area {
                area: area.to_string(),
            },
        ),
        [Some(realm), Some(area), Some(resource), None]
            if !area.contains('*') && !resource.contains('*') =>
        {
            (
                realm,
                StreamDropTarget::Resource {
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

fn validate_selector(realm: &str, target: &StreamDropTarget) -> Result<(), String> {
    crate::auth::validate_realm_format(realm).map_err(|error| error.to_string())?;
    let literal = |segment: &str| !segment.is_empty() && !segment.contains(['*', '/']);
    let (area, resource) = match target {
        StreamDropTarget::Realm => ("*", "*"),
        StreamDropTarget::Area { area } if literal(area) => (area.as_str(), "*"),
        StreamDropTarget::Resource { area, resource } if literal(area) && literal(resource) => {
            (area.as_str(), resource.as_str())
        }
        _ => return Err("DROP area and resource must be literal non-empty segments".to_string()),
    };
    if area == super::INTERNAL_REALM_SEGMENT || resource == super::INTERNAL_AREA_SEGMENT {
        return Err("DROP scope contains a reserved Stream segment".to_string());
    }
    if crate::utils::route_shape::contains_control_character(area)
        || crate::utils::route_shape::contains_control_character(resource)
    {
        return Err("route must not contain control characters".to_string());
    }
    let route_bytes = "stream://".len() + realm.len() + 1 + area.len() + 1 + resource.len();
    let maximum = crate::utils::route_shape::MAX_ROUTE_BYTES;
    if route_bytes > maximum {
        return Err(format!("route exceeds maximum length of {maximum} bytes"));
    }
    Ok(())
}

fn authorization_route(realm: &str, target: &StreamDropTarget) -> String {
    match target {
        StreamDropTarget::Realm => format!("stream://{realm}/*/*"),
        StreamDropTarget::Area { area } => format!("stream://{realm}/{area}/*"),
        StreamDropTarget::Resource { area, resource } => {
            format!("stream://{realm}/{area}/{resource}")
        }
    }
}

#[cfg(test)]
mod tests;
