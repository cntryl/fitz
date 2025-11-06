//! Route parsing and validation helpers used by transports and control
//! components.
//!
//! Routes follow the canonical form:
//!
//! ```text
//! scheme://realm/area/resource[/operation]
//! ```
//!
//! Notes:
//! - `control://` routes are system-scoped and may omit the realm segment.
//! - Bare or development routes (e.g. `ntc/...`) are handled elsewhere as a
//!   convenience; this module focuses on scheme-prefixed parsing and realm
//!   enforcement.

/// Supported route schemes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `notice://...` routes for ephemeral notices
    Notice,
    /// `stream://...` append-only streams
    Stream,
    /// `queue://...` queue/lease semantics
    Queue,
    /// `rpc://...` RPC-style services
    Rpc,
    /// `inbox://...` per-session reply-inboxes
    Inbox,
    /// `control://...` broker control plane routes (system scoped)
    Control,
    /// `kv://...` key-value storage
    Kv,
    /// `lease://...` lease management
    Lease,
}

impl Scheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scheme::Notice => "notice",
            Scheme::Stream => "stream",
            Scheme::Queue => "queue",
            Scheme::Rpc => "rpc",
            Scheme::Inbox => "inbox",
            Scheme::Control => "control",
            Scheme::Kv => "kv",
            Scheme::Lease => "lease",
        }
    }
}

impl TryFrom<&str> for Scheme {
    type Error = &'static str;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "notice" => Ok(Scheme::Notice),
            "stream" => Ok(Scheme::Stream),
            "queue" => Ok(Scheme::Queue),
            "rpc" => Ok(Scheme::Rpc),
            "inbox" => Ok(Scheme::Inbox),
            "control" => Ok(Scheme::Control),
            "kv" => Ok(Scheme::Kv),
            "lease" => Ok(Scheme::Lease),
            _ => Err("unknown scheme"),
        }
    }
}

/// Structured representation of a parsed route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The parsed scheme
    pub scheme: Scheme,
    /// Optional tenant realm (present for tenant-scoped schemes)
    pub realm: Option<String>,
    /// Optional area segment
    pub area: Option<String>,
    /// Optional resource segment
    pub resource: Option<String>,
    /// Optional operation segment (e.g. sub-action)
    pub operation: Option<String>,
    /// The original, raw route string as provided
    pub raw: String,
}

/// Parse a route string into a `Route` structure.
///
/// Returns `Err(&'static str)` when the input fails minimal validation (for
/// example missing scheme delimiter or missing realm for tenant-scoped
/// schemes).
///
/// `control://` routes may omit the realm; other schemes require a realm
/// segment.
pub fn parse_route(s: &str) -> Result<Route, &'static str> {
    // Expect something like: scheme://...
    let (scheme_str, rest) = s.split_once("://").ok_or("missing scheme delimiter")?;
    let scheme = Scheme::try_from(scheme_str)?;
    let mut realm: Option<String> = None;
    let mut area: Option<String> = None;
    let mut resource: Option<String> = None;
    let mut operation: Option<String> = None;

    if rest.is_empty() {
        // control:// may be bare
        if scheme != Scheme::Control {
            return Err("missing realm");
        }
    } else {
        let mut parts = rest.split('/').filter(|p| !p.is_empty());
        if scheme == Scheme::Control {
            // control can have arbitrary path; no realm enforcement here
            realm = parts.next().map(|s| s.to_string());
            area = parts.next().map(|s| s.to_string());
            resource = parts.next().map(|s| s.to_string());
            operation = parts.next().map(|s| s.to_string());
        } else {
            // realm required for tenant-scoped schemes
            match parts.next() {
                Some(r) => realm = Some(r.to_string()),
                None => return Err("missing realm"),
            }
            area = parts.next().map(|s| s.to_string());
            resource = parts.next().map(|s| s.to_string());
            operation = parts.next().map(|s| s.to_string());
        }
    }

    Ok(Route {
        scheme,
        realm,
        area,
        resource,
        operation,
        raw: s.to_string(),
    })
}

/// Returns true when the provided `jwt_realm` is authorized to access the
/// `route` per the broker's realm enforcement rules.
///
/// Rules applied:
/// - For `control://` and `inbox://` schemes the realm check is bypassed.
/// - For other schemes the route's `realm` must equal the `jwt_realm`.
pub fn realm_matches(route: &Route, jwt_realm: &str) -> bool {
    if route.scheme == Scheme::Control || route.scheme == Scheme::Inbox {
        return true;
    }
    match &route.realm {
        Some(r) => r == jwt_realm,
        None => false,
    }
}

/// Validate a notice publish route.
/// Publish routes MUST be complete: notice://{realm}/{area}/{resource}/{operation}
pub fn validate_notice_publish(route: &Route) -> Result<(), &'static str> {
    if route.scheme != Scheme::Notice {
        return Err("route must be notice:// scheme");
    }
    if route.realm.is_none() {
        return Err("publish route must have realm");
    }
    if route.area.is_none() {
        return Err("publish route must have area");
    }
    if route.resource.is_none() {
        return Err("publish route must have resource");
    }
    if route.operation.is_none() {
        return Err("publish route must have operation");
    }
    Ok(())
}

/// Validate a notice subscription route.
/// Subscription routes MUST have at least realm and can use wildcards.
/// Valid patterns:
/// - notice://{realm}/*
/// - notice://{realm}/{area}/*
/// - notice://{realm}/{area}/{resource}/*
/// - notice://{realm}/{area}/{resource}/{operation}
pub fn validate_notice_subscription(route_str: &str) -> Result<(), &'static str> {
    // Check for wildcard patterns
    if route_str.ends_with("/*") {
        // Strip the /* and parse the prefix
        let prefix = &route_str[..route_str.len() - 2];
        let route = parse_route(prefix)?;
        if route.scheme != Scheme::Notice {
            return Err("subscription must be notice:// scheme");
        }
        if route.realm.is_none() {
            return Err("subscription must have at least realm");
        }
        return Ok(());
    }

    // Parse as complete route
    let route = parse_route(route_str)?;
    if route.scheme != Scheme::Notice {
        return Err("subscription must be notice:// scheme");
    }
    if route.realm.is_none() {
        return Err("subscription must have at least realm");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_validate_complete_publish_route() {
        // Arrange
        let route = parse_route("notice://realm1/area1/resource1/operation1").unwrap();

        // Act
        let result = validate_notice_publish(&route);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_publish_route_without_operation() {
        // Arrange
        let route = parse_route("notice://realm1/area1/resource1").unwrap();

        // Act
        let result = validate_notice_publish(&route);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "publish route must have operation");
    }

    #[test]
    fn should_reject_publish_route_without_resource() {
        // Arrange
        let route = parse_route("notice://realm1/area1").unwrap();

        // Act
        let result = validate_notice_publish(&route);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "publish route must have resource");
    }

    #[test]
    fn should_reject_publish_route_without_area() {
        // Arrange
        let route = parse_route("notice://realm1").unwrap();

        // Act
        let result = validate_notice_publish(&route);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "publish route must have area");
    }

    #[test]
    fn should_validate_subscription_with_realm_wildcard() {
        // Arrange
        let route_str = "notice://realm1/*";

        // Act
        let result = validate_notice_subscription(route_str);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_validate_subscription_with_area_wildcard() {
        // Arrange
        let route_str = "notice://realm1/area1/*";

        // Act
        let result = validate_notice_subscription(route_str);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_validate_subscription_with_resource_wildcard() {
        // Arrange
        let route_str = "notice://realm1/area1/resource1/*";

        // Act
        let result = validate_notice_subscription(route_str);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_validate_subscription_with_complete_route() {
        // Arrange
        let route_str = "notice://realm1/area1/resource1/operation1";

        // Act
        let result = validate_notice_subscription(route_str);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_subscription_without_realm() {
        // Arrange
        let route_str = "notice://*";

        // Act
        let result = validate_notice_subscription(route_str);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_subscription_with_wrong_scheme() {
        // Arrange
        let route_str = "queue://realm1/*";

        // Act
        let result = validate_notice_subscription(route_str);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "subscription must be notice:// scheme");
    }
}
