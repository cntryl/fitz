//! Shared bounds for application-visible routes and permission patterns.

/// Maximum UTF-8 byte length of a route or route-shaped permission pattern.
pub const MAX_ROUTE_BYTES: usize = 4 * 1024;

/// Maximum number of non-empty path segments in a route or pattern.
pub const MAX_ROUTE_SEGMENTS: usize = 64;

/// Validate the resource bounds shared by routes and permission patterns.
///
/// # Errors
///
/// Returns an error when the route exceeds the byte or segment limit.
pub fn validate_route_shape(route: &str) -> Result<(), String> {
    if route.len() > MAX_ROUTE_BYTES {
        return Err(format!(
            "route exceeds maximum length of {MAX_ROUTE_BYTES} bytes"
        ));
    }

    let path = route
        .split_once("://")
        .map_or(route, |(_scheme, path)| path);
    let segment_count = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .count();
    if segment_count > MAX_ROUTE_SEGMENTS {
        return Err(format!(
            "route exceeds maximum of {MAX_ROUTE_SEGMENTS} segments"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_route_over_byte_limit() {
        let route = format!("notice://{}", "a".repeat(MAX_ROUTE_BYTES));
        assert!(validate_route_shape(&route).is_err());
    }

    #[test]
    fn should_reject_route_over_segment_limit() {
        let route = format!("notice://{}", vec!["a"; MAX_ROUTE_SEGMENTS + 1].join("/"));
        assert!(validate_route_shape(&route).is_err());
    }

    #[test]
    fn should_accept_route_at_segment_limit() {
        let route = format!("notice://{}", vec!["a"; MAX_ROUTE_SEGMENTS].join("/"));
        assert!(validate_route_shape(&route).is_ok());
    }
}
