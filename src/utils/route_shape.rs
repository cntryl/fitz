//! Shared bounds for application-visible routes and permission patterns.

/// Maximum UTF-8 byte length of a route or route-shaped permission pattern.
pub const MAX_ROUTE_BYTES: usize = 4 * 1024;

/// Maximum number of non-empty path segments in a route or pattern.
pub const MAX_ROUTE_SEGMENTS: usize = 64;

/// Whether a route or pattern contains a control character.
///
/// Domain storage keys join route segments with a raw `0x00` separator, so a
/// segment carrying one would alias another resource's keys. Every control
/// character is refused, matching the realm rule, rather than only `0x00`.
#[must_use]
pub fn contains_control_character(route: &str) -> bool {
    route.chars().any(char::is_control)
}

/// Validate the resource bounds shared by routes and permission patterns.
///
/// # Errors
///
/// Returns an error when the route contains a control character or exceeds
/// the byte or segment limit.
pub fn validate_route_shape(route: &str) -> Result<(), String> {
    if contains_control_character(route) {
        return Err("route must not contain control characters".to_string());
    }
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
    fn should_reject_route_containing_control_characters() {
        assert!(validate_route_shape("notice://acme/app/\0orders").is_err());
        assert!(validate_route_shape("queue://acme/*/jobs\u{1b}").is_err());
    }

    #[test]
    fn should_accept_route_at_segment_limit() {
        let route = format!("notice://{}", vec!["a"; MAX_ROUTE_SEGMENTS].join("/"));
        assert!(validate_route_shape(&route).is_ok());
    }
}
