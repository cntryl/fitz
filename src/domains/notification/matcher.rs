//! Wildcard route matching for notifications
//!
//! Notifications support NATS-like wildcard routing with two wildcard types:
//!
//! # Wildcard Syntax
//!
//! - `*` (single-level): Matches any sequence of characters within a single path segment
//!   - Example: `notify://acme/orders/*` matches:
//!     - `notify://acme/orders/create`
//!     - `notify://acme/orders/update`
//!     - `notify://acme/orders/delete`
//!   - But NOT `notify://acme/orders/items/create` (different level)
//!
//! - `**` (multi-level): Matches zero or more complete path segments
//!   - Example: `notify://acme/**` matches:
//!     - `notify://acme/orders`
//!     - `notify://acme/orders/create`
//!     - `notify://acme/inventory/check`
//!   - But only within the realm prefix
//!
//! # Path Structure
//!
//! Routes are treated as hierarchical paths split by `/`.
//! Matching is purely structural and has no domain semantics.
//!
//! # Isolation
//!
//! Wildcards apply **only within the same RouteFamily**.
//! The RouteFamily ID must match exactly; wildcards never cross family boundaries.

use crate::transport::routing::Route;

/// Wildcard pattern for notification subscriptions
///
/// A pattern is a route that may contain `*` and `**` wildcards.
/// Patterns are matched against published routes to determine fan-out targets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pattern {
    /// The full route pattern (e.g., "notify://realm/area/*")
    route: String,
    segments: Vec<PatternSegment>,
}

/// A single segment of a wildcard pattern
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PatternSegment {
    /// Literal string (e.g., "orders")
    Literal(String),
    /// Single-level wildcard `*` (matches one segment)
    Star,
    /// Multi-level wildcard `**` (matches zero or more segments)
    DoubleStar,
}

impl Pattern {
    /// Create a new pattern from a route string
    pub fn new(route: &str) -> Self {
        let segments = parse_pattern(route);
        Self {
            route: route.to_string(),
            segments,
        }
    }

    /// Get the original route pattern
    pub fn route(&self) -> &str {
        &self.route
    }

    /// Check if this pattern matches a given route
    pub fn matches(&self, route: &Route) -> bool {
        let route_str = route.as_str();

        // Extract path after scheme (e.g., "notify://realm/area/resource" -> "realm/area/resource")
        let path = if let Some(idx) = route_str.find("://") {
            &route_str[idx + 3..]
        } else {
            route_str
        };

        let route_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        match_segments(&self.segments, &route_segments)
    }
}

/// Parse a route pattern into segments
fn parse_pattern(route: &str) -> Vec<PatternSegment> {
    // Extract the path part after scheme (e.g., "notify://realm/area/*" -> "realm/area/*")
    let path = if let Some(idx) = route.find("://") {
        &route[idx + 3..]
    } else {
        route
    };

    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|segment| match segment {
            "**" => PatternSegment::DoubleStar,
            "*" => PatternSegment::Star,
            literal => PatternSegment::Literal(literal.to_string()),
        })
        .collect()
}

/// Match route segments against pattern segments
fn match_segments(patterns: &[PatternSegment], route: &[&str]) -> bool {
    match (patterns.first(), route.first()) {
        // Both empty: perfect match
        (None, None) => true,
        // Pattern exhausted but route remains: no match
        (None, Some(_)) => false,
        // Route exhausted but pattern remains: check if remaining pattern can match empty
        (Some(_), None) => {
            // Only ** can match zero segments
            patterns
                .iter()
                .all(|p| matches!(p, PatternSegment::DoubleStar))
        }
        // Pattern is **
        (Some(PatternSegment::DoubleStar), Some(_)) => {
            // Option 1: ** matches zero segments, skip to next pattern
            if match_segments(&patterns[1..], route) {
                return true;
            }
            // Option 2: ** matches one or more segments, consume one segment
            match_segments(patterns, &route[1..])
        }
        // Pattern is *
        (Some(PatternSegment::Star), Some(_)) => {
            // * matches exactly one segment
            match_segments(&patterns[1..], &route[1..])
        }
        // Pattern is literal
        (Some(PatternSegment::Literal(pat)), Some(route_seg)) => {
            // Literal must match exactly
            if pat == *route_seg {
                match_segments(&patterns[1..], &route[1..])
            } else {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(path: &str) -> Route {
        Route::new(path.to_string())
    }

    #[test]
    fn should_match_exact_route() {
        let pattern = Pattern::new("notify://acme/orders/create");
        assert!(pattern.matches(&route("notify://acme/orders/create")));
    }

    #[test]
    fn should_not_match_different_route() {
        let pattern = Pattern::new("notify://acme/orders/create");
        assert!(!pattern.matches(&route("notify://acme/orders/update")));
    }

    #[test]
    fn should_match_single_star_wildcard() {
        let pattern = Pattern::new("notify://acme/orders/*");
        assert!(pattern.matches(&route("notify://acme/orders/create")));
        assert!(pattern.matches(&route("notify://acme/orders/update")));
        assert!(pattern.matches(&route("notify://acme/orders/delete")));
    }

    #[test]
    fn should_not_match_across_single_star_boundary() {
        let pattern = Pattern::new("notify://acme/orders/*");
        // * only matches one segment, not nested paths
        assert!(!pattern.matches(&route("notify://acme/orders/items/create")));
    }

    #[test]
    fn should_match_double_star_from_middle() {
        let pattern = Pattern::new("notify://acme/**/created");
        assert!(pattern.matches(&route("notify://acme/created")));
        assert!(pattern.matches(&route("notify://acme/orders/created")));
        assert!(pattern.matches(&route("notify://acme/orders/items/created")));
    }

    #[test]
    fn should_match_double_star_at_end() {
        let pattern = Pattern::new("notify://acme/orders/**");
        assert!(pattern.matches(&route("notify://acme/orders")));
        assert!(pattern.matches(&route("notify://acme/orders/create")));
        assert!(pattern.matches(&route("notify://acme/orders/items/create")));
    }

    #[test]
    fn should_not_match_double_star_across_unrelated_prefix() {
        let pattern = Pattern::new("notify://acme/**");
        assert!(pattern.matches(&route("notify://acme/orders")));
        assert!(pattern.matches(&route("notify://acme/inventory")));
        // ** does not skip scheme or realm boundary
        assert!(!pattern.matches(&route("notify://other/orders")));
    }

    #[test]
    fn should_match_multiple_wildcards() {
        let pattern = Pattern::new("notify://acme/*/*/created");
        assert!(pattern.matches(&route("notify://acme/orders/create/created")));
        assert!(pattern.matches(&route("notify://acme/inventory/check/created")));
        assert!(!pattern.matches(&route("notify://acme/orders/created")));
    }

    #[test]
    fn should_match_pattern_without_scheme() {
        let pattern = Pattern::new("acme/orders/*");
        assert!(pattern.matches(&route("acme/orders/create")));
        assert!(pattern.matches(&route("acme/orders/update")));
    }

    #[test]
    fn should_match_double_star_with_no_segments() {
        let pattern = Pattern::new("notify://acme/**");
        assert!(pattern.matches(&route("notify://acme")));
    }

    #[test]
    fn should_not_match_literal_when_pattern_expects_wildcard() {
        let pattern = Pattern::new("notify://acme/*/created");
        assert!(!pattern.matches(&route("notify://acme/orders/update/created")));
    }
}
