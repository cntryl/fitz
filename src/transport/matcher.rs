//! Wildcard route matching for transport patterns
//!
//! Supports NATS-like wildcard routing with two wildcard types.
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

/// Wildcard pattern for route subscriptions
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
pub enum PatternSegment {
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
pub fn parse_pattern_segments(route: &str) -> Vec<PatternSegment> {
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

/// Extract path segments from a route string as Strings
pub fn extract_route_segments(route: &str) -> Vec<String> {
    let path = if let Some(idx) = route.find("://") {
        &route[idx + 3..]
    } else {
        route
    };

    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Match pattern segments against route segments using index-based recursion
/// Used by both Pattern matching and SubscriptionIndex suffix matching
pub fn match_pattern_segments(
    patterns: &[PatternSegment],
    pat_idx: usize,
    route: &[String],
    route_idx: usize,
) -> bool {
    // Both exhausted: match
    if pat_idx >= patterns.len() && route_idx >= route.len() {
        return true;
    }

    // Pattern exhausted but route remains: no match
    if pat_idx >= patterns.len() {
        return false;
    }

    // Route exhausted but pattern remains: only ** can match empty
    if route_idx >= route.len() {
        return patterns[pat_idx..]
            .iter()
            .all(|p| matches!(p, PatternSegment::DoubleStar));
    }

    match &patterns[pat_idx] {
        PatternSegment::DoubleStar => {
            // Option 1: ** matches zero segments
            if match_pattern_segments(patterns, pat_idx + 1, route, route_idx) {
                return true;
            }
            // Option 2: ** matches one or more segments
            match_pattern_segments(patterns, pat_idx, route, route_idx + 1)
        }
        PatternSegment::Star => {
            // * matches exactly one segment
            match_pattern_segments(patterns, pat_idx + 1, route, route_idx + 1)
        }
        PatternSegment::Literal(pat) => {
            // Literal must match exactly
            if pat == &route[route_idx] {
                match_pattern_segments(patterns, pat_idx + 1, route, route_idx + 1)
            } else {
                false
            }
        }
    }
}

/// Parse a route pattern into segments (internal, kept for backward compat)
fn parse_pattern(route: &str) -> Vec<PatternSegment> {
    parse_pattern_segments(route)
}

/// Match route segments against pattern segments using index-based iteration
/// (avoids recursive slicing overhead)
#[inline]
fn match_segments(patterns: &[PatternSegment], route: &[&str]) -> bool {
    match_segments_indexed(patterns, 0, route, 0)
}

/// Index-based matching function (avoids slice allocation on each recursion)
#[inline]
fn match_segments_indexed(
    patterns: &[PatternSegment],
    pat_idx: usize,
    route: &[&str],
    route_idx: usize,
) -> bool {
    // Both exhausted: match
    if pat_idx >= patterns.len() && route_idx >= route.len() {
        return true;
    }

    // Pattern exhausted but route remains: no match
    if pat_idx >= patterns.len() {
        return false;
    }

    // Route exhausted but pattern remains: only ** can match empty
    if route_idx >= route.len() {
        return patterns[pat_idx..]
            .iter()
            .all(|p| matches!(p, PatternSegment::DoubleStar));
    }

    match &patterns[pat_idx] {
        PatternSegment::DoubleStar => {
            // Option 1: ** matches zero segments, skip to next pattern
            if match_segments_indexed(patterns, pat_idx + 1, route, route_idx) {
                return true;
            }
            // Option 2: ** matches one or more segments, consume one segment
            match_segments_indexed(patterns, pat_idx, route, route_idx + 1)
        }
        PatternSegment::Star => {
            // * matches exactly one segment
            match_segments_indexed(patterns, pat_idx + 1, route, route_idx + 1)
        }
        PatternSegment::Literal(pat) => {
            // Literal must match exactly
            if pat == route[route_idx] {
                match_segments_indexed(patterns, pat_idx + 1, route, route_idx + 1)
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
        // Arrange
        let pattern = Pattern::new("notify://acme/orders/*");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_single_star_wildcard_update() {
        // Arrange
        let pattern = Pattern::new("notify://acme/orders/*");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/update"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_single_star_wildcard_delete() {
        // Arrange
        let pattern = Pattern::new("notify://acme/orders/*");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/delete"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_across_single_star_boundary() {
        let pattern = Pattern::new("notify://acme/orders/*");
        // * only matches one segment, not nested paths
        assert!(!pattern.matches(&route("notify://acme/orders/items/create")));
    }

    #[test]
    fn should_match_double_star_from_middle() {
        // Arrange
        let pattern = Pattern::new("notify://acme/**/created");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_from_middle_no_segments() {
        // Arrange
        let pattern = Pattern::new("notify://acme/**/created");

        // Act
        let result = pattern.matches(&route("notify://acme/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_from_middle_many_segments() {
        // Arrange
        let pattern = Pattern::new("notify://acme/**/created");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/items/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_at_end() {
        // Arrange
        let pattern = Pattern::new("notify://acme/orders/**");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_at_end_no_segments() {
        // Arrange
        let pattern = Pattern::new("notify://acme/orders/**");

        // Act
        let result = pattern.matches(&route("notify://acme/orders"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_at_end_many_segments() {
        // Arrange
        let pattern = Pattern::new("notify://acme/orders/**");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/items/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_double_star_across_unrelated_prefix() {
        // Arrange
        let pattern = Pattern::new("notify://acme/**");

        // Act
        let result = pattern.matches(&route("notify://other/orders"));

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_match_multiple_wildcards() {
        // Arrange
        let pattern = Pattern::new("notify://acme/*/*/created");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/create/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_multiple_wildcards_inventory() {
        // Arrange
        let pattern = Pattern::new("notify://acme/*/*/created");

        // Act
        let result = pattern.matches(&route("notify://acme/inventory/check/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_multiple_wildcards_insufficient_segments() {
        // Arrange
        let pattern = Pattern::new("notify://acme/*/*/created");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/created"));

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_match_pattern_without_scheme() {
        // Arrange
        let pattern = Pattern::new("acme/orders/*");

        // Act
        let result = pattern.matches(&route("acme/orders/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_pattern_without_scheme_update() {
        // Arrange
        let pattern = Pattern::new("acme/orders/*");

        // Act
        let result = pattern.matches(&route("acme/orders/update"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_with_no_segments() {
        // Arrange
        let pattern = Pattern::new("notify://acme/**");

        // Act
        let result = pattern.matches(&route("notify://acme"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_with_related_prefix() {
        // Arrange
        let pattern = Pattern::new("notify://acme/**");

        // Act
        let result = pattern.matches(&route("notify://acme/orders"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_literal_when_pattern_expects_wildcard() {
        // Arrange
        let pattern = Pattern::new("notify://acme/*/created");

        // Act
        let result = pattern.matches(&route("notify://acme/orders/update/created"));

        // Assert
        assert!(!result);
    }
}
