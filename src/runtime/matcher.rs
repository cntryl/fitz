// LAYER: RUNTIME
//! Wildcard route matching for runtime patterns
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

use crate::runtime::routing::Route;
use smallvec::SmallVec;

type RouteSegments<'a> = SmallVec<[&'a str; 8]>;

/// Wildcard pattern for route subscriptions
///
/// A pattern is a route that may contain `*` and `**` wildcards.
/// Patterns are matched against published routes to determine fan-out targets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pattern {
    /// The full route pattern (e.g., "notify://realm/area/*")
    route: String,
    scheme: Option<String>,
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
    #[inline]
    pub fn new(route: &str) -> Self {
        let (scheme, segments) = parse_pattern(route);
        Self {
            route: route.to_string(),
            scheme,
            segments,
        }
    }

    /// Get the original route pattern
    #[inline]
    pub fn route(&self) -> &str {
        &self.route
    }

    /// Check if this pattern matches a given route
    #[inline]
    pub fn matches(&self, route: &Route) -> bool {
        self.matches_str(route.as_str())
    }

    /// Check if this pattern matches a raw route string.
    #[inline]
    pub fn matches_str(&self, route: &str) -> bool {
        let (route_scheme, route_segments) = split_route(route);

        if let Some(pattern_scheme) = self.scheme.as_deref() {
            if route_scheme != Some(pattern_scheme) {
                return false;
            }
        }

        match_segments(&self.segments, &route_segments)
    }
}

/// Parse a route pattern into segments
pub fn parse_pattern_segments(route: &str) -> Vec<PatternSegment> {
    split_route(route)
        .1
        .into_iter()
        .map(|segment| match segment {
            "**" => PatternSegment::DoubleStar,
            "*" => PatternSegment::Star,
            literal => PatternSegment::Literal(literal.to_string()),
        })
        .collect()
}

/// Extract an optional scheme and borrowed path segments from a route string.
#[inline]
fn split_route(route: &str) -> (Option<&str>, RouteSegments<'_>) {
    let (scheme, path) = if let Some(idx) = route.find("://") {
        (Some(&route[..idx]), &route[idx + 3..])
    } else {
        (None, route)
    };

    let segments = path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect::<RouteSegments<'_>>();
    (scheme, segments)
}

fn parse_pattern(route: &str) -> (Option<String>, Vec<PatternSegment>) {
    let (scheme, segments) = split_route(route);
    let segments = segments
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(|segment| match segment {
            "**" => PatternSegment::DoubleStar,
            "*" => PatternSegment::Star,
            literal => PatternSegment::Literal(literal.to_string()),
        })
        .collect();
    (scheme.map(str::to_string), segments)
}

/// Extract path segments from a route string as Strings
/// DEPRECATED: Use extract_route_segments_borrowed for zero-copy matching
pub fn extract_route_segments(route: &str) -> Vec<String> {
    split_route(route)
        .1
        .into_iter()
        .map(|s| s.to_string())
        .collect()
}

/// Extract path segments from a route string as borrowed string slices
/// Zero-copy variant for hot-path matching
#[inline]
pub fn extract_route_segments_borrowed(route: &str) -> RouteSegments<'_> {
    split_route(route).1
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

/// Match pattern segments against route segments with borrowed strings
/// Zero-copy variant for hot-path matching
#[inline]
pub fn match_pattern_segments_borrowed(
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
            // Option 1: ** matches zero segments
            if match_pattern_segments_borrowed(patterns, pat_idx + 1, route, route_idx) {
                return true;
            }
            // Option 2: ** matches one or more segments
            match_pattern_segments_borrowed(patterns, pat_idx, route, route_idx + 1)
        }
        PatternSegment::Star => {
            // * matches exactly one segment
            match_pattern_segments_borrowed(patterns, pat_idx + 1, route, route_idx + 1)
        }
        PatternSegment::Literal(pat) => {
            // Literal must match exactly
            if pat.as_str() == route[route_idx] {
                match_pattern_segments_borrowed(patterns, pat_idx + 1, route, route_idx + 1)
            } else {
                false
            }
        }
    }
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
        let pattern = Pattern::new("notice://acme/orders/create");
        assert!(pattern.matches(&route("notice://acme/orders/create")));
    }

    #[test]
    fn should_not_match_different_route() {
        let pattern = Pattern::new("notice://acme/orders/create");
        assert!(!pattern.matches(&route("notice://acme/orders/update")));
    }

    #[test]
    fn should_match_single_star_wildcard() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/*");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_single_star_wildcard_update() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/*");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/update"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_single_star_wildcard_delete() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/*");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/delete"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_across_single_star_boundary() {
        let pattern = Pattern::new("notice://acme/orders/*");
        // * only matches one segment, not nested paths
        assert!(!pattern.matches(&route("notice://acme/orders/items/create")));
    }

    #[test]
    fn should_match_double_star_from_middle() {
        // Arrange
        let pattern = Pattern::new("notice://acme/**/created");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_from_middle_no_segments() {
        // Arrange
        let pattern = Pattern::new("notice://acme/**/created");

        // Act
        let result = pattern.matches(&route("notice://acme/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_from_middle_many_segments() {
        // Arrange
        let pattern = Pattern::new("notice://acme/**/created");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/items/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_at_end() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/**");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_at_end_no_segments() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/**");

        // Act
        let result = pattern.matches(&route("notice://acme/orders"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_at_end_many_segments() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/**");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/items/create"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_double_star_across_unrelated_prefix() {
        // Arrange
        let pattern = Pattern::new("notice://acme/**");

        // Act
        let result = pattern.matches(&route("notice://other/orders"));

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_match_multiple_wildcards() {
        // Arrange
        let pattern = Pattern::new("notice://acme/*/*/created");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/create/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_multiple_wildcards_inventory() {
        // Arrange
        let pattern = Pattern::new("notice://acme/*/*/created");

        // Act
        let result = pattern.matches(&route("notice://acme/inventory/check/created"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_multiple_wildcards_insufficient_segments() {
        // Arrange
        let pattern = Pattern::new("notice://acme/*/*/created");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/created"));

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
        let pattern = Pattern::new("notice://acme/**");

        // Act
        let result = pattern.matches(&route("notice://acme"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_double_star_with_related_prefix() {
        // Arrange
        let pattern = Pattern::new("notice://acme/**");

        // Act
        let result = pattern.matches(&route("notice://acme/orders"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_literal_when_pattern_expects_wildcard() {
        // Arrange
        let pattern = Pattern::new("notice://acme/*/created");

        // Act
        let result = pattern.matches(&route("notice://acme/orders/update/created"));

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_not_match_different_scheme_with_same_path() {
        // Arrange
        let pattern = Pattern::new("notice://acme/orders/**");

        // Act
        let notify_match = pattern.matches(&route("notify://acme/orders/create"));
        let queue_match = pattern.matches(&route("queue://acme/orders/create"));

        // Assert
        assert!(!notify_match);
        assert!(!queue_match);
    }

    #[test]
    fn should_allow_scheme_agnostic_pattern_only_when_pattern_has_no_scheme() {
        // Arrange
        let pattern = Pattern::new("acme/orders/**");

        // Act
        let notice_match = pattern.matches(&route("notice://acme/orders/create"));
        let queue_match = pattern.matches(&route("queue://acme/orders/create"));

        // Assert
        assert!(notice_match);
        assert!(queue_match);
    }
}
