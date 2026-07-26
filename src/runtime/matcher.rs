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
//! Wildcards apply **only within the same `RouteFamily`**.
//! The `RouteFamily` ID must match exactly; wildcards never cross family boundaries.

use crate::runtime::routing::Route;
use smallvec::SmallVec;

type RouteSegments<'a> = SmallVec<[&'a str; 16]>;

/// Wildcard pattern for route subscriptions
///
/// A pattern is a route that may contain `*` and `**` wildcards.
/// Patterns are matched against published routes to determine fan-out targets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pattern {
    /// The full route pattern (e.g., `<notify://realm/area/*>`)
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
    #[must_use]
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
    #[must_use]
    pub fn route(&self) -> &str {
        &self.route
    }

    /// Check if this pattern matches a given route
    #[inline]
    #[must_use]
    pub fn matches(&self, route: &Route) -> bool {
        self.matches_str(route.as_str())
    }

    /// Check if this pattern matches a raw route string.
    #[inline]
    #[must_use]
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
#[must_use]
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
/// Optimized to minimize allocations and use faster byte scanning.
#[inline]
fn split_route(route: &str) -> (Option<&str>, RouteSegments<'_>) {
    let bytes = route.as_bytes();

    // Fast path: scan for "://" using bytes directly
    let path_start = if bytes.len() >= 3 {
        // SIMD-like scan: check if "://" pattern exists
        for i in 0..bytes.len().saturating_sub(2) {
            if bytes[i] == b':' && bytes[i + 1] == b'/' && bytes[i + 2] == b'/' {
                // Found scheme://
                let scheme = &route[..i];
                return (Some(scheme), collect_path_segments_fast(&route[i + 3..]));
            }
        }
        None
    } else {
        None
    };

    (path_start, collect_path_segments_fast(route))
}

/// Fast path segment collection without intermediate allocations.
#[inline]
fn collect_path_segments_fast(path: &str) -> RouteSegments<'_> {
    let mut segments = RouteSegments::new();
    for seg in path.split('/') {
        if !seg.is_empty() {
            segments.push(seg);
        }
    }
    segments
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
/// DEPRECATED: Use `extract_route_segments_borrowed` for zero-copy matching
#[must_use]
pub fn extract_route_segments(route: &str) -> Vec<String> {
    split_route(route)
        .1
        .into_iter()
        .map(std::string::ToString::to_string)
        .collect()
}

/// Extract path segments from a route string as borrowed string slices
/// Zero-copy variant for hot-path matching
#[inline]
#[must_use]
pub fn extract_route_segments_borrowed(route: &str) -> RouteSegments<'_> {
    split_route(route).1
}

/// Match pattern segments against route segments without recursion.
/// Used by both Pattern matching and `SubscriptionIndex` suffix matching
#[must_use]
pub fn match_pattern_segments(
    patterns: &[PatternSegment],
    pat_idx: usize,
    route: &[String],
    route_idx: usize,
) -> bool {
    match_segments_dynamic(patterns, pat_idx, route, route_idx)
}

/// Match pattern segments against route segments with borrowed strings
/// Zero-copy variant for hot-path matching
#[inline]
#[must_use]
pub fn match_pattern_segments_borrowed(
    patterns: &[PatternSegment],
    pat_idx: usize,
    route: &[&str],
    route_idx: usize,
) -> bool {
    match_segments_dynamic(patterns, pat_idx, route, route_idx)
}

/// Match route segments against pattern segments with bounded heap state.
#[inline]
fn match_segments(patterns: &[PatternSegment], route: &[&str]) -> bool {
    match_segments_dynamic(patterns, 0, route, 0)
}

fn match_segments_dynamic<T: AsRef<str>>(
    patterns: &[PatternSegment],
    pat_idx: usize,
    route: &[T],
    route_idx: usize,
) -> bool {
    let patterns = patterns.get(pat_idx..).unwrap_or_default();
    let route = route.get(route_idx..).unwrap_or_default();
    let mut previous: SmallVec<[bool; 32]> = std::iter::repeat_n(false, route.len() + 1).collect();
    previous[0] = true;

    for pattern in patterns {
        let mut current: SmallVec<[bool; 32]> =
            std::iter::repeat_n(false, route.len() + 1).collect();
        match pattern {
            PatternSegment::DoubleStar => {
                current[0] = previous[0];
                for index in 1..=route.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            PatternSegment::Star => {
                current[1..].copy_from_slice(&previous[..route.len()]);
            }
            PatternSegment::Literal(literal) => {
                for index in 1..=route.len() {
                    current[index] = previous[index - 1] && literal == route[index - 1].as_ref();
                }
            }
        }
        previous = current;
    }

    previous[route.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(path: &str) -> Route {
        Route::new(path)
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

    #[test]
    fn should_match_long_route_without_recursion() {
        // Arrange
        let pattern = Pattern::new("notice://**");
        let long_route = format!("notice://{}", vec!["a"; 32_760].join("/"));

        // Act
        let result = pattern.matches_str(&long_route);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_multiple_double_stars_deterministically() {
        // Arrange
        let pattern = Pattern::new("notice://**/orders/**/created");

        // Act
        let result = pattern.matches(&route(
            "notice://prod/west/orders/customer/priority/created",
        ));

        // Assert
        assert!(result);
    }
}
