// LAYER: RUNTIME
//! Wildcard route matching for runtime patterns
//!
//! Supports NATS-like wildcard routing with two wildcard types.
//!
//! # Wildcard Syntax
//!
//! - `*` (single-level): Matches any sequence of characters within a single path segment
//!   - Example: `notice://acme/orders/*` matches:
//!     - `notice://acme/orders/create`
//!     - `notice://acme/orders/update`
//!     - `notice://acme/orders/delete`
//!   - But NOT `notice://acme/orders/items/create` (different level)
//!
//! - `**` (multi-level): Matches zero or more complete path segments
//!   - Example: `notice://acme/**` matches:
//!     - `notice://acme/orders`
//!     - `notice://acme/orders/create`
//!     - `notice://acme/inventory/check`
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
use std::collections::{HashSet, VecDeque};

type RouteSegments<'a> = SmallVec<[&'a str; 16]>;
const MAX_PATTERN_COVERAGE_STATES: usize = 16_384;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternDepth {
    Flexible,
    CanMatch(usize),
}

/// Compile and validate a registration pattern for a domain.
///
/// Registration patterns require the expected scheme, non-empty path
/// segments, and whole-segment wildcards. `CanMatch` additionally rejects a
/// pattern that can never match a concrete route of the requested depth.
///
/// # Errors
///
/// Returns an error when the scheme, segments, wildcard tokens, or requested
/// concrete depth are invalid.
pub fn compile_registration_pattern(
    route: &str,
    expected_scheme: &str,
    depth: PatternDepth,
) -> Result<Pattern, String> {
    crate::utils::route_shape::validate_route_shape(route)?;
    let Some((scheme, path)) = route.split_once("://") else {
        return Err(format!(
            "subscription pattern must use {expected_scheme}://"
        ));
    };
    if scheme != expected_scheme {
        return Err(format!(
            "subscription pattern scheme must be {expected_scheme}"
        ));
    }
    if path.is_empty() || path.split('/').any(str::is_empty) {
        return Err("subscription pattern must contain non-empty segments".to_string());
    }

    let segments: Vec<&str> = path.split('/').collect();
    if segments
        .iter()
        .any(|segment| segment.contains('*') && !matches!(*segment, "*" | "**"))
    {
        return Err("wildcards must occupy a complete segment".to_string());
    }
    if segments.windows(2).any(|pair| pair == ["**", "**"]) {
        return Err("adjacent ** segments are not allowed".to_string());
    }
    if let PatternDepth::CanMatch(required) = depth {
        let fixed = segments.iter().filter(|segment| **segment != "**").count();
        let flexible = segments.contains(&"**");
        if fixed > required || (!flexible && fixed != required) {
            return Err(format!(
                "subscription pattern cannot match a route with {required} segments"
            ));
        }
    }

    Ok(Pattern::new(route))
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

    /// Return whether this registration contains a wildcard segment.
    #[inline]
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.segments
            .iter()
            .any(|segment| matches!(segment, PatternSegment::Star | PatternSegment::DoubleStar))
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

    /// Return whether this pattern covers every route matched by `requested`.
    ///
    /// This is the authorization relation for wildcard registrations: matching
    /// the registration string itself is insufficient because wildcard tokens
    /// are data to the ordinary route matcher. The product-state walk below
    /// proves language inclusion for the complete `*`/`**` grammar.
    #[must_use]
    pub fn covers(&self, requested: &Self) -> bool {
        match (self.scheme.as_deref(), requested.scheme.as_deref()) {
            (Some(allowed), Some(requested)) if allowed != requested => return false,
            (Some(_), None) => return false,
            _ => {}
        }

        pattern_language_is_subset(&requested.segments, &self.segments)
    }

    /// Return whether this pattern covers every route matched by `requested`
    /// that is exactly `depth` segments long.
    ///
    /// `covers` proves containment over the unrestricted `*`/`**` language,
    /// where a bare `**` is a strict superset of any fixed-depth pattern (it
    /// also matches every other depth) — so for a domain whose concrete
    /// routes are always exactly `depth` segments (Lease's `CanMatch(3)`,
    /// e.g.), a grant spelled `lease://*/*/*#read` would never cover a
    /// requested selector spelled `lease://**`, even though both describe
    /// the identical set of concrete three-segment Lease routes. This
    /// restricts the same product-automaton proof `covers` uses to routes of
    /// exactly `depth` segments, so the two languages are compared over the
    /// domain's actual concrete-route universe rather than the full
    /// unbounded wildcard grammar.
    #[must_use]
    pub fn covers_at_fixed_depth(&self, requested: &Self, depth: usize) -> bool {
        match (self.scheme.as_deref(), requested.scheme.as_deref()) {
            (Some(allowed), Some(requested)) if allowed != requested => return false,
            (Some(_), None) => return false,
            _ => {}
        }

        pattern_language_is_subset_at_depth(&requested.segments, &self.segments, depth)
    }
}

fn pattern_language_is_subset(requested: &[PatternSegment], allowed: &[PatternSegment]) -> bool {
    if requested.len() >= u128::BITS as usize || allowed.len() >= u128::BITS as usize {
        return false;
    }

    let mut symbols = vec![None];
    for segment in requested.iter().chain(allowed) {
        if let PatternSegment::Literal(literal) = segment {
            let symbol = Some(literal.as_str());
            if !symbols.contains(&symbol) {
                symbols.push(symbol);
            }
        }
    }

    let start = (epsilon_closure(requested, 1), epsilon_closure(allowed, 1));
    let mut pending = VecDeque::from([start]);
    let mut visited = HashSet::from([start]);

    while let Some((requested_states, allowed_states)) = pending.pop_front() {
        if accepts(requested_states, requested.len()) && !accepts(allowed_states, allowed.len()) {
            return false;
        }

        for symbol in &symbols {
            let next_requested = transition(requested, requested_states, *symbol);
            if next_requested == 0 {
                continue;
            }
            let next = (next_requested, transition(allowed, allowed_states, *symbol));
            if visited.insert(next) {
                if visited.len() > MAX_PATTERN_COVERAGE_STATES {
                    return false;
                }
                pending.push_back(next);
            }
        }
    }

    true
}

/// Like `pattern_language_is_subset`, but restricted to routes of exactly
/// `depth` segments: every state reached by consuming fewer than `depth`
/// symbols is transient (never checked for acceptance), and only states
/// reached by exactly `depth` symbols are compared. See
/// `Pattern::covers_at_fixed_depth`.
fn pattern_language_is_subset_at_depth(
    requested: &[PatternSegment],
    allowed: &[PatternSegment],
    depth: usize,
) -> bool {
    if requested.len() >= u128::BITS as usize || allowed.len() >= u128::BITS as usize {
        return false;
    }

    let mut symbols = vec![None];
    for segment in requested.iter().chain(allowed) {
        if let PatternSegment::Literal(literal) = segment {
            let symbol = Some(literal.as_str());
            if !symbols.contains(&symbol) {
                symbols.push(symbol);
            }
        }
    }

    let mut frontier =
        HashSet::from([(epsilon_closure(requested, 1), epsilon_closure(allowed, 1))]);

    for _ in 0..depth {
        let mut next_frontier = HashSet::with_capacity(frontier.len());
        for (requested_states, allowed_states) in &frontier {
            for symbol in &symbols {
                let next_requested = transition(requested, *requested_states, *symbol);
                if next_requested == 0 {
                    continue;
                }
                next_frontier.insert((
                    next_requested,
                    transition(allowed, *allowed_states, *symbol),
                ));
                if next_frontier.len() > MAX_PATTERN_COVERAGE_STATES {
                    return false;
                }
            }
        }
        if next_frontier.is_empty() {
            // `requested` cannot reach `depth` symbols at all, so it
            // matches no route of that length: vacuously covered.
            return true;
        }
        frontier = next_frontier;
    }

    frontier
        .into_iter()
        .all(|(requested_states, allowed_states)| {
            !accepts(requested_states, requested.len()) || accepts(allowed_states, allowed.len())
        })
}

fn epsilon_closure(pattern: &[PatternSegment], mut states: u128) -> u128 {
    for (index, segment) in pattern.iter().enumerate() {
        if states & (1 << index) != 0 && matches!(segment, PatternSegment::DoubleStar) {
            states |= 1 << (index + 1);
        }
    }
    states
}

fn transition(pattern: &[PatternSegment], states: u128, symbol: Option<&str>) -> u128 {
    let mut next = 0;
    for (index, segment) in pattern.iter().enumerate() {
        if states & (1 << index) == 0 {
            continue;
        }
        match segment {
            PatternSegment::Literal(literal) if symbol == Some(literal.as_str()) => {
                next |= 1 << (index + 1);
            }
            PatternSegment::Star => next |= 1 << (index + 1),
            PatternSegment::DoubleStar => next |= 1 << index,
            PatternSegment::Literal(_) => {}
        }
    }
    epsilon_closure(pattern, next)
}

fn accepts(states: u128, pattern_len: usize) -> bool {
    states & (1 << pattern_len) != 0
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
#[inline]
fn split_route(route: &str) -> (Option<&str>, RouteSegments<'_>) {
    match crate::runtime::routing::split_scheme(route) {
        Some((scheme, path)) => (Some(scheme), collect_path_segments_fast(path)),
        None => (None, collect_path_segments_fast(route)),
    }
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
    // `split_route` already drops empty segments.
    let (scheme, segments) = split_route(route);
    let segments = segments
        .into_iter()
        .map(|segment| match segment {
            "**" => PatternSegment::DoubleStar,
            "*" => PatternSegment::Star,
            literal => PatternSegment::Literal(literal.to_string()),
        })
        .collect();
    (scheme.map(str::to_string), segments)
}

/// Extract path segments from a route string as borrowed string slices
/// Zero-copy variant for hot-path matching
#[inline]
#[must_use]
pub fn extract_route_segments_borrowed(route: &str) -> RouteSegments<'_> {
    split_route(route).1
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
    fn should_not_cover_deeper_registration_given_fixed_depth_permission() {
        // Arrange
        let permission = Pattern::new("rpc://acme/orders/*/*");
        let registration = Pattern::new("rpc://acme/orders/**/**");

        // Act
        let covers = permission.covers(&registration);

        // Assert
        assert!(!covers);
    }

    #[test]
    fn should_cover_double_star_alias_at_fixed_depth_given_equivalent_literal_star_permission() {
        // Arrange: `lease://*/*/*` and `lease://**` describe the identical
        // set of three-segment Lease routes, but unrestricted `covers`
        // treats `**` as also matching every other depth, so it must fail
        // here — that's the bug this depth-bounded variant fixes.
        let permission = Pattern::new("lease://*/*/*");
        let requested = Pattern::new("lease://**");

        // Act
        let unbounded_covers = permission.covers(&requested);
        let depth_bounded_covers = permission.covers_at_fixed_depth(&requested, 3);

        // Assert
        assert!(
            !unbounded_covers,
            "unrestricted covers must still see ** as a strict superset"
        );
        assert!(depth_bounded_covers);
    }

    #[test]
    fn should_cover_realm_scoped_double_star_alias_at_fixed_depth() {
        // Arrange
        let permission = Pattern::new("lease://acme/*/*");
        let requested = Pattern::new("lease://acme/**");

        // Act / Assert
        assert!(permission.covers_at_fixed_depth(&requested, 3));
    }

    #[test]
    fn should_not_cover_broader_realm_at_fixed_depth() {
        // Arrange: a grant scoped to "acme" must not cover a selector that
        // can also match other realms, even when both are bounded to depth
        // three.
        let permission = Pattern::new("lease://acme/*/*");
        let requested = Pattern::new("lease://*/*/*");

        // Act / Assert
        assert!(!permission.covers_at_fixed_depth(&requested, 3));
    }

    #[test]
    fn should_cover_multi_double_star_alias_at_fixed_depth() {
        // Arrange: `lease://**/x/**` matches every 3-segment route with "x"
        // in any position; a full `lease://*/*/*#read` grant covers all of
        // them, but no single literal-or-* 3-segment permission narrower
        // than that could, since the requested language is itself a union
        // of three distinct shapes (x first, middle, or last).
        let full_grant = Pattern::new("lease://*/*/*");
        let realm_scoped_grant = Pattern::new("lease://acme/*/*");
        let requested = Pattern::new("lease://**/x/**");

        // Act / Assert
        assert!(full_grant.covers_at_fixed_depth(&requested, 3));
        assert!(!realm_scoped_grant.covers_at_fixed_depth(&requested, 3));
    }

    #[test]
    fn should_cover_narrower_registration_given_recursive_permission() {
        // Arrange
        let permission = Pattern::new("rpc://acme/orders/**");
        let registration = Pattern::new("rpc://acme/orders/*/*");

        // Act
        let covers = permission.covers(&registration);

        // Assert
        assert!(covers);
    }

    #[test]
    fn should_not_cover_registration_on_different_scheme() {
        // Arrange
        let permission = Pattern::new("queue://acme/**");
        let registration = Pattern::new("rpc://acme/orders/*");

        // Act
        let covers = permission.covers(&registration);

        // Assert
        assert!(!covers);
    }

    #[test]
    fn should_cover_registration_given_scheme_agnostic_permission() {
        // Arrange
        let permission = Pattern::new("**");
        let registration = Pattern::new("rpc://acme/orders/**");

        // Act
        let covers = permission.covers(&registration);

        // Assert
        assert!(covers);
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
        let unregistered_scheme_match = pattern.matches(&route("notify://acme/orders/create"));
        let queue_match = pattern.matches(&route("queue://acme/orders/create"));

        // Assert
        assert!(!unregistered_scheme_match);
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

    #[test]
    fn should_compile_registration_wildcards_in_any_segment() {
        // Arrange
        let patterns = ["notice://*/orders/created", "notice://acme/*/**"];

        // Act
        let results: Vec<_> = patterns
            .iter()
            .map(|pattern| compile_registration_pattern(pattern, "notice", PatternDepth::Flexible))
            .collect();

        // Assert
        assert!(results.iter().all(Result::is_ok));
    }

    #[test]
    fn should_reject_adjacent_double_star_registration_segments() {
        // Arrange
        let patterns = [
            "notice://acme/**/**",
            "queue://acme/**/**",
            "rpc://acme/**/**",
            "schedule://acme/**/**",
        ];

        // Act
        let results = patterns.map(|pattern| {
            compile_registration_pattern(
                pattern,
                pattern.split("://").next().unwrap(),
                PatternDepth::Flexible,
            )
        });

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_reject_malformed_registration_patterns() {
        // Arrange
        let patterns = [
            "rpc://acme//run",
            "notice://acme/order*",
            "queue://acme/orders",
        ];

        // Act
        let results: Vec<_> = patterns
            .iter()
            .map(|pattern| compile_registration_pattern(pattern, "rpc", PatternDepth::Flexible))
            .collect();

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_reject_registration_pattern_exceeding_shared_route_shape_bounds() {
        // Arrange
        let too_many_segments = format!(
            "notice://{}",
            vec!["a"; crate::utils::route_shape::MAX_ROUTE_SEGMENTS + 1].join("/")
        );
        let too_many_bytes = format!(
            "notice://{}",
            "a".repeat(crate::utils::route_shape::MAX_ROUTE_BYTES)
        );

        // Act
        let segment_result =
            compile_registration_pattern(&too_many_segments, "notice", PatternDepth::Flexible);
        let byte_result =
            compile_registration_pattern(&too_many_bytes, "notice", PatternDepth::Flexible);

        // Assert
        assert!(segment_result.is_err());
        assert!(byte_result.is_err());
    }

    #[test]
    fn should_require_schedule_pattern_to_match_four_segments() {
        // Arrange
        let valid = ["schedule://*/jobs/*/run", "schedule://**"];
        let invalid = ["schedule://acme/jobs/run", "schedule://a/b/c/d/e"];

        // Act
        let valid_results = valid.map(|pattern| {
            compile_registration_pattern(pattern, "schedule", PatternDepth::CanMatch(4))
        });
        let invalid_results = invalid.map(|pattern| {
            compile_registration_pattern(pattern, "schedule", PatternDepth::CanMatch(4))
        });

        // Assert
        assert!(valid_results.iter().all(Result::is_ok));
        assert!(invalid_results.iter().all(Result::is_err));
    }
}
