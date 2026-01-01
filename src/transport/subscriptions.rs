//! High-performance subscription index for route pattern matching
//!
//! # Design
//!
//! Uses a per-RouteFamily trie to index subscriptions by route pattern.
//! Patterns are parsed once at insert time; matching is O(depth + matches).
//!
//! # Trie Structure
//!
//! Each node can have:
//! - Literal children: exact segment matches
//! - Star child: single-segment wildcard `*`
//! - Terminals: subscriptions with exact match at this node
//! - Double-star subscriptions: patterns with `**` at this position, storing suffix patterns
//!
//! # ** Handling
//!
//! When a pattern contains `**`, we split at the wildcard boundary:
//! - `a/b/**/c/d` becomes prefix `[a, b]`, suffix `[c, d]`
//! - The suffix is stored with the subscription at the prefix node
//! - During matching, we try suffix patterns against all possible remaining segments

use crate::transport::routing::{Route, RouteFamily};
use crate::transport::matcher::{PatternSegment, parse_pattern_segments, extract_route_segments, match_pattern_segments};
use std::collections::HashMap;

/// Unique subscription identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// A node in the route pattern trie
struct TrieNode {
    /// Children for literal segment matches
    literals: HashMap<String, Box<TrieNode>>,
    /// Child for single-segment wildcard `*`
    star: Option<Box<TrieNode>>,
    /// Subscriptions with exact match at this node
    terminals: Vec<SubscriptionId>,
    /// Subscriptions with `**` at this position
    /// Each tuple: (subscription_id, pattern_suffix_after_double_star)
    double_star: Vec<(SubscriptionId, Vec<PatternSegment>)>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            literals: HashMap::new(),
            star: None,
            terminals: Vec::new(),
            double_star: Vec::new(),
        }
    }
}

/// High-performance subscription index for wildcard route matching
///
/// - Insert: O(depth) time, O(1) allocation (no hash collisions in worst case)
/// - Remove: O(depth + nodes_to_clean) time
/// - Match: O(depth + matches) time, minimal allocation (only result vec growth)
pub struct SubscriptionIndex {
    /// Trie root per RouteFamily
    roots: HashMap<RouteFamily, Box<TrieNode>>,
}

impl SubscriptionIndex {
    /// Create a new empty subscription index
    pub fn new() -> Self {
        Self {
            roots: HashMap::new(),
        }
    }

    /// Insert a subscription by pattern
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily for isolation
    /// - `pattern`: The route pattern (may contain `*` and `**` wildcards)
    /// - `subscription_id`: Unique identifier for this subscription
    pub fn insert(&mut self, family_id: RouteFamily, pattern: &Route, subscription_id: SubscriptionId) {
        let segments = parse_pattern_segments(pattern.as_str());
        let root = self
            .roots
            .entry(family_id)
            .or_insert_with(|| Box::new(TrieNode::new()));

        insert_into_trie(root, &segments, 0, subscription_id);
    }

    /// Remove a subscription
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily
    /// - `pattern`: The original route pattern
    /// - `subscription_id`: Subscription to remove
    pub fn remove(&mut self, family_id: RouteFamily, pattern: &Route, subscription_id: SubscriptionId) {
        let segments = parse_pattern_segments(pattern.as_str());
        if let Some(root) = self.roots.get_mut(&family_id) {
            remove_from_trie(root, &segments, 0, subscription_id);
        }
    }

    /// Find all subscriptions matching a route
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily (must match insertion family)
    /// - `route`: The published route to match against all patterns
    ///
    /// # Returns
    /// Vector of matching subscription IDs (may contain duplicates if pattern/subscriber pair added multiple times)
    pub fn match_all(&self, family_id: RouteFamily, route: &Route) -> Vec<SubscriptionId> {
        let route_segments = extract_route_segments(route.as_str());
        let root = match self.roots.get(&family_id) {
            Some(r) => r,
            None => return Vec::new(),
        };

        let mut results = Vec::new();
        collect_matches(root, &route_segments, 0, &mut results);
        results
    }
}

impl Default for SubscriptionIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Insert subscription into trie
fn insert_into_trie(
    node: &mut TrieNode,
    segments: &[PatternSegment],
    seg_idx: usize,
    subscription_id: SubscriptionId,
) {
    if seg_idx >= segments.len() {
        // Pattern exhausted: this is an exact match
        node.terminals.push(subscription_id);
        return;
    }

    match &segments[seg_idx] {
        PatternSegment::DoubleStar => {
            // ** followed by remaining pattern becomes a suffix
            let suffix = segments[seg_idx + 1..].to_vec();
            node.double_star.push((subscription_id, suffix));
        }
        PatternSegment::Star => {
            // Create or get * child and continue
            let star_child = node.star.get_or_insert_with(|| Box::new(TrieNode::new()));
            insert_into_trie(star_child, segments, seg_idx + 1, subscription_id);
        }
        PatternSegment::Literal(lit) => {
            // Create or get literal child and continue
            let literal_child = node
                .literals
                .entry(lit.clone())
                .or_insert_with(|| Box::new(TrieNode::new()));
            insert_into_trie(literal_child, segments, seg_idx + 1, subscription_id);
        }
    }
}

/// Remove subscription from trie
fn remove_from_trie(
    node: &mut TrieNode,
    segments: &[PatternSegment],
    seg_idx: usize,
    subscription_id: SubscriptionId,
) {
    if seg_idx >= segments.len() {
        node.terminals.retain(|&id| id != subscription_id);
        return;
    }

    match &segments[seg_idx] {
        PatternSegment::DoubleStar => {
            let suffix = &segments[seg_idx + 1..];
            node.double_star
                .retain(|(id, suf)| !(id == &subscription_id && suf.as_slice() == suffix));
        }
        PatternSegment::Star => {
            if let Some(star_child) = &mut node.star {
                remove_from_trie(star_child, segments, seg_idx + 1, subscription_id);
            }
        }
        PatternSegment::Literal(lit) => {
            if let Some(literal_child) = node.literals.get_mut(lit) {
                remove_from_trie(literal_child, segments, seg_idx + 1, subscription_id);
            }
        }
    }
}

/// Collect all matching subscriptions from trie
fn collect_matches(
    node: &TrieNode,
    route_segments: &[String],
    seg_idx: usize,
    results: &mut Vec<SubscriptionId>,
) {
    // Collect terminal matches only if we've consumed all route segments
    if seg_idx >= route_segments.len() {
        results.extend_from_slice(&node.terminals);
    }

    // For ** subscriptions, check if suffix matches remaining route
    for (id, suffix) in &node.double_star {
        if suffix.is_empty() {
            // ** with no suffix matches everything after this point
            results.push(*id);
        } else if matches_suffix(suffix, route_segments, seg_idx) {
            results.push(*id);
        }
    }

    // If all route segments consumed, stop traversal
    if seg_idx >= route_segments.len() {
        return;
    }

    let current_segment = &route_segments[seg_idx];

    // Try literal match
    if let Some(child) = node.literals.get(current_segment) {
        collect_matches(child, route_segments, seg_idx + 1, results);
    }

    // Try * match
    if let Some(child) = &node.star {
        collect_matches(child, route_segments, seg_idx + 1, results);
    }
}

/// Check if a suffix pattern matches remaining route segments
/// Tries matching suffix against all possible starting positions in the remaining route
fn matches_suffix(suffix: &[PatternSegment], route: &[String], start_idx: usize) -> bool {
    if suffix.is_empty() {
        return true;
    }
    
    // Try matching suffix starting at each position from start_idx onwards
    for try_idx in start_idx..=route.len() {
        if match_pattern_segments(suffix, 0, route, try_idx) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(s: &str) -> Route {
        Route::new(s.to_string())
    }

    fn family(id: u64) -> RouteFamily {
        RouteFamily::new(id)
    }

    fn sub_id(n: u64) -> SubscriptionId {
        SubscriptionId(n)
    }

    #[test]
    fn should_match_exact_pattern() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let pattern = route("notify://realm/orders/create");

        // Act
        index.insert(f, &pattern, sub_id(1));
        let matches = index.match_all(f, &pattern);

        // Assert
        assert_eq!(matches, vec![sub_id(1)]);
    }

    #[test]
    fn should_not_match_different_route() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/create"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/update"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_match_single_star_wildcard() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

        // Act
        let matches_create = index.match_all(f, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(matches_create, vec![sub_id(1)]);
    }

    #[test]
    fn should_not_cross_star_boundary() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/items/create"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_match_double_star_zero_segments() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm"));

        // Assert
        assert_eq!(matches, vec![sub_id(1)]);
    }

    #[test]
    fn should_match_double_star_multiple_segments() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders"));

        // Assert
        assert_eq!(matches, vec![sub_id(1)]);
    }

    #[test]
    fn should_match_double_star_with_suffix() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**/created"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/created"));

        // Assert
        assert_eq!(matches, vec![sub_id(1)]);
    }

    #[test]
    fn should_not_cross_realm_with_double_star() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://acme/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://other/orders"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_isolate_by_route_family() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f1 = family(1);
        let f2 = family(2);
        index.insert(f1, &route("notify://realm/orders/*"), sub_id(1));
        index.insert(f2, &route("notify://realm/orders/*"), sub_id(2));

        // Act
        let f1_matches = index.match_all(f1, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(f1_matches, vec![sub_id(1)]);
    }

    #[test]
    fn should_isolate_families_independently() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f1 = family(1);
        let f2 = family(2);
        index.insert(f1, &route("notify://realm/orders/*"), sub_id(1));
        index.insert(f2, &route("notify://realm/orders/*"), sub_id(2));

        // Act
        let f2_matches = index.match_all(f2, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(f2_matches, vec![sub_id(2)]);
    }

    #[test]
    fn should_handle_multiple_subscribers_to_same_pattern() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let pattern = route("notify://realm/orders/*");
        index.insert(f, &pattern, sub_id(1));
        index.insert(f, &pattern, sub_id(2));

        // Act
        let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
        matches.sort_by_key(|id| id.0);

        // Assert
        assert_eq!(matches, vec![sub_id(1), sub_id(2)]);
    }

    #[test]
    fn should_remove_subscription() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let pattern = route("notify://realm/orders/*");
        index.insert(f, &pattern, sub_id(1));

        // Act
        index.remove(f, &pattern, sub_id(1));
        let matches = index.match_all(f, &route("notify://realm/orders/create"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_handle_mixed_patterns() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
        index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
        index.insert(f, &route("notify://realm/**"), sub_id(3));

        // Act
        let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
        matches.sort_by_key(|id| id.0);

        // Assert
        assert_eq!(matches, vec![sub_id(1), sub_id(2), sub_id(3)]);
    }
}
