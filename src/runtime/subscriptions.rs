// LAYER: RUNTIME
//! High-performance subscription index for route pattern matching
//!
//! # Design
//!
//! Uses a per-RouteFamily trie to index subscriptions by route pattern.
//! Patterns are parsed once at insert time; matching is O(depth + matches).
//!
//! Routes follow: `{scheme}://{realm}/{area}/{resource}/{operation}`
//! where scheme indicates intent, and realm/area/resource/operation are user-defined.
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

use crate::runtime::matcher::{
    extract_route_segments_borrowed, match_pattern_segments, match_pattern_segments_borrowed,
    parse_pattern_segments, PatternSegment,
};
use crate::runtime::routing::{Route, RouteFamily};
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::Arc;

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
    terminals: SmallVec<[SubscriptionId; 8]>,
    /// Subscriptions with `**` at this position
    /// Each tuple: (subscription_id, pattern_suffix_after_double_star)
    /// Uses Arc to avoid cloning suffixes when multiple subscriptions share the same pattern
    double_star: SmallVec<[(SubscriptionId, Arc<Vec<PatternSegment>>); 4]>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            literals: HashMap::new(),
            star: None,
            terminals: SmallVec::new(),
            double_star: SmallVec::new(),
        }
    }
}

/// High-performance subscription index for wildcard route matching
///
/// - Insert: O(depth) time, O(1) allocation (no hash collisions in worst case)
/// - Remove: O(depth + nodes_to_clean) time
/// - Match: O(depth + matches) time, minimal allocation (only result vec growth)
///
/// Uses RwLock for high read concurrency - matching takes read lock,
/// insert/remove take write lock.
pub struct SubscriptionIndex {
    /// Trie root per RouteFamily (protected by RwLock for concurrent reads)
    roots: RwLock<HashMap<RouteFamily, Box<TrieNode>>>,
}

impl SubscriptionIndex {
    /// Create a new empty subscription index
    pub fn new() -> Self {
        Self {
            roots: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a subscription by pattern
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily for isolation
    /// - `pattern`: The route pattern (may contain `*` and `**` wildcards)
    /// - `subscription_id`: Unique identifier for this subscription
    pub fn insert(&self, family_id: RouteFamily, pattern: &Route, subscription_id: SubscriptionId) {
        let segments = parse_pattern_segments(pattern.as_str());
        let mut roots = self.roots.write();
        let root = roots
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
    pub fn remove(&self, family_id: RouteFamily, pattern: &Route, subscription_id: SubscriptionId) {
        let segments = parse_pattern_segments(pattern.as_str());
        let mut roots = self.roots.write();
        if let Some(root) = roots.get_mut(&family_id) {
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
        let route_segments = extract_route_segments_borrowed(route.as_str());
        let roots = self.roots.read();
        let root = match roots.get(&family_id) {
            Some(r) => r,
            None => return Vec::new(),
        };

        let mut results = Vec::new();
        collect_matches_borrowed(root, &route_segments, 0, &mut results);
        results
    }

    /// Find all subscriptions matching a route with pre-allocated capacity
    ///
    /// Use this when you expect a specific number of matches to avoid re-allocations.
    pub fn match_all_with_capacity(
        &self,
        family_id: RouteFamily,
        route: &Route,
        capacity: usize,
    ) -> Vec<SubscriptionId> {
        let route_segments = extract_route_segments_borrowed(route.as_str());
        let roots = self.roots.read();
        let root = match roots.get(&family_id) {
            Some(r) => r,
            None => return Vec::new(),
        };

        let mut results = Vec::with_capacity(capacity);
        collect_matches_borrowed(root, &route_segments, 0, &mut results);
        results
    }

    /// Count subscriptions in a specific RouteFamily (for diagnostics/metrics)
    pub fn count_subscriptions(&self, family_id: RouteFamily) -> usize {
        let roots = self.roots.read();
        roots
            .get(&family_id)
            .map(|root| count_node(root))
            .unwrap_or(0)
    }

    /// Get approximate memory usage of a specific RouteFamily index
    ///
    /// Includes trie nodes, but not String allocations or Arc payloads.
    pub fn approx_memory_usage(&self, family_id: RouteFamily) -> usize {
        let roots = self.roots.read();
        roots
            .get(&family_id)
            .map(|root| approx_node_memory(root))
            .unwrap_or(0)
    }

    /// Extract realm from a route string (first segment after scheme://)
    ///
    /// Routes follow: `{scheme}://{realm}/{area}/{resource}/{operation}`
    /// This extracts the realm for realm-aware optimization.
    ///
    /// Returns None if the route cannot be parsed to extract realm.
    pub fn extract_realm(route: &Route) -> Option<&str> {
        let s = route.as_str();
        // Skip scheme://
        if let Some(after_scheme) = s.split_once("://") {
            // Get first segment after scheme
            if let Some(realm) = after_scheme.1.split('/').next() {
                if !realm.is_empty() {
                    return Some(realm);
                }
            }
        }
        None
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
            // Use Arc to avoid cloning if multiple subs share this suffix
            let suffix = Arc::new(segments[seg_idx + 1..].to_vec());
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
        node.terminals.retain(|id| id != &subscription_id);
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
#[allow(dead_code)]
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
/// Optimized with early exit for short suffixes
#[allow(dead_code)]
fn matches_suffix(suffix: &[PatternSegment], route: &[String], start_idx: usize) -> bool {
    if suffix.is_empty() {
        return true;
    }

    // Fast path: if suffix is very short, use linear scan
    if suffix.len() <= 2 {
        // For short suffixes, just check each starting position sequentially
        for try_idx in start_idx..=route.len() {
            if match_pattern_segments(suffix, 0, route, try_idx) {
                return true;
            }
        }
    } else {
        // For longer suffixes, still try all positions
        for try_idx in start_idx..=route.len() {
            if match_pattern_segments(suffix, 0, route, try_idx) {
                return true;
            }
        }
    }
    false
}

/// Collect matches using borrowed strings (zero-copy variant)
#[inline]
fn collect_matches_borrowed(
    node: &TrieNode,
    route_segments: &[&str],
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
        } else if matches_suffix_borrowed(suffix, route_segments, seg_idx) {
            results.push(*id);
        }
    }

    // If all route segments consumed, stop traversal
    if seg_idx >= route_segments.len() {
        return;
    }

    let current_segment = route_segments[seg_idx];

    // Try literal match
    if let Some(child) = node.literals.get(current_segment) {
        collect_matches_borrowed(child, route_segments, seg_idx + 1, results);
    }

    // Try * match
    if let Some(child) = &node.star {
        collect_matches_borrowed(child, route_segments, seg_idx + 1, results);
    }
}

/// Check if a suffix pattern matches remaining route segments (borrowed string variant)
/// Tries matching suffix against all possible starting positions in the remaining route
#[inline]
fn matches_suffix_borrowed(suffix: &[PatternSegment], route: &[&str], start_idx: usize) -> bool {
    if suffix.is_empty() {
        return true;
    }

    // Try matching suffix at each possible starting position
    for try_idx in start_idx..=route.len() {
        if match_pattern_segments_borrowed(suffix, 0, route, try_idx) {
            return true;
        }
    }
    false
}

/// Count total subscriptions in a trie node and its children
fn count_node(node: &TrieNode) -> usize {
    let mut count = node.terminals.len() + node.double_star.len();
    for child in node.literals.values() {
        count += count_node(child);
    }
    if let Some(child) = &node.star {
        count += count_node(child);
    }
    count
}

/// Approximate memory usage of a trie node and its children (bytes)
/// Includes TrieNode struct, HashMap, and SmallVec allocations, but not String data
fn approx_node_memory(node: &TrieNode) -> usize {
    // TrieNode: HashMap(48) + Option(24) + SmallVec(24) + SmallVec(24) = ~120 bytes
    let mut size = 120;

    // HashMap entries: each entry ~48 bytes (key pointer + value box)
    size += node.literals.len() * 48;

    // Recursively count children
    for child in node.literals.values() {
        size += approx_node_memory(child);
    }
    if let Some(child) = &node.star {
        size += approx_node_memory(child);
    }

    size
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
        let index = SubscriptionIndex::new();
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
