// LAYER: RUNTIME
//! High-performance subscription index for route pattern matching
//!
//! # Design
//!
//! Uses a per-RouteFamily segment trie to index subscriptions by route pattern.
//! Patterns are parsed once at insert time; matching is O(depth + matches).
//!
//! Routes follow: `{scheme}://{realm}/{area}/{resource}/{operation}`
//! where scheme indicates intent, and realm/area/resource/operation are user-defined.
//!
//! # Trie Structure
//!
//! Nodes are stored in a flat `Vec<Node>` and referenced by `NodeId` (u32).
//! Each node can have:
//! - Exact children: literal segment matches keyed by interned `SegmentId`
//! - Star child: single-segment wildcard `*`
//! - Terminals: subscriptions whose pattern ends at this node
//! - Double-star: patterns with `**` at this position, storing suffix patterns
//!
//! # Matching
//!
//! Iterative frontier traversal — no recursion. Two `SmallVec` scratch buffers
//! are swapped per route segment. At each frontier node, double-star entries
//! are checked against the remaining route suffix, then exact and star children
//! form the next frontier.
//!
//! # ** Handling
//!
//! When a pattern contains `**`, we split at the wildcard boundary:
//! - `a/b/**/c/d` becomes prefix `[a, b]`, suffix `[c, d]`
//! - The suffix is stored with the subscription at the prefix node
//! - During matching, we try suffix patterns against all possible remaining segments

use crate::runtime::matcher::{
    extract_route_segments_borrowed, parse_pattern_segments, PatternSegment,
};
use crate::runtime::routing::{Route, RouteFamily};
use ahash::AHashMap;
use fxhash::FxBuildHasher;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::Arc;

use self::segments_cache::SegmentsCache;

type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;

type NodeId = u32;
type SegmentId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompiledPatternSegment {
    Exact(SegmentId),
    Star,
    DoubleStar,
}

#[derive(Debug, Clone, Copy)]
struct CompiledRouteSegment {
    exact_id: Option<SegmentId>,
}

struct SegmentInterner {
    ids: AHashMap<String, SegmentId>,
}

impl SegmentInterner {
    fn new() -> Self {
        Self {
            ids: AHashMap::new(),
        }
    }

    #[inline]
    fn intern(&mut self, value: &str) -> SegmentId {
        if let Some(&id) = self.ids.get(value) {
            return id;
        }

        let id = self.ids.len() as SegmentId;
        self.ids.insert(value.to_string(), id);
        id
    }

    #[inline]
    fn lookup(&self, value: &str) -> Option<SegmentId> {
        self.ids.get(value).copied()
    }
}

impl Default for SegmentInterner {
    fn default() -> Self {
        Self::new()
    }
}

mod segments_cache {
    use crate::runtime::matcher::{parse_pattern_segments, PatternSegment};
    use fxhash::FxBuildHasher;
    use std::collections::HashMap;
    use std::sync::Arc;

    const CACHE_MAX_ENTRIES: usize = 4096;
    type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;

    pub struct SegmentsCache {
        cache: FastMap<String, Arc<Vec<PatternSegment>>>,
    }

    impl SegmentsCache {
        pub fn new() -> Self {
            Self {
                cache: HashMap::with_capacity_and_hasher(256, FxBuildHasher::default()),
            }
        }

        pub fn get_or_parse(&mut self, route: &str) -> Arc<Vec<PatternSegment>> {
            if let Some(cached) = self.cache.get(route) {
                return Arc::clone(cached);
            }

            let segments = Arc::new(parse_pattern_segments(route));

            // Keep existing hot entries instead of flushing the whole cache.
            // Once full, we intentionally avoid inserting new routes to preserve
            // stable hit rates for already-hot keys.
            if self.cache.len() < CACHE_MAX_ENTRIES {
                self.cache.insert(route.to_string(), Arc::clone(&segments));
            }

            segments
        }

        #[allow(dead_code)]
        pub fn clear(&mut self) {
            self.cache.clear();
        }

        #[cfg(test)]
        pub fn len(&self) -> usize {
            self.cache.len()
        }
    }

    impl Default for SegmentsCache {
        fn default() -> Self {
            Self::new()
        }
    }
}

/// Unique subscription identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Common match result buffer for subscription lookups.
pub type SubscriptionMatches = SmallVec<[SubscriptionId; 8]>;

/// A node in the route pattern trie.
///
/// Nodes live in a flat `Vec<Node>` for cache locality. Children are
/// referenced by `NodeId` (u32) instead of heap-allocated pointers.
struct Node {
    /// Children keyed by interned literal segment id.
    exact: AHashMap<SegmentId, NodeId>,
    /// Child for single-segment wildcard `*`.
    single_wildcard: Option<NodeId>,
    /// Subscriptions whose pattern ends exactly at this node.
    terminals: SmallVec<[SubscriptionId; 2]>,
    /// Subscriptions with `**` at this position.
    /// Parallel vectors: double_star_subs[i] has suffix double_star_suffixes[i].
    /// Uses Arc to share suffixes when multiple subscriptions share the same pattern.
    double_star_subs: SmallVec<[SubscriptionId; 2]>,
    double_star_suffixes: SmallVec<[Arc<[CompiledPatternSegment]>; 2]>,
}

impl Node {
    fn new() -> Self {
        Self {
            exact: AHashMap::new(),
            single_wildcard: None,
            terminals: SmallVec::new(),
            double_star_subs: SmallVec::new(),
            double_star_suffixes: SmallVec::new(),
        }
    }
}

/// High-performance subscription index for wildcard route matching
///
/// - Insert: O(depth) time
/// - Remove: O(depth) time
/// - Match: O(depth + active_nodes * double_star_work) time
pub struct SubscriptionIndex {
    /// Flat node pool. NodeIds refer into this vector; per-family roots are
    /// recorded in `family_roots`.
    nodes: Vec<Node>,
    /// Trie root per RouteFamily.
    family_roots: FastMap<RouteFamily, NodeId>,
    /// Cache for parsed pattern segments to avoid re-parsing same routes.
    segments_cache: SegmentsCache,
    /// Interned IDs for exact literal segments used in hot trie lookups.
    segment_interner: SegmentInterner,
}

impl SubscriptionIndex {
    /// Create a new empty subscription index
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            family_roots: HashMap::with_capacity_and_hasher(8, FxBuildHasher::default()),
            segments_cache: SegmentsCache::new(),
            segment_interner: SegmentInterner::new(),
        }
    }

    /// Allocate a new node and return its ID
    fn alloc_node(&mut self) -> NodeId {
        let id = self.nodes.len() as NodeId;
        self.nodes.push(Node::new());
        id
    }

    /// Get or create the root node for a RouteFamily
    fn get_or_create_root(&mut self, family_id: RouteFamily) -> NodeId {
        if let Some(&root) = self.family_roots.get(&family_id) {
            return root;
        }
        let root = self.alloc_node();
        self.family_roots.insert(family_id, root);
        root
    }

    /// Insert a subscription by pattern
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily for isolation
    /// - `pattern`: The route pattern (may contain `*` and `**` wildcards)
    /// - `subscription_id`: Unique identifier for this subscription
    pub fn insert(
        &mut self,
        family_id: RouteFamily,
        pattern: &Route,
        subscription_id: SubscriptionId,
    ) {
        let segments = self.compile_pattern_segments_intern(pattern.as_str());
        let root = self.get_or_create_root(family_id);
        self.insert_into_trie(root, &segments, 0, subscription_id);
    }

    /// Insert multiple subscriptions in a single batched operation (reduces
    /// allocation overhead and improves cache locality in batch setups).
    pub fn insert_batch(&mut self, family_id: RouteFamily, items: &[(Route, SubscriptionId)]) {
        if items.is_empty() {
            return;
        }

        // Reserve enough nodes for the typical case of mostly-literal patterns.
        self.nodes.reserve(items.len() * 2);

        let root = self.get_or_create_root(family_id);
        for (pattern, subscription_id) in items {
            let segments = self.compile_pattern_segments_intern(pattern.as_str());
            self.insert_into_trie(root, &segments, 0, *subscription_id);
        }
    }

    /// Remove a subscription
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily
    /// - `pattern`: The original route pattern
    /// - `subscription_id`: Subscription to remove
    pub fn remove(
        &mut self,
        family_id: RouteFamily,
        pattern: &Route,
        subscription_id: SubscriptionId,
    ) {
        let Some(segments) = self.compile_pattern_segments_lookup(pattern.as_str()) else {
            return;
        };
        if let Some(&root) = self.family_roots.get(&family_id) {
            self.remove_from_trie(root, &segments, 0, subscription_id);
        }
    }

    /// Find all subscriptions matching a route
    ///
    /// # Arguments
    /// - `family_id`: RouteFamily (must match insertion family)
    /// - `route`: The published route to match against all patterns
    ///
    /// # Returns
    /// Vector of matching subscription IDs.
    ///
    /// Match order is not part of the API contract and may vary with internal
    /// trie layout and removal order.
    #[inline]
    pub fn match_all(&self, family_id: RouteFamily, route: &Route) -> SubscriptionMatches {
        self.match_all_route_str_with_capacity(family_id, route.as_str(), 8)
    }

    /// Find all subscriptions matching a raw route string.
    #[inline]
    pub fn match_all_route_str(&self, family_id: RouteFamily, route: &str) -> SubscriptionMatches {
        self.match_all_route_str_with_capacity(family_id, route, 8)
    }

    /// Find all subscriptions matching a route with pre-allocated capacity
    ///
    /// Use this when you expect a specific number of matches to avoid re-allocations.
    pub fn match_all_with_capacity(
        &self,
        family_id: RouteFamily,
        route: &Route,
        capacity: usize,
    ) -> SubscriptionMatches {
        self.match_all_route_str_with_capacity(family_id, route.as_str(), capacity)
    }

    /// Find all subscriptions matching a raw route string with pre-allocated capacity.
    pub fn match_all_route_str_with_capacity(
        &self,
        family_id: RouteFamily,
        route: &str,
        capacity: usize,
    ) -> SubscriptionMatches {
        let route_segments = extract_route_segments_borrowed(route);
        let compiled_route_segments = self.compile_route_segments_lookup(&route_segments);
        let Some(&root) = self.family_roots.get(&family_id) else {
            return SubscriptionMatches::new();
        };

        let mut results = SubscriptionMatches::with_capacity(capacity);

        // Iterative frontier traversal — no recursion.
        // Two SmallVec buffers swapped per segment. Stack-allocated for up to
        // 32 active nodes (sufficient for routes with moderate wildcard fan-out).
        let mut current: SmallVec<[NodeId; 16]> = SmallVec::new();
        let mut next: SmallVec<[NodeId; 16]> = SmallVec::new();
        current.push(root);

        for (seg_idx, segment) in compiled_route_segments.iter().enumerate() {
            next.clear();
            for &node_id in &current {
                let node = &self.nodes[node_id as usize];

                // Follow exact child (most common path — check first).
                if let Some(exact_id) = segment.exact_id {
                    if let Some(&child) = node.exact.get(&exact_id) {
                        next.push(child);
                    }
                }
                // Follow * child.
                if let Some(child) = node.single_wildcard {
                    next.push(child);
                }
                // Check double-star entries against remaining route (rare).
                if !node.double_star_subs.is_empty() {
                    for (sub_id, suffix) in node
                        .double_star_subs
                        .iter()
                        .zip(node.double_star_suffixes.iter())
                    {
                        if suffix.is_empty()
                            || matches_suffix_compiled(suffix, &compiled_route_segments, seg_idx)
                        {
                            push_unique_subscription(&mut results, *sub_id);
                        }
                    }
                }
            }
            std::mem::swap(&mut current, &mut next);
        }

        // All route segments consumed — collect terminals from final frontier.
        for &node_id in &current {
            let node = &self.nodes[node_id as usize];
            if !node.terminals.is_empty() {
                results.extend_from_slice(&node.terminals);
            }
            // Check empty-suffix double-star entries (match end of route).
            if !node.double_star_subs.is_empty() {
                for (sub_id, suffix) in node
                    .double_star_subs
                    .iter()
                    .zip(node.double_star_suffixes.iter())
                {
                    if suffix.is_empty() {
                        push_unique_subscription(&mut results, *sub_id);
                    }
                }
            }
        }

        results
    }

    /// Count subscriptions in a specific RouteFamily (for diagnostics/metrics)
    pub fn count_subscriptions(&self, family_id: RouteFamily) -> usize {
        let Some(&root) = self.family_roots.get(&family_id) else {
            return 0;
        };
        // Iterative count — avoids stack overflow on deep tries.
        let mut count = 0usize;
        let mut stack: SmallVec<[NodeId; 16]> = SmallVec::new();
        stack.push(root);
        while let Some(node_id) = stack.pop() {
            let node = &self.nodes[node_id as usize];
            count += node.terminals.len() + node.double_star_subs.len();
            for &child in node.exact.values() {
                stack.push(child);
            }
            if let Some(child) = node.single_wildcard {
                stack.push(child);
            }
        }
        count
    }

    /// Insert subscription into trie (recursive walk)
    fn insert_into_trie(
        &mut self,
        node_id: NodeId,
        segments: &[CompiledPatternSegment],
        seg_idx: usize,
        subscription_id: SubscriptionId,
    ) {
        if seg_idx >= segments.len() {
            // Pattern exhausted: terminal subscriber at this node.
            let node = &mut self.nodes[node_id as usize];
            if !node.terminals.contains(&subscription_id) {
                node.terminals.push(subscription_id);
            }
            return;
        }

        match &segments[seg_idx] {
            CompiledPatternSegment::DoubleStar => {
                // ** followed by remaining pattern becomes a stored suffix.
                let suffix: Arc<[CompiledPatternSegment]> =
                    Arc::from(segments[seg_idx + 1..].to_vec());
                let node = &mut self.nodes[node_id as usize];
                // Prevent duplicate (same sub_id + same suffix).
                let is_dup = node
                    .double_star_subs
                    .iter()
                    .zip(node.double_star_suffixes.iter())
                    .any(|(id, suf)| *id == subscription_id && suf.as_ref() == suffix.as_ref());
                if !is_dup {
                    node.double_star_subs.push(subscription_id);
                    node.double_star_suffixes.push(suffix);
                }
            }
            CompiledPatternSegment::Star => {
                // Get or create * child.
                if self.nodes[node_id as usize].single_wildcard.is_none() {
                    let child = self.alloc_node();
                    self.nodes[node_id as usize].single_wildcard = Some(child);
                }
                let child = self.nodes[node_id as usize].single_wildcard.unwrap();
                self.insert_into_trie(child, segments, seg_idx + 1, subscription_id);
            }
            CompiledPatternSegment::Exact(seg_id) => {
                // Get or create literal child.
                let child = if let Some(&child) = self.nodes[node_id as usize].exact.get(seg_id) {
                    child
                } else {
                    let child = self.alloc_node();
                    self.nodes[node_id as usize].exact.insert(*seg_id, child);
                    child
                };
                self.insert_into_trie(child, segments, seg_idx + 1, subscription_id);
            }
        }
    }

    /// Remove subscription from trie
    fn remove_from_trie(
        &mut self,
        node_id: NodeId,
        segments: &[CompiledPatternSegment],
        seg_idx: usize,
        subscription_id: SubscriptionId,
    ) {
        if seg_idx >= segments.len() {
            self.nodes[node_id as usize]
                .terminals
                .retain(|id| id != &subscription_id);
            return;
        }

        match &segments[seg_idx] {
            CompiledPatternSegment::DoubleStar => {
                let suffix = &segments[seg_idx + 1..];
                let node = &mut self.nodes[node_id as usize];
                let mut i = 0;
                while i < node.double_star_subs.len() {
                    if node.double_star_subs[i] == subscription_id
                        && node.double_star_suffixes[i].as_ref() == suffix
                    {
                        node.double_star_subs.swap_remove(i);
                        node.double_star_suffixes.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
            }
            CompiledPatternSegment::Star => {
                if let Some(child) = self.nodes[node_id as usize].single_wildcard {
                    self.remove_from_trie(child, segments, seg_idx + 1, subscription_id);
                }
            }
            CompiledPatternSegment::Exact(seg_id) => {
                if let Some(&child) = self.nodes[node_id as usize].exact.get(seg_id) {
                    self.remove_from_trie(child, segments, seg_idx + 1, subscription_id);
                }
            }
        }
    }

    #[inline]
    fn compile_pattern_segments_intern(&mut self, route: &str) -> Vec<CompiledPatternSegment> {
        let parsed = self.segments_cache.get_or_parse(route);
        let mut compiled = Vec::with_capacity(parsed.len());
        for segment in parsed.iter() {
            match segment {
                PatternSegment::Literal(value) => {
                    compiled.push(CompiledPatternSegment::Exact(
                        self.segment_interner.intern(value),
                    ));
                }
                PatternSegment::Star => compiled.push(CompiledPatternSegment::Star),
                PatternSegment::DoubleStar => compiled.push(CompiledPatternSegment::DoubleStar),
            }
        }
        compiled
    }

    #[inline]
    fn compile_pattern_segments_lookup(&self, route: &str) -> Option<Vec<CompiledPatternSegment>> {
        let parsed = parse_pattern_segments(route);
        let mut compiled = Vec::with_capacity(parsed.len());
        for segment in parsed {
            match segment {
                PatternSegment::Literal(value) => {
                    let seg_id = self.segment_interner.lookup(&value)?;
                    compiled.push(CompiledPatternSegment::Exact(seg_id));
                }
                PatternSegment::Star => compiled.push(CompiledPatternSegment::Star),
                PatternSegment::DoubleStar => compiled.push(CompiledPatternSegment::DoubleStar),
            }
        }
        Some(compiled)
    }

    #[inline]
    fn compile_route_segments_lookup(
        &self,
        route_segments: &[&str],
    ) -> SmallVec<[CompiledRouteSegment; 8]> {
        let mut compiled =
            SmallVec::<[CompiledRouteSegment; 8]>::with_capacity(route_segments.len());
        for segment in route_segments {
            compiled.push(CompiledRouteSegment {
                exact_id: self.segment_interner.lookup(segment),
            });
        }
        compiled
    }
}

impl Default for SubscriptionIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a suffix pattern matches remaining route segments at any starting position.
#[inline]
fn matches_suffix_compiled(
    suffix: &[CompiledPatternSegment],
    route: &[CompiledRouteSegment],
    start_idx: usize,
) -> bool {
    if suffix.is_empty() {
        return true;
    }
    for try_idx in start_idx..=route.len() {
        if match_compiled_pattern_segments(suffix, 0, route, try_idx) {
            return true;
        }
    }
    false
}

#[inline]
fn match_compiled_pattern_segments(
    pattern_segments: &[CompiledPatternSegment],
    pattern_idx: usize,
    route_segments: &[CompiledRouteSegment],
    route_idx: usize,
) -> bool {
    if pattern_idx >= pattern_segments.len() {
        return route_idx >= route_segments.len();
    }

    match pattern_segments[pattern_idx] {
        CompiledPatternSegment::DoubleStar => {
            if pattern_idx + 1 >= pattern_segments.len() {
                true
            } else {
                for skip_count in route_idx..=route_segments.len() {
                    if match_compiled_pattern_segments(
                        pattern_segments,
                        pattern_idx + 1,
                        route_segments,
                        skip_count,
                    ) {
                        return true;
                    }
                }
                false
            }
        }
        CompiledPatternSegment::Star => {
            if route_idx >= route_segments.len() {
                false
            } else {
                match_compiled_pattern_segments(
                    pattern_segments,
                    pattern_idx + 1,
                    route_segments,
                    route_idx + 1,
                )
            }
        }
        CompiledPatternSegment::Exact(expected_id) => {
            if route_idx >= route_segments.len() {
                false
            } else {
                route_segments[route_idx].exact_id == Some(expected_id)
                    && match_compiled_pattern_segments(
                        pattern_segments,
                        pattern_idx + 1,
                        route_segments,
                        route_idx + 1,
                    )
            }
        }
    }
}

#[inline]
fn push_unique_subscription(results: &mut SubscriptionMatches, subscription_id: SubscriptionId) {
    if !results.contains(&subscription_id) {
        results.push(subscription_id);
    }
}

#[cfg(test)]
mod tests {
    use super::segments_cache::SegmentsCache;
    use super::*;

    fn route(s: &str) -> Route {
        Route::new(s)
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
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
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
        assert_eq!(matches_create.as_slice(), &[sub_id(1)]);
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
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
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
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_not_duplicate_empty_suffix_double_star_matches() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
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
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
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
        assert_eq!(f1_matches.as_slice(), &[sub_id(1)]);
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
        assert_eq!(f2_matches.as_slice(), &[sub_id(2)]);
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
        assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2)]);
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
        assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2), sub_id(3)]);
    }

    #[test]
    fn should_match_sparse_route_family_without_preallocating_gaps() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let sparse_family = family(1_000_000);
        let pattern = route("notify://realm/orders/*");

        // Act
        index.insert(sparse_family, &pattern, sub_id(7));
        let matches = index.match_all(sparse_family, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(7)]);
    }

    #[test]
    fn should_bound_segments_cache_without_flushing_existing_entries() {
        // Arrange
        let mut cache = SegmentsCache::new();
        let pinned_route = "notify://realm/orders/pinned";

        // Act
        let original = cache.get_or_parse(pinned_route);
        for index in 0..4096 {
            let route = format!("notify://realm/orders/{index}");
            let _ = cache.get_or_parse(&route);
        }
        let reloaded = cache.get_or_parse(pinned_route);

        // Assert
        assert!(Arc::ptr_eq(&original, &reloaded));
        assert_eq!(cache.len(), 4096);
    }

    #[test]
    fn should_match_double_star_suffix_with_many_segments() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**/created"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/items/created"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_match_double_star_at_end() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_match_double_star_at_end_no_segments() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_match_double_star_at_end_many_segments() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/items/create"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_not_match_double_star_suffix_when_missing() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://acme/**/created"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://acme/orders/updated"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_remove_one_of_many_subscriptions() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let pattern = route("notify://realm/orders/*");
        index.insert(f, &pattern, sub_id(1));
        index.insert(f, &pattern, sub_id(2));
        index.insert(f, &pattern, sub_id(3));

        // Act
        index.remove(f, &pattern, sub_id(2));
        let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
        matches.sort_by_key(|id| id.0);

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(3)]);
    }

    #[test]
    fn should_match_single_star_for_create_operation() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_match_single_star_for_update_operation() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/update"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_match_single_star_for_delete_operation() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/*"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/delete"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_match_star_with_multiple_wildcards() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/*/*/created"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/create/created"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_not_match_insufficient_segments_for_star_chain() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/*/*/created"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/orders/created"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_match_route_after_batch_insert() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let batch: Vec<(Route, SubscriptionId)> = vec![
            (route("notify://realm/orders/create"), sub_id(1)),
            (route("notify://realm/orders/*"), sub_id(2)),
            (route("notify://realm/**"), sub_id(3)),
        ];

        // Act
        index.insert_batch(f, &batch);
        let mut matches = index.match_all(f, &route("notify://realm/orders/create"));
        matches.sort_by_key(|id| id.0);

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2), sub_id(3)]);
    }

    #[test]
    fn should_count_subscriptions_across_patterns() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
        index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
        index.insert(f, &route("notify://realm/**"), sub_id(3));

        // Act
        let count = index.count_subscriptions(f);

        // Assert
        assert_eq!(count, 3);
    }

    #[test]
    fn should_count_zero_for_unknown_family() {
        // Arrange
        let index = SubscriptionIndex::new();

        // Act
        let count = index.count_subscriptions(family(999));

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn should_match_empty_route_family() {
        // Arrange
        let index = SubscriptionIndex::new();

        // Act
        let matches = index.match_all(family(1), &route("notify://realm/orders/create"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_prevent_duplicate_terminal_subscribers() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let pattern = route("notify://realm/orders/create");

        // Act
        index.insert(f, &pattern, sub_id(1));
        index.insert(f, &pattern, sub_id(1));
        let matches = index.match_all(f, &route("notify://realm/orders/create"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_prevent_duplicate_double_star_subscribers() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        let pattern = route("notify://realm/**");

        // Act
        index.insert(f, &pattern, sub_id(1));
        index.insert(f, &pattern, sub_id(1));
        let matches = index.match_all(f, &route("notify://realm/orders"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_handle_remove_from_empty_index() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);

        // Act — should not panic
        index.remove(f, &route("notify://realm/orders/*"), sub_id(1));

        // Assert
        assert_eq!(index.count_subscriptions(f), 0);
    }

    #[test]
    fn should_handle_remove_nonexistent_pattern() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/create"), sub_id(1));

        // Act — remove a different pattern, should not panic
        index.remove(f, &route("notify://realm/orders/update"), sub_id(2));

        // Assert — original subscription still there
        assert_eq!(index.count_subscriptions(f), 1);
    }

    #[test]
    fn should_not_match_double_star_suffix_at_end() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://acme/**/created"), sub_id(1));

        // Act — route ends before suffix "created"
        let matches = index.match_all(f, &route("notify://acme/orders"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_handle_shallow_routes() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_handle_deep_routes_with_star() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/*/*/*/*"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/a/b/c/d"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_not_match_deep_route_with_shallow_star_pattern() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/*/*/*/*"), sub_id(1));

        // Act — only 3 segments after realm, pattern needs 4
        let matches = index.match_all(f, &route("notify://realm/a/b/c"));

        // Assert
        assert!(matches.is_empty());
    }

    #[test]
    fn should_match_double_star_suffix_deep() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/**/action"), sub_id(1));

        // Act
        let matches = index.match_all(f, &route("notify://realm/a/b/c/action"));

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1)]);
    }

    #[test]
    fn should_collect_results_with_capacity() {
        // Arrange
        let mut index = SubscriptionIndex::new();
        let f = family(1);
        index.insert(f, &route("notify://realm/orders/create"), sub_id(1));
        index.insert(f, &route("notify://realm/orders/*"), sub_id(2));
        index.insert(f, &route("notify://realm/**"), sub_id(3));

        // Act
        let mut matches =
            index.match_all_with_capacity(f, &route("notify://realm/orders/create"), 3);
        matches.sort_by_key(|id| id.0);

        // Assert
        assert_eq!(matches.as_slice(), &[sub_id(1), sub_id(2), sub_id(3)]);
    }

    // Property-based tests

    proptest::proptest! {
        #[test]
        fn should_match_bruteforce_scan_for_generated_patterns(
            // Generate up to 8 patterns, each with up to 6 segments
            patterns in proptest::collection::vec(
                proptest::string::string_regex("[a-z]{1,5}").unwrap(),
                1..=8,
            ),
            route_str in proptest::string::string_regex("[a-z]{1,5}(/[a-z]{1,5}){0,5}").unwrap(),
        ) {
            use crate::runtime::matcher::Pattern;

            // Arrange
            let f = family(1);
            let mut index = SubscriptionIndex::new();

            // Insert each pattern as an exact route (no wildcards for simplicity)
            for (i, p) in patterns.iter().enumerate() {
                let full_pattern = format!("test://{}", p);
                index.insert(f, &route(&full_pattern), sub_id(i as u64));
            }

            let full_route = format!("test://{}", route_str);
            let r = route(&full_route);

            // Act
            // Get trie matches
            let mut trie_matches = index.match_all(f, &r);
            trie_matches.sort_by_key(|id| id.0);

            // Brute-force scan: check each pattern with Pattern::matches
            let mut bf_matches = SubscriptionMatches::new();
            for (i, p) in patterns.iter().enumerate() {
                let full_pattern = format!("test://{}", p);
                let pattern = Pattern::new(&full_pattern);
                if pattern.matches(&r) {
                    bf_matches.push(sub_id(i as u64));
                }
            }
            bf_matches.sort_by_key(|id| id.0);

            // Assert
            assert_eq!(trie_matches, bf_matches, "trie diverged from brute-force for patterns={:?} route={}", patterns, full_route);
        }

        #[test]
        fn should_preserve_roundtrip_after_insert_remove(
            pattern_str in proptest::string::string_regex("[a-z]{1,5}(/[a-z]{1,5}){0,3}").unwrap(),
            route_str in proptest::string::string_regex("[a-z]{1,5}(/[a-z]{1,5}){0,5}").unwrap(),
        ) {
            use crate::runtime::matcher::Pattern;

            // Arrange
            let f = family(1);
            let mut index = SubscriptionIndex::new();
            let full_pattern = format!("test://{}", pattern_str);
            let full_route = format!("test://{}", route_str);
            let p = route(&full_pattern);
            let r = route(&full_route);

            // Act
            index.insert(f, &p, sub_id(42));
            let matches_after_insert = index.match_all(f, &r);
            index.remove(f, &p, sub_id(42));
            let matches_after_remove = index.match_all(f, &r);

            // Assert
            // After remove, subscription 42 should never appear
            assert!(!matches_after_remove.contains(&sub_id(42)));

            // If it matched before, it should have contained 42
            let pattern = Pattern::new(&full_pattern);
            if pattern.matches(&r) {
                assert!(matches_after_insert.contains(&sub_id(42)));
            }
        }
    }
}
