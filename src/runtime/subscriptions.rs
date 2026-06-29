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

    fn add_terminal(&mut self, subscription_id: SubscriptionId) {
        if !self.terminals.contains(&subscription_id) {
            self.terminals.push(subscription_id);
        }
    }

    fn remove_terminal(&mut self, subscription_id: SubscriptionId) {
        self.terminals.retain(|id| *id != subscription_id);
    }

    fn add_double_star(
        &mut self,
        subscription_id: SubscriptionId,
        suffix: Arc<[CompiledPatternSegment]>,
    ) {
        let is_duplicate = self
            .double_star_subs
            .iter()
            .zip(self.double_star_suffixes.iter())
            .any(|(id, existing_suffix)| {
                *id == subscription_id && existing_suffix.as_ref() == suffix.as_ref()
            });

        if !is_duplicate {
            self.double_star_subs.push(subscription_id);
            self.double_star_suffixes.push(suffix);
        }
    }

    fn remove_double_star(
        &mut self,
        subscription_id: SubscriptionId,
        suffix: &[CompiledPatternSegment],
    ) {
        let mut index = 0;
        while index < self.double_star_subs.len() {
            if self.double_star_subs[index] == subscription_id
                && self.double_star_suffixes[index].as_ref() == suffix
            {
                self.double_star_subs.swap_remove(index);
                self.double_star_suffixes.swap_remove(index);
            } else {
                index += 1;
            }
        }
    }

    fn collect_double_star_matches(
        &self,
        route_segments: &[CompiledRouteSegment],
        segment_index: usize,
        results: &mut SubscriptionMatches,
    ) {
        for (subscription_id, suffix) in self
            .double_star_subs
            .iter()
            .zip(self.double_star_suffixes.iter())
        {
            if suffix.is_empty() || matches_suffix_compiled(suffix, route_segments, segment_index) {
                push_unique_subscription(results, *subscription_id);
            }
        }
    }

    fn collect_final_matches(&self, results: &mut SubscriptionMatches) {
        if !self.terminals.is_empty() {
            results.extend_from_slice(&self.terminals);
        }

        for (subscription_id, suffix) in self
            .double_star_subs
            .iter()
            .zip(self.double_star_suffixes.iter())
        {
            if suffix.is_empty() {
                push_unique_subscription(results, *subscription_id);
            }
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
                    node.collect_double_star_matches(
                        &compiled_route_segments,
                        seg_idx,
                        &mut results,
                    );
                }
            }
            std::mem::swap(&mut current, &mut next);
        }

        // All route segments consumed — collect terminals from final frontier.
        for &node_id in &current {
            self.nodes[node_id as usize].collect_final_matches(&mut results);
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
            self.nodes[node_id as usize].add_terminal(subscription_id);
            return;
        }

        match &segments[seg_idx] {
            CompiledPatternSegment::DoubleStar => {
                // ** followed by remaining pattern becomes a stored suffix.
                let suffix: Arc<[CompiledPatternSegment]> =
                    Arc::from(segments[seg_idx + 1..].to_vec());
                self.nodes[node_id as usize].add_double_star(subscription_id, suffix);
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
            self.nodes[node_id as usize].remove_terminal(subscription_id);
            return;
        }

        match &segments[seg_idx] {
            CompiledPatternSegment::DoubleStar => {
                let suffix = &segments[seg_idx + 1..];
                self.nodes[node_id as usize].remove_double_star(subscription_id, suffix);
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
        Self::compile_pattern_segments_from_iter(parsed.iter(), |value| {
            Some(self.segment_interner.intern(value))
        })
        .expect("segment interning should not fail")
    }

    #[inline]
    fn compile_pattern_segments_lookup(&self, route: &str) -> Option<Vec<CompiledPatternSegment>> {
        let parsed = parse_pattern_segments(route);
        Self::compile_pattern_segments_from_iter(parsed.iter(), |value| {
            self.segment_interner.lookup(value)
        })
    }

    #[inline]
    fn compile_pattern_segments_from_iter<'a, I, F>(
        segments: I,
        mut resolve_exact_id: F,
    ) -> Option<Vec<CompiledPatternSegment>>
    where
        I: IntoIterator<Item = &'a PatternSegment>,
        F: FnMut(&str) -> Option<SegmentId>,
    {
        let segments = segments.into_iter();
        let (lower_bound, _) = segments.size_hint();
        let mut compiled = Vec::with_capacity(lower_bound);

        for segment in segments {
            match segment {
                PatternSegment::Literal(value) => {
                    compiled.push(CompiledPatternSegment::Exact(resolve_exact_id(value)?));
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
mod tests;
