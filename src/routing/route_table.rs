use fxhash::FxHashMap;
use smallvec::SmallVec;

use crate::core::domain::SubSender;

/// Route Family identifier (tenant / shard / CF boundary)
pub type RouteFamilyId = u32;

/// Default route family for tests / legacy callers
pub const DEFAULT_RF: RouteFamilyId = 0;

/// Number of shards for route table (must be power of 2 for mask)
/// 16–64 is a good range; 32 is a nice balance for 100M subs.
const SHARD_COUNT: usize = 32;
const SHARD_MASK: usize = SHARD_COUNT - 1;

/// Max segments we care about in a route (`scheme://realm/area/resource/op`)
const MAX_SEGMENTS: usize = 8;

/// Max matches we'll return per lookup without reallocating.
/// In practice, most routes have < 16 subscribers.
const MAX_MATCHES: usize = 256;

/// Basic subscription entry
#[derive(Debug, Clone)]
pub struct RtSubscription {
    pub id: u64,
    pub route_pattern: String,
    pub channel_id: u32,
    pub sender: SubSender,
}

/// Compact subscription list:
/// - Inline up to 4 IDs (no heap)
/// - Spills to heap only when needed (big fan-out tenants)
#[derive(Debug, Clone, Default)]
struct SubList {
    ids: SmallVec<[u64; 4]>,
}

impl SubList {
    #[inline]
    fn insert_if_absent(&mut self, id: u64) {
        if !self.ids.contains(&id) {
            self.ids.push(id);
        }
    }

    #[inline]
    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(u64) -> bool,
    {
        self.ids.retain(|id| f(*id));
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[inline]
    fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.ids.iter().copied()
    }
}

/// One node in the route trie:
/// - exact_subs: subscriptions that match this path exactly
/// - trailing_wildcard_subs: subscriptions like "a/b/*" anchored here
/// - children: exact segment children
/// - wildcard_child: matches "*" at this position
#[derive(Debug, Default)]
struct TrieNode {
    exact_subs: SubList,
    trailing_wildcard_subs: SubList,
    children: FxHashMap<String, TrieNode>,
    wildcard_child: Option<Box<TrieNode>>,
}

impl TrieNode {
    /// Is this node completely empty (used for pruning)
    fn is_empty(&self) -> bool {
        self.exact_subs.is_empty()
            && self.trailing_wildcard_subs.is_empty()
            && self.children.is_empty()
            && self.wildcard_child.is_none()
    }
}

/// Per-RF trie
#[derive(Debug)]
struct RouteTrie {
    root: TrieNode,
    global_subs: SubList, // pattern == "*"
}

impl RouteTrie {
    fn new() -> Self {
        Self {
            root: TrieNode::default(),
            global_subs: SubList::default(),
        }
    }
}

/// One shard of the route table
#[derive(Debug, Default)]
struct RouteTableShard {
    /// Authoritative subs by ID (for remove / cleanup)
    subs: FxHashMap<u64, RtSubscription>,
    /// Route tries per RF
    tries: FxHashMap<RouteFamilyId, RouteTrie>,
}

impl RouteTableShard {
    fn new() -> Self {
        Self {
            subs: FxHashMap::default(),
            tries: FxHashMap::default(),
        }
    }

    fn insert(&mut self, rf: RouteFamilyId, sub: RtSubscription) {
        let id = sub.id;
        let pattern = sub.route_pattern.clone();

        self.insert_into_trie(rf, &pattern, id);
        self.subs.insert(id, sub);
    }

    fn remove(&mut self, rf: RouteFamilyId, id: u64) -> Option<RtSubscription> {
        let sub = self.subs.remove(&id)?;
        self.remove_from_trie(rf, &sub.route_pattern, id);
        Some(sub)
    }

    fn cleanup_channel(&mut self, rf: RouteFamilyId, channel_id: u32) {
        let mut to_remove = SmallVec::<[u64; 16]>::new();
        for (id, sub) in &self.subs {
            if sub.channel_id == channel_id {
                to_remove.push(*id);
            }
        }
        for id in to_remove {
            let _ = self.remove(rf, id);
        }
    }

    fn insert_into_trie(&mut self, rf: RouteFamilyId, pattern: &str, sub_id: u64) {
        let trie = self.tries.entry(rf).or_insert_with(RouteTrie::new);

        if pattern == "*" {
            trie.global_subs.insert_if_absent(sub_id);
            return;
        }

        let has_trailing_wildcard = pattern.ends_with("/*");
        let segments: SmallVec<[&str; MAX_SEGMENTS]> =
            pattern.split('/').take(MAX_SEGMENTS).collect();

        let segments_to_traverse = if has_trailing_wildcard && !segments.is_empty() {
            &segments[..segments.len() - 1]
        } else {
            &segments[..]
        };

        let mut node = &mut trie.root;

        for seg in segments_to_traverse {
            if *seg == "*" {
                node = node
                    .wildcard_child
                    .get_or_insert_with(|| Box::new(TrieNode::default()));
            } else {
                node = node
                    .children
                    .entry((*seg).to_string())
                    .or_insert_with(TrieNode::default);
            }
        }

        if has_trailing_wildcard {
            node.trailing_wildcard_subs.insert_if_absent(sub_id);
        } else {
            node.exact_subs.insert_if_absent(sub_id);
        }
    }

    fn remove_from_trie(&mut self, rf: RouteFamilyId, pattern: &str, sub_id: u64) {
        let Some(trie) = self.tries.get_mut(&rf) else {
            return;
        };

        if pattern == "*" {
            trie.global_subs.retain(|id| id != sub_id);
            return;
        }

        let has_trailing_wildcard = pattern.ends_with("/*");
        let segments: SmallVec<[&str; MAX_SEGMENTS]> =
            pattern.split('/').take(MAX_SEGMENTS).collect();

        let segments_to_traverse = if has_trailing_wildcard && !segments.is_empty() {
            &segments[..segments.len() - 1]
        } else {
            &segments[..]
        };

        fn remove_rec(
            node: &mut TrieNode,
            segments: &[&str],
            sub_id: u64,
            is_trailing: bool,
            depth: usize,
        ) -> bool {
            if depth == segments.len() {
                if is_trailing {
                    node.trailing_wildcard_subs.retain(|id| id != sub_id);
                } else {
                    node.exact_subs.retain(|id| id != sub_id);
                }
                return node.is_empty();
            }

            let seg = segments[depth];

            if seg == "*" {
                if let Some(child) = node.wildcard_child.as_mut() {
                    let prune =
                        remove_rec(child, segments, sub_id, is_trailing, depth + 1);
                    if prune {
                        node.wildcard_child = None;
                    }
                }
            } else if let Some(child) = node.children.get_mut(seg) {
                let prune =
                    remove_rec(child, segments, sub_id, is_trailing, depth + 1);
                if prune {
                    node.children.remove(seg);
                }
            }

            node.is_empty()
        }

        remove_rec(&mut trie.root, segments_to_traverse, sub_id, has_trailing_wildcard, 0);
    }

    /// Zero-alloc match inside a shard:
    /// - `route_segments` lives on caller's stack
    /// - `scratch_ids` is a fixed stack array for all shards
    fn matching_ids_into(
        &self,
        rf: RouteFamilyId,
        route_segments: &[&str],
        scratch_ids: &mut [u64; MAX_MATCHES],
        count: &mut usize,
    ) {
        let Some(trie) = self.tries.get(&rf) else {
            return;
        };

        // Global wildcard
        for id in trie.global_subs.iter() {
            if *count < MAX_MATCHES && !scratch_ids[..*count].contains(&id) {
                scratch_ids[*count] = id;
                *count += 1;
            }
        }

        fn walk(
            node: &TrieNode,
            segs: &[&str],
            depth: usize,
            scratch_ids: &mut [u64; MAX_MATCHES],
            count: &mut usize,
        ) {
            if depth == segs.len() {
                for id in node.exact_subs.iter().chain(node.trailing_wildcard_subs.iter()) {
                    if *count < MAX_MATCHES && !scratch_ids[..*count].contains(&id) {
                        scratch_ids[*count] = id;
                        *count += 1;
                    }
                }
                return;
            }

            let seg = segs[depth];

            if let Some(child) = node.children.get(seg) {
                walk(child, segs, depth + 1, scratch_ids, count);
            }

            if let Some(child) = node.wildcard_child.as_ref() {
                walk(child, segs, depth + 1, scratch_ids, count);
            }

            // Hierarchical prefix + trailing wildcard match
            for id in node.exact_subs.iter().chain(node.trailing_wildcard_subs.iter()) {
                if *count < MAX_MATCHES && !scratch_ids[..*count].contains(&id) {
                    scratch_ids[*count] = id;
                    *count += 1;
                }
            }
        }

        walk(&trie.root, route_segments, 0, scratch_ids, count);
    }
}

/// Sharded route table:
/// - sharded by RF for locality & future parallelism
/// - zero-alloc hotpath for matches
pub struct RouteTable {
    shards: [RouteTableShard; SHARD_COUNT],
}

impl RouteTable {
    pub fn new() -> Self {
        // Manual init because arrays require Copy/Default trick
        let mut shards: [RouteTableShard; SHARD_COUNT] = {
            // SAFETY: we immediately overwrite every element
            let mut arr: [RouteTableShard; SHARD_COUNT] =
                unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
            for slot in &mut arr {
                *slot = RouteTableShard::new();
            }
            arr
        };

        Self { shards }
    }

    #[inline]
    fn shard_index(rf: RouteFamilyId) -> usize {
        (rf as usize) & SHARD_MASK
    }

    fn shard_mut(&mut self, rf: RouteFamilyId) -> &mut RouteTableShard {
        &mut self.shards[Self::shard_index(rf)]
    }

    fn shard_ref(&self, rf: RouteFamilyId) -> &RouteTableShard {
        &self.shards[Self::shard_index(rf)]
    }

    pub fn insert(&mut self, rf: RouteFamilyId, sub: RtSubscription) {
        self.shard_mut(rf).insert(rf, sub);
    }

    pub fn remove(&mut self, rf: RouteFamilyId, id: u64) -> Option<RtSubscription> {
        self.shard_mut(rf).remove(rf, id)
    }

    pub fn cleanup_channel(&mut self, rf: RouteFamilyId, channel_id: u32) {
        self.shard_mut(rf).cleanup_channel(rf, channel_id);
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.subs.len()).sum()
    }

    /// Zero-alloc hot path:
    /// - route split into stack array
    /// - scratch_ids is stack array
    /// - no heap allocations until we build the final Vec
    pub fn matching_subscribers(
        &self,
        rf: RouteFamilyId,
        route: &str,
    ) -> Vec<RtSubscription> {
        // Split route once into stack-backed array
        let mut segs: [&str; MAX_SEGMENTS] = [""; MAX_SEGMENTS];
        let mut seg_len = 0;
        for (i, seg) in route.split('/').enumerate().take(MAX_SEGMENTS) {
            segs[i] = seg;
            seg_len += 1;
        }
        let route_segments = &segs[..seg_len];

        let shard = self.shard_ref(rf);

        // Collect IDs into stack buffer
        let mut ids_buf: [u64; MAX_MATCHES] = [0; MAX_MATCHES];
        let mut count = 0usize;

        shard.matching_ids_into(rf, route_segments, &mut ids_buf, &mut count);

        // Single Vec allocation at the very end
        let mut out = Vec::with_capacity(count);
        for id in ids_buf[..count].iter().copied() {
            if let Some(sub) = shard.subs.get(&id) {
                out.push(sub.clone());
            }
        }
        out
    }
}

// ============================================================================
// Legacy pattern matching function - kept for unit tests
// Production code uses trie-based matching in walk_matches() for O(depth) complexity
// ============================================================================

/// Zero-allocation route matcher using iterators instead of Vec collections.
/// Supports: exact match, global wildcard (*), trailing wildcard (a/b/*),
/// mid-path wildcards (a/*/c), and hierarchical prefix matching (a/b matches a/b/c).
///
/// NOTE: This function is kept for backwards compatibility with unit tests.
/// Production matching uses the trie-based approach in `find_matches()`.
#[cfg(test)]
#[inline]
fn route_matches(_rf: RouteFamilyId, pattern: &str, route: &str) -> bool {
    // Fast path: global wildcard
    if pattern == "*" {
        return true;
    }

    // Fast path: exact match
    if pattern == route {
        return true;
    }

    // Handle patterns with wildcards (mid-path or trailing)
    if pattern.contains('*') {
        // Check for trailing wildcard efficiently
        let has_trailing_wildcard = pattern.ends_with("/*");

        // Match segments using iterators (zero allocations)
        let mut pattern_iter = pattern.split('/');
        let mut route_iter = route.split('/');

        loop {
            match (pattern_iter.next(), route_iter.next()) {
                (Some("*"), _)
                    if has_trailing_wildcard && pattern_iter.clone().next().is_none() =>
                {
                    // Trailing wildcard matches everything remaining (including nothing)
                    return true;
                }
                (Some("*"), Some(_)) => {
                    // Mid-path wildcard matches this segment, continue
                    continue;
                }
                (Some(p), Some(r)) if p == r => {
                    // Exact segment match, continue
                    continue;
                }
                (Some(_), Some(_)) => {
                    // Segment mismatch
                    return false;
                }
                (None, None) => {
                    // Both exhausted, perfect match
                    return true;
                }
                (None, Some(_)) => {
                    // Pattern exhausted but route has more segments
                    // This shouldn't happen with proper trailing wildcard handling
                    return false;
                }
                (Some(_), None) => {
                    // Route exhausted but pattern has more
                    return false;
                }
            }
        }
    }

    // Hierarchical matching without wildcards
    // Optimized: avoid format!() allocation, check boundary byte
    if route.len() > pattern.len()
        && route.as_bytes()[pattern.len()] == b'/'
        && route.starts_with(pattern)
    {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn should_insert_and_match() {
        let mut rt = RouteTable::new();
        let (tx, _rx) = mpsc::channel(10);
        let sub = RtSubscription {
            id: 1,
            route_pattern: "a/b/*".to_string(),
            channel_id: 1,
            sender: tx,
        };

        rt.insert(DEFAULT_RF, sub);
        let matches = rt.matching_subscribers(DEFAULT_RF, "a/b/c");
        assert_eq!(matches.len(), 1);
    }

    #[tokio::test]
    async fn should_remove_and_cleanup() {
        let mut rt = RouteTable::new();
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 1,
                route_pattern: "r1".to_string(),
                channel_id: 1,
                sender: tx1,
            },
        );
        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 2,
                route_pattern: "r2".to_string(),
                channel_id: 2,
                sender: tx2,
            },
        );

        rt.cleanup_channel(DEFAULT_RF, 1);
        assert_eq!(rt.len(), 1);
        assert!(rt.remove(DEFAULT_RF, 2).is_some());
        assert_eq!(rt.len(), 0);
    }

    // ========================================================================
    // COMPREHENSIVE ROUTE MATCHING TESTS
    // ========================================================================

    #[test]
    fn should_match_exact_routes() {
        // Arrange
        let test_cases = vec![
            (
                "scheme://realm/area/resource/op",
                "scheme://realm/area/resource/op",
                true,
            ),
            (
                "scheme://realm/area/resource",
                "scheme://realm/area/resource",
                true,
            ),
            ("scheme://realm/area", "scheme://realm/area", true),
            ("scheme://realm", "scheme://realm", true),
            ("a/b/c/d", "a/b/c/d", true),
            // Note: patterns match hierarchically, so "a/b/c" matches "a/b/c/d"
            ("a/b/c", "a/b/c/d", true),
            ("a/b/c/d", "a/b/c", false), // Parent route doesn't match child pattern
            ("scheme://realm1/area", "scheme://realm2/area", false),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_global_wildcard() {
        // Arrange
        let test_cases = vec![
            ("*", "anything", true),
            ("*", "scheme://realm/area/resource/op", true),
            ("*", "a/b/c/d/e/f", true),
            ("*", "", true),
            ("*", "single", true),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_trailing_wildcard_at_realm() {
        // Arrange
        let test_cases = vec![
            ("scheme://realm/*", "scheme://realm/area", true),
            ("scheme://realm/*", "scheme://realm/area/resource", true),
            ("scheme://realm/*", "scheme://realm/area/resource/op", true),
            ("scheme://realm/*", "scheme://realm", true), // Exact match to prefix
            ("scheme://realm/*", "scheme://realm2/area", false),
            ("scheme://realm/*", "scheme://other/area", false),
            ("scheme://realm/*", "different://realm/area", false),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_trailing_wildcard_at_area() {
        // Arrange
        let test_cases = vec![
            (
                "scheme://realm/area/*",
                "scheme://realm/area/resource",
                true,
            ),
            (
                "scheme://realm/area/*",
                "scheme://realm/area/resource/op",
                true,
            ),
            ("scheme://realm/area/*", "scheme://realm/area", true), // Exact match to prefix
            ("scheme://realm/area/*", "scheme://realm", false),
            ("scheme://realm/area/*", "scheme://realm/other", false),
            (
                "scheme://realm/area/*",
                "scheme://realm/area2/resource",
                false,
            ),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_trailing_wildcard_at_resource() {
        // Arrange
        let test_cases = vec![
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area/resource/op",
                true,
            ),
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area/resource/op1",
                true,
            ),
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area/resource/op/sub",
                true,
            ),
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area/resource",
                true,
            ), // Exact match to prefix
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area",
                false,
            ),
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area/other",
                false,
            ),
            (
                "scheme://realm/area/resource/*",
                "scheme://realm/area/resource2/op",
                false,
            ),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_hierarchical_prefix_without_wildcard() {
        // Arrange
        let test_cases = vec![
            // Pattern "a/b" should match "a/b/c", "a/b/c/d", etc.
            ("a/b", "a/b/c", true),
            ("a/b", "a/b/c/d", true),
            ("a/b", "a/b/c/d/e", true),
            ("a/b", "a/b", true), // Exact match
            ("a/b", "a/c", false),
            ("a/b", "a", false),
            ("a/b", "a/bc", false), // Not a path separator boundary
            // Multi-level patterns
            ("scheme://realm/area", "scheme://realm/area/resource", true),
            (
                "scheme://realm/area",
                "scheme://realm/area/resource/op",
                true,
            ),
            ("scheme://realm/area", "scheme://realm/area", true),
            ("scheme://realm/area", "scheme://realm/other", false),
            ("scheme://realm/area", "scheme://realm", false),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_not_match_partial_segments() {
        // Arrange
        let test_cases = vec![
            ("scheme://realm", "scheme://realm123", false),
            ("scheme://realm", "scheme://realm-prod", false),
            ("scheme://rea", "scheme://realm", false),
            ("a/b", "a/bc", false),
            ("a/b", "a/b-test", false),
            ("abc", "abcd", false),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_handle_edge_cases() {
        // Arrange
        let test_cases = vec![
            // Empty strings
            ("", "", true), // Exact match
            ("", "a", false),
            ("a", "", false),
            // Single segments
            ("a", "a", true),
            ("a", "b", false),
            ("a", "a/b", true), // Hierarchical match
            // Note: Trailing slashes in patterns don't work as wildcards
            // The pattern "a/b/" will be normalized during matching
            ("a/b", "a/b/c", true), // Hierarchical match works
            // Wildcard patterns
            ("*", "*", true),
            ("a/*", "a/*", true),   // Literal match
            ("a/*", "a/b/*", true), // Trailing wildcard matches hierarchically
            ("a/*", "a/b/c", true), // Trailing wildcard matches children
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_complex_real_world_patterns() {
        // Arrange
        let test_cases = vec![
            // Realm-wide alerts
            ("scheme://acme/*", "scheme://acme/prod/syslog/error", true),
            ("scheme://acme/*", "scheme://acme/dev/app/warning", true),
            ("scheme://acme/*", "scheme://acme/staging/db/critical", true),
            ("scheme://acme/*", "scheme://other/prod/syslog/error", false),
            // Environment-specific
            (
                "scheme://acme/prod/*",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            ("scheme://acme/prod/*", "scheme://acme/prod/app/info", true),
            (
                "scheme://acme/prod/*",
                "scheme://acme/dev/syslog/error",
                false,
            ),
            // Service-specific
            (
                "scheme://acme/prod/syslog/*",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            (
                "scheme://acme/prod/syslog/*",
                "scheme://acme/prod/syslog/warning",
                true,
            ),
            (
                "scheme://acme/prod/syslog/*",
                "scheme://acme/prod/app/error",
                false,
            ),
            // Exact operation subscription
            (
                "scheme://acme/prod/syslog/critical",
                "scheme://acme/prod/syslog/critical",
                true,
            ),
            (
                "scheme://acme/prod/syslog/critical",
                "scheme://acme/prod/syslog/error",
                false,
            ),
            // Hierarchical without explicit wildcard
            (
                "scheme://acme/prod/syslog",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            (
                "scheme://acme/prod/syslog",
                "scheme://acme/prod/syslog/warning",
                true,
            ),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_multiple_subscribers_to_same_route() {
        // Arrange
        let mut rt = RouteTable::new();
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let (tx3, _rx3) = mpsc::channel(10);

        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 1,
                route_pattern: "scheme://acme/*".to_string(),
                channel_id: 1,
                sender: tx1,
            },
        );
        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 2,
                route_pattern: "scheme://acme/prod/*".to_string(),
                channel_id: 2,
                sender: tx2,
            },
        );
        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 3,
                route_pattern: "scheme://acme/prod/syslog/error".to_string(),
                channel_id: 3,
                sender: tx3,
            },
        );

        // Act
        let matches = rt.matching_subscribers(DEFAULT_RF, "scheme://acme/prod/syslog/error");

        // Assert
        assert_eq!(matches.len(), 3);
    }

    #[test]
    fn should_not_match_when_no_patterns_fit() {
        // Arrange
        let mut rt = RouteTable::new();
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 1,
                route_pattern: "scheme://acme/prod/*".to_string(),
                channel_id: 1,
                sender: tx1,
            },
        );
        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 2,
                route_pattern: "scheme://acme/staging/*".to_string(),
                channel_id: 2,
                sender: tx2,
            },
        );

        // Act
        let matches = rt.matching_subscribers(DEFAULT_RF, "scheme://other/prod/syslog/error");

        // Assert
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn should_match_single_mid_path_wildcard() {
        // Arrange
        let test_cases = vec![
            // Single wildcard in middle
            (
                "scheme://acme/*/syslog/error",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            (
                "scheme://acme/*/syslog/error",
                "scheme://acme/dev/syslog/error",
                true,
            ),
            (
                "scheme://acme/*/syslog/error",
                "scheme://acme/staging/syslog/error",
                true,
            ),
            (
                "scheme://acme/*/syslog/error",
                "scheme://acme/prod/app/error",
                false,
            ),
            (
                "scheme://acme/*/syslog/error",
                "scheme://other/prod/syslog/error",
                false,
            ),
            (
                "scheme://acme/*/syslog/error",
                "scheme://acme/syslog/error",
                false,
            ), // Too few segments
            (
                "scheme://acme/*/syslog/error",
                "scheme://acme/prod/dev/syslog/error",
                false,
            ), // Too many segments
            // Different positions
            ("a/*/c", "a/b/c", true),
            ("a/*/c", "a/x/c", true),
            ("a/*/c", "a/b/d", false),
            ("a/*/c", "a/c", false),
            ("a/*/c", "a/b/c/d", false),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_multiple_mid_path_wildcards() {
        // Arrange
        let test_cases = vec![
            // Double wildcard
            (
                "scheme://acme/*/*/error",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            (
                "scheme://acme/*/*/error",
                "scheme://acme/dev/app/error",
                true,
            ),
            (
                "scheme://acme/*/*/error",
                "scheme://acme/staging/database/error",
                true,
            ),
            (
                "scheme://acme/*/*/error",
                "scheme://other/prod/syslog/error",
                false,
            ),
            ("scheme://acme/*/*/error", "scheme://acme/prod/error", false), // Too few segments
            (
                "scheme://acme/*/*/error",
                "scheme://acme/prod/syslog/app/error",
                false,
            ), // Too many segments
            // Triple wildcard
            ("a/*/*/*/e", "a/b/c/d/e", true),
            ("a/*/*/*/e", "a/x/y/z/e", true),
            ("a/*/*/*/e", "a/b/c/e", false),
            ("a/*/*/*/e", "a/b/c/d/f", false),
            // Mixed with exact segments
            (
                "scheme://*/prod/*/error",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            (
                "scheme://*/prod/*/error",
                "scheme://other/prod/app/error",
                true,
            ),
            (
                "scheme://*/prod/*/error",
                "scheme://acme/dev/syslog/error",
                false,
            ),
            ("scheme://*/prod/*/error", "scheme://acme/prod/error", false),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_match_mid_path_wildcard_with_trailing_wildcard() {
        // Arrange
        let test_cases = vec![
            // Mid-path wildcard + trailing wildcard
            (
                "scheme://acme/*/syslog/*",
                "scheme://acme/prod/syslog/error",
                true,
            ),
            (
                "scheme://acme/*/syslog/*",
                "scheme://acme/prod/syslog/warning",
                true,
            ),
            (
                "scheme://acme/*/syslog/*",
                "scheme://acme/dev/syslog/critical",
                true,
            ),
            (
                "scheme://acme/*/syslog/*",
                "scheme://acme/prod/syslog/error/detail",
                true,
            ), // Hierarchical match
            (
                "scheme://acme/*/syslog/*",
                "scheme://acme/prod/app/error",
                false,
            ),
            (
                "scheme://acme/*/syslog/*",
                "scheme://other/prod/syslog/error",
                false,
            ),
            // Multiple mid-path + trailing
            (
                "scheme://*/prod/*/log/*",
                "scheme://acme/prod/app/log/info",
                true,
            ),
            (
                "scheme://*/prod/*/log/*",
                "scheme://other/prod/db/log/error",
                true,
            ),
            (
                "scheme://*/prod/*/log/*",
                "scheme://acme/prod/app/log/info/detail",
                true,
            ),
            (
                "scheme://*/prod/*/log/*",
                "scheme://acme/dev/app/log/info",
                false,
            ),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    #[test]
    fn should_handle_edge_cases_with_wildcards() {
        // Arrange
        let test_cases = vec![
            // Wildcard at start
            ("*/b/c", "a/b/c", true),
            ("*/b/c", "x/b/c", true),
            ("*/b/c", "a/x/c", false),
            // All wildcards except last
            ("*/*/*/d", "a/b/c/d", true),
            ("*/*/*/d", "x/y/z/d", true),
            ("*/*/*/d", "a/b/d", false),
            // Exact match still works
            ("a/*/c", "a/*/c", true),
            // Single segment with wildcard (not meaningful but should work)
            ("*", "anything", true),
        ];

        for (pattern, route, expected) in test_cases {
            // Act
            let result = route_matches(DEFAULT_RF, pattern, route);

            // Assert
            assert_eq!(
                result, expected,
                "Pattern '{}' vs route '{}' should be {}",
                pattern, route, expected
            );
        }
    }

    // ========================================================================
    // MULTI-COLUMN FAMILY TESTS - Verify CF Isolation
    // ========================================================================

    #[tokio::test]
    async fn should_isolate_subscriptions_between_cfs() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_TENANT_A: u32 = 1;
        const CF_TENANT_B: u32 = 2;

        let (tx_a, _rx_a) = mpsc::channel(10);
        let (tx_b, _rx_b) = mpsc::channel(10);

        let sub_a = RtSubscription {
            id: 1,
            route_pattern: "app/alerts/*".to_string(),
            channel_id: 10,
            sender: tx_a,
        };

        let sub_b = RtSubscription {
            id: 2,
            route_pattern: "app/alerts/*".to_string(),
            channel_id: 20,
            sender: tx_b,
        };

        // Act: Insert same pattern into different CFs
        rt.insert(CF_TENANT_A, sub_a);
        rt.insert(CF_TENANT_B, sub_b);

        // Assert: Verify each CF has its own subscription
        let matches_a = rt.matching_subscribers(CF_TENANT_A, "app/alerts/error");
        let matches_b = rt.matching_subscribers(CF_TENANT_B, "app/alerts/error");

        assert_eq!(matches_a.len(), 1, "CF A should have exactly 1 match");
        assert_eq!(matches_b.len(), 1, "CF B should have exactly 1 match");
        assert_eq!(matches_a[0].id, 1, "CF A match should have id=1");
        assert_eq!(matches_b[0].id, 2, "CF B match should have id=2");
    }

    #[tokio::test]
    async fn should_not_return_other_cf_subscriptions() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_PROD: u32 = 1;
        const CF_DEV: u32 = 2;

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        // Insert pattern into PROD CF
        rt.insert(
            CF_PROD,
            RtSubscription {
                id: 100,
                route_pattern: "system/alerts/*".to_string(),
                channel_id: 1,
                sender: tx1,
            },
        );

        // Insert different pattern into DEV CF
        rt.insert(
            CF_DEV,
            RtSubscription {
                id: 101,
                route_pattern: "system/alerts/*".to_string(),
                channel_id: 2,
                sender: tx2,
            },
        );

        // Act: Query PROD with a matching route
        let matches_prod = rt.matching_subscribers(CF_PROD, "system/alerts/critical");

        // Assert: Should only get the PROD subscription, not DEV
        assert_eq!(matches_prod.len(), 1);
        assert_eq!(
            matches_prod[0].id, 100,
            "Should only return PROD subscription"
        );
        assert_eq!(matches_prod[0].channel_id, 1);
    }

    #[tokio::test]
    async fn should_maintain_separate_tries_per_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_1: u32 = 1;
        const CF_2: u32 = 2;
        const CF_3: u32 = 3;

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let (tx3, _rx3) = mpsc::channel(10);

        // Insert hierarchical patterns into each CF
        rt.insert(
            CF_1,
            RtSubscription {
                id: 1,
                route_pattern: "realm1/area/*".to_string(),
                channel_id: 10,
                sender: tx1,
            },
        );

        rt.insert(
            CF_2,
            RtSubscription {
                id: 2,
                route_pattern: "realm2/area/*".to_string(),
                channel_id: 20,
                sender: tx2,
            },
        );

        rt.insert(
            CF_3,
            RtSubscription {
                id: 3,
                route_pattern: "realm1/area/*".to_string(),
                channel_id: 30,
                sender: tx3,
            },
        );

        // Act: Query each CF
        let cf1_matches = rt.matching_subscribers(CF_1, "realm1/area/resource");
        let cf2_matches = rt.matching_subscribers(CF_2, "realm2/area/resource");
        let cf3_matches = rt.matching_subscribers(CF_3, "realm1/area/resource");
        let cf1_realm2 = rt.matching_subscribers(CF_1, "realm2/area/resource");

        // Assert: Each CF returns only its own subscriptions
        assert_eq!(cf1_matches.len(), 1);
        assert_eq!(cf1_matches[0].id, 1);

        assert_eq!(cf2_matches.len(), 1);
        assert_eq!(cf2_matches[0].id, 2);

        assert_eq!(cf3_matches.len(), 1);
        assert_eq!(cf3_matches[0].id, 3);

        // CF1 should not match realm2 pattern (even though other CFs have realm2)
        assert_eq!(cf1_realm2.len(), 0);
    }

    #[tokio::test]
    async fn should_support_multiple_subscriptions_per_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_MULTI: u32 = 5;

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let (tx3, _rx3) = mpsc::channel(10);

        // Insert multiple patterns into the same CF
        rt.insert(
            CF_MULTI,
            RtSubscription {
                id: 1,
                route_pattern: "app/*".to_string(),
                channel_id: 100,
                sender: tx1,
            },
        );

        rt.insert(
            CF_MULTI,
            RtSubscription {
                id: 2,
                route_pattern: "app/alerts/*".to_string(),
                channel_id: 101,
                sender: tx2,
            },
        );

        rt.insert(
            CF_MULTI,
            RtSubscription {
                id: 3,
                route_pattern: "app/alerts/critical".to_string(),
                channel_id: 102,
                sender: tx3,
            },
        );

        // Act: Query with a specific route
        let matches = rt.matching_subscribers(CF_MULTI, "app/alerts/critical");

        // Assert: Should match all three patterns (hierarchical matching)
        assert_eq!(
            matches.len(),
            3,
            "Should match all 3 patterns hierarchically"
        );
        let ids: Vec<u64> = matches.iter().map(|s| s.id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    #[test]
    fn should_remove_subscription_only_from_specific_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_A: u32 = 10;
        const CF_B: u32 = 20;

        let (tx_a, _rx_a) = mpsc::channel(10);
        let (tx_b, _rx_b) = mpsc::channel(10);

        let sub_a = RtSubscription {
            id: 1,
            route_pattern: "service/*".to_string(),
            channel_id: 1,
            sender: tx_a,
        };

        let sub_b = RtSubscription {
            id: 2,
            route_pattern: "service/*".to_string(),
            channel_id: 2,
            sender: tx_b,
        };

        rt.insert(CF_A, sub_a);
        rt.insert(CF_B, sub_b);

        // Act
        let removed = rt.remove(CF_A, 1);

        // Assert
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, 1);

        // CF_A should no longer have the subscription
        let cf_a_matches = rt.matching_subscribers(CF_A, "service/endpoint");
        assert_eq!(cf_a_matches.len(), 0);

        // CF_B should still have its subscription
        let cf_b_matches = rt.matching_subscribers(CF_B, "service/endpoint");
        assert_eq!(cf_b_matches.len(), 1);
        assert_eq!(cf_b_matches[0].id, 2);
    }

    #[test]
    fn should_cleanup_channel_only_in_specific_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_PRIMARY: u32 = 100;
        const CF_SECONDARY: u32 = 200;
        const CHANNEL_ID: u32 = 5;

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let (tx3, _rx3) = mpsc::channel(10);
        let (tx4, _rx4) = mpsc::channel(10);

        // Insert subscriptions from CHANNEL_ID into both CFs
        rt.insert(
            CF_PRIMARY,
            RtSubscription {
                id: 1,
                route_pattern: "r1".to_string(),
                channel_id: CHANNEL_ID,
                sender: tx1,
            },
        );

        rt.insert(
            CF_PRIMARY,
            RtSubscription {
                id: 2,
                route_pattern: "r2".to_string(),
                channel_id: CHANNEL_ID,
                sender: tx2,
            },
        );

        // Insert subscriptions from different channel into CF_SECONDARY
        rt.insert(
            CF_SECONDARY,
            RtSubscription {
                id: 3,
                route_pattern: "r3".to_string(),
                channel_id: CHANNEL_ID,
                sender: tx3,
            },
        );

        rt.insert(
            CF_SECONDARY,
            RtSubscription {
                id: 4,
                route_pattern: "r4".to_string(),
                channel_id: 999, // Different channel
                sender: tx4,
            },
        );

        // Act
        rt.cleanup_channel(CF_PRIMARY, CHANNEL_ID);

        // Assert
        assert_eq!(rt.len(), 2, "Should have 2 subscriptions remaining");

        // CF_PRIMARY should be empty for that channel
        let cf_primary_remaining = rt.matching_subscribers(CF_PRIMARY, "r1");
        assert_eq!(cf_primary_remaining.len(), 0);

        // CF_SECONDARY should still have CHANNEL_ID's subscription
        let cf_secondary_remaining = rt.matching_subscribers(CF_SECONDARY, "r3");
        assert_eq!(cf_secondary_remaining.len(), 1);
        assert_eq!(cf_secondary_remaining[0].id, 3);

        // CF_SECONDARY should still have the subscription from channel 999
        let cf_secondary_999 = rt.matching_subscribers(CF_SECONDARY, "r4");
        assert_eq!(cf_secondary_999.len(), 1);
        assert_eq!(cf_secondary_999[0].id, 4);
    }

    #[test]
    fn should_handle_empty_queries_for_nonexistent_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_EXISTS: u32 = 1;
        const CF_MISSING: u32 = 999;

        let (tx, _rx) = mpsc::channel(10);

        rt.insert(
            CF_EXISTS,
            RtSubscription {
                id: 1,
                route_pattern: "test/route".to_string(),
                channel_id: 1,
                sender: tx,
            },
        );

        // Act
        let matches = rt.matching_subscribers(CF_MISSING, "test/route");

        // Assert
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn should_support_global_wildcard_per_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_GLOBAL_ENABLED: u32 = 1;
        const CF_NO_GLOBAL: u32 = 2;

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);

        // CF_GLOBAL_ENABLED has a global wildcard subscription
        rt.insert(
            CF_GLOBAL_ENABLED,
            RtSubscription {
                id: 1,
                route_pattern: "*".to_string(),
                channel_id: 1,
                sender: tx1,
            },
        );

        // CF_NO_GLOBAL has a specific pattern
        rt.insert(
            CF_NO_GLOBAL,
            RtSubscription {
                id: 2,
                route_pattern: "specific/route".to_string(),
                channel_id: 2,
                sender: tx2,
            },
        );

        // Act
        let matches_enabled = rt.matching_subscribers(CF_GLOBAL_ENABLED, "any/random/route");
        let matches_disabled = rt.matching_subscribers(CF_NO_GLOBAL, "any/random/route");

        // Assert
        assert_eq!(matches_enabled.len(), 1);
        assert_eq!(matches_enabled[0].id, 1);

        assert_eq!(matches_disabled.len(), 0);
    }

    #[test]
    fn should_handle_complex_multi_cf_scenario() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_TENANT_ACME: u32 = 1;
        const CF_TENANT_WIDGETS: u32 = 2;

        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        let (tx3, _rx3) = mpsc::channel(10);
        let (tx4, _rx4) = mpsc::channel(10);
        let (tx5, _rx5) = mpsc::channel(10);

        // ACME tenant subscriptions
        rt.insert(
            CF_TENANT_ACME,
            RtSubscription {
                id: 1,
                route_pattern: "acme/*".to_string(),
                channel_id: 100,
                sender: tx1,
            },
        );

        rt.insert(
            CF_TENANT_ACME,
            RtSubscription {
                id: 2,
                route_pattern: "acme/prod/*".to_string(),
                channel_id: 101,
                sender: tx2,
            },
        );

        // WIDGETS tenant subscriptions
        rt.insert(
            CF_TENANT_WIDGETS,
            RtSubscription {
                id: 3,
                route_pattern: "widgets/*".to_string(),
                channel_id: 200,
                sender: tx3,
            },
        );

        rt.insert(
            CF_TENANT_WIDGETS,
            RtSubscription {
                id: 4,
                route_pattern: "widgets/alerts/*".to_string(),
                channel_id: 201,
                sender: tx4,
            },
        );

        // Cross-tenant subscription (both have generic monitoring)
        rt.insert(
            CF_TENANT_ACME,
            RtSubscription {
                id: 5,
                route_pattern: "*".to_string(),
                channel_id: 102,
                sender: tx5,
            },
        );

        // Act
        let acme_prod = rt.matching_subscribers(CF_TENANT_ACME, "acme/prod/alerts");
        let acme_any = rt.matching_subscribers(CF_TENANT_ACME, "anything");
        let widgets_alert = rt.matching_subscribers(CF_TENANT_WIDGETS, "widgets/alerts/critical");
        let widgets_other = rt.matching_subscribers(CF_TENANT_WIDGETS, "acme/prod/alerts");

        // Assert
        assert_eq!(
            acme_prod.len(),
            3,
            "ACME prod should match 3 patterns (including global)"
        );
        let acme_prod_ids: Vec<u64> = acme_prod.iter().map(|s| s.id).collect();
        assert!(acme_prod_ids.contains(&1));
        assert!(acme_prod_ids.contains(&2));
        assert!(acme_prod_ids.contains(&5)); // Global wildcard

        assert_eq!(
            acme_any.len(),
            1,
            "Any route should only match ACME's global wildcard"
        );
        assert_eq!(acme_any[0].id, 5);

        assert_eq!(
            widgets_alert.len(),
            2,
            "WIDGETS alerts should match 2 patterns"
        );
        let widget_ids: Vec<u64> = widgets_alert.iter().map(|s| s.id).collect();
        assert!(widget_ids.contains(&3));
        assert!(widget_ids.contains(&4));

        // WIDGETS tenant should NOT see ACME routes (even though ACME has global wildcard)
        assert_eq!(
            widgets_other.len(),
            0,
            "WIDGETS should not see ACME-only routes"
        );
    }
}
