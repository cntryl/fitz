use fxhash::FxHashMap;
use smallvec::SmallVec;
use std::sync::Arc;

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

/// Type alias for subscriber slabs
type SubSlab = Arc<[RtSubscription]>;

/// Basic subscription entry - pure metadata, no I/O primitives
/// Transport layer maintains the mapping from channel_id -> actual channel
#[derive(Debug, Clone)]
pub struct RtSubscription {
    pub id: u64,
    pub route_pattern: String,
    pub channel_id: u32,
}

/// One node in the route trie:
/// - exact_subs: subscriptions that match this path exactly
/// - trailing_wildcard_subs: subscriptions like "a/b/*" anchored here
/// - children: exact segment children
/// - wildcard_child: matches "*" at this position
#[derive(Debug, Default, Clone)]
struct TrieNode {
    exact_subs: Option<SubSlab>,
    trailing_wildcard_subs: Option<SubSlab>,
    children: FxHashMap<String, TrieNode>,
    wildcard_child: Option<Box<TrieNode>>,
}

impl TrieNode {}

/// Fanout result: zero-allocation iterator over matched subscribers
/// Uses SmallVec to avoid heap allocation for typical fanout sizes
#[derive(Debug)]
pub struct Fanout<'a> {
    slabs: SmallVec<[&'a SubSlab; 4]>,
    slab_index: usize,
    item_index: usize,
}

impl<'a> Iterator for Fanout<'a> {
    type Item = &'a RtSubscription;

    fn next(&mut self) -> Option<Self::Item> {
        while self.slab_index < self.slabs.len() {
            let slab = self.slabs[self.slab_index];
            if self.item_index < slab.len() {
                let item = &slab[self.item_index];
                self.item_index += 1;
                return Some(item);
            } else {
                self.slab_index += 1;
                self.item_index = 0;
            }
        }
        None
    }
}

/// Per-RF trie
#[derive(Debug, Clone)]
struct RouteTrie {
    root: TrieNode,
    global_subs: Option<SubSlab>, // pattern == "*"
}

impl RouteTrie {
    fn new() -> Self {
        Self {
            root: TrieNode::default(),
            global_subs: None,
        }
    }
}

/// One shard of the route table
#[derive(Debug, Default, Clone)]
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

        self.insert_into_trie(rf, &pattern, &sub);
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

    fn insert_into_trie(&mut self, rf: RouteFamilyId, pattern: &str, sub: &RtSubscription) {
        let trie = self.tries.entry(rf).or_insert_with(RouteTrie::new);

        if pattern == "*" {
            trie.global_subs = Some(if let Some(ref existing) = trie.global_subs {
                let mut v = (**existing).to_vec();
                v.push(sub.clone());
                Arc::from(v)
            } else {
                Arc::from(vec![sub.clone()])
            });
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
                node = node.children.entry((*seg).to_string()).or_default();
            }
        }

        if has_trailing_wildcard {
            node.trailing_wildcard_subs =
                Some(if let Some(ref existing) = node.trailing_wildcard_subs {
                    let mut v = (**existing).to_vec();
                    v.push(sub.clone());
                    Arc::from(v)
                } else {
                    Arc::from(vec![sub.clone()])
                });
        } else {
            node.exact_subs = Some(if let Some(ref existing) = node.exact_subs {
                let mut v = (**existing).to_vec();
                v.push(sub.clone());
                Arc::from(v)
            } else {
                Arc::from(vec![sub.clone()])
            });
        }
    }

    fn remove_from_trie(&mut self, rf: RouteFamilyId, pattern: &str, sub_id: u64) {
        let Some(trie) = self.tries.get_mut(&rf) else {
            return;
        };

        if pattern == "*" {
            trie.global_subs = trie.global_subs.as_ref().and_then(|existing| {
                let v: Vec<_> = existing
                    .iter()
                    .filter(|s| s.id != sub_id)
                    .cloned()
                    .collect();
                if v.is_empty() {
                    None
                } else {
                    Some(Arc::from(v))
                }
            });
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
        ) {
            if depth == segments.len() {
                if is_trailing {
                    node.trailing_wildcard_subs =
                        node.trailing_wildcard_subs.as_ref().and_then(|existing| {
                            let v: Vec<_> = existing
                                .iter()
                                .filter(|s| s.id != sub_id)
                                .cloned()
                                .collect();
                            if v.is_empty() {
                                None
                            } else {
                                Some(Arc::from(v))
                            }
                        });
                } else {
                    node.exact_subs = node.exact_subs.as_ref().and_then(|existing| {
                        let v: Vec<_> = existing
                            .iter()
                            .filter(|s| s.id != sub_id)
                            .cloned()
                            .collect();
                        if v.is_empty() {
                            None
                        } else {
                            Some(Arc::from(v))
                        }
                    });
                }
                return;
            }

            let seg = segments[depth];

            if seg == "*" {
                if let Some(child) = node.wildcard_child.as_mut() {
                    remove_rec(child, segments, sub_id, is_trailing, depth + 1);
                }
            } else if let Some(child) = node.children.get_mut(seg) {
                remove_rec(child, segments, sub_id, is_trailing, depth + 1);
            }
        }

        remove_rec(
            &mut trie.root,
            segments_to_traverse,
            sub_id,
            has_trailing_wildcard,
            0,
        );
    }

    /// Zero-alloc match inside a shard: collect slabs (optimized with SmallVec)
    fn matching_slabs(
        &self,
        rf: RouteFamilyId,
        route_segments: &[&str],
    ) -> SmallVec<[&SubSlab; 4]> {
        let Some(trie) = self.tries.get(&rf) else {
            return SmallVec::new();
        };

        let mut slabs = SmallVec::new();

        // Global wildcard
        if let Some(ref slab) = trie.global_subs {
            slabs.push(slab);
        }

        fn walk<'a>(
            node: &'a TrieNode,
            segs: &[&str],
            depth: usize,
            slabs: &mut SmallVec<[&'a SubSlab; 4]>,
        ) {
            if depth == segs.len() {
                if let Some(ref slab) = node.exact_subs {
                    slabs.push(slab);
                }
                if let Some(ref slab) = node.trailing_wildcard_subs {
                    slabs.push(slab);
                }
                return;
            }

            let seg = segs[depth];

            if let Some(child) = node.children.get(seg) {
                walk(child, segs, depth + 1, slabs);
            }

            if let Some(child) = node.wildcard_child.as_ref() {
                walk(child, segs, depth + 1, slabs);
            }

            // Hierarchical prefix + trailing wildcard match
            if let Some(ref slab) = node.exact_subs {
                slabs.push(slab);
            }
            if let Some(ref slab) = node.trailing_wildcard_subs {
                slabs.push(slab);
            }
        }

        walk(&trie.root, route_segments, 0, &mut slabs);
        slabs
    }
}

/// Sharded route table:
/// - sharded by RF for locality & future parallelism
/// - zero-alloc hotpath for matches
#[derive(Debug, Clone)]
pub struct RouteTable {
    shards: [RouteTableShard; SHARD_COUNT],
}

impl RouteTable {
    pub fn new() -> Self {
        // Manual init because arrays require Copy/Default trick
        // Prefer safe initialization to avoid undefined behavior with `MaybeUninit`
        let shards: [RouteTableShard; SHARD_COUNT] =
            std::array::from_fn(|_| RouteTableShard::new());

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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Zero-alloc hot path: return iterator over matched subscribers
    pub fn matching_subscribers(&self, rf: RouteFamilyId, route: &str) -> Fanout<'_> {
        // Split route once into stack-backed array
        let mut segs: [&str; MAX_SEGMENTS] = [""; MAX_SEGMENTS];
        let mut seg_len = 0;
        for (i, seg) in route.split('/').enumerate().take(MAX_SEGMENTS) {
            segs[i] = seg;
            seg_len += 1;
        }
        let route_segments = &segs[..seg_len];

        let shard = self.shard_ref(rf);

        let slabs = shard.matching_slabs(rf, route_segments);

        Fanout {
            slabs,
            slab_index: 0,
            item_index: 0,
        }
    }
}

impl Default for RouteTable {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn should_insert_and_match() {
        // Arrange
        let mut rt = RouteTable::new();
        let sub = RtSubscription {
            id: 1,
            route_pattern: "a/b/*".to_string(),
            channel_id: 1,
        };
        rt.insert(DEFAULT_RF, sub);

        // Act
        let matches: Vec<_> = rt.matching_subscribers(DEFAULT_RF, "a/b/c").collect();

        // Assert
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn should_remove_and_cleanup() {
        // Arrange
        let mut rt = RouteTable::new();
        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 1,
                route_pattern: "r1".to_string(),
                channel_id: 1,
            },
        );
        rt.insert(
            DEFAULT_RF,
            RtSubscription {
                id: 2,
                route_pattern: "r2".to_string(),
                channel_id: 2,
            },
        );

        // Act
        rt.cleanup_channel(DEFAULT_RF, 1);

        // Assert
        assert_eq!(rt.len(), 1);
        assert!(rt.remove(DEFAULT_RF, 2).is_some());
        assert_eq!(rt.len(), 0);
    }

    #[test]
    fn should_verify_route_matching_matrix() {
        // Arrange
        struct Case<'a> {
            pattern: &'a str,
            route: &'a str,
            expected: bool,
        }

        let cases = [
            // Exact routes
            Case {
                pattern: "scheme://realm/area/resource/op",
                route: "scheme://realm/area/resource/op",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/resource",
                route: "scheme://realm/area/resource",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area",
                route: "scheme://realm/area",
                expected: true,
            },
            Case {
                pattern: "scheme://realm",
                route: "scheme://realm",
                expected: true,
            },
            Case {
                pattern: "a/b/c/d",
                route: "a/b/c/d",
                expected: true,
            },
            Case {
                pattern: "a/b/c",
                route: "a/b/c/d",
                expected: true,
            },
            Case {
                pattern: "a/b/c/d",
                route: "a/b/c",
                expected: false,
            },
            Case {
                pattern: "scheme://realm1/area",
                route: "scheme://realm2/area",
                expected: false,
            },
            // Global wildcard
            Case {
                pattern: "*",
                route: "anything",
                expected: true,
            },
            Case {
                pattern: "*",
                route: "scheme://realm/area/resource/op",
                expected: true,
            },
            Case {
                pattern: "*",
                route: "a/b/c/d/e/f",
                expected: true,
            },
            Case {
                pattern: "*",
                route: "",
                expected: true,
            },
            Case {
                pattern: "*",
                route: "single",
                expected: true,
            },
            // Trailing wildcard at realm
            Case {
                pattern: "scheme://realm/*",
                route: "scheme://realm/area",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/*",
                route: "scheme://realm/area/resource",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/*",
                route: "scheme://realm/area/resource/op",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/*",
                route: "scheme://realm",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/*",
                route: "scheme://realm2/area",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/*",
                route: "scheme://other/area",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/*",
                route: "different://realm/area",
                expected: false,
            },
            // Trailing wildcard at area
            Case {
                pattern: "scheme://realm/area/*",
                route: "scheme://realm/area/resource",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/*",
                route: "scheme://realm/area/resource/op",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/*",
                route: "scheme://realm/area",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/*",
                route: "scheme://realm",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/area/*",
                route: "scheme://realm/other",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/area/*",
                route: "scheme://realm/area2/resource",
                expected: false,
            },
            // Trailing wildcard at resource
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area/resource/op",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area/resource/op1",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area/resource/op/sub",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area/resource",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area/other",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/area/resource/*",
                route: "scheme://realm/area/resource2/op",
                expected: false,
            },
            // Hierarchical prefix without wildcard
            Case {
                pattern: "a/b",
                route: "a/b/c",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/b/c/d",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/b/c/d/e",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/b",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/c",
                expected: false,
            },
            Case {
                pattern: "a/b",
                route: "a",
                expected: false,
            },
            Case {
                pattern: "a/b",
                route: "a/bc",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/area",
                route: "scheme://realm/area/resource",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area",
                route: "scheme://realm/area/resource/op",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area",
                route: "scheme://realm/area",
                expected: true,
            },
            Case {
                pattern: "scheme://realm/area",
                route: "scheme://realm/other",
                expected: false,
            },
            Case {
                pattern: "scheme://realm/area",
                route: "scheme://realm",
                expected: false,
            },
            // Partial segments
            Case {
                pattern: "scheme://realm",
                route: "scheme://realm123",
                expected: false,
            },
            Case {
                pattern: "scheme://realm",
                route: "scheme://realm-prod",
                expected: false,
            },
            Case {
                pattern: "scheme://rea",
                route: "scheme://realm",
                expected: false,
            },
            Case {
                pattern: "a/b",
                route: "a/bc",
                expected: false,
            },
            Case {
                pattern: "a/b",
                route: "a/b-test",
                expected: false,
            },
            Case {
                pattern: "abc",
                route: "abcd",
                expected: false,
            },
            // Edge cases
            Case {
                pattern: "",
                route: "",
                expected: true,
            },
            Case {
                pattern: "",
                route: "a",
                expected: false,
            },
            Case {
                pattern: "a",
                route: "",
                expected: false,
            },
            Case {
                pattern: "a",
                route: "a",
                expected: true,
            },
            Case {
                pattern: "a",
                route: "a/b",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/b/c",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/b/c",
                expected: true,
            },
            Case {
                pattern: "a/b",
                route: "a/b/c/d",
                expected: true,
            },
            Case {
                pattern: "*",
                route: "*",
                expected: true,
            },
            Case {
                pattern: "a/*",
                route: "a/*",
                expected: true,
            },
            Case {
                pattern: "a/*",
                route: "a/b/*",
                expected: true,
            },
            Case {
                pattern: "a/*",
                route: "a/b/c",
                expected: true,
            },
            // Single mid-path wildcard
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://acme/prod/syslog/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://acme/dev/syslog/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://acme/staging/syslog/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://acme/prod/app/error",
                expected: false,
            },
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://other/prod/syslog/error",
                expected: false,
            },
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://acme/syslog/error",
                expected: false,
            },
            Case {
                pattern: "scheme://acme/*/syslog/error",
                route: "scheme://acme/prod/dev/syslog/error",
                expected: false,
            },
            Case {
                pattern: "a/*/c",
                route: "a/b/c",
                expected: true,
            },
            Case {
                pattern: "a/*/c",
                route: "a/x/c",
                expected: true,
            },
            Case {
                pattern: "a/*/c",
                route: "a/b/d",
                expected: false,
            },
            Case {
                pattern: "a/*/c",
                route: "a/c",
                expected: false,
            },
            Case {
                pattern: "a/*/c",
                route: "a/b/c/d",
                expected: false,
            },
            // Multiple mid-path wildcards
            Case {
                pattern: "scheme://acme/*/*/error",
                route: "scheme://acme/prod/syslog/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/*/error",
                route: "scheme://acme/dev/app/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/*/error",
                route: "scheme://acme/staging/database/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/*/error",
                route: "scheme://other/prod/syslog/error",
                expected: false,
            },
            Case {
                pattern: "scheme://acme/*/*/error",
                route: "scheme://acme/prod/error",
                expected: false,
            },
            Case {
                pattern: "scheme://acme/*/*/error",
                route: "scheme://acme/prod/syslog/app/error",
                expected: false,
            },
            Case {
                pattern: "a/*/*/*/e",
                route: "a/b/c/d/e",
                expected: true,
            },
            Case {
                pattern: "a/*/*/*/e",
                route: "a/x/y/z/e",
                expected: true,
            },
            Case {
                pattern: "a/*/*/*/e",
                route: "a/b/c/e",
                expected: false,
            },
            Case {
                pattern: "a/*/*/*/e",
                route: "a/b/c/d/f",
                expected: false,
            },
            Case {
                pattern: "scheme://*/prod/*/error",
                route: "scheme://acme/prod/syslog/error",
                expected: true,
            },
            Case {
                pattern: "scheme://*/prod/*/error",
                route: "scheme://other/prod/app/error",
                expected: true,
            },
            Case {
                pattern: "scheme://*/prod/*/error",
                route: "scheme://acme/dev/syslog/error",
                expected: false,
            },
            Case {
                pattern: "scheme://*/prod/*/error",
                route: "scheme://acme/prod/error",
                expected: false,
            },
            // Mid-path wildcard with trailing wildcard
            Case {
                pattern: "scheme://acme/*/syslog/*",
                route: "scheme://acme/prod/syslog/error",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/*",
                route: "scheme://acme/prod/syslog/warning",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/*",
                route: "scheme://acme/dev/syslog/critical",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/*",
                route: "scheme://acme/prod/syslog/error/detail",
                expected: true,
            },
            Case {
                pattern: "scheme://acme/*/syslog/*",
                route: "scheme://acme/prod/app/error",
                expected: false,
            },
            Case {
                pattern: "scheme://acme/*/syslog/*",
                route: "scheme://other/prod/syslog/error",
                expected: false,
            },
            // Edge cases with wildcards
            Case {
                pattern: "*/b/c",
                route: "a/b/c",
                expected: true,
            },
            Case {
                pattern: "*/b/c",
                route: "x/b/c",
                expected: true,
            },
            Case {
                pattern: "*/b/c",
                route: "a/x/c",
                expected: false,
            },
            Case {
                pattern: "*/*/*/d",
                route: "a/b/c/d",
                expected: true,
            },
            Case {
                pattern: "*/*/*/d",
                route: "x/y/z/d",
                expected: true,
            },
            Case {
                pattern: "*/*/*/d",
                route: "a/b/d",
                expected: false,
            },
            Case {
                pattern: "*",
                route: "anything",
                expected: true,
            },
        ];

        // Act
        for c in cases {
            let result = route_matches(DEFAULT_RF, c.pattern, c.route);

            // Assert
            assert_eq!(
                result, c.expected,
                "Pattern '{}' vs route '{}' should be {}",
                c.pattern, c.route, c.expected
            );
        }
    }

    // ========================================================================
    // MULTI-COLUMN FAMILY TESTS - Verify CF Isolation
    // ========================================================================

    #[test]
    fn should_isolate_subscriptions_between_cfs() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_TENANT_A: u32 = 1;
        const CF_TENANT_B: u32 = 2;
        let sub_a = RtSubscription {
            id: 1,
            route_pattern: "app/alerts/*".to_string(),
            channel_id: 10,
        };
        let sub_b = RtSubscription {
            id: 2,
            route_pattern: "app/alerts/*".to_string(),
            channel_id: 20,
        };
        rt.insert(CF_TENANT_A, sub_a);
        rt.insert(CF_TENANT_B, sub_b);

        // Act
        let matches_a: Vec<_> = rt
            .matching_subscribers(CF_TENANT_A, "app/alerts/error")
            .collect();
        let matches_b: Vec<_> = rt
            .matching_subscribers(CF_TENANT_B, "app/alerts/error")
            .collect();

        // Assert
        assert_eq!(matches_a.len(), 1, "CF A should have exactly 1 match");
        assert_eq!(matches_b.len(), 1, "CF B should have exactly 1 match");
        assert_eq!(matches_a[0].id, 1, "CF A match should have id=1");
        assert_eq!(matches_b[0].id, 2, "CF B match should have id=2");
    }

    #[test]
    fn should_not_return_other_cf_subscriptions() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_PROD: u32 = 1;
        const CF_DEV: u32 = 2;
        rt.insert(
            CF_PROD,
            RtSubscription {
                id: 100,
                route_pattern: "system/alerts/*".to_string(),
                channel_id: 1,
            },
        );
        rt.insert(
            CF_DEV,
            RtSubscription {
                id: 101,
                route_pattern: "system/alerts/*".to_string(),
                channel_id: 2,
            },
        );

        // Act
        let matches_prod: Vec<_> = rt
            .matching_subscribers(CF_PROD, "system/alerts/critical")
            .collect();

        // Assert
        assert_eq!(matches_prod.len(), 1);
        assert_eq!(
            matches_prod[0].id, 100,
            "Should only return PROD subscription"
        );
        assert_eq!(matches_prod[0].channel_id, 1);
    }

    #[test]
    fn should_maintain_separate_tries_per_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_1: u32 = 1;
        const CF_2: u32 = 2;
        const CF_3: u32 = 3;
        rt.insert(
            CF_1,
            RtSubscription {
                id: 1,
                route_pattern: "realm1/area/*".to_string(),
                channel_id: 10,
            },
        );
        rt.insert(
            CF_2,
            RtSubscription {
                id: 2,
                route_pattern: "realm2/area/*".to_string(),
                channel_id: 20,
            },
        );
        rt.insert(
            CF_3,
            RtSubscription {
                id: 3,
                route_pattern: "realm1/area/*".to_string(),
                channel_id: 30,
            },
        );

        // Act
        let cf1_matches: Vec<_> = rt
            .matching_subscribers(CF_1, "realm1/area/resource")
            .collect();
        let cf2_matches: Vec<_> = rt
            .matching_subscribers(CF_2, "realm2/area/resource")
            .collect();
        let cf3_matches: Vec<_> = rt
            .matching_subscribers(CF_3, "realm1/area/resource")
            .collect();
        let cf1_realm2: Vec<_> = rt
            .matching_subscribers(CF_1, "realm2/area/resource")
            .collect();

        // Assert
        assert_eq!(cf1_matches.len(), 1);
        assert_eq!(cf1_matches[0].id, 1);

        assert_eq!(cf2_matches.len(), 1);
        assert_eq!(cf2_matches[0].id, 2);

        assert_eq!(cf3_matches.len(), 1);
        assert_eq!(cf3_matches[0].id, 3);

        // CF1 should not match realm2 pattern (even though other CFs have realm2)
        assert_eq!(cf1_realm2.len(), 0);
    }

    #[test]
    fn should_support_multiple_subscriptions_per_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_MULTI: u32 = 5;
        rt.insert(
            CF_MULTI,
            RtSubscription {
                id: 1,
                route_pattern: "app/*".to_string(),
                channel_id: 100,
            },
        );
        rt.insert(
            CF_MULTI,
            RtSubscription {
                id: 2,
                route_pattern: "app/alerts/*".to_string(),
                channel_id: 101,
            },
        );
        rt.insert(
            CF_MULTI,
            RtSubscription {
                id: 3,
                route_pattern: "app/alerts/critical".to_string(),
                channel_id: 102,
            },
        );

        // Act
        let matches: Vec<_> = rt
            .matching_subscribers(CF_MULTI, "app/alerts/critical")
            .collect();

        // Assert
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

        let sub_a = RtSubscription {
            id: 1,
            route_pattern: "service/*".to_string(),
            channel_id: 1,
        };

        let sub_b = RtSubscription {
            id: 2,
            route_pattern: "service/*".to_string(),
            channel_id: 2,
        };

        rt.insert(CF_A, sub_a);
        rt.insert(CF_B, sub_b);

        // Act
        let removed = rt.remove(CF_A, 1);

        // Assert
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().id, 1);

        // CF_A should no longer have the subscription
        let cf_a_matches: Vec<_> = rt.matching_subscribers(CF_A, "service/endpoint").collect();
        assert_eq!(cf_a_matches.len(), 0);

        // CF_B should still have its subscription
        let cf_b_matches: Vec<_> = rt.matching_subscribers(CF_B, "service/endpoint").collect();
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

        // Insert subscriptions from CHANNEL_ID into both CFs
        rt.insert(
            CF_PRIMARY,
            RtSubscription {
                id: 1,
                route_pattern: "r1".to_string(),
                channel_id: CHANNEL_ID,
            },
        );

        rt.insert(
            CF_PRIMARY,
            RtSubscription {
                id: 2,
                route_pattern: "r2".to_string(),
                channel_id: CHANNEL_ID,
            },
        );

        // Insert subscriptions from different channel into CF_SECONDARY
        rt.insert(
            CF_SECONDARY,
            RtSubscription {
                id: 3,
                route_pattern: "r3".to_string(),
                channel_id: CHANNEL_ID,
            },
        );

        rt.insert(
            CF_SECONDARY,
            RtSubscription {
                id: 4,
                route_pattern: "r4".to_string(),
                channel_id: 999, // Different channel
            },
        );

        // Act
        rt.cleanup_channel(CF_PRIMARY, CHANNEL_ID);

        // Assert
        assert_eq!(rt.len(), 2, "Should have 2 subscriptions remaining");

        // CF_PRIMARY should be empty for that channel
        let cf_primary_remaining: Vec<_> = rt.matching_subscribers(CF_PRIMARY, "r1").collect();
        assert_eq!(cf_primary_remaining.len(), 0);

        // CF_SECONDARY should still have CHANNEL_ID's subscription
        let cf_secondary_remaining: Vec<_> = rt.matching_subscribers(CF_SECONDARY, "r3").collect();
        assert_eq!(cf_secondary_remaining.len(), 1);
        assert_eq!(cf_secondary_remaining[0].id, 3);

        // CF_SECONDARY should still have the subscription from channel 999
        let cf_secondary_999: Vec<_> = rt.matching_subscribers(CF_SECONDARY, "r4").collect();
        assert_eq!(cf_secondary_999.len(), 1);
        assert_eq!(cf_secondary_999[0].id, 4);
    }

    #[test]
    fn should_handle_empty_queries_for_nonexistent_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_EXISTS: u32 = 1;
        const CF_MISSING: u32 = 999;

        rt.insert(
            CF_EXISTS,
            RtSubscription {
                id: 1,
                route_pattern: "test/route".to_string(),
                channel_id: 1,
            },
        );

        // Act
        let matches: Vec<_> = rt.matching_subscribers(CF_MISSING, "test/route").collect();

        // Assert
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn should_support_global_wildcard_per_cf() {
        // Arrange
        let mut rt = RouteTable::new();
        const CF_GLOBAL_ENABLED: u32 = 1;
        const CF_NO_GLOBAL: u32 = 2;

        // CF_GLOBAL_ENABLED has a global wildcard subscription
        rt.insert(
            CF_GLOBAL_ENABLED,
            RtSubscription {
                id: 1,
                route_pattern: "*".to_string(),
                channel_id: 1,
            },
        );

        // CF_NO_GLOBAL has a specific pattern
        rt.insert(
            CF_NO_GLOBAL,
            RtSubscription {
                id: 2,
                route_pattern: "specific/route".to_string(),
                channel_id: 2,
            },
        );

        // Act
        let matches_enabled: Vec<_> = rt
            .matching_subscribers(CF_GLOBAL_ENABLED, "any/random/route")
            .collect();
        let matches_disabled: Vec<_> = rt
            .matching_subscribers(CF_NO_GLOBAL, "any/random/route")
            .collect();

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

        // ACME tenant subscriptions
        rt.insert(
            CF_TENANT_ACME,
            RtSubscription {
                id: 1,
                route_pattern: "acme/*".to_string(),
                channel_id: 100,
            },
        );

        rt.insert(
            CF_TENANT_ACME,
            RtSubscription {
                id: 2,
                route_pattern: "acme/prod/*".to_string(),
                channel_id: 101,
            },
        );

        // WIDGETS tenant subscriptions
        rt.insert(
            CF_TENANT_WIDGETS,
            RtSubscription {
                id: 3,
                route_pattern: "widgets/*".to_string(),
                channel_id: 200,
            },
        );

        rt.insert(
            CF_TENANT_WIDGETS,
            RtSubscription {
                id: 4,
                route_pattern: "widgets/alerts/*".to_string(),
                channel_id: 201,
            },
        );

        // Cross-tenant subscription (both have generic monitoring)
        rt.insert(
            CF_TENANT_ACME,
            RtSubscription {
                id: 5,
                route_pattern: "*".to_string(),
                channel_id: 102,
            },
        );

        // Act
        let acme_prod: Vec<_> = rt
            .matching_subscribers(CF_TENANT_ACME, "acme/prod/alerts")
            .collect();
        let acme_any: Vec<_> = rt
            .matching_subscribers(CF_TENANT_ACME, "anything")
            .collect();
        let widgets_alert: Vec<_> = rt
            .matching_subscribers(CF_TENANT_WIDGETS, "widgets/alerts/critical")
            .collect();
        let widgets_other: Vec<_> = rt
            .matching_subscribers(CF_TENANT_WIDGETS, "acme/prod/alerts")
            .collect();

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
