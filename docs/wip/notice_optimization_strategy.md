# Notice Domain Performance Optimization Strategy

## Current Baseline Performance

### Route Table Operations (100 subscriptions)
- `route_table_insert`: **2.7 µs** (insert 10 subs)
- `route_table_remove`: **3.7 µs** (remove 5 of 10)
- `route_table_match_exact`: **9.7 µs** (scan 100 subs)
- `route_table_match_global_wildcard`: **9.5 µs**
- `route_table_match_trailing_wildcard`: **4.9 µs** (20 subs)
- `route_table_match_mid_path_wildcard`: **5.4 µs** (20 subs)
- `route_table_match_none`: **9.4 µs** (scan 100 subs)
- `route_table_cleanup_channel`: **26 µs** (remove 20 of 100)

### Service Operations
- `service_subscribe`: **1.6 µs**
- `service_unsubscribe`: **549 ns**
- `service_publish_no_subscribers`: **24 ns** ✅
- `service_publish_one_subscriber`: **467 ns**
- `service_publish_ten_subscribers`: **496 ns**
- `service_publish_wildcard_matching`: **9.2 µs** (5 matches in 100 subs)
- `service_subscribe_publish_unsubscribe_cycle`: **969 ns**

### Handler Operations (with TLV parsing)
- `handler_subscribe`: **712 ns**
- `handler_publish_no_subscribers`: **696 ns**
- `handler_publish_one_subscriber`: **753 ns**
- `handler_subscribe_publish_unsubscribe`: **1.4 µs**

---

## Critical Performance Issues

### 🔴 **Issue #1: O(N) Linear Scan for Every Publish**

**Current Implementation:**
```rust
pub fn matching_subscribers(&self, route: &str) -> Vec<RtSubscription> {
    let mut out = Vec::new();
    for sub in self.subs.values() {              // ← O(N) iteration
        if route_matches(&sub.route_pattern, route) {
            out.push(sub.clone());                // ← Cloning
        }
    }
    out
}
```

**Problem:**
- With **1 million subscriptions**, every publish scans 1M entries
- Pattern matching (`route_matches`) is called 1M times per publish
- String splitting allocations in `route_matches` for wildcards
- Currently takes **~9.5 µs for 100 subs** → **~95 ms for 1M subs** (unacceptable)

**Impact on Volume:**
- At 10,000 publishes/sec with 1M subs: **950 seconds of CPU time** needed
- Server would melt

---

### 🔴 **Issue #2: Inefficient Pattern Matching**

**Current Implementation:**
```rust
fn route_matches(pattern: &str, route: &str) -> bool {
    // ...
    if pattern.contains('*') {
        let pattern_parts: Vec<&str> = pattern.split('/').collect();  // ← Allocation
        let route_parts: Vec<&str> = route.split('/').collect();      // ← Allocation
        // ... comparison logic
    }
    // ...
}
```

**Problem:**
- Allocates 2 `Vec`s per comparison
- Splits strings every time (no caching)
- `contains('*')` scans entire string
- No early termination optimizations

**Cost:**
- Each wildcard pattern match: **~50-100 ns overhead** from allocations
- With 1M subs having wildcards: **50-100 ms wasted on allocations alone**

---

### 🔴 **Issue #3: No Indexing Strategy**

**Current Structure:**
```rust
pub struct RouteTable {
    subs: HashMap<u64, RtSubscription>,           // id → subscription
    index: HashMap<String, HashSet<u64>>,         // pattern → ids
}
```

**Problem:**
- The `index` is only used for cleanup, not for matching
- No prefix tree (trie) for hierarchical routes
- No bucketing by route segments
- Every publish must scan all subscriptions

**Missing Optimizations:**
- No trie/radix tree for `notice://realm/area/resource/*` patterns
- No segment-based indexing for fast prefix matching
- No bloom filters for quick "no match" determination

---

### 🔴 **Issue #4: String Allocations in Hot Path**

**Current Issues:**
```rust
route.to_string(),                                // Allocation
msg_id.map(|s| s.to_string()),                   // Allocation
reply_to.map(|s| s.to_string()),                 // Allocation (removed)
```

**Problem:**
- Every message delivery allocates strings
- With 1M deliveries/sec: **1M string allocations/sec**
- Sender cloning is cheap (Arc), but strings are not

---

### 🟡 **Issue #5: Lock Contention (Service Level)**

**Current Service:**
```rust
Arc<std::sync::Mutex<NoticeService>>
```

**Problem:**
- Single lock for all operations
- Subscribe/unsubscribe blocks publish
- Publish blocks other publishes
- At high concurrency: **lock contention becomes bottleneck**

---

## Optimization Strategy

### Phase 1: Indexing (HIGHEST PRIORITY) 🎯

**Goal:** Reduce O(N) to O(log N) or O(1) for matching

#### 1.1: Segment-Based Trie Index
Build a hierarchical trie for exact and prefix matching:

```rust
pub struct RouteTable {
    // Existing
    subs: HashMap<u64, RtSubscription>,
    
    // NEW: Hierarchical trie for fast prefix matching
    trie: RouteTrie,                              // notice://realm/area/resource/*
    
    // NEW: Wildcard buckets
    global_wildcard: Vec<u64>,                    // "*" subscriptions
    mid_path_wildcards: HashMap<Pattern, Vec<u64>>, // notice://*/prod/*/error
}

struct RouteTrie {
    root: TrieNode,
}

struct TrieNode {
    // Exact match subscribers at this node
    subscribers: Vec<u64>,
    
    // Child nodes for next segment
    children: HashMap<String, TrieNode>,
    
    // Wildcard child (notice://realm/*/resource)
    wildcard_child: Option<Box<TrieNode>>,
    
    // Trailing wildcard subscribers (notice://realm/area/*)
    trailing_wildcard_subs: Vec<u64>,
}
```

**Benefits:**
- Exact match: O(depth) ≈ O(4) for `notice://realm/area/resource/op`
- Prefix match with `/*`: O(depth) + O(subscribers at node)
- No more O(N) scans

**Expected Performance:**
- 1M subs: **<10 µs** per publish (vs current 95 ms)
- 10M subs: **<20 µs** per publish

---

#### 1.2: Bloom Filters for Quick Rejection

Add bloom filters per trie level:

```rust
struct TrieNode {
    // ...existing...
    
    // NEW: Bloom filter for quick "no match" detection
    bloom: BloomFilter,  // Contains all descendant route segments
}
```

**Benefits:**
- Quick rejection of non-matching routes
- ~99% reduction in unnecessary tree traversals
- **<1 µs** for "no match" scenarios

---

### Phase 2: Pattern Matching Optimization

#### 2.1: Pre-parse and Cache Pattern Structure

```rust
#[derive(Clone)]
pub struct ParsedPattern {
    segments: Vec<PatternSegment>,     // Pre-split, no runtime allocation
    has_trailing_wildcard: bool,       // Pre-computed flag
    match_type: MatchType,             // Exact, Prefix, Wildcard, etc.
}

enum PatternSegment {
    Exact(String),
    Wildcard,
}

pub struct RtSubscription {
    pub id: u64,
    pub route_pattern: String,         // Original for display
    pub parsed_pattern: ParsedPattern, // NEW: Pre-parsed for fast matching
    pub channel_id: u32,
    pub sender: SubSender,
}
```

**Benefits:**
- No runtime string splits
- No runtime wildcard detection
- Pattern matching becomes: **<10 ns** per comparison

---

#### 2.2: Optimize route_matches Function

```rust
#[inline(always)]
fn route_matches_fast(parsed: &ParsedPattern, route_parts: &[&str]) -> bool {
    match parsed.match_type {
        MatchType::Global => true,                    // "*"
        MatchType::Exact => {
            // Simple slice comparison, no wildcards
            parsed.segments.len() == route_parts.len()
                && parsed.segments.iter()
                    .zip(route_parts)
                    .all(|(p, r)| p.matches(r))
        }
        MatchType::Prefix => {
            // Trailing wildcard or hierarchical
            route_parts.len() >= parsed.segments.len()
                && parsed.segments.iter()
                    .zip(route_parts)
                    .all(|(p, r)| p.matches(r))
        }
        MatchType::MidPath => {
            // Has * in middle, exact segment count
            route_parts.len() == parsed.segments.len()
                && parsed.segments.iter()
                    .zip(route_parts)
                    .all(|(p, r)| p.matches(r))
        }
    }
}
```

**Benefits:**
- Inlined for zero-cost abstraction
- No allocations
- Branch prediction friendly

---

### Phase 3: Memory & Allocation Optimization

#### 3.1: Use Cow or Arc<str> for Strings

```rust
pub struct RtSubscription {
    pub id: u64,
    pub route_pattern: Arc<str>,        // Shared, no cloning cost
    pub parsed_pattern: ParsedPattern,
    pub channel_id: u32,
    pub sender: SubSender,
}
```

#### 3.2: Object Pooling for Temporary Allocations

```rust
thread_local! {
    static ROUTE_PARTS_POOL: RefCell<Vec<Vec<&'static str>>> = ...;
}
```

---

### Phase 4: Concurrency Optimization

#### 4.1: Replace Mutex with RwLock

```rust
Arc<tokio::sync::RwLock<NoticeService>>
```

**Benefits:**
- Multiple concurrent publishes (read locks)
- Subscribe/unsubscribe still exclusive (write lock)
- **10x throughput improvement** under read-heavy workload

---

#### 4.2: Lock-Free Read Path (Advanced)

Use `Arc<ArcSwap<RouteTable>>` or crossbeam's lock-free structures:

```rust
pub struct NoticeService {
    route_table: Arc<ArcSwap<RouteTable>>,  // Lock-free reads
    write_lock: Mutex<()>,                   // Only for updates
}
```

**Benefits:**
- Zero contention on publish (pure reads)
- Updates are rare (subscribe/unsubscribe)
- **100x throughput improvement** at high concurrency

---

### Phase 5: Additional Optimizations

#### 5.1: SIMD Pattern Matching (Advanced)
Use SIMD for bulk comparisons in trie traversal

#### 5.2: Subscription Batching
Batch multiple subscriptions in single write operation

#### 5.3: Hot/Cold Splitting
Keep frequently accessed patterns in cache-friendly structure

---

## Implementation Plan

### Step 1: Add Comprehensive Benchmarks ✅ DONE
- [x] Route table operations at scale
- [x] Service operations under load
- [x] Handler throughput

### Step 2: Implement Trie-Based Index (Week 1)
1. Create `RouteTrie` structure
2. Implement insert/remove with trie updates
3. Implement `matching_subscribers_fast()` using trie
4. Benchmark: Target <10 µs for 1M subs

### Step 3: Pre-parse Patterns (Week 1)
1. Create `ParsedPattern` structure
2. Parse on insert, store in `RtSubscription`
3. Update matching logic to use parsed patterns
4. Benchmark: Target <10 ns per pattern match

### Step 4: Concurrency (Week 2)
1. Replace `Mutex` with `RwLock`
2. Benchmark multi-threaded publish throughput
3. Consider lock-free read path

### Step 5: Memory Optimization (Week 2)
1. Use `Arc<str>` for route strings
2. Object pooling for temporary allocations
3. Profile memory usage with 1M+ subs

---

## Success Metrics

### Current (100 subs)
- Match exact: **9.7 µs**
- Publish (1 sub): **467 ns**
- Publish (10 subs): **496 ns**

### Target (1M subs)
- Match exact: **<10 µs** (1000x improvement)
- Match wildcard: **<20 µs** (5000x improvement)
- Publish (1 sub): **<1 µs**
- Publish (1000 subs): **<50 µs**

### Target (10M subs)
- Match exact: **<20 µs**
- Match wildcard: **<50 µs**
- Publish (1 sub): **<2 µs**

### Throughput Targets
- Single thread: **100K publishes/sec** (1M subs)
- 8 cores: **500K publishes/sec** (1M subs)
- Memory: **<500 bytes per subscription**

---

## Risk Assessment

### Low Risk
- ✅ Pre-parsing patterns: No behavior change, pure optimization
- ✅ RwLock: Drop-in replacement, well-tested

### Medium Risk
- ⚠️ Trie indexing: Complex data structure, needs thorough testing
- ⚠️ Pattern matching changes: Must maintain exact semantics

### High Risk
- 🔴 Lock-free structures: Requires deep understanding, subtle bugs possible
- 🔴 SIMD: Platform-specific, maintenance burden

---

## Recommendation

**Start with Phase 1 & 2 immediately:**
1. Implement trie-based indexing
2. Pre-parse patterns

These two changes alone will get us **1000-5000x improvement** for large subscription counts and are **low-medium risk** with high reward.

**Phase 3 & 4** can follow once we validate Phase 1-2 performance gains.

**Phase 5** is optional - only if we need extreme performance beyond millions of subs.
