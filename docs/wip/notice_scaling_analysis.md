# Notice Domain Scaling Analysis

## Confirmed O(N) Linear Scaling Behavior

### Benchmark Results: Exact Match Routing

| Subscriptions | Time (µs) | Time per Sub (ns) | Scaling Factor |
|--------------|-----------|-------------------|----------------|
| 100          | 9.7       | 97                | 1x             |
| 1,000        | 92.2      | 92.2              | 9.5x           |
| 10,000       | 942.7     | 94.3              | 97x            |
| 100,000      | 11,965    | 119.7             | 1233x          |

**Analysis:**
- Clear O(N) linear relationship: ~10x time for 10x subscriptions
- Time per subscription remains constant: **~90-120 ns per subscription scanned**
- At **1 million subscriptions**: Projected ~120 ms per publish (UNACCEPTABLE)

### Benchmark Results: Wildcard Match Routing

| Subscriptions | Time (µs) | Time per Sub (ns) | Scaling Factor |
|--------------|-----------|-------------------|----------------|
| 20 (baseline)| 4.9       | 245               | 1x             |
| 1,000        | 115.7     | 115.7             | 23.6x          |
| 10,000       | 1,166.6   | 116.7             | 238x           |
| 100,000      | 15,010    | 150.1             | 3063x          |

**Analysis:**
- Also O(N) linear scaling
- ~50% slower than exact match (wildcard pattern parsing overhead)
- At **1 million subscriptions**: Projected ~150 ms per publish

---

## Bottleneck Analysis

### 🔴 Critical Issue: matching_subscribers() Implementation

**Current Code:**
```rust
pub fn matching_subscribers(&self, route: &str) -> Vec<RtSubscription> {
    let mut out = Vec::new();
    for sub in self.subs.values() {              // O(N) - ALL subscriptions
        if route_matches(&sub.route_pattern, route) {
            out.push(sub.clone());
        }
    }
    out
}
```

**Cost Breakdown (per publish):**
1. Iterate ALL subscriptions: **O(N)**
2. Pattern matching each subscription:
   - Exact match: **~90 ns per sub**
   - Wildcard match: **~150 ns per sub** (includes string splits)
3. Clone matching subscriptions: **~50 ns per match**

### 🔴 Secondary Issue: route_matches() Allocations

**Current Code:**
```rust
fn route_matches(pattern: &str, route: &str) -> bool {
    if pattern.contains('*') {
        let pattern_parts: Vec<&str> = pattern.split('/').collect();  // Allocation!
        let route_parts: Vec<&str> = route.split('/').collect();      // Allocation!
        // ... comparison
    }
    // ...
}
```

**Costs:**
- `Vec` allocation: **~20 ns**
- `contains('*')` scan: **~5 ns**
- Total overhead per wildcard match: **~25-30 ns** (30% of total time)

---

## Scalability Projections

### Target: 1 Million Subscriptions

**Current Architecture:**
- Exact match: **~120 ms per publish**
- Wildcard match: **~150 ms per publish**
- Throughput: **6-8 publishes/second** (completely unacceptable)

**Volume Scenarios:**
| Publish Rate | CPU Time Required | Feasibility |
|--------------|-------------------|-------------|
| 10/sec       | 1.2-1.5 seconds   | ❌ Impossible (>100% CPU) |
| 100/sec      | 12-15 seconds     | ❌ Impossible (>1000% CPU) |
| 1,000/sec    | 120-150 seconds   | ❌ Impossible (>10000% CPU) |

### Target: 10 Million Subscriptions

**Projected Performance:**
- Exact match: **~1.2 seconds per publish**
- Wildcard match: **~1.5 seconds per publish**
- Throughput: **<1 publish/second**

---

## Required Performance Targets

### For 1M Subscriptions
- **Target**: <10 µs per publish (1000x improvement)
- **Throughput**: 100K publishes/sec (single thread)
- **Algorithm**: O(log N) or O(1) lookup required

### For 10M Subscriptions
- **Target**: <20 µs per publish (60,000x improvement)
- **Throughput**: 50K publishes/sec (single thread)
- **Algorithm**: O(log N) required

---

## Optimization Strategy (Prioritized)

### Phase 1: Trie-Based Indexing (CRITICAL) 🔴

**Goal**: Replace O(N) scan with O(depth) trie traversal

**Approach**:
```rust
pub struct RouteTrie {
    root: TrieNode,
}

struct TrieNode {
    // Subscriptions at this exact path
    subscribers: Vec<u64>,
    
    // Child nodes (next segment)
    children: HashMap<String, TrieNode>,
    
    // Wildcard child (notice://realm/*/resource)
    wildcard_child: Option<Box<TrieNode>>,
    
    // Trailing wildcard subs (notice://realm/area/*)
    trailing_wildcard_subs: Vec<u64>,
}
```

**Expected Performance**:
- Exact match: **O(depth) ≈ O(4)** for `notice://realm/area/resource/op`
- Lookup time: **<1 µs** (1000x improvement)
- Works at any scale: 1M or 10M subscriptions

**Benefits**:
- ✅ Sub-linear complexity
- ✅ No full table scan
- ✅ Cache-friendly (follows route path)
- ✅ Supports all wildcard patterns

---

### Phase 2: Pre-parsed Patterns (HIGH PRIORITY) 🟠

**Goal**: Eliminate runtime string parsing overhead

**Approach**:
```rust
#[derive(Clone)]
pub struct ParsedPattern {
    segments: Vec<PatternSegment>,     // Pre-split
    match_type: MatchType,             // Pre-computed
    has_trailing_wildcard: bool,
}

enum PatternSegment {
    Exact(String),
    Wildcard,
}

pub struct RtSubscription {
    pub id: u64,
    pub route_pattern: String,         // Display
    pub parsed_pattern: ParsedPattern, // Fast matching
    pub channel_id: u32,
    pub sender: Sender<NoticeMessage>,
}
```

**Expected Performance**:
- Pattern match: **<10 ns** (10x improvement)
- Zero allocations on hot path
- 30% reduction in per-subscription match time

---

### Phase 3: Concurrent Access (MEDIUM PRIORITY) 🟡

**Goal**: Allow concurrent publishes without blocking

**Approach**:
```rust
// Replace Mutex with RwLock
Arc<tokio::sync::RwLock<NoticeService>>

// Or lock-free reads (advanced)
Arc<ArcSwap<RouteTable>>
```

**Expected Performance**:
- Multi-threaded throughput: **8-16x with 8-16 cores**
- No lock contention on publish (read-only operations)

---

### Phase 4: Memory Optimization (LOW PRIORITY) 🟢

**Goal**: Reduce memory footprint and allocation overhead

**Approaches**:
- Use `Arc<str>` instead of `String` for routes
- Object pooling for temporary allocations
- Compact data structures

**Expected Benefits**:
- Lower memory usage: **<500 bytes per subscription**
- Better cache locality
- Reduced GC pressure (less cloning)

---

## Implementation Timeline

### Week 1: Trie Implementation
1. **Day 1-2**: Design and implement `RouteTrie` structure
2. **Day 3-4**: Implement insert/remove with trie updates
3. **Day 5**: Implement `matching_subscribers_fast()` using trie
4. **Goal**: <10 µs for 1M subscriptions

### Week 2: Pattern Pre-parsing
1. **Day 1-2**: Create `ParsedPattern` structure
2. **Day 3**: Update insert logic to parse patterns
3. **Day 4-5**: Update matching logic and benchmark
4. **Goal**: <10 ns per pattern match

### Week 3: Concurrency
1. **Day 1-2**: Replace Mutex with RwLock
2. **Day 3-4**: Multi-threaded benchmarks
3. **Day 5**: Consider lock-free reads if needed
4. **Goal**: 8x throughput improvement with 8 cores

---

## Success Metrics

### Performance Targets
| Metric                        | Current (1M subs) | Target (1M subs) | Improvement |
|------------------------------|-------------------|------------------|-------------|
| Exact match time             | 120 ms            | <10 µs           | 12,000x     |
| Wildcard match time          | 150 ms            | <20 µs           | 7,500x      |
| Single-thread throughput     | 8/sec             | 100K/sec         | 12,500x     |
| Multi-thread throughput (8c) | 8/sec             | 500K/sec         | 62,500x     |

### Validation Benchmarks
- [ ] route_table_match_exact_1m: <10 µs
- [ ] route_table_match_wildcard_1m: <20 µs
- [ ] service_publish_concurrent_8threads: >500K ops/sec
- [ ] memory_usage_1m_subs: <500 MB (500 bytes/sub)

---

## Risk Assessment

### Phase 1: Trie Implementation
- **Risk**: Medium (complex data structure)
- **Mitigation**: Thorough unit tests for all wildcard patterns
- **Fallback**: Keep existing implementation as backup

### Phase 2: Pre-parsing
- **Risk**: Low (pure optimization, no behavior change)
- **Mitigation**: Exhaustive test coverage
- **Fallback**: N/A (safe change)

### Phase 3: Concurrency
- **Risk**: Medium (concurrency bugs)
- **Mitigation**: Stress tests, race detection
- **Fallback**: Keep Mutex as option

---

## Conclusion

The current O(N) linear scan architecture **cannot support millions of subscriptions** as required. Confirmed scaling data shows:

- **100 subs → 9.7 µs** ✅ Acceptable
- **1K subs → 92 µs** ⚠️ Borderline
- **10K subs → 943 µs** ❌ Problematic
- **100K subs → 12 ms** ❌ Unacceptable
- **1M subs → 120 ms (projected)** 🔴 Critical failure

**Immediate action required**: Implement trie-based indexing (Phase 1) to achieve O(log N) or O(depth) complexity. This is the ONLY way to reach the target of handling millions of subscriptions with high-volume publishes.

**Next Steps**:
1. Begin trie implementation design
2. Create detailed trie API specification
3. Implement and benchmark against 1M+ subscriptions
4. Validate all wildcard patterns work correctly
