# Fitz Project TODO

Actionable items only. Completed items removed. Last updated: January 18, 2026

---

## SECTION 1: Domain Implementation Gaps

### KV Domain - Missing Integration Tests & Benchmarks

**Current State**: 8 unit tests only (~6.8% coverage)  
**Missing**: Integration tests, E2E tests, all 4 benchmark tiers

**API Reference** (from src/domains/kv/):
```rust
// Protocol: KvMessage enum variants
Begin { route_family, realm, area, resource, mode: TxMode, write_options }
Commit
Rollback
Get { route_family, resource, key: Bytes }
Put { route_family, resource, key: Bytes, value: Bytes }
Insert { route_family, resource, key: Bytes, value: Bytes }
Delete { route_family, resource, key: Bytes }
DeleteRange { route_family, resource, start: Bytes, end: Bytes }
Scan { route_family, resource, query: ScanQuery }

// Response types
enum KvResponse {
    Ok,
    Value(Bytes),
    Pairs(Vec<KvPair>),
    Error(KvError),
}

enum KvError {
    NoActiveTx,
    TxAlreadyActive,
    ResourceMismatch,
    InsertConflict,
    InvalidFamily,
    // ... others
}

// Actor API
impl KvActor {
    pub fn new(store: Arc<MidgeEngine>) -> Self
    pub fn handle(&mut self, msg: KvMessage) -> KvResponse
}
```

**TODO Items**:

- [ ] **kv-unit-tests-complete** - Add missing test cases in src/domains/kv/actor.rs
  - [ ] `Delete` operation - test delete of existing key, verify it's gone
  - [ ] `Scan` operation - test ScanQuery with start/end/limit/reverse
  - [ ] `Commit` behavior - verify committed data persists
  - [ ] `Rollback` behavior - verify rolled-back changes don't persist
  - [ ] Error mapping - test KvError::InsertConflict on duplicate insert
  - [ ] Key scoping - test two resources don't interfere (prefix isolation)
  - Acceptance: All 8 operations + error cases tested, 20+ new test functions

- [ ] **kv-integration-tests** - Create tests/kv_e2e_basic.rs
  - Pattern: Copy from tests/rpc_e2e_basic.rs structure
  - Setup: Create EngineTestbed, setup KV actor
  - Test: Full workflow: Begin(realm1/area1/resource1) → Put → Get → Commit
  - Test: Cross-resource isolation: Begin(resource1), try Put to resource2 → error
  - Test: Cross-family isolation: RouteFamily(1) and RouteFamily(2) separate CFs
  - Acceptance: 15+ integration test functions covering transaction lifecycle

- [ ] **kv-benchmarks** - Create 4 benchmark tiers (pattern: src/benches/config.rs)
  
  **benches/tier1_hotpath_kv.rs** - Pure operation latency
  ```rust
  // Setup outside b.iter()
  let mut actor = create_bench_kv_actor(...);
  actor.begin(rf, mode, write_opts);
  
  group.bench_function("put_single_key", |b| {
    b.iter(|| actor.handle_put(black_box(key), black_box(value)))
  });
  // Measure: Get, Put, Insert, Delete in isolation (< 10 µs each)
  ```
  
  **benches/tier2_subsystem_kv.rs** - Transaction lifecycle
  ```rust
  // Measure: Begin → 10× Put → Commit cycle
  // Baseline: ~50-100 µs per transaction
  ```
  
  **benches/tier3_system_kv.rs** - Concurrent families
  ```rust
  // Two families hammered simultaneously
  // Baseline: No contention, compare to tier2
  ```
  
  **benches/tier4_integration_kv.rs** - Persistence
  ```rust
  // Create, close, reopen → verify data survives
  // Baseline: Startup recovery < 1 ms
  ```
  - Acceptance: All 4 tier files created, added to Cargo.toml [[bench]] entries

---

### Schedule Domain - Missing Integration Tests & Benchmarks

**Current State**: 4 serialization unit tests only (~3.4% coverage)  
**Missing**: Integration tests, E2E tests, benchmarks

**API Reference** (from src/domains/schedule/):
```rust
// Protocol
pub struct SchedulePayload {
    pub cron: String,      // "0 9 * * MON" format (minute hour day month weekday)
    pub resource: String,  // "billing/charges"
    pub operation: String, // "process"
}

pub enum ScheduleMessage {
    Create { route: Route, payload: Bytes },  // payload = TLV-encoded SchedulePayload
    Delete { id: u64 },
    Tick,  // Called periodically to fire due schedules
}

// Actor API
pub struct ScheduleActor {
    pub fn create_schedule(&mut self, route: Route, payload: Bytes) -> Result<u64, String>
    pub fn delete_schedule(&mut self, id: u64) -> Result<(), String>
    pub fn tick(&mut self) -> Vec<(Route, Bytes)>  // Returns schedules that fired
}

// Cron parsing
CronSchedule::parse("0 9 * * 1-5") -> CronSchedule
// Supports: * (all), */2 (step), 1-5 (range), 1,3,5 (list), combinations
```

**TODO Items**:

- [ ] **schedule-unit-tests-complete** - Extend src/domains/schedule/actor.rs tests
  - [ ] `CronSchedule::parse` - test valid expressions (*, step, range, CSV, combinations)
  - [ ] `CronSchedule::parse` - test invalid expressions (out of bounds, bad syntax)
  - [ ] `matches_dt` - test cron matching for specific timestamps (9 AM weekdays, etc)
  - [ ] Cron field ranges: minute 0-59, hour 0-23, day 1-31, month 1-12, weekday 0-6
  - Acceptance: 20+ cron validation tests

- [ ] **schedule-integration-tests** - Create tests/schedule_e2e_basic.rs
  - Pattern: Copy rpc_e2e_basic.rs or lease_e2e_basic.rs structure
  - Setup: Create ScheduleActor with mock Clock
  - Test: Create schedule → tick at scheduled time → verify notice emitted
  - Test: Delete schedule → tick → no notice emitted
  - Test: Persistence: Create, close, reopen → schedule still there
  - Test: Multiple schedules, fire at different times
  - Acceptance: 15+ integration test functions

- [ ] **schedule-benchmarks** - Create benches/tier4_integration_schedule.rs
  ```rust
  // Setup: 100 schedules, clock mock
  // b.iter(): Create new schedule + delete old one (churn)
  // Measure: 1000 create/delete cycles
  // Baseline: < 1 ms per cycle (mostly persistence overhead)
  ```
  - Acceptance: Benchmark file created, added to Cargo.toml [[bench]]

---

### Lease Domain - Complete Benchmarks

**Current State**: 32 unit tests + 3 integration files ✅, but missing tier1/3/4 benchmarks  
**Missing**: Tier 1, 3, 4 benchmarks (tier 2 exists)

**Existing Tier 2**: benches/tier2_subsystem_lease.rs (488 lines, covers acquire/renew/release/check)

**TODO Items**:

- [ ] **lease-benchmarks-tier1** - Create benches/tier1_hotpath_lease.rs
  ```rust
  // Pattern from tier1_hotpath_matcher.rs
  group.sampling_mode(criterion::SamplingMode::Flat);
  
  // Measure: acquire(), renew(), release(), check() pure operations
  // Baseline: < 1 µs each (faster than subsystem)
  // Each operation tested independently
  ```
  - Acceptance: 4 functions (one per operation), < 1 µs latency each
  
- [ ] **lease-benchmarks-tier3** - Create benches/tier3_system_lease.rs
  ```rust
  // 10 concurrent families, hammer one family while others are idle
  // Measure: Contention impact on acquire/renew
  // Baseline: Should match tier1 + lock overhead
  ```
  - Acceptance: Contention measurement, no exponential degradation
  
- [ ] **lease-benchmarks-tier4** - Create benches/tier4_integration_lease.rs
  ```rust
  // Realistic: 100 concurrent leases expiring staggered
  // Mock clock advances through expiration window
  // Measure: Expiration handling throughput
  // Baseline: 1000+ expiration checks/sec
  ```
  - Acceptance: Benchmark file, realistic expiration patterns
  
- [ ] Update `Cargo.toml` - Add 3 [[bench]] entries
  ```toml
  [[bench]]
  name = "tier1_hotpath_lease"
  harness = false

  [[bench]]
  name = "tier3_system_lease"
  harness = false

  [[bench]]
  name = "tier4_integration_lease"
  harness = false
  ```

---

## SECTION 2: Performance Optimizations

### Priority 1 (CRITICAL): Queue Batch Latency Regression

**Baseline** (from January 18 benchmarks):
```
queue_batch_latency_reserve/1   : 1.95 µs    (baseline)
queue_batch_latency_reserve/10  : 28.29 µs   (14.5× multiplier, expected ~10×)
queue_batch_latency_reserve/100 : 218.08 µs  (111.9× multiplier, expected ~100×)
```
**Issue**: Exponential scaling instead of linear. ~20-50× slower than expected.

**Code Location**: src/domains/queue/queue_actor.rs
- `handle_reserve()` (lines ~250-320)
- `handle_enqueue_batch()` (lines ~350-450)

**Root Cause Investigation**:
```bash
# Step 1: Profile the hot path
cargo flamegraph --bench tier2_subsystem_queue \
  --profile=release \
  -- --bench queue_batch_latency_reserve/10

# Look for:
# 1. Mutex lock time (std::sync::Mutex::lock)
# 2. Vec reallocation (alloc patterns)
# 3. Malloc overhead (multiple allocations per message)
# 4. Per-ID transaction overhead (N Midge transactions instead of 1)
```

**Fix Options** (pick one, expected improvement 10-30%):

**Option A: Switch to parking_lot::Mutex** (Recommended - fastest, minimal changes)
```rust
// Cargo.toml
[dependencies]
parking_lot = "0.12"

// src/domains/queue/queue_actor.rs
// Replace: let messages = std::sync::Mutex::new(...);
// With: let messages = parking_lot::Mutex::new(...);

// parking_lot is faster for contended locks (no kernel wait queues)
// Expected: 15-20% faster on batch operations
```

**Option B: Pre-allocate buffers**
```rust
// In handle_reserve()
// Before: Vec::new() grows with each push
// After: Vec::with_capacity(expected_batch_size)

let mut messages = Vec::with_capacity(32);  // or actual expected size
for msg in incoming {
    messages.push(msg);  // Single allocation, no re-growth
}
// Expected: 5-15% faster (reduces alloc overhead)
```

**Option C: Lock-free for small batches**
```rust
// For batch < 32, use SmallVec to avoid heap allocation
use smallvec::SmallVec;
let messages: SmallVec<[Message; 32]> = SmallVec::new();
// Expected: 3-10% faster (stack storage, no malloc)
```

**Verification**:
```bash
# After fix, run benchmark
cargo bench --bench tier2_subsystem_queue

# Acceptance criteria:
# - queue_batch_latency_reserve/10 < 30 µs (was 28.29, target: no worse)
# - No regression elsewhere (single message ops same as before)
# - Linear scaling restored: /1 → /10 ~10× slower, /100 ~10× slower than /10
```

---

### Priority 2 (HIGH): Stream State Accumulation

**Baseline** (from January 18 benchmarks):
```
stream_single_append_read  : 1.14 ns   (optimized away by black_box)
stream_batch_50appends    : 23.78 ns   (actual operation)
stream_scan_4partitions   : 24.67 ns   (good)
stream_long_running_20ops : 25.50 ns + drift (REGRESSED +3.7% after 20 ops)
```
**Issue**: Memory accumulates over 20 operations → +3.7% latency on the 20th operation.

**Code Location**: src/domains/stream/stream_actor.rs
- Offset map likely not cleaned (tracks consumed offsets)
- Watermarks not trimmed (high water mark per partition)
- Committed ranges not compacted (list grows unbounded)

**Investigation**:
```rust
// Add instrumentation in StreamActor
println!("offset_map len: {}", self.offset_map.len());
println!("watermarks len: {}", self.watermarks.len());
println!("committed_ranges len: {}", self.committed_ranges.len());

// Run: cargo test --lib domains::stream::
// Expected: All three grow by 1 per operation, no cleanup
```

**Fix**: Periodic cleanup every 20 operations
```rust
// In StreamActor::process_operation() or handle()
self.operations_count += 1;
if self.operations_count % 20 == 0 {
    // Clean stale entries
    self.offset_map.retain(|_k, v| v.is_recent());
    self.committed_ranges.compact();  // Merge adjacent ranges
    self.watermark.trim_before(now - 5_minutes);
}

// Or more aggressive: cleanup every operation for non-contended case
if should_cleanup(self.operations_count) {
    ...
}
```

**Verification**:
```bash
cargo bench --bench tier4_integration_stream

# Acceptance criteria:
# - stream_long_running_20ops < 10.5 ns (was 25.5 ns, expect linear again)
# - No regression on single operations
# - Reduced memory footprint (profile RSS)
```

---

### Priority 3 (MEDIUM): Fanout Cache Pressure

**Baseline** (from January 18 benchmarks):
```
pipeline_decode_into_route_fanout/16B_1sub  : 33.7 µs  (baseline)
pipeline_decode_into_route_fanout/16B_64sub : 42.3 µs  (1.26× multiplier for 64 subs, OK)
pipeline_decode_into_route_fanout/256B_1sub : 39.8 µs  (2× payload = 2× latency, expected)
pipeline_decode_into_route_fanout/256B_64sub: 47.3 µs  (1.19× multiplier for payload, regressed!)
```
**Issue**: 256B payload to 64 subscribers = +3.3% slower than expected. Cache line conflict.

**Code Location**: src/domains/notice/ (fanout dispatch)
- SubscriberNode memory layout may not be cache-aligned
- Dispatching 64 nodes sequentially may cause L1/L2 cache misses

**Investigation**:
```bash
# Measure node size and alignment
echo "struct SubscriberNode {" && cargo expand --lib domains::notice::bench \
  | grep -A 20 "struct SubscriberNode"

# Profile with perf
perf stat -e cache-references,cache-misses,L1-dcache-load-misses \
  cargo bench --bench tier3_system_notice -- \
  pipeline_decode_into_route_fanout/256B_64sub
```

**Fix Option A: Cache-align hot struct** (Fastest, no logic change)
```rust
// src/domains/notice/bench.rs or wherever SubscriberNode is defined
#[repr(align(64))]  // 64 bytes = typical L1 cache line
pub struct SubscriberNode {
    route_hash: u64,
    // ... other fields (padding as needed)
}
// Expected: 5-10% faster (each sub on separate cache line)
```

**Fix Option B: Batch dispatch** (More complex, better scaling)
```rust
// Process subscribers in batches of 4
for subs in subscribers.chunks(4) {
    // Single lock acquisition covers 4 dispatches
    let _guard = lock.acquire();
    for sub in subs {
        dispatch(sub);
    }
    drop(_guard);
}
// Expected: 3-8% faster (reduced lock overhead)
```

**Verification**:
```bash
cargo bench --bench tier3_system_notice

# Acceptance criteria:
# - 256B_64sub < 50 µs (was 47.3 µs, just ensure no worse)
# - Multiplier should be < 1.2× (matching 16B behavior)
```

---

### Priority 4 (MEDIUM): Benchmark Stability

**Current Issue**: 20-30% outlier rate, only 10 samples per benchmark
- Makes it hard to detect real regressions (noise > signal)
- CI regression detection will be unreliable

**Files to Update**:
- benches/tier2_subsystem_queue.rs (lines ~40-60 in each group setup)
- benches/tier3_system_notice.rs
- benches/tier4_integration_*.rs (all of them)

**Fix**: Increase sample size and measurement time
```rust
// Before
group.sample_size(10);
group.measurement_time(Duration::from_millis(500));

// After
group.sample_size(50);  // 5× more samples
group.measurement_time(Duration::from_secs(2));  // 4× longer measurement
group.outlier_detection_method(criterion::OutlierDetectionMethod::Tukey);  // Better filtering
```

**Expected Result**: <5% variance across samples (instead of 20-30%)

---

### Priority 5 (MEDIUM): CI Regression Detection

**Goal**: Prevent performance regressions from reaching main branch

**Implementation**:
```bash
# Create .github/workflows/benchmark.yml
# (Template at end of this TODO)

# On every push:
# 1. Run cargo bench (all tiers)
# 2. Compare against baseline (stored in repo or S3)
# 3. Fail if any benchmark regresses >5%
# 4. Post results as PR comment

# Baseline storage strategy:
# Option A: Store in repo (git-lfs for large files)
# Option B: Upload to S3 bucket, fetch in CI
# Option C: Use criterion's built-in baseline comparison
```

**Verification**:
```bash
# Locally test CI workflow
cargo bench > /tmp/results.txt
# (Script compares against stored baseline)

---

## SECTION 3: Code Quality & Violations

See VIOLATIONS.md for full evidence. Quick action items:

- [ ] **V-001: Queue enqueue_batch not atomic**
  - **Location**: src/domains/queue/queue_actor.rs:266-485
  - **Problem**: Multiple Midge transactions: one per ID allocation + one batch write
  - **Fix**: Move ID allocation into batch transaction (single write)
  - **Impact**: Atomicity + performance (fewer transactions)
  
- [ ] **V-002: Queue visible_at_ms uses Instant not epoch**
  - **Location**: src/domains/queue/queue_actor.rs:65-67, 420-426
  - **Problem**: Persisted `visible_at_ms` is Instant delta, not SystemTime epoch
  - **Fix**: Switch to std::time::SystemTime::UNIX_EPOCH for epoch_ms
  - **Impact**: Persistence correctness across process restarts
  
- [ ] **V-003: Queue restart recovery not implemented**
  - **Location**: src/domains/queue/queue_actor.rs:211-241
  - **Problem**: Startup only recovers next_id, not message state
  - **Fix**: Scan persisted messages, rebuild ready/delayed queues
  - **Impact**: True durability (messages survive process crash)
  
- [ ] **V-004: Schedule emits notices without area segment**
  - **Location**: src/domains/schedule/actor.rs:186-195
  - **Problem**: Routes like "notice://realm/resource/operation" (missing area)
  - **Fix**: Include area: "notice://realm/area/resource/operation"
  - **Impact**: Consistency with Fitz routing model (realm/area/resource/operation)
  
- [ ] **V-005: Schedule hard-codes WriteOptions::sync()**
  - **Location**: src/domains/schedule/store.rs:37-41, 51-55
  - **Problem**: All writes force fsync, no user control
  - **Fix**: Accept WriteOptions parameter or config on ScheduleActor creation
  - **Impact**: Follows "no hidden defaults" principle (like KV and Stream)

---

## SECTION 4: Testing & Coverage

### Immediate Action Items

- [ ] **kv-integration-tests** - Create `tests/kv_integration.rs`
  - Full transaction lifecycle, cross-family isolation, write options
  
- [ ] **kv-unit-tests-complete** - Add: Delete, Scan, Commit, Rollback, error mapping, key scoping
  
- [ ] **kv-benchmarks** - Create all 4 tiers (tier1/2/3/4_kv.rs)

- [ ] **schedule-integration-tests** - Create `tests/schedule_integration.rs`
  - Cron parsing, validation, persistence across restarts
  
- [ ] **schedule-unit-tests-complete** - Add cron field ranges and special patterns
  
- [ ] **schedule-benchmarks** - Create tier4_integration_schedule.rs

- [ ] **lease-benchmarks-tier1** - Create benches/tier1_hotpath_lease.rs
  
- [ ] **lease-benchmarks-tier3** - Create benches/tier3_system_lease.rs
  
- [ ] **lease-benchmarks-tier4** - Create benches/tier4_integration_lease.rs
  
- [ ] Update `Cargo.toml` [[bench]] entries for 6 new benchmark files

---

## SECTION 4: Testing & Coverage

### Week 1 (Immediate)
- [ ] Queue: Profile batch latency, implement fix
- [ ] Stream: Implement state cleanup
- [ ] KV: Create integration tests with proper API

### Week 2
- [ ] Queue: Verify batch latency improvements
- [ ] Schedule: Create integration tests
- [ ] Fanout: Profile and implement cache optimization

### Week 3
- [ ] Lease: Complete benchmarks (tier 1, 3, 4)
- [ ] Benchmark: Increase sample size, add CI regression detection
- [ ] All: Verify 276 tests still passing

### Week 4
- [ ] Code violations: Fix V-001 through V-005
- [ ] Documentation: Remove old analysis docs
- [ ] Final: Full benchmark suite + verification

---

## Quick Reference: Commands

```bash
# Run all tests (should be 276)
cargo test --lib

# Run all benchmarks (all 4 tiers)
cargo bench

# Single domain tests
cargo test --lib domains::kv::
cargo test --lib domains::schedule::
cargo test --lib domains::lease::
cargo test --lib domains::notice::
cargo test --lib domains::queue::
cargo test --lib domains::rpc::
cargo test --lib domains::stream::

# Single benchmark suite
cargo bench --bench tier1_hotpath_matcher
cargo bench --bench tier2_subsystem_queue
cargo bench --bench tier3_system_notice
cargo bench --bench tier4_integration_rpc

# Profile with flamegraph (install: cargo install flamegraph)
cargo flamegraph --bench tier2_subsystem_queue

# Check test count per domain
foreach ($d in @("kv", "schedule", "lease", "notice", "queue", "rpc", "stream")) {
  cargo test --lib domains::$d:: 2>&1 | grep "test result:"
}
```

## CI Regression Detection Template

Save as `.github/workflows/benchmark.yml`:

```yaml
name: Benchmark Regression Detection

on: [push, pull_request]

jobs:
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - uses: dtolnay/rust-toolchain@stable
      
      - name: Run benchmarks
        run: cargo bench --bench tier2_subsystem_queue --bench tier3_system_notice
        
      - name: Save baseline
        if: github.ref == 'refs/heads/main'
        run: |
          mkdir -p .benchmarks
          cp target/criterion/* .benchmarks/ || true
          git add .benchmarks
          git commit -m "benchmark baseline" || true
          git push || true
      
      - name: Compare against baseline
        if: github.ref != 'refs/heads/main'
        run: |
          # After running benchmarks above, check criterion output
          # Criterion auto-compares against stored baseline
          # Exit code 0 = no regression, >0 = regression detected
          exit 0  # Configure based on criterion output
```

## Success Criteria (Remove from TODO When Complete)

- [ ] KV: 25+ unit tests + 3 integration + 4 benchmarks
- [ ] Schedule: 12+ unit tests + 3 integration + 1 benchmark  
- [ ] Lease: 4 benchmarks (tier1/3/4 added)
- [ ] Queue batch latency: <10% variance (not 10-50× exponential)
- [ ] Stream: no +3.7% regression
- [ ] Fanout: <50 µs for 64 subscribers
- [ ] Code violations V-001 through V-005 fixed
- [ ] CI regression detection in place

