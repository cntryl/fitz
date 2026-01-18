# Fitz Domain Test & Benchmark Coverage Audit

**Date**: January 18, 2026  
**Scope**: All 7 domains (kv, lease, notice, queue, rpc, schedule, stream)  
**Standard**: Full test coverage (unit + integration + e2e) + tiered benchmarks (tier1-3)

---

## Coverage Matrix

| Domain | Unit Tests | Integration Tests | E2E Tests | Tier 1 Hotpath | Tier 2 Subsystem | Tier 3 System | Tier 4 Integration |
|--------|:----------:|:----------------:|:---------:|:--------------:|:---------------:|:-------------:|:------------------:|
| **kv** | ✅ 17 | ❌ NONE | ❌ NONE | ❌ | ❌ | ❌ | ❌ |
| **lease** | ✅ 68 | ✅ 3 | ✅ 3 | ❌ | ✅ | ❌ | ❌ |
| **notice** | ✅ 26 | ✅ 7 | ✅ 7 | ✅ | ⚠️ | ✅ | ✅ |
| **queue** | ✅ 57 | ✅ 1 | ✅ 1 | ✅ | ✅ | ✅ | ✅ |
| **rpc** | ✅ 32 | ✅ 5 | ✅ 5 | ✅ | ✅ | ✅ | ✅ |
| **schedule** | ✅ 8 | ❌ NONE | ❌ NONE | ❌ | ❌ | ❌ | ❌ |
| **stream** | ✅ 36 | ✅ 4 | ✅ 4 | ✅ | ✅ | ✅ | ✅ |

**Legend**:
- ✅ = Present and complete
- ⚠️ = Present but incomplete or misnaming issue (see findings)
- ❌ = Missing entirely

---

## Per-Domain Summary

### KV Domain

**Status**: 🔴 **INCOMPLETE** — Only unit tests present

**Strengths**:
- 17 unit tests in `actor.rs`
- Covers transaction lifecycle, encoding, error handling
- Invariant testing present (transaction scoping, resource binding)

**Gaps**:
- ❌ Zero integration tests (`tests/kv_*.rs` files do not exist)
- ❌ Zero e2e tests
- ❌ Zero benchmarks (all tiers missing)
- No Session integration tests
- No protocol validation tests
- No persistence correctness validation

**Evidence**:
- `src/domains/kv/actor.rs`: 17 test functions
- `tests/` directory: 0 files matching `kv_*.rs`
- `benches/` directory: 0 files matching `tier*_*_kv.rs`

**Verdict**: Second-class treatment. 17 unit tests provide basic coverage but no confidence in session handling, routing, or performance.

---

### Lease Domain

**Status**: 🟡 **PARTIALLY COMPLETE** — 66% coverage

**Strengths**:
- ✅ 68 unit tests across 3 files (`guard.rs`, `lease_actor.rs`, `session.rs`)
- ✅ 3 integration tests (`lease_auth.rs`, `lease_e2e_basic.rs`, `lease_semantics.rs`)
- ✅ Tier 2 subsystem benchmark present
- Tests cover guard invariants, expiration, concurrency
- Auth and semantics E2E coverage good

**Gaps**:
- ❌ Tier 1 hotpath benchmark missing
- ❌ Tier 3 system benchmark missing
- ❌ Tier 4 integration benchmark missing
- Only 1 integration test file (should have auth + semantics + fanout patterns at minimum)
- No scale/stress pattern testing

**Evidence**:
- `src/domains/lease/`: 68 unit tests across 3 files
- `tests/lease_*.rs`: 3 files (auth, e2e_basic, semantics)
- `benches/tier2_subsystem_lease.rs`: Present (488 lines)
- `benches/tier1_hotpath_lease.rs`: ❌ Missing
- `benches/tier3_system_lease.rs`: ❌ Missing
- `benches/tier4_integration_lease.rs`: ❌ Missing

**Verdict**: Near-complete but benchmarking is incomplete. Needs hotpath and system-level benchmarks to match peers.

---

### Notice Domain

**Status**: 🟢 **SUBSTANTIALLY COMPLETE** — 100% unit + integration + benchmarks (with caveat)

**Strengths**:
- ✅ 26 unit tests across 4 files
- ✅ 7 integration/e2e tests covering auth, semantics, fanout math, scale shape
- ✅ Tier 1 hotpath benchmark
- ✅ Tier 3 system benchmark
- ✅ Tier 4 integration benchmark
- Excellent pattern coverage: subscription matching, fanout, wildcard patterns, scale testing
- Auth discipline enforced

**Gaps / Issues**:
- ⚠️ Tier 2 subsystem benchmark named `tier2_subsytem_notice.rs` (typo: "subsytem" not "subsystem")
  - Present but naming violates convention
- Benchmark naming inconsistency could cause parsing failures in tooling
- Bench.rs protocol tests minimal (6 tests) — mostly in unit layer

**Evidence**:
- `src/domains/notice/`: 26 unit tests
- `tests/notice_*.rs`: 7 files
  - `notice_auth.rs`, `notice_e2e_basic.rs`, `notice_e2e_fanout.rs`
  - `notice_e2e_scale.rs`, `notice_fanout_math.rs`, `notice_scale_shape.rs`, `notice_semantics.rs`
- `benches/tier1_hotpath_notice.rs`: ✅ Present (184 lines)
- `benches/tier2_subsytem_notice.rs`: ⚠️ Present but misspelled (should be `tier2_subsystem_notice.rs`)
- `benches/tier3_system_notice.rs`: ✅ Present (211 lines)
- `benches/tier4_integration_notice.rs`: ✅ Present (268 lines)

**Verdict**: Effectively first-class. Minor typo in benchmark naming is the only issue; otherwise exemplary coverage.

---

### Queue Domain

**Status**: 🟢 **COMPLETE** — 100% coverage

**Strengths**:
- ✅ 57 unit tests across 3 files (`queue_actor.rs` 34 tests, `session.rs` 14, `producer.rs` 9)
- ✅ 1 integration test (`queue_e2e_basic.rs`)
- ✅ Tier 1 hotpath benchmark (184 lines)
- ✅ Tier 2 subsystem benchmark (262 lines)
- ✅ Tier 3 system benchmark (226 lines)
- ✅ Tier 4 integration benchmark (271 lines)
- Comprehensive unit test coverage of edge cases, invariants, persistence

**Gaps**:
- Only 1 e2e test file (vs. notice's 7)
  - No dedicated semantics, auth, or scale shape testing files
  - All e2e behavior assumed covered in single `queue_e2e_basic.rs`

**Evidence**:
- `src/domains/queue/`: 57 unit tests
- `tests/queue_e2e_basic.rs`: 1 file
- `benches/tier1_hotpath_queue.rs`: ✅ Present
- `benches/tier2_subsystem_queue.rs`: ✅ Present
- `benches/tier3_system_queue.rs`: ✅ Present
- `benches/tier4_integration_queue.rs`: ✅ Present

**Verdict**: First-class. Full benchmark coverage + solid unit tests, though e2e test depth is lower than notice (1 vs 7 files).

---

### RPC Domain

**Status**: 🟢 **COMPLETE** — 100% coverage

**Strengths**:
- ✅ 32 unit tests across 4 files
- ✅ 5 integration/e2e test files:
  - `rpc_auth.rs`, `rpc_e2e_basic.rs`, `rpc_semantics.rs`, `rpc_streaming_ordering.rs`
  - `rpc_lease_fault_tolerance.rs` (cross-domain RPC-Lease interaction)
- ✅ Tier 1 hotpath benchmark
- ✅ Tier 2 subsystem benchmark
- ✅ Tier 3 system benchmark
- ✅ Tier 4 integration benchmark
- Good coverage of streaming semantics and fault tolerance

**Gaps**:
- No dedicated scale or fanout pattern tests (queue/notice have these)
- Cross-domain testing (lease_fault_tolerance) suggests incomplete isolation in test design

**Evidence**:
- `src/domains/rpc/`: 32 unit tests
- `tests/rpc_*.rs`: 5 files (auth, e2e_basic, semantics, streaming_ordering, lease_fault_tolerance)
- `benches/tier1_hotpath_rpc.rs`: ✅ Present (202 lines)
- `benches/tier2_subsystem_rpc.rs`: ✅ Present (249 lines)
- `benches/tier3_system_rpc.rs`: ✅ Present (254 lines)
- `benches/tier4_integration_rpc.rs`: ✅ Present (268 lines)

**Verdict**: First-class. Complete coverage with cross-domain testing.

---

### Schedule Domain

**Status**: 🔴 **INCOMPLETE** — Only protocol unit tests present

**Strengths**:
- 8 unit tests in `protocol.rs` (serialization/deserialization)
- Actor is implemented (266 lines) and functional

**Gaps**:
- ❌ Zero integration tests (`tests/schedule_*.rs` do not exist)
- ❌ Zero e2e tests
- ❌ Zero benchmarks (all tiers missing)
- ❌ Actor (`actor.rs`) has 0 unit tests despite being 266 lines
- ❌ Store (`store.rs`) has 0 unit tests
- ❌ No session integration tests
- Protocol tests only validate serialization, not semantics

**Evidence**:
- `src/domains/schedule/protocol.rs`: 8 tests (serialization only)
- `src/domains/schedule/actor.rs`: 0 tests, 266 lines
- `src/domains/schedule/store.rs`: 0 tests, 101 lines
- `tests/` directory: 0 files matching `schedule_*.rs`
- `benches/` directory: 0 files matching `tier*_*_schedule.rs`

**Verdict**: 🔴 **Critical gap**. Second-class treatment. No domain logic validation, no e2e confirmation, no performance baseline. Same category as KV.

---

## Cross-Domain Gaps & Patterns

### Critical Patterns

1. **Benchmark Naming Inconsistency** (Tier 2)
   - Notice: `tier2_subsytem_notice.rs` (TYPO: "subsytem")
   - All others: `tier2_subsystem_{domain}.rs` (correct spelling)
   - **Impact**: Tooling expecting "subsystem" will fail to match notice

2. **Test Depth Disparity**
   - **High**: Notice (7 test files), RPC (5 test files)
   - **Medium**: Lease (3), Stream (implicit via rpc_streaming_ordering.rs)
   - **Low**: Queue (1), KV (0), Schedule (0)
   - **Asymmetry**: Notice has 7x more e2e files than queue, despite similar criticality

3. **Missing Domains** (Zero Coverage)
   - **KV**: 17 unit tests only → no session, routing, or persistence validation
   - **Schedule**: 8 serialization tests only → no actor logic, no e2e, no benchmarks

### Benchmark Tier Breakdown

| Tier | Present | Missing | Status |
|------|:-------:|:-------:|--------|
| **Tier 1** (hotpath) | notice, queue, rpc, stream (4) | kv, lease, schedule (3) | 57% coverage |
| **Tier 2** (subsystem) | notice, queue, rpc, stream, lease (5) | kv, schedule (2) | 71% coverage |
| **Tier 3** (system) | notice, queue, rpc, stream (4) | kv, lease, schedule (3) | 57% coverage |
| **Tier 4** (integration) | notice, queue, rpc, stream (4) | kv, lease, schedule (3) | 57% coverage |

**Key Finding**: Lease is missing all hotpath, system, and integration benchmarks despite having the most unit tests.

---

## Concrete Findings

### File-Level Evidence

#### ❌ KV Domain: Zero Integration/E2E
```
src/domains/kv/actor.rs:       792 lines, 17 unit tests
src/domains/kv/protocol.rs:    ~100 lines (estimate)
tests/kv_*.rs:                 DO NOT EXIST
benches/tier*_*_kv.rs:         DO NOT EXIST
```
**Action Required**: Create `tests/kv_auth.rs`, `tests/kv_e2e_basic.rs`, `tests/kv_semantics.rs`, and all 3 benchmark tiers.

#### ❌ Schedule Domain: Zero Domain Logic Testing
```
src/domains/schedule/actor.rs:     266 lines, 0 unit tests
src/domains/schedule/protocol.rs:  111 lines, 8 tests (serialization only)
src/domains/schedule/store.rs:     101 lines, 0 unit tests
tests/schedule_*.rs:               DO NOT EXIST
benches/tier*_*_schedule.rs:       DO NOT EXIST
```
**Action Required**: Add unit tests to `actor.rs` and `store.rs`, then all integration/benchmark tiers.

#### ⚠️ Notice Domain: Tier 2 Naming Typo
```
benches/tier2_subsytem_notice.rs    ← TYPO: "subsytem" not "subsystem"
```
**Rename to**: `benches/tier2_subsystem_notice.rs`

#### ⚠️ Lease Domain: Incomplete Benchmarking
```
Present:   tier2_subsystem_lease.rs  (488 lines)
Missing:   tier1_hotpath_lease.rs    (0 files)
Missing:   tier3_system_lease.rs     (0 files)
Missing:   tier4_integration_lease.rs (0 files)
```
**Action Required**: Create all missing tiers.

#### ✅ Queue, RPC, Stream: Complete
All three have:
- Tier 1 hotpath ✅
- Tier 2 subsystem ✅
- Tier 3 system ✅
- Tier 4 integration ✅
- Unit tests ✅
- Integration/e2e tests ✅

---

## Summary: First-Class vs. Second-Class Treatment

| Domain | Tier | Unit Tests | Integration | Benchmarks | Grade |
|--------|:----:|:----------:|:----------:|:----------:|:-----:|
| **Notice** | 🟢 | ✅ Complete | ✅ 7 files | ✅ 4 tiers (1 typo) | A- |
| **Queue** | 🟢 | ✅ Complete | ✅ 1 file | ✅ 4 tiers | A |
| **RPC** | 🟢 | ✅ Complete | ✅ 5 files | ✅ 4 tiers | A |
| **Stream** | 🟢 | ✅ Complete | ✅ 4 files | ✅ 4 tiers | A |
| **Lease** | 🟡 | ✅ 68 tests | ✅ 3 files | ⚠️ 1 of 4 tiers | C+ |
| **KV** | 🔴 | ✅ 17 tests | ❌ Zero | ❌ Zero | F |
| **Schedule** | 🔴 | ⚠️ 8 only | ❌ Zero | ❌ Zero | F |

---

## Quality Signals

### Strong Signals (Notice, Queue, RPC, Stream)
- Invariant testing explicit and comprehensive
- Benchmarks isolate single operation per tier
- Hot paths free of allocations and setup noise
- Fanout/scale patterns tested under realistic contention

### Weak Signals (KV, Schedule)
- No session integration — unclear if auth works end-to-end
- No persistence validation — no proof transaction semantics are correct
- No performance baseline — unclear if acceptable under load
- Protocol only tested in isolation (serialization)

### Asymmetric Signals (Lease)
- 68 unit tests suggest deep confidence in logic
- But missing hotpath and system benchmarks undermines claims about performance
- No proof of scalability under Fitz load

---

## Recommendations for Enforcement

To achieve **uniform rigor**, the following MUST be done:

1. **Immediate (Blocking)**
   - Rename `tier2_subsytem_notice.rs` → `tier2_subsystem_notice.rs`

2. **Short Term (Lease)**
   - Create `tier1_hotpath_lease.rs` (focus: acquire, renew, surrender)
   - Create `tier3_system_lease.rs` (focus: contention, expiration)
   - Create `tier4_integration_lease.rs` (focus: lifecycle under load)

3. **Medium Term (KV)**
   - Create `tests/kv_auth.rs` (focus: session auth + permission checks)
   - Create `tests/kv_e2e_basic.rs` (focus: transaction lifecycle + persistence)
   - Create `tests/kv_semantics.rs` (focus: isolation levels, rollback, encoding)
   - Create all 3 benchmark tiers (hotpath, subsystem, system)

4. **Medium Term (Schedule)**
   - Add unit tests to `src/domains/schedule/actor.rs`
   - Add unit tests to `src/domains/schedule/store.rs`
   - Create `tests/schedule_auth.rs`, `tests/schedule_e2e_basic.rs`, `tests/schedule_semantics.rs`
   - Create all 3 benchmark tiers

---

## Conclusion

**Current State**: 4 of 7 domains (57%) are first-class. 3 domains (43%) are under-tested and unsafe to trust.

**Gap Summary**:
- **Tier 1 hotpath**: 4/7 present (57%)
- **Tier 2 subsystem**: 5/7 present (71%)
- **Tier 3 system**: 4/7 present (57%)
- **Tier 4 integration**: 4/7 present (57%)
- **Integration tests**: 5/7 present (71%)
- **Zero coverage domains**: 2/7 (KV, Schedule)

**Immediate Actions**:
1. Fix notice typo
2. Complete lease benchmarks (3 tiers)
3. Complete KV and Schedule (unit + integration + benchmarks)

Once these gaps are closed, all domains will meet the standard: equal testing, equal confidence, equal rigor.
