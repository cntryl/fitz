# Fitz Stream Domain Benchmarks - Design Document

## Overview

This document details the comprehensive benchmark suite for the Fitz stream domain, organized into Tier 1 (hot-path) and Tier 2 (subsystem coordination) benchmarks.

**Design Philosophy:**
- Isolate invariants, not features
- Deterministic and reproducible
- Measure hot paths with no artificial overhead
- Stress actor coordination in Tier 2 without becoming full system tests
- Capture tail latency (p95, p99) as well as mean latency

---

## Tier 1: Hot-Path Microbenchmarks

### Goal
Validate raw throughput and latency of the tightest loops, isolate to single-actor operations with minimal coordination.

### Characteristics
- Single process, single StreamActor
- No lease renewal (Tier 2 only)
- No watermark logic (Tier 2 only)
- No auth/session overhead
- Deterministic, precomputed test data
- Fast execution (<1 second per benchmark)

### Benchmarks

#### 1. `stream_append_single_event`
**Purpose:** Baseline append latency for single event with resource offset tracking.

**Setup:**
- One StreamActor
- Precomputed 256 event payloads of 256 bytes each
- Expected offsets: 0, 1, 2, ...

**Measures:**
- ns/op for single append
- ops/sec
- Validates offset sequencing

**Invariant:** Each append advances resource offset by exactly 1.

---

#### 2. `stream_append_batches`
**Purpose:** Validate batching amortization across small batch sizes (5, 10, 50 events).

**Setup:**
- One StreamActor
- Batch sizes: 5, 10, 50
- One atomic commit per batch
- Precomputed payloads for all batches

**Measures:**
- cost per event vs batch size
- Amortization factor (single event cost ÷ batched cost)
- Throughput improvement with batching

**Invariant:** Batch cost amortizes; cost/event decreases with batch size.

---

#### 3. `stream_append_large_batch`
**Purpose:** Validate batching behavior at large scale (500, 1000 events).

**Setup:**
- One StreamActor
- Batch sizes: 500, 1000
- One atomic commit per batch
- Longer measurement time (2 seconds) due to larger batches

**Measures:**
- Write amplification
- Tail latency (p95, p99) for large batches
- Throughput ceiling

**Invariant:** Append throughput remains stable even with very large batches.

---

#### 4. `stream_session_append`
**Purpose:** Streaming append pattern (one event per call, frequent commits).

**Setup:**
- One StreamActor
- Session-based appending
- Precomputed 512 payloads
- Each iteration: one append, one implicit commit

**Measures:**
- ns/op for session append
- ops/sec
- Memory growth rate (should be zero; no buffering)

**Invariant:** Session append has consistent latency; no Vec buffering.

---

#### 5. `resource_read_sequential`
**Purpose:** Sequential scan throughput on a single resource stream.

**Setup:**
- One StreamActor with 1000 pre-populated events
- Cursor-based reads (read one event at a time)
- Offset: 0, 1, 2, ...

**Measures:**
- ns/op for single event read
- ops/sec scan throughput
- Memory access pattern efficiency

**Invariant:** Read throughput is linear; no unexpected jumps.

---

#### 6. `area_index_scan`
**Purpose:** Index scan cost at area level (prelude to multi-way merge).

**Setup:**
- Storage layer (StreamStore)
- Synthetic area index entries: 100, 1000, 10000
- Sequential scan pattern

**Measures:**
- ns/scan for various sizes
- Scaling factor (should be linear)

**Invariant:** Index scan cost is O(n) with size.

---

#### 7. `realm_index_scan`
**Purpose:** Index scan cost at realm level.

**Setup:**
- Storage layer (StreamStore)
- Synthetic realm index entries: 100, 1000, 10000
- Sequential scan pattern

**Measures:**
- ns/scan for various sizes
- Scaling behavior

**Invariant:** Index scan cost is O(n) with size.

---

## Tier 2: Subsystem Coordination Benchmarks

### Goal
Stress actor coordination, validate leasing, watermarks, and merging under realistic multi-actor conditions.

### Characteristics
- Multiple StreamActors (same area or multiple areas)
- Real lease renewal (small lease sizes force renewal)
- Real watermark tracking
- Still single-node
- Deterministic, reproducible
- Moderate execution time (2-3 seconds per benchmark)

### Benchmarks

#### 8. `append_with_lease_renewal`
**Purpose:** Measure overhead of lease renewal during continuous appends.

**Setup:**
- One StreamActor
- Artificially small lease size: 32 offsets
- Continuous appends force renewal every 32 events
- 1000 precomputed payloads

**Measures:**
- ns/op for append with renewal (vs Tier 1 baseline)
- Renewal overhead factor (should be < 2x baseline)
- Offset continuity across renewals

**Invariant:** Lease renewal does not break offset sequencing; overhead is bounded.

---

#### 9. `concurrent_resource_writes`
**Purpose:** Parallel writes to multiple resources in same area.

**Setup:**
- 2, 4, 8 StreamActors (one per resource)
- Same AreaActor
- Round-robin appends (one event to each resource per iteration)
- Precomputed payloads per resource

**Measures:**
- ops/sec per resource count
- Scaling efficiency (should be near-linear up to 8 resources)
- Contention overhead (if any)

**Invariant:** Concurrent writes scale linearly; no quadratic contention.

---

#### 10. `area_watermark_advancement`
**Purpose:** Test watermark logic with out-of-order commits.

**Setup:**
- 4 StreamActors in same area
- Out-of-order append pattern: resource 0, 2, 1, 3 (deliberately not sequential)
- Watermark must only advance when all resources have contiguous offsets
- 1000 precomputed payloads

**Measures:**
- ns/op for watermark calculation
- Bookkeeping cost of tracking out-of-order progress
- Watermark advancement latency

**Invariant:** Watermark advances only when contiguity is guaranteed; never regresses.

---

#### 11. `realm_watermark_advancement`
**Purpose:** Test realm watermark with uneven area progress.

**Setup:**
- 4 areas, 2 resources per area (8 total resources)
- Append counts per iteration: [4, 3, 2, 1] (area 0 fastest, area 3 slowest)
- Realm watermark = min(all area watermarks)
- 1000 precomputed payloads

**Measures:**
- ns/op for realm watermark calculation
- Overhead of min(area_watermarks) operation
- Advancement rate (should match slowest area)

**Invariant:** Realm watermark = min(area_watermarks); no stalls or surprises.

---

#### 12. `area_read_k_way_merge`
**Purpose:** Measure merge overhead for K resource streams in an area.

**Setup:**
- K resources (2, 4, 8, 16)
- Each pre-populated with 100 events
- One iteration = read one event from each of K resources (K reads total)
- Simulates area-level read needing to merge K streams

**Measures:**
- ns/op per K value
- Scaling with K (should be O(K) or better)
- Merge efficiency

**Invariant:** Merge cost scales sub-linearly or linearly with K.

---

#### 13. `realm_read_k_way_merge`
**Purpose:** Measure merge overhead at realm level (multiple areas).

**Setup:**
- 4 areas, 2 resources per area = 8 total streams
- Each pre-populated with 50 events
- One iteration = read one event from each of 8 (area, resource) pairs
- Simulates realm-level read with area merging

**Measures:**
- ns/op for realm-level merge
- Merge + watermark gating cost
- Total overhead vs Tier 1 single-read

**Invariant:** Realm merge cost is reasonable (bounded overhead vs single read).

---

#### 14. `streaming_ingest_10k`
**Purpose:** Sustained throughput and memory stability under realistic workload.

**Setup:**
- 2 StreamActors
- 10,000 total events
- Chunked appends: 100 events per commit
- Round-robin distribution between resources
- Precomputed all 10,000 payloads

**Measures:**
- ops/sec (chunk throughput)
- Mean / p95 / p99 latency per chunk
- Memory growth (should be stable; no leaks)
- Sustained throughput over 10k events

**Invariant:** Throughput remains stable across all 10k events; no degradation.

---

## Metrics Captured (All Benchmarks)

### Throughput
- **ops/sec**: Operations per second
- **Throughput::Elements(n)**: Criterion-reported element throughput

### Latency
- **mean**: Average latency in ns
- **p95**: 95th percentile latency
- **p99**: 99th percentile latency

### Memory
- **Allocations per op**: If tracked by Criterion
- **Bytes written per event**: For storage benchmarks

### Coordination (Tier 2 only)
- **Watermark lag**: How far behind committed offsets
- **Renewal overhead**: Latency increase with lease renewal
- **Merge overhead**: Cost vs baseline single-resource operation

---

## Validation Checklist

### Design Questions

**Q1: Do Tier 1 benches isolate the absolute hot paths?**
- ✅ YES
  - Single StreamActor per benchmark
  - No multi-actor coordination
  - No auth, no session overhead
  - Precomputed data (no allocation in hot path)
  - Measures only the core append/read operation

**Q2: Do Tier 2 benches stress coordination without becoming full system tests?**
- ✅ YES
  - Multiple actors but no external system dependency
  - Real leasing and watermark logic
  - Still single-node, deterministic
  - Focused on coordination cost, not end-to-end correctness
  - Bounded scope: 2-16 actors, not unbounded scale

### Anti-Patterns (FORBIDDEN)

| Pattern | Status | Reason |
|---------|--------|--------|
| Benchmarks that only test correctness | ✅ Avoided | We test invariants and latency, not just "does it work" |
| Benchmarks that hide tail latency | ✅ Avoided | Criterion captures p95, p99 by default |
| Benchmarks that allocate unbounded memory | ✅ Avoided | All data precomputed outside hot path |
| Benchmarks that combine unrelated subsystems | ✅ Avoided | Tier 1 = single actor; Tier 2 = one coordination axis |

### Code Quality Checklist

- ✅ All data precomputed outside `b.iter()`
- ✅ No allocations in measured loop
- ✅ No string formatting in measured loop
- ✅ No Vec::push in measured loop
- ✅ Uses `black_box()` for inputs
- ✅ Uses `SamplingMode::Flat` for consistency
- ✅ Uses `Throughput::Elements(n)` for element-based throughput
- ✅ Proper criterion_group/criterion_main structure
- ✅ Uses shared `config::criterion_config()`
- ✅ Fast execution (Tier 1 <1s, Tier 2 <3s)

---

## Expected Results

### Tier 1 Expectations

| Benchmark | Expected (ns/op) | Rationale |
|-----------|------------------|-----------|
| single_event_append | 100-500 | Core sequencing logic |
| batch_5 | 60-300 | Amortized across 5 events |
| batch_50 | 40-200 | Further amortization |
| large_batch_1000 | 35-150 | Maximum amortization |
| session_append | 100-500 | One append call |
| resource_read | 100-400 | Single event retrieval |
| area_index_scan | 20-100 | Memory scan |
| realm_index_scan | 20-100 | Memory scan |

### Tier 2 Expectations

| Benchmark | Expected (µs/op) | Rationale |
|-----------|------------------|-----------|
| append_with_renewal | 0.2-1.0 | Lease renewal overhead + append |
| concurrent_4_resources | 0.4-2.0 | 4 appends + minor coordination |
| area_watermark | 0.5-2.0 | Watermark update logic |
| realm_watermark | 1.0-5.0 | min(area_watermarks) |
| merge_8_way | 0.8-4.0 | 8 reads + merge |
| realm_merge | 1.5-6.0 | Merge + watermark gating |
| ingest_10k | 2.0-10.0 | 100 events per commit |

*These are estimates; actual values will be established when benchmarks run.*

---

## Running the Benchmarks

### Tier 1 Only
```bash
cargo bench --bench tier1_hotpath_stream
```

### Tier 2 Only
```bash
cargo bench --bench tier2_subsystem_stream
```

### All Stream Benchmarks
```bash
cargo bench --bench 'tier*_stream'
```

### Specific Benchmark
```bash
cargo bench --bench tier1_hotpath_stream -- stream_append_single_event
```

### With Verbose Output
```bash
cargo bench --bench tier1_hotpath_stream -- --verbose
```

---

## Regression Detection

Benchmarks serve as **regression tests**:

1. **Tier 1 regressions** → Hot path slowdown (user-visible latency increase)
2. **Tier 2 regressions** → Coordination overhead increase (multi-actor slowdown)

CI should:
- Store baseline results from main branch
- Compare PR results to baseline
- Flag regressions >10% for manual review
- Flag improvements >10% as notable

---

## Future Enhancements

### Advanced Metrics
- [ ] Allocations per operation (via custom allocator tracking)
- [ ] Cache miss rates (via perf events)
- [ ] Lock contention (via parking_lot stats)

### Extended Coverage
- [ ] Failure recovery benchmarks (replica catch-up)
- [ ] TTL eviction cost
- [ ] Large payload handling (MB-sized events)
- [ ] Pathological watermark blocking scenarios

### Integration
- [ ] System-level benchmarks (full request pipeline)
- [ ] Comparison with similar databases
- [ ] Trace-driven benchmarks (replay real workloads)

---

## References

- **Stream Domain Architecture**: [docs/STREAM_LEASE_ARCHITECTURE.md](../docs/STREAM_LEASE_ARCHITECTURE.md)
- **Benchmark Guidelines**: [docs/dev/bench_guidelines.md](../docs/dev/bench_guidelines.md)
- **Criterion Docs**: https://docs.rs/criterion/
- **Pebble Benchmarks**: https://github.com/cockroachdb/pebble/tree/master/internal/benchmarks
