# Fitz Stream Domain Benchmarks

## Overview

Comprehensive benchmark suite for the Fitz stream domain, designed to validate invariants, measure hot-path performance, and stress test multi-actor coordination.

**Structure:**
- **Tier 1**: Hot-path microbenchmarks (single-actor, minimal coordination)
- **Tier 2**: Subsystem coordination benchmarks (multi-actor, real leasing/watermarks)

---

## Quick Start

### Run All Stream Benchmarks
```bash
cargo bench --bench 'tier*_stream'
```

### Run Tier 1 Only (Hot-Path)
```bash
cargo bench --bench tier1_hotpath_stream
```

### Run Tier 2 Only (Coordination)
```bash
cargo bench --bench tier2_subsystem_stream
```

### Run Specific Benchmark
```bash
cargo bench --bench tier1_hotpath_stream -- stream_append_single_event
```

### Establish Baseline for Regression Detection
```bash
cargo bench --bench tier1_hotpath_stream -- --save-baseline stream_baseline
cargo bench --bench tier2_subsystem_stream -- --save-baseline stream_baseline
```

### Compare Against Baseline
```bash
cargo bench --bench tier1_hotpath_stream -- --baseline stream_baseline
```

---

## Benchmark Overview

### Tier 1: Hot-Path Microbenchmarks (7 benchmarks)

Measure raw throughput and latency of tightest loops with **zero coordination overhead**.

| # | Benchmark | Measures | Invariant |
|---|---|---|---|
| 1 | **stream_append_single_event** | Single event append latency | Offset increments by 1 |
| 2 | **stream_append_batches** | Batch amortization (5, 10, 50) | Cost/event decreases with batch size |
| 3 | **stream_append_large_batch** | Large batch (500, 1000) | Throughput stable at scale |
| 4 | **stream_session_append** | Streaming append pattern | No unbounded buffering |
| 5 | **resource_read_sequential** | Read scan throughput | Linear scan with offset count |
| 6 | **area_index_scan** | Area index scan cost | O(n) scaling with size |
| 7 | **realm_index_scan** | Realm index scan cost | O(n) scaling with size |

**Characteristics:**
- Single StreamActor (no coordination)
- All data precomputed outside hot path
- No allocations in measured loop
- Expected execution time: ~1.5 seconds

---

### Tier 2: Subsystem Coordination Benchmarks (7 benchmarks)

Stress **multi-actor coordination** (leasing, watermarks, merging) without becoming full system tests.

| # | Benchmark | Measures | Invariant |
|---|---|---|---|
| 8 | **append_with_lease_renewal** | Lease renewal overhead | Renewal ≤ 2× baseline latency |
| 9 | **concurrent_resource_writes** | Parallel writes (2, 4, 8 actors) | Scaling is linear (no quadratic contention) |
| 10 | **area_watermark_advancement** | Watermark with out-of-order commits | Advances only on contiguity |
| 11 | **realm_watermark_advancement** | Realm watermark with uneven areas | realm_wm = min(area_wm) |
| 12 | **area_read_k_way_merge** | K-way merge efficiency (K=2,4,8,16) | Cost ≤ O(K) |
| 13 | **realm_read_k_way_merge** | Realm merge (4 areas × 2 resources) | Multi-level merge bounded |
| 14 | **streaming_ingest_10k** | Sustained throughput (10k events) | Throughput stable, no degradation |

**Characteristics:**
- Multiple StreamActors (2-16)
- Real lease renewal, watermark tracking
- Still single-node, deterministic
- Expected execution time: ~2.5-3 seconds

---

## Design Validation

### Q: Do Tier 1 benches isolate the absolute hot paths?
**✅ YES**
- Single actor, zero coordination overhead
- No lease renewal, no watermark logic, no auth
- Pure append/read/scan operations

### Q: Do Tier 2 benches stress coordination without becoming full system tests?
**✅ YES**
- Real coordination (leasing, watermarks, merging)
- Multiple actors stress multi-actor paths
- No external systems (no replication, no RPC)
- Single-node, deterministic, fully controlled

---

## Metrics Captured

### All Benchmarks
- **Throughput**: ops/sec, elements/sec
- **Latency**: ns/op (Tier 1) or µs/op (Tier 2)
- **Distribution**: mean, p95, p99 (via Criterion)

### Tier 2 Additional
- **Scaling**: cost vs actor count, K, area count
- **Stability**: sustained throughput (no degradation)
- **Overhead**: coordination cost vs baseline

---

## Anti-Patterns: Avoided ✅

| Anti-Pattern | Status | Reason |
|---|---|---|
| Only test correctness | ✅ Avoided | Measure latency, throughput, scaling |
| Hide tail latency | ✅ Avoided | Criterion captures p95, p99 |
| Allocate unbounded memory | ✅ Avoided | All data precomputed, bounded |
| Combine unrelated subsystems | ✅ Avoided | Each bench = 1 operation or 1 axis |

---

## Code Quality

- ✅ All data precomputed outside `b.iter()`
- ✅ No allocations in measured loop
- ✅ Uses `black_box()` for variable inputs
- ✅ Uses `SamplingMode::Flat` for consistency
- ✅ Proper criterion_group/criterion_main structure
- ✅ Uses shared `config::criterion_config()`
- ✅ Fast execution (<1.5s Tier 1, <3s Tier 2)

---

## Expected Performance

### Tier 1 (Single-Actor, No Coordination)
- **single_event_append**: 100-500 ns
- **batch_5**: 60-300 ns/event (amortized)
- **batch_50**: 40-200 ns/event (amortized)
- **large_batch_1000**: 35-150 ns/event (amortized)
- **session_append**: 100-500 ns
- **resource_read**: 100-400 ns
- **area_index_scan**: 20-100 ns/entry (memory scan)

### Tier 2 (Multi-Actor, Coordination)
- **lease_renewal**: 500-1000 ns (vs append baseline)
- **concurrent_4_resources**: 0.4-2.0 µs/round
- **area_watermark**: 0.5-2.0 µs
- **realm_watermark**: 1.0-5.0 µs
- **area_merge_8_way**: 0.8-4.0 µs
- **realm_merge**: 1.5-6.0 µs
- **sustained_ingest**: 2.0-10.0 µs/chunk

*These are estimates; actual baselines established on first run.*

---

## Documentation

- **[STREAM_BENCHMARK_DESIGN.md](STREAM_BENCHMARK_DESIGN.md)** — Detailed specification for each benchmark
- **[STREAM_BENCHMARK_VALIDATION.md](STREAM_BENCHMARK_VALIDATION.md)** — Design validation and invariant coverage
- **[STREAM_BENCHMARK_SUMMARY.md](STREAM_BENCHMARK_SUMMARY.md)** — Implementation summary

---

## File Structure

```
benches/
├── tier1_hotpath_stream.rs       ← 7 hot-path benchmarks
├── tier2_subsystem_stream.rs     ← 7 subsystem benchmarks
└── config.rs                     ← Shared Criterion configuration

docs/
├── STREAM_BENCHMARK_DESIGN.md    ← Detailed design specification
├── STREAM_BENCHMARK_VALIDATION.md ← Design validation report
└── STREAM_BENCHMARK_SUMMARY.md   ← Implementation summary
```

---

## Regression Detection

### Setup Baseline
```bash
cargo bench --bench 'tier*_stream' -- --save-baseline stream_v1.0
```

### Check for Regressions
```bash
cargo bench --bench 'tier*_stream' -- --baseline stream_v1.0
```

Criterion will report:
- ✅ **Improvements** (>5% faster)
- ⚠️ **Regressions** (>5% slower)
- 📊 **Unchanged** (within noise)

**CI Policy:**
- Regressions >10% require manual review
- Regressions >20% fail CI
- Improvements >10% documented as notable

---

## Next Steps

1. **Run Tier 1 benchmarks** and establish baseline
2. **Run Tier 2 benchmarks** and establish baseline
3. **Store baselines** in CI environment
4. **Integrate into CI** to detect regressions
5. **Monitor performance** over time

---

## References

- [Stream Domain Architecture](./STREAM_LEASE_ARCHITECTURE.md)
- [Benchmark Guidelines](./dev/bench_guidelines.md)
- [Criterion Documentation](https://docs.rs/criterion/)
- [Pebble Benchmarks](https://github.com/cockroachdb/pebble/tree/master/internal/benchmarks)

---

**Status:** ✅ Complete and ready for baseline establishment.
