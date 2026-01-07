# Queue Benchmark Suite Documentation

## Overview

The Fitz Queue domain includes a comprehensive 3-tier benchmark suite designed to validate world-class performance targets and prove the actor-based implementation's efficiency.

**Performance Targets:**
- **Throughput**: 100k-300k msg/sec per queue (single-threaded actor)
- **Latency**: <5µs p50 enqueue, <10µs p50 reserve, <5µs p50 complete
- **Durability**: All operations persisted to Midge (LSM storage)
- **Stability**: No memory leaks, predictable performance under sustained load

## Benchmark Tiers

### Tier 1: Hot Path Microbenchmarks (`tier1_hotpath_queue.rs`)

**Purpose:** Measure PURE actor operation costs WITHOUT scheduler/mailbox overhead.

These benchmarks call QueueActor methods directly to isolate the hot path performance. This is the theoretical maximum throughput for a single queue.

**Benchmarks:**

1. **`bench_enqueue_only`**
   - Measures: Pure enqueue throughput (no reserve or complete)
   - Target: <5µs p50, 200k+ msg/sec
   - Validates: Midge write cost, VecDeque push, serialization overhead

2. **`bench_reserve_only_empty`**
   - Measures: Reserve on empty queue (fast path)
   - Target: <1µs p50 (empty check only)
   - Validates: Empty queue detection cost

3. **`bench_reserve_only_full`**
   - Measures: Reserve on pre-filled queue
   - Target: <10µs p50
   - Validates: VecDeque pop, HashMap insert, token generation

4. **`bench_complete_only`**
   - Measures: Message completion throughput
   - Target: <5µs p50
   - Validates: HashMap remove, token validation, Midge delete

5. **`bench_delayed_enqueue_fire`**
   - Measures: Delayed message processing (BinaryHeap → VecDeque)
   - Target: <10µs p50
   - Validates: BinaryHeap pop cost, delayed queue efficiency

6. **`bench_lease_expiry_requeue`**
   - Measures: Lease expiration and requeue cost
   - Target: <10µs p50
   - Validates: Inflight → ready transition, attempt increment, Midge update

7. **`bench_batch_reserve_scaling`**
   - Measures: Reserve scaling with batch_size 1, 10, 100
   - Target: Linear scaling
   - Validates: Bulk pop/insert efficiency

**How to Run:**
```bash
cargo bench --bench tier1_hotpath_queue
```

**Expected Output:**
```
hotpath_queue_enqueue/enqueue_no_delay
                        time:   [3.2µs 3.5µs 3.8µs]
                        thrpt:  [263k elem/s 286k elem/s 313k elem/s]

hotpath_queue_reserve/reserve_empty_queue
                        time:   [450ns 480ns 510ns]
                        thrpt:  [1.96M elem/s 2.08M elem/s 2.22M elem/s]

hotpath_queue_reserve/reserve_full_queue
                        time:   [8.5µs 9.2µs 10.1µs]
                        thrpt:  [99k elem/s 109k elem/s 118k elem/s]
```

---

### Tier 2: Subsystem Stress Benchmarks (`tier2_subsystem_queue.rs`)

**Purpose:** Measure CONTENTION and LOAD under realistic usage patterns.

These benchmarks stress the actor under sustained load scenarios (high throughput, batch processing, churn). They measure the actor's ability to handle production-level load without scheduler overhead.

**Benchmarks:**

1. **`bench_enqueue_reserve_complete_loop`**
   - Pattern: Full message lifecycle (enqueue → reserve → complete)
   - Target: 50k+ msg/sec sustained
   - Validates: Full cycle throughput, memory stability

2. **`bench_batch_reserve_stress`**
   - Pattern: Pre-fill queue, sustained batch reserves (sizes 1, 10, 100)
   - Target: Linear scaling with batch size
   - Validates: Bulk operations, VecDeque/HashMap efficiency

3. **`bench_lease_churn_stress`**
   - Pattern: Reserve with short leases, expiry, requeue
   - Target: 10k+ expirations/sec
   - Validates: Timer heap efficiency, requeue throughput

4. **`bench_delayed_message_stress`**
   - Pattern: Enqueue delayed messages, process firing
   - Target: 50k+ delayed msg/sec
   - Validates: BinaryHeap performance, delayed → ready transition

5. **`bench_dlq_threshold_stress`**
   - Pattern: Reserve → expire cycles until DLQ threshold
   - Target: Efficient DLQ detection and cleanup
   - Validates: Attempt tracking, DLQ policy overhead

6. **`bench_high_volume_enqueue`**
   - Pattern: 1000 continuous enqueues (no reserves)
   - Target: 100k+ msg/sec enqueue-only
   - Validates: Write throughput to Midge, VecDeque growth

**How to Run:**
```bash
cargo bench --bench tier2_subsystem_queue
```

**Expected Output:**
```
subsystem_queue_churn/enqueue_reserve_complete_cycle
                        time:   [15µs 18µs 22µs]
                        thrpt:  [45k elem/s 56k elem/s 67k elem/s]

subsystem_queue_batch_reserve/10
                        time:   [85µs 92µs 101µs]
                        thrpt:  [99k elem/s 109k elem/s 118k elem/s]

subsystem_queue_high_volume/enqueue_1000_messages
                        time:   [3.2ms 3.5ms 3.9ms]
                        thrpt:  [256k elem/s 286k elem/s 313k elem/s]
```

---

### Tier 3: System Pressure Benchmarks (`tier3_system_queue.rs`)

**Purpose:** Measure FULL SYSTEM throughput under realistic production scenarios.

These benchmarks simulate real-world usage patterns: sustained load, workload mixes, cold start recovery, and extreme contention. They prove the queue can handle production traffic.

**Benchmarks:**

1. **`bench_sustained_load_throughput`**
   - Pattern: Continuous enqueue + reserve for 1 second
   - Target: 50k+ msg/sec sustained (1 second runtime)
   - Validates: Steady-state throughput, no degradation over time

2. **`bench_mixed_workload_realistic`**
   - Pattern: 70% immediate, 20% delayed, 10% DLQ (realistic production mix)
   - Target: 50k+ msg/sec with heterogeneous workload
   - Validates: Coordinated data structures, no bottlenecks

3. **`bench_cold_start_recovery`**
   - Pattern: Pre-fill 1000 messages, drop actor, respawn, measure recovery
   - Target: <100ms recovery for 1000 messages
   - Validates: Midge load time, recovery throughput

4. **`bench_high_contention_scenario`**
   - Pattern: Queue oscillates between empty and full (worst case)
   - Target: 20k+ msg/sec under extreme contention
   - Validates: Performance under rapid state changes

**How to Run:**
```bash
cargo bench --bench tier3_system_queue
```

**Expected Output:**
```
system_queue_sustained/sustained_1sec_enqueue_reserve
                        time:   [1.02s 1.05s 1.08s]
                        thrpt:  [46k elem/s 48k elem/s 49k elem/s]

system_queue_mixed_workload/70_immediate_20_delayed_10_dlq
                        time:   [1.8ms 2.1ms 2.4ms]
                        thrpt:  [42k elem/s 48k elem/s 56k elem/s]

system_queue_cold_start/recover_1000_messages
                        time:   [45ms 52ms 61ms]
                        thrpt:  [16k elem/s 19k elem/s 22k elem/s]
```

---

## Benchmark Architecture

### Data Flow

```
Tier 1: QueueActor direct calls
  ↓
[handle_enqueue/reserve/complete]
  ↓
Hot path measurement (theory max)

Tier 2: Sustained load patterns
  ↓
[Actor under stress, no scheduler]
  ↓
Realistic throughput measurement

Tier 3: Production scenarios
  ↓
[Full system simulation]
  ↓
Real-world validation
```

### Performance Expectations

**Tier 1 (Hot Path):**
- Enqueue: 3-5µs, 200k-300k msg/sec
- Reserve (empty): 400-500ns, 2M+ msg/sec
- Reserve (full): 8-10µs, 100k-120k msg/sec
- Complete: 4-6µs, 170k-250k msg/sec

**Tier 2 (Stress):**
- Full cycle: 15-20µs, 50k-67k msg/sec
- Batch reserve (x10): 85-100µs, 100k-120k msg/sec
- High-volume enqueue (x1000): 3-4ms, 250k-330k msg/sec

**Tier 3 (System):**
- Sustained load: 50k msg/sec over 1 second
- Mixed workload: 42k-56k msg/sec (70/20/10 mix)
- Cold start recovery: 16k-22k msg/sec (52ms for 1000 messages)

---

## Running All Benchmarks

```bash
# Run all queue benchmarks
cargo bench --bench tier1_hotpath_queue --bench tier2_subsystem_queue --bench tier3_system_queue

# Run specific tier
cargo bench --bench tier1_hotpath_queue

# Run specific benchmark
cargo bench --bench tier1_hotpath_queue enqueue_only

# Generate HTML reports
cargo bench --bench tier1_hotpath_queue -- --save-baseline queue_baseline
```

---

## Interpreting Results

### Key Metrics

1. **Time (µs/ms)**: Latency per operation
   - Lower is better
   - Watch for p50, p95, p99 percentiles

2. **Throughput (elem/s)**: Operations per second
   - Higher is better
   - Should match 1/time calculation

3. **Stability**: Narrow confidence intervals
   - Small ranges indicate stable performance
   - Wide ranges suggest jitter or GC pauses

### Warning Signs

❌ **Performance Regressions:**
- Latency >2x target (e.g., enqueue >10µs)
- Throughput <50% target (e.g., <100k msg/sec)
- Wide confidence intervals (>30% variance)

❌ **Memory Issues:**
- Increasing memory usage over sustained runs
- Crash on recovery benchmarks
- OOM on high-volume benchmarks

✅ **Expected Behavior:**
- Consistent latencies across runs
- Linear scaling with batch size
- No degradation on sustained load
- Fast cold start recovery (<100ms)

---

## Benchmark Configuration

All benchmarks use shared configuration from `benches/config.rs`:
- **Warm-up**: 100ms
- **Measurement**: 500ms
- **Sample Size**: 10
- **Sampling Mode**: Flat (consistent measurements)

---

## CI Integration

Benchmarks should be run in CI to detect regressions:

```yaml
# .github/workflows/benchmarks.yml
- name: Run Queue Benchmarks
  run: |
    cargo bench --bench tier1_hotpath_queue -- --save-baseline main
    cargo bench --bench tier2_subsystem_queue -- --save-baseline main
    cargo bench --bench tier3_system_queue -- --save-baseline main
```

---

## Troubleshooting

### Slow Benchmarks

If benchmarks are running slower than expected:

1. **Check Midge Performance**: Ensure SSD, not HDD
2. **Disable Background Processes**: Close browsers, IDEs
3. **Use Release Build**: `cargo bench` (not `cargo test`)
4. **Check CPU Frequency**: Ensure not throttled

### Failed Benchmarks

If benchmarks fail or panic:

1. **Check Midge Errors**: Look for "Failed to persist" errors
2. **Check Disk Space**: Midge needs space for durability
3. **Check Permissions**: tempfile needs write access
4. **Check Logs**: Look for stderr output (DLQ messages)

---

## Future Improvements

Potential benchmark additions:

1. **Multi-threaded Benchmarks**: Spawn real Scheduler, measure mailbox overhead
2. **Notice Integration**: Measure notice emission performance
3. **Authorization Overhead**: Measure SessionActor wrapping cost
4. **Network Benchmarks**: Measure over WebSocket/HTTP
5. **Distributed Benchmarks**: Multi-node queue coordination

---

## Summary

The Queue benchmark suite provides comprehensive validation of the actor-based queue implementation:

- **Tier 1**: Proves theoretical maximum throughput (hot path)
- **Tier 2**: Validates sustained load handling (stress patterns)
- **Tier 3**: Confirms production readiness (realistic scenarios)

**Expected Results:** 100k-300k msg/sec throughput, <10µs latencies, stable under sustained load, fast recovery.
