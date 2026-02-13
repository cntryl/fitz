# Integration Benchmarks

This directory contains integration benchmarks for the Fitz Go client library. These benchmarks test realistic workflows and operation throughput under sustained concurrent load with a real Fitz broker.

## Overview

Integration benchmarks differ from unit/subsystem benchmarks by:
- **Real broker**: Tests against an actual Fitz server, not mocks
- **Two benchmark types**:
  - **Workflow benchmarks**: Measure multi-operation sequences (e.g., Begin→Get→Put→Commit)
  - **Throughput benchmarks**: Measure single-operation performance (e.g., Put within existing transaction)
- **Latency percentiles**: Measures p50, p95, p99, p99.9 latencies, not just throughput
- **Concurrent load**: Varies client count and measures scaling/contention effects
- **Payload size variation**: Tests memory impact and bandwidth efficiency

## Benchmark Methodology

### Understanding Network Round Trips (RTTs)

Performance varies dramatically based on how many network round trips each operation requires:

- **Notice Publish**: 0 RTTs (fire-and-forget, no response wait) → 30,000+ ops/sec
- **Queue Send**: 1 RTT (send + wait for ack) → 1000-2000 ops/sec expected
- **KV Put (in transaction)**: 1 RTT → 1000-2000 ops/sec expected
- **KV Put Workflow**: 3 RTTs (Begin + Put + Commit) → 100-500 ops/sec expected
- **KV Transaction Workflow**: 4 RTTs (Begin + Get + Put + Commit) → 100-500 ops/sec expected

**Critical**: Do not compare fire-and-forget operations (Notice) to request-response operations (KV, Queue, Lease). They measure fundamentally different things.

### Workflow vs Throughput Benchmarks

**Workflow Benchmarks** (e.g., `BenchmarkKvTransactionWorkflow`):
- Measure end-to-end operation sequences
- Include transaction management overhead
- Test realistic usage patterns
- Useful for: capacity planning, SLA validation, workflow optimization

**Throughput Benchmarks** (e.g., `BenchmarkKvPutInTransaction`):
- Measure single operations within existing context
- Minimize overhead to isolate operation performance
- Match server tier 1 benchmark methodology
- Useful for: performance regression testing, server comparison, optimization validation

## Prerequisites

**Running Fitz Broker**

Integration benchmarks require a real Fitz broker running locally. Ensure it's started before running benchmarks:

```bash
# TCP on localhost:4091
./fitz server --listen 127.0.0.1:4091

# WebSocket on localhost:4090 (in another terminal)
./fitz server --listen-ws 127.0.0.1:4090/ws
```

If the broker is not available, benchmarks will fail on the first connection attempt.

## Running Benchmarks

### All Integration Benchmarks
```bash
cd clients/fitz-go
go test ./benches/integration -bench=. -benchtime=30s -benchmem -v
```

### Specific Domain
```bash
# KV only
go test ./benches/integration -bench=BenchmarkKv -benchtime=30s -benchmem -v

# Notice (pub/sub)
go test ./benches/integration -bench=BenchmarkNotice -benchtime=30s -benchmem -v

# Queue
go test ./benches/integration -bench=BenchmarkQueue -benchtime=30s -benchmem -v

# Lease
go test ./benches/integration -bench=BenchmarkLease -benchtime=30s -benchmem -v
```

### Single Scenario
```bash
# Only KV transaction benchmark
go test ./benches/integration -bench=BenchmarkKvTransaction -benchtime=30s -benchmem -v

# Only queue enqueue/receive
go test ./benches/integration -bench=BenchmarkQueueEnqueueReserve -benchtime=30s -benchmem -v
```

## Benchmarks

### KV Domain

#### BenchmarkKvTransactionWorkflow
Tests complete transaction workflows: Begin → Get → Put → Commit (4 network round trips)

**What it measures**: End-to-end transaction latency including transaction management overhead

**Scenarios:**
- 1 client, 10B payload: baseline, minimal contention
- 10 clients, 1KB payload: moderate concurrency
- 50 clients, 10KB payload: high contention, large payloads

**Expected Performance:**
- 100-500 ops/sec (at 4 RTTs × 500µs = 2-4ms per workflow)
- P99 latency: 2-10ms depending on concurrency

**Measurements:**
- Throughput (ops/sec): complete transactions per second
- Latency (p50/p95/p99/p99.9): time to complete transaction cycle
- Scaling: how throughput changes with client count

#### BenchmarkKvPutWorkflow
Tests PUT workflow: Begin → Put → Commit (3 network round trips)

**What it measures**: Single-value write latency with transaction management

**Scenarios:**
- 1/10/50 clients with 10B/1KB/10KB payloads

**Expected Performance:**
- 100-500 ops/sec (at 3 RTTs)
- P99 latency: 1.5-8ms

**Measurements:**
- Throughput: PUT workflows per second
- Latency distribution: impact of payload size
- Scaling: PUT performance under concurrency

#### BenchmarkKvPutInTransaction
Tests raw PUT operation throughput within existing transaction (1 network round trip)

**What it measures**: Pure Put operation performance, no transaction overhead

**Scenarios:**
- 1/10/50 clients with 10B/1KB/10KB payloads

**Expected Performance:**
- 1000-2000 ops/sec (at 1 RTT × 500µs = 500µs-1ms per op)
- P99 latency: 500µs-2ms

**Measurements:**
- Throughput: individual put operations per second within pre-created transaction
- Latency distribution: impact of payload size
- Scaling: concurrent Put performance

**Note**: This matches Fitz server tier 1 benchmark methodology for apples-to-apples comparison.

#### BenchmarkKvGetInTransaction
Tests raw GET operation throughput within existing transaction (1 network round trip)

**What it measures**: Pure Get operation performance, no transaction overhead

**Scenarios:**
- 1/10/50 clients with 10B/1KB/10KB payloads

**Expected Performance:**
- 1000-2000 ops/sec (at 1 RTT)
- P99 latency: 500µs-2ms

**Measurements:**
- Throughput: individual get operations per second within pre-created transaction
- Latency distribution: impact of value size
- Scaling: concurrent Get performance

### Notice Domain (Pub/Sub)

#### BenchmarkNoticePublish
Tests fire-and-forget publish operations (0 network round trips - no response wait)

**What it measures**: Encode and send latency only, not delivery confirmation

**Scenarios:**
- 1/10 clients with 10B/1KB payloads

**Expected Performance:**
- 30,000+ ops/sec (fire-and-forget send, ~10µs encode/send time)
- P99 latency: <1ms

**Measurements:**
- Throughput: fire-and-forget sends per second
- Latency: encode + TCP send time (does NOT include server processing or delivery)

**Critical**: This benchmark is NOT comparable to request-response operations. Notice uses `SendOneWay` which returns immediately without waiting for server acknowledgment. This is why it's 1000× faster than KV/Queue/Lease operations.

**Measurements:**
- Throughput: messages published and consumed per second
- Latency: time from publish to subscriber receiving
- Fanout scaling: how performance degrades with subscriber count

#### BenchmarkNoticePublish
Tests individual Publish operations

**Scenarios:**
- 1/10/50 clients with 10B/1KB/10KB payloads

**Measurements:**
- Throughput: publishes per second
- Latency: impact of payload size
- Scaling: publish performance under concurrent publishers

### Queue Domain

#### BenchmarkQueueEnqueueReserve
Tests producer-consumer queue operations

**Scenarios:**
- 10 producers, 10 consumers, 100B messages: balanced load
- 5 producers, 25 consumers, 1KB messages: more consumers
- 1 producer, 50 consumers, 10KB messages: extreme fanout

**Measurements:**
- Throughput: items enqueued and consumed per second
- Latency: enqueue and reserve latency
- Scaling: how throughput scales with consumer count

#### BenchmarkQueueSend
### Queue Domain

#### BenchmarkQueueEnqueue
Tests FIFO queue Send operations (1 network round trip)

**What it measures**: Queue send throughput including server acknowledgment

**Scenarios:**
- 1/10 clients with 10B/1KB payloads

**Expected Performance:**
- 1000-2000 ops/sec (at 1 RTT × 500µs = 500µs-1ms per op)
- P99 latency: 500µs-2ms

**Current Performance:**
- 10-98 ops/sec (observed)
- **Issue**: Server-side bottleneck identified, requires profiling (likely disk I/O or lock contention)

**Measurements:**
- Throughput: enqueues per second
- Latency: send + ack time
- Scaling: enqueue performance under concurrency

### Lease Domain

#### BenchmarkLeaseAcquireContention
Tests lease acquisition under intentional contention (1 RTT + contention wait)

**What it measures**: Serialized lock acquisition when multiple clients compete for same lease

**Scenarios:**
- 1 client: baseline (no contention)
- 10 clients: high contention (10 clients compete for 1 lease)
- 50 clients: extreme contention (50 clients compete for 5 leases)

**Expected Performance:**
- 10-100 ops/sec (serialized acquisition with contention waits)
- High error rate expected (clients timeout waiting for lease)
- P99 latency: 100ms+ (includes waiting for other clients to release)

**Measurements:**
- Throughput: successful acquisitions per second
- Error rate: failed acquisitions due to contention
- Latency: time to acquire contested lease

**Note**: This benchmark intentionally creates contention to test lock behavior. For throughput testing, use `BenchmarkLeaseAcquireThroughput`.

#### BenchmarkLeaseAcquireThroughput
Tests lease acquisition throughput without contention (1 network round trip)

**What it measures**: Pure lease acquisition performance when each client acquires unique leases

**Scenarios:**
- 1/10/50 clients acquiring unique leases

**Expected Performance:**
- 1000-2000 ops/sec (at 1 RTT)
- P99 latency: 500µs-2ms
- Low error rate (no contention)

**Measurements:**
- Throughput: acquisitions per second
- Latency: time to acquire uncontested lease
- Scaling: concurrent acquisition performance

**Note**: This matches Fitz server tier 1 benchmark methodology for apples-to-apples comparison.

## Interpreting Results

### Expected Output Format

```
BenchmarkKvTransaction/KvTransaction_clients=1_payload=10B-12
KvTransaction (clients=1, payloadSize=10B): 31457 ops in 30.000s (1048.57 ops/sec)
  Latency: p50=969µs p95=1.2ms p99=1.5ms p999=2.3ms (min=512µs, max=5.2ms, mean=953µs)
PASS
```

### Key Metrics

**Throughput (ops/sec)**
- Higher is better
- Baseline: Single client should achieve 100-1000s of ops/sec depending on operation complexity
- Scaling: Throughput should increase with client count (up to CPU count)

**P50 Latency (Median)**
- Typical response time for most operations
- Should be 1-10ms for network operations
- KV: 500µs-2ms (fast operations)
- Notice: 200µs-1ms (optimized pub/sub)
- Queue: 1-5ms (includes broker queuing)

**P95 Latency**
- 95th percentile (outlier bound)
- Should be <2x P50 under normal load
- Indicates tail latency behavior

**P99 Latency**
- Worst-case latency for 99% of operations
- Should be <5x P50
- Indicates presence of hotspots/contention

**P99.9 Latency**
- Extreme outliers (0.1% of requests)
- Used to detect GC pauses, scheduler delays

### Performance Targets

**KV Transactions**
- Single client: 500+ ops/sec (2ms per transaction)
- 10 clients: 2000+ ops/sec (10x scaling)
- 100 clients: 10000+ ops/sec (expected: ~5-10x scaling from contention)

**Notice (Pub/Sub)**
- Single pub/sub: 10000+ publishes/sec (100µs latency)
- 10 subs: 5000+ ops/sec (subscribers add overhead)
- 100 subs: 1000+ ops/sec (fanout overhead)

**Queue**
- Single producer: 1000+ enqueues/sec
- 10 producers, 10 consumers: 5000+ ops/sec
- High consumer ratio: throughput limited by RPC fanout

**Lease**
- Acquire (no contention): 5000+ ops/sec
- Acquire (high contention): 500+ ops/sec (lock wait)
- Renew: 10000+ ops/sec (fast path)

## Regression Detection

To detect performance regressions:

1. **Establish baseline** with a known-good version:
   ```bash
   go test ./benches/integration -bench=. -benchtime=30s > baseline.txt
   ```

2. **Compare after changes**:
   ```bash
   go test ./benches/integration -bench=. -benchtime=30s > current.txt
   ```

3. **Run diff/comparison**:
   ```bash
   # Manual inspection for outliers
   diff baseline.txt current.txt
   
   # Rule of thumb: >10% change in throughput or >20% change in p99 latency is significant
   ```

## Troubleshooting

### "connection refused" or "connection reset"

Broker is not running. Start Fitz broker:
```bash
./fitz server --listen 127.0.0.1:4091
```

### Very high latencies (>100ms)

Possible causes:
- Broker disk I/O contention (check via iostat/perfmon)
- Network congestion
- CPU throttling
- Excessive GC (check via `GODEBUG=gctrace=1`)

Solution:
- Reduce payload sizes to isolate network vs disk issues
- Reduce concurrent client count to reduce load
- Profile with pprof: `go test -cpuprofile=cpu.prof -memprofile=mem.prof ./benches/integration`

### Intermittent errors ("lease held", "not found", etc.)

Expected behavior with high contention scenarios. Lease contention benchmarks intentionally compete for exclusive leases.

Solutions:
- Increase number of unique resources (e.g., 10 leases instead of 1)
- Reduce client count
- Check error count in results (ErrorCount)

## Extending Benchmarks

To add a new benchmark:

1. **Create benchmark file** (e.g., `rpc_bench.go`)
2. **Use integration harness pattern**:
   ```go
   func BenchmarkRpcCall(b *testing.B) {
       harness := benchkit.NewIntegrationHarness("localhost:4091", 50)
       defer harness.CloseAll()
       
       // Your scenarios here
   }
   ```
3. **Use histogram for latency collection**:
   ```go
   bm.RecordOperation(latency)
   bm.RecordError()
   ```
4. **Report results**:
   ```go
   result := benchkit.Run(harness, benchmark, runFunc)
   b.Logf("%s", result)
   ```

## Integration with CI/CD

For automated performance testing:

```bash
#!/bin/bash
cd clients/fitz-go

# Run benchmarks
go test ./benches/integration -bench=. -benchtime=30s -benchmem > current.json

# Compare to baseline (if baseline exists)
if [ -f baseline.json ]; then
    # Parse and compare results
    # Fail if regression >10%
    python3 scripts/compare_benchmarks.py baseline.json current.json || exit 1
fi
```

## See Also

- [OPTIMIZATION_COMPLETE.md](../../OPTIMIZATION_COMPLETE.md) - Zero-copy optimizations
- [Performance.md](../../docs/Performance.md) - Configuration and tuning
- [internal/benchkit](../../internal/benchkit/) - Histogram and reporter utilities
