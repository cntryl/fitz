# Hotpath Benchmarks - Domain Handler->Service Layer Performance Testing

## Overview

Created comprehensive hotpath benchmarks to measure the full handler->service layer performance for each domain. These benchmarks answer critical questions about:

1. **High Volume Support**: Can we handle 10k+ sequential operations?
2. **High Concurrency**: How do parallel operations from multiple clients perform?
3. **Performance Breakdown Points**: Where does performance degrade (payload sizes, key counts, contention)?

## Benchmark Design Philosophy

Each domain benchmark follows this structure:

### Sequential Operations
- Tests basic handler overhead with 100, 1k, 10k operations
- Measures throughput (ops/sec) and latency
- Identifies baseline performance

### Concurrent Operations
- Simulates multiple clients using Arc-shared domains
- Tests multi-tenant scenarios (different route families)
- Measures contention and locking overhead

### Payload/Parameter Variations
- Tests performance with varying data sizes (64B to 16KB)
- Identifies memory allocation overhead
- Finds optimal payload sizes

### Stress Tests
- High-frequency operations to find breaking points
- Resource contention scenarios
- Helps identify bottlenecks

## Completed Benchmarks

### ✅ KV Domain (`benches/hotpath/kv_core.rs`)

**Tests:**
- `bench_sequential_put`: 100/1k/10k PUT operations
- `bench_sequential_get`: 100/1k/10k GET operations (with warm-up)
- `bench_concurrent_mixed`: 100 concurrent mixed read/write ops
- `bench_payload_sizes`: 64B to 16KB payload sizes

**Key Metrics:**
- Throughput: elements/sec
- Latency: time per operation
- Payload size impact on performance

**Run with:**
```bash
cargo bench --bench hotpath_kv_core
```

### ✅ Notice Domain (`benches/hotpath/notice_core.rs`)

**Tests:**
- `bench_sequential_publish_no_subscribers`: 100/1k/10k publishes (no subscribers)
- `bench_message_sizes`: 64B to 16KB message payloads
- `bench_concurrent_multitenant_publish`: 10 tenants × 10 messages
- `bench_wildcard_matching`: Route wildcard pattern complexity
- `bench_high_frequency_publish`: 1000 rapid-fire publishes

**Key Metrics:**
- Publish throughput with/without subscribers
- Message size impact
- Multi-tenant isolation overhead
- Wildcard matching performance

**Run with:**
```bash
cargo bench --bench hotpath_notice_core
```

## Benchmarks To Create

### 🔄 RPC Domain (`benches/hotpath/rpc_core.rs`)

**Recommended Tests:**
- Sequential request/reply cycles
- Inbox allocation performance
- Concurrent RPC requests with different correlation IDs
- Handler registration/matching overhead
- Reply routing and authorization performance
- Stream response handling (with TAG_SEQ, TAG_STREAM_END)

**Key Questions:**
- How many concurrent RPC sessions can we support?
- What's the overhead of inbox allocation/authorization?
- How does reply routing scale with concurrent requests?

### 🔄 Queue Domain (`benches/hotpath/queue_core.rs`)

**Recommended Tests:**
- Sequential enqueue operations
- Sequential dequeue operations
- Concurrent producer/consumer scenarios
- Queue depth impact on performance
- Message ordering guarantees under load
- Priority queue performance (if supported)

**Key Questions:**
- Peak enqueue/dequeue throughput?
- How does queue depth affect performance?
- Concurrent producer/consumer contention?

### 🔄 Stream Domain (`benches/hotpath/stream_core.rs`)

**Recommended Tests:**
- Sequential append operations
- Sequential read operations (event by ID, by range)
- Concurrent append from multiple clients
- Watermark tracking overhead
- Stream compaction impact
- Large event batch performance

**Key Questions:**
- Append throughput under load?
- Read performance with varying stream sizes?
- Concurrent writer contention?

### 🔄 Control Domain (`benches/hotpath/control_core.rs`)

**Recommended Tests:**
- Sequential control command processing
- Heartbeat overhead
- Configuration update propagation
- Shutdown coordination performance
- Notice integration performance

**Key Questions:**
- Control plane overhead?
- How fast can we propagate configuration changes?
- Heartbeat frequency vs overhead tradeoff?

### 🔄 Lease Domain (Enhanced)

**Current:** `benches/hotpath/lease_core.rs` has low-level microbenchmarks (token generation, UUID formatting)

**Should Add:**
- Sequential acquire/renew/surrender cycles
- Lease contention scenarios (multiple clients for same resource)
- TTL expiration handling
- Token validation overhead
- Multi-tenant lease isolation

**Template provided above but not yet applied to lease_core.rs**

## Benchmark Execution Strategy

### Individual Domain Testing
```bash
# Test specific domain
cargo bench --bench hotpath_kv_core
cargo bench --bench hotpath_notice_core
cargo bench --bench hotpath_rpc_core      # TODO
cargo bench --bench hotpath_queue_core    # TODO
cargo bench --bench hotpath_stream_core   # TODO
cargo bench --bench hotpath_control_core  # TODO
```

### Full Hotpath Suite
```bash
# Run all hotpath benchmarks
cargo bench hotpath
```

### Continuous Performance Monitoring
```bash
# Save baseline
cargo bench hotpath -- --save-baseline main

# After optimization
cargo bench hotpath -- --baseline main
```

## Performance Goals

Based on industry standards and Fitz's use case:

### Throughput Targets
- **KV Operations**: 100k+ ops/sec (sequential), 50k+ ops/sec (concurrent)
- **Notice Publish**: 50k+ msgs/sec (no subscribers), 20k+ msgs/sec (with subscribers)
- **RPC Requests**: 20k+ req/sec
- **Queue Operations**: 50k+ enqueue/sec, 50k+ dequeue/sec
- **Stream Appends**: 30k+ events/sec

### Latency Targets
- **p50**: < 100μs
- **p99**: < 1ms
- **p99.9**: < 10ms

## Optimization Opportunities

### What To Look For

1. **Lock Contention**
   - If concurrent benchmarks show poor scaling vs sequential
   - Consider: lock-free data structures, sharding, read-copy-update

2. **Memory Allocation**
   - If payload size benchmarks show super-linear growth
   - Consider: object pooling, arena allocation, stack allocation

3. **Parsing Overhead**
   - If TLV parsing dominates operation time
   - Consider: zero-copy parsing, pre-parsed payloads, batch parsing

4. **Route Matching**
   - If wildcard/pattern matching is slow
   - Consider: compiled route tables, trie structures, hash-based dispatch

## Integration With Existing Benchmarks

### Hotpath Benchmarks (This Initiative)
- **Focus**: Full handler->service layer
- **Purpose**: End-to-end performance, realistic workloads
- **Location**: `benches/hotpath/{domain}_core.rs`

### Subsystem Benchmarks (Existing)
- **Focus**: Service layer only
- **Purpose**: Isolated service performance without TLV/routing overhead
- **Location**: `benches/subsystem/{domain}_service.rs`

### Microbenchmarks (Existing)
- **Focus**: Individual functions/algorithms
- **Purpose**: Low-level optimization targets
- **Location**: Mixed in hotpath and subsystem files

## Next Steps

1. **Create remaining domain benchmarks**
   - RPC: Focus on inbox management and correlation
   - Queue: Focus on FIFO ordering and contention
   - Stream: Focus on append throughput and watermarks
   - Control: Focus on command dispatch overhead

2. **Run baseline benchmarks**
   - Establish performance baseline for main branch
   - Document current performance characteristics

3. **Identify optimization targets**
   - Use benchmark results to find bottlenecks
   - Prioritize optimizations with biggest impact

4. **Continuous monitoring**
   - Add benchmark runs to CI/CD
   - Track performance regressions
   - Celebrate performance improvements

## Usage Examples

### Run Single Domain
```bash
# KV domain performance
cargo bench --bench hotpath_kv_core

# Notice domain performance
cargo bench --bench hotpath_notice_core
```

### Run Specific Test
```bash
# Just concurrent tests
cargo bench --bench hotpath_kv_core concurrent

# Just payload size tests
cargo bench --bench hotpath_kv_core payload_sizes
```

### Compare Before/After
```bash
# Before optimization
cargo bench hotpath -- --save-baseline before

# Make changes...

# After optimization
cargo bench hotpath -- --baseline before
```

### Generate HTML Report
```bash
cargo bench hotpath --  --verbose
# Results in target/criterion/{bench_name}/report/index.html
```

## Performance Analysis Tools

### Criterion Output
- Automatically generates HTML reports with graphs
- Shows throughput, latency distributions
- Detects performance regressions

### Flamegraphs
```bash
# Install cargo-flamegraph
cargo install flamegraph

# Generate flamegraph for specific benchmark
cargo flamegraph --bench hotpath_kv_core
```

### Profiling
```bash
# Use perf (Linux)
cargo build --release --benches
perf record --call-graph dwarf ./target/release/deps/hotpath_kv_core-*
perf report

# Use Instruments (macOS)
cargo instruments --bench hotpath_kv_core --template time
```

## Success Metrics

### Quantitative
- [ ] All domains meet throughput targets
- [ ] Latency p99 < 1ms for all operations
- [ ] Linear scaling up to 10 concurrent clients
- [ ] Graceful degradation beyond capacity limits

### Qualitative
- [ ] Benchmarks clearly identify bottlenecks
- [ ] Easy to add new benchmark scenarios
- [ ] Results reproducible across runs
- [ ] Performance regressions caught quickly

---

**Status**: 2/7 domains completed (KV, Notice)
**Next**: Complete RPC, Queue, Stream, Control, and enhance Lease benchmarks
