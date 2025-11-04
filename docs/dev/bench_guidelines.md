# Benchmark Guidelines

**Version:** 1.0  
**Last Updated:** October 20, 2025  
**Project:** Fitz Message Broker

## Table of Contents

- [Philosophy](#philosophy)
- [File Organization](#file-organization)
- [Benchmark Structure](#benchmark-structure)
- [Naming Conventions](#naming-conventions)
- [Configuration Patterns](#configuration-patterns)
- [Best Practices](#best-practices)
- [Benchmark Categories](#benchmark-categories)
- [CI and Local Workflows](#ci-and-local-workflows)
- [Quick Reference](#quick-reference)
- [Document History](#document-history)

---

## Philosophy

Benchmarks in Fitz measure **real-world message broker performance** across routes, schemes, and transports while maintaining **fast feedback loops** for daily development.

### Core Principles

1. **Benchmarks ≠ Tests**
   - Tests verify correctness, benchmarks measure speed and scaling
   - Benchmarks should not test functionality (that's what tests are for)
   - Focus on realistic messaging workloads, not edge cases

2. **Fast Feedback First**
   - Default configuration runs in seconds, not minutes
   - Developers should run benchmarks frequently during development
   - Long, statistically rigorous runs reserved for release profiling

3. **Measure What Matters**
   - Focus on user-facing performance (message throughput, publish latency, routing speed)
   - Avoid micro-optimizing insignificant code paths
   - Profile first, benchmark second

4. **Reproducibility**
   - Benchmarks must produce consistent results across runs
   - Use deterministic data, not random values
   - Document environmental factors that affect results

---

## File Organization

### Directory Structure

```
benches/
├── router.rs          # Route matching and subscription routing
├── frame.rs           # Frame encoding/decoding performance
├── protocol.rs        # Protocol parsing and validation
├── memstore.rs        # In-memory storage operations
├── notice.rs          # Notice scheme publish/subscribe
├── stream.rs          # Stream scheme append/consume
├── queue.rs           # Queue scheme lease/ack/nack
├── rpc.rs             # RPC request/response latency
├── inbox.rs           # Inbox ephemeral messaging
├── session.rs         # Session handshake and management
├── mux.rs             # Channel multiplexing
├── authz.rs           # Authorization check performance
├── transport_ws.rs    # WebSocket transport throughput
├── engine.rs          # Full engine workflows (heavy)
└── end_to_end.rs      # Complete broker workflows (heavy)
```

### Organization Principles

- **One file per subsystem** - Each benchmark file focuses on a single module or scheme
- **Minimal external dependencies** - Keep benchmark files self-contained
- **Clear naming** - File names match the module or scheme being benchmarked
- **Logical grouping** - Related benchmarks in the same file

---

## Benchmark Structure

### Standard Template

Every benchmark should follow the AAA (Arrange-Act-Assert) pattern:

```rust
use criterion::{black_box, Criterion, criterion_group, criterion_main};

fn bench_router_match_1k_routes(c: &mut Criterion) {
    // Arrange: Setup (outside b.iter for minimal overhead)
    let router = setup_router_with_routes(1000);
    let test_routes: Vec<String> = (0..1000)
        .map(|i| format!("notice://realm{}/area/resource", i))
        .collect();
    
    c.bench_function("router_match_1k_routes", |b| {
        let mut idx = 0;
        b.iter(|| {
            // Act: The operation being measured
            let route = &test_routes[idx % test_routes.len()];
            let matches = black_box(router.find_subscribers(route));
            idx += 1;
            black_box(matches)
        });
    });
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(200))
        .measurement_time(std::time::Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_router_match_1k_routes
}
criterion_main!(benches);
```

### Key Components

1. **Setup (Arrange)** - Create test data outside `b.iter()`
2. **Measurement (Act)** - The operation being benchmarked inside `b.iter()`
3. **Prevention** - Use `black_box()` to prevent compiler optimizations
4. **Configuration** - Custom Criterion config for fast iteration

---

## Naming Conventions

### Format

Use the pattern: `{subsystem}_{operation}_{scale}_{variant?}`

**Examples:**
```rust
// Routing
router_match_1k_routes           // Match against 1K registered routes
router_wildcard_match_10k        // Wildcard matching with 10K routes
router_dispatch_fanout_100       // Fan-out to 100 subscribers

// Protocol
frame_encode_pub_1k              // Encode 1K PUB frames
frame_decode_dat_10k             // Decode 10K DAT frames
frame_parse_tlv_nested           // Parse nested TLV structures

// Schemes
notice_publish_fanout_10         // Publish to 10 notice subscribers
stream_append_sequential_1k      // Sequential stream appends (1K)
queue_lease_batch_100            // Lease 100 queue messages
rpc_request_response_latency     // RPC round-trip latency
inbox_ephemeral_delivery         // Inbox message delivery

// Storage
memstore_append_1k               // Append 1K messages
memstore_reserve_extend_ack      // Queue reserve/extend/ack cycle
memstore_stream_consume_10k      // Consume 10K stream events

// Transport
ws_frame_throughput_1k           // WebSocket frame throughput (1K msgs)
session_handshake_latency        // Session AUTH handshake time
mux_channel_demux_1k             // Demux 1K frames across channels

// Authorization
authz_permission_check_1k        // 1K permission checks
authz_grant_match_wildcard       // Wildcard grant matching
authz_tenant_isolation_check     // Tenant isolation verification
```

### Scale Indicators

- `1k`, `10k`, `50k`, `100k` - Number of operations/messages
- `small`, `medium`, `large` - Relative sizes
- `sequential`, `random` - Access patterns
- `fanout_N` - Number of subscribers/consumers

### Variant Suffixes

- `_latency` - Single operation latency
- `_throughput` - Operations per second
- `_fanout` - One-to-many delivery
- `_batch` - Batched operations
- `_concurrent` - Concurrent access
- `_wildcard` - Wildcard pattern matching

---

## Configuration Patterns

### Fast Iteration (Default)

For daily development work:

```rust
fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10)                    // 10 samples (default: 100)
        .warm_up_time(Duration::from_millis(200))  // 0.2s warmup (default: 3s)
        .measurement_time(Duration::from_secs(1))  // 1s measurement (default: 5s)
        .noise_threshold(0.05)              // 5% noise tolerance (default: 1%)
}
```

**Target runtime:** 1-3 seconds per benchmark

### Release Profiling

For detailed performance analysis:

```rust
fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(100)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10))
        .noise_threshold(0.01)
}
```

**Target runtime:** 10-30 seconds per benchmark

### Heavy Benchmarks

For system-level benchmarks (CI only):

```rust
#[cfg(feature = "perf")]
fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(50)
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(20))
}
```

**Target runtime:** 20-60 seconds per benchmark

---

## Best Practices

### DO ✅

| Practice | Rationale | Example |
|----------|-----------|---------|
| **Use `black_box()`** | Prevents compiler from optimizing away the work | `black_box(engine.publish(&route, body))` |
| **Pre-allocate inputs** | Measure only the target operation, not allocation | Setup routes/messages before `b.iter()` |
| **Use deterministic data** | Ensures reproducible results | `format!("notice://realm{}/area", i)` |
| **Warm the cache** | Measure steady-state performance | Multiple iterations before measurement |
| **Document what's measured** | Makes intent clear for reviewers | `// Measures notice fanout throughput` |
| **Group related benchmarks** | Easier to compare and analyze | All router benchmarks in `router.rs` |
| **Use realistic scales** | 1K-10K for most benchmarks | Avoid 1M+ unless profiling |
| **Test scheme semantics** | Benchmark each scheme separately | Notice vs Stream vs Queue vs RPC |

### DON'T ❌

| Anti-pattern | Problem | Fix |
|--------------|---------|-----|
| **Allocate in `b.iter()`** | Measures allocation, not logic | Move allocation outside |
| **Use random data** | Results vary across runs | Use deterministic sequences |
| **Ignore warm-up** | First-run effects skew results | Configure proper warm-up time |
| **Benchmark too much** | Slow feedback loop | Break into smaller benchmarks |
| **Test correctness** | That's what tests are for | Only measure performance |
| **Forget `black_box()`** | Compiler removes "dead" code | Wrap inputs and outputs |
| **Mix I/O unnecessarily** | Introduces variability | Use in-memory storage when possible |
| **Include network overhead** | Non-deterministic latency | Use loopback or in-process transport |

### Common Patterns

#### Pattern 1: Message Throughput

```rust
fn bench_notice_publish_throughput(c: &mut Criterion) {
    let engine = setup_test_engine();
    let route = "notice://test/area/resource";
    let messages: Vec<Vec<u8>> = (0..10000)
        .map(|i| format!("msg{:08}", i).into_bytes())
        .collect();
    
    c.bench_function("notice_publish_10k", |b| {
        b.iter(|| {
            for msg in &messages {
                black_box(engine.publish(route, msg).await.unwrap());
            }
        });
    });
}
```

#### Pattern 2: Routing Latency

```rust
fn bench_router_match_latency(c: &mut Criterion) {
    // Setup: Pre-populate router with subscriptions
    let mut router = Router::new();
    for i in 0..1000 {
        let route = format!("notice://realm{}/area/resource", i);
        router.subscribe(&route, dummy_sender()).await.unwrap();
    }
    
    c.bench_function("router_match_latency", |b| {
        let mut i = 0;
        b.iter(|| {
            let route = format!("notice://realm{}/area/resource", i % 1000);
            black_box(router.find_subscribers(&route));
            i += 1;
        });
    });
}
```

#### Pattern 3: Scheme Comparison

```rust
fn bench_scheme_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheme_publish_latency");
    
    let schemes = ["notice", "stream", "queue", "rpc"];
    
    for scheme in schemes {
        let engine = setup_test_engine();
        let route = format!("{}://test/area/resource", scheme);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(scheme),
            &route,
            |b, route| {
                b.iter(|| {
                    black_box(engine.publish(route, b"test").await.unwrap());
                });
            },
        );
    }
    
    group.finish();
}
```

#### Pattern 4: Concurrent Sessions

```rust
fn bench_concurrent_sessions(c: &mut Criterion) {
    let mut group = c.benchmark_group("session_concurrent");
    
    for num_sessions in [1, 10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_sessions),
            &num_sessions,
            |b, &sessions| {
                b.iter(|| {
                    let handles: Vec<_> = (0..sessions)
                        .map(|_| spawn_session_task())
                        .collect();
                    // Measure concurrent session handling
                    futures::future::join_all(handles).await
                });
            },
        );
    }
    
    group.finish();
}
```

---

## Benchmark Categories

### 1. Microbenchmarks

**Purpose:** Measure single operations in isolation

**Characteristics:**
- Focus on one method or function
- Minimal setup overhead
- Runs in < 100ms per iteration
- Used for algorithmic optimization

**Examples:**
```rust
// Single method performance
frame_encode_single_pub
frame_decode_single_dat
router_match_exact_route
authz_check_single_permission
route_parse_normalize
```

### 2. Subsystem Benchmarks

**Purpose:** Measure combined component performance

**Characteristics:**
- Multiple operations in sequence
- Representative of real usage
- Runs in 1-5 seconds total
- Used for feature development

**Examples:**
```rust
// Combined operations
session_handshake_auth_subscribe
stream_append_consume_ack
queue_lease_extend_complete
rpc_publish_wait_reply
notice_subscribe_dispatch_fanout
```

### 3. Scheme Benchmarks

**Purpose:** Measure scheme-specific performance

**Characteristics:**
- Full scheme workflow end-to-end
- Includes routing and storage
- Tests scheme semantics
- 5-10 seconds per benchmark

**Examples:**
```rust
// Scheme workflows
notice_best_effort_fanout_100
stream_append_ordered_1k
queue_visibility_timeout_workflow
rpc_request_response_timeout
inbox_ephemeral_lifecycle
```

### 4. System Benchmarks

**Purpose:** Measure end-to-end broker performance

**Characteristics:**
- Full engine workflows
- Multiple transports and sessions
- Runs in 10-60 seconds
- Used for release profiling
- Gated behind `perf` feature

**Examples:**
```rust
#[cfg(feature = "perf")]
// Full system workflows
engine_multi_tenant_isolation_10k
engine_mixed_schemes_concurrent
transport_ws_10k_messages
broker_session_churn_100
end_to_end_publish_subscribe_1m
```

---

## CI and Local Workflows

### Local Development

#### Quick iteration during development:
```bash
# Fast mode (1-3 seconds per benchmark)
cargo bench -- --quick

# Or with explicit fast settings
cargo bench -- \
  --warm-up-time 0.2 \
  --measurement-time 1 \
  --sample-size 10
```

#### Single benchmark:
```bash
# Run specific benchmark
cargo bench bloom_insert

# With filter
cargo bench -- bloom
```

#### Watch mode for TDD:
```bash
# Re-run on file changes
cargo watch -x "bench -- --quick"
```

### CI Pipeline

#### Pull Request Checks:
```bash
# Fast benchmarks only (no perf feature)
cargo bench --no-fail-fast -- \
  --warm-up-time 0.5 \
  --measurement-time 2 \
  --sample-size 20
```

#### Nightly Performance Runs:
```bash
# Full profiling with perf feature
cargo bench --release --features perf -- \
  --sample-size 100 \
  --measurement-time 10
```

#### Baseline Comparison:
```bash
# Save baseline
cargo bench --bench bloom -- --save-baseline main

# Compare against baseline
cargo bench --bench bloom -- --baseline main
```

### Profiling Integration

#### With flamegraph:
```bash
# Generate flamegraph
cargo flamegraph --bench bloom -- --bench

# Or with perf
perf record --call-graph dwarf cargo bench --bench bloom
perf report
```

#### With criterion:
```bash
# HTML reports generated automatically
# View at: target/criterion/report/index.html
cargo bench

# Open report
open target/criterion/report/index.html  # macOS
xdg-open target/criterion/report/index.html  # Linux
start target/criterion/report/index.html  # Windows
```

---

## Quick Reference

### Benchmark File Template

```rust
use criterion::{black_box, Criterion, criterion_group, criterion_main};
use fitz::core::engine::start_engine;
use fitz::storage::mem::MemStore;
use std::sync::Arc;
use tokio::sync::Mutex;

fn bench_notice_publish_1k(c: &mut Criterion) {
    // Arrange: Setup broker with in-memory storage
    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = Arc::new(Mutex::new(MemStore::new()));
    let engine = start_engine(store);
    
    let messages: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("msg{:08}", i).into_bytes())
        .collect();
    
    c.bench_function("notice_publish_1k", |b| {
        b.iter(|| {
            rt.block_on(async {
                for msg in &messages {
                    black_box(
                        engine.publish(
                            "notice://test/area/resource".to_string(),
                            format!("id-{}", msg.len()),
                            msg.clone(),
                            None,
                            None,
                            false,
                            None,
                        ).await.unwrap()
                    );
                }
            })
        });
    });
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(200))
        .measurement_time(std::time::Duration::from_secs(1))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = bench_notice_publish_1k
}
criterion_main!(benches);
```

### Reviewer Checklist

When reviewing benchmark PRs:

- [ ] **Performance:** Benchmark runs in < 5 seconds locally
- [ ] **Focus:** Measures one clear behavior/operation
- [ ] **Reproducibility:** Uses deterministic data (no randomness)
- [ ] **Configuration:** Appropriate warm-up and measurement times
- [ ] **Stability:** Results stable across runs (< 5% variance)
- [ ] **Prevention:** Uses `black_box()` on inputs and outputs
- [ ] **Naming:** Descriptive name following conventions
- [ ] **Documentation:** Clear comments explaining what's measured
- [ ] **Category:** Appropriately categorized (micro/subsystem/scheme/system)
- [ ] **Async Handling:** Proper tokio runtime usage
- [ ] **Scheme Specific:** Tests appropriate scheme semantics
- [ ] **CI:** Heavy benchmarks gated behind `perf` feature

### Common Commands

```bash
# Run all benchmarks (fast mode)
cargo bench -- --quick

# Run specific subsystem
cargo bench router

# Run all scheme benchmarks
cargo bench -- notice stream queue rpc inbox

# Run with detailed output
cargo bench -- --verbose

# Save baseline for comparison
cargo bench -- --save-baseline main

# Compare against baseline
cargo bench -- --baseline main

# Generate flamegraph
cargo flamegraph --bench router

# List all benchmarks
cargo bench -- --list
```

### Performance Targets

| Benchmark Type | Target Runtime | Sample Size | Measurement Time |
|---------------|----------------|-------------|------------------|
| Microbenchmark | < 2 seconds | 10 | 1 second |
| Subsystem | 2-5 seconds | 20 | 2 seconds |
| Scheme | 5-10 seconds | 20 | 3 seconds |
| System | 10-60 seconds | 50 | 10 seconds |

### Scale Guidelines

| Scale | Use Case | Message/Route Count |
|-------|----------|---------------------|
| **Small** | Quick iteration | 100 - 1,000 |
| **Medium** | Representative workload | 1,000 - 10,000 |
| **Large** | Stress testing | 10,000 - 50,000 |
| **XLarge** | Release profiling only | 50,000+ |

### Message Broker Specific Metrics

| Metric | What It Measures | Key Benchmarks |
|--------|------------------|----------------|
| **Publish Latency** | Time to accept and route a message | `notice_publish_latency`, `stream_append_latency` |
| **Throughput** | Messages per second | `notice_throughput_10k`, `queue_throughput_5k` |
| **Fanout** | One-to-many delivery time | `notice_fanout_100`, `router_dispatch_fanout` |
| **Routing Speed** | Route matching performance | `router_match_1k`, `router_wildcard_10k` |
| **Session Overhead** | Session management cost | `session_handshake_latency`, `session_concurrent` |
| **Frame Encoding** | Protocol overhead | `frame_encode_pub`, `frame_decode_dat` |
| **Authorization** | AuthZ check cost | `authz_permission_check`, `authz_grant_match` |
| **Storage Latency** | Backend operation time | `memstore_append`, `memstore_reserve_ack` |

---

## Document History

| Date | Version | Changes |
|------|---------|---------|
| 2025-10-20 | 1.0 | Initial version tailored for Fitz message broker |

### Contributors

- Fitz development team
- Adapted from Shale benchmark guidelines

---

## Appendix: Fitz-Specific Considerations

### Scheme Semantics to Benchmark

Each scheme has different performance characteristics:

1. **notice://** - Best-effort, drop-on-backpressure
   - Benchmark: fanout speed, subscriber count impact, backpressure handling
   
2. **stream://** - Append-only log with ordering
   - Benchmark: append throughput, consume latency, offset seeking
   
3. **queue://** - Visibility timeout, at-least-once
   - Benchmark: lease latency, extend/ack cycles, DLQ movement
   
4. **rpc://** - Request/response with timeout
   - Benchmark: round-trip latency, timeout handling, concurrent requests
   
5. **inbox://** - Ephemeral per-session
   - Benchmark: creation/cleanup overhead, delivery latency

### Multi-Tenant Benchmarking

When benchmarking tenant isolation:
- Use distinct tenant IDs in test data
- Measure cross-tenant permission checks
- Benchmark tenant namespace lookup overhead
- Test storage isolation performance impact

### Transport Agnostic

Benchmarks should work with any transport:
- Use in-process/loopback for determinism
- WebSocket benchmarks measure framing overhead
- Test frame multiplexing (mux) separately from transport

