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
- [Stress benchmark contract (Tier 3/4)](#stress-benchmark-contract-tier-34)
- [Performance targets](#performance-targets)
- [Quick Reference](#quick-reference)
- [Document History](#document-history)

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

## File Organization

### Tier Layout and Directory Structure

Benchmarks are organized in four tiers. Use the shared config and naming below.

| Tier       | Kind        | Tool      | Location                         | Scope                                                                                                                                     |
| ---------- | ----------- | --------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Tier 1** | Hotpath     | Criterion | `benches/tier1_hotpath_*.rs`     | Pure sync internals (routing, envelope, matcher, TLV, mux, permissions, context, actor_messaging). Target: stable medians; avoid headline results below 0.05µs. |
| **Tier 2** | Subsystem   | Criterion | `benches/tier2_subsystem_*.rs`   | Scheduler, mailbox, subscriptions, TLV pipeline. Target: &lt;3s.                                                                          |
| **Tier 3** | System      | Stress    | `benches/tier3_system_*.rs`      | One bench per domain (kv, lease, notice, queue, rpc, schedule, stream). In-process actor + test engine, no network. Target: &lt;10s.      |
| **Tier 4** | Integration | Stress    | `benches/tier4_integration_*.rs` | Same domains; full stack (direct → encoded → TCP → WebSocket → multiclient). Target: identify E2E performance cliffs.                     |

```
benches/
├── criterion_config.rs    # Shared Criterion config (use criterion_config_for_tier1/2())
├── stress_config.rs       # Stress run configuration helper
├── tier1_hotpath_matcher.rs
├── tier1_hotpath_tlv.rs
├── tier1_hotpath_mux.rs
├── tier1_hotpath_actor_messaging.rs
├── tier1_hotpath_envelope.rs
├── tier1_hotpath_routing.rs
├── tier1_hotpath_permissions.rs
├── tier1_hotpath_context.rs
├── tier2_subsystem_mailbox.rs
├── tier2_subsystem_scheduler.rs
├── tier2_subsystem_subscriptions.rs
├── tier2_subsystem_tlv_pipeline.rs
├── tier3_system_kv.rs
├── tier3_system_lease.rs
├── tier3_system_notice.rs
├── tier3_system_queue.rs
├── tier3_system_rpc.rs
├── tier3_system_schedule.rs
├── tier3_system_stream.rs
├── tier4_integration_kv.rs
├── tier4_integration_lease.rs
├── tier4_integration_notice.rs
├── tier4_integration_queue.rs
├── tier4_integration_rpc.rs
├── tier4_integration_schedule.rs
└── tier4_integration_stream.rs
```

### Organization Principles

- **One file per subsystem/domain** - Tier1/2: one module per file; Tier3/4: one domain per file.
- **Shared config** - Tier1/2 use `benches/criterion_config.rs` (`criterion_config_for_tier1()` / `criterion_config_for_tier2()`); Tier3/4 use `benches/stress_config.rs` and env vars (see [Stress configuration](#stress-configuration-tier-3-and-4)).
- **Clear naming** - Files follow `tierN_{hotpath|subsystem|system|integration}_{name}.rs`.
- **Logical grouping** - Related benchmarks in the same file; use a single Criterion group name per file (e.g. `hotpath_routing`) and `Throughput::Elements(N)` for comparability.

## Benchmark Structure

### Standard Template

Every benchmark should follow the AAA (Arrange-Act-Assert) pattern:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_router_match_1k_routes(c: &mut Criterion) {
    // Arrange: Setup (outside b.iter for minimal overhead)
    let router = setup_router_with_routes(1000);
    let test_routes: Vec<String> = (0..1000)
        .map(|i| format!("notice://realm{}/area/resource", i))
        .collect();

    let mut group = c.benchmark_group("hotpath_router");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("router_match_1k_routes", |b| {
        let mut idx = 0;
        b.iter(|| {
            // Act: The operation being measured
            let route = &test_routes[idx % test_routes.len()];
            let matches = black_box(router.find_subscribers(route));
            idx += 1;
            black_box(matches)
        });
    });
    group.finish();
}
criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_router_match_1k_routes
}
criterion_main!(benches);
```

### Key Components

1. **Setup (Arrange)** - Create test data outside `b.iter()`
2. **Measurement (Act)** - The operation being benchmarked inside `b.iter()`
3. **Prevention** - Use `black_box()` to prevent compiler optimizations
4. **Configuration** - Custom Criterion config for fast iteration

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

## Configuration Patterns

### Criterion (Tier 1 and 2)

All Criterion benchmarks use the shared config from `benches/criterion_config.rs`. Do not define a local `configure_criterion()`; use the shared helper so the report stays comparable across files.

Current shared settings:

- `warm_up_time`: 2s
- `measurement_time`: 5s
- `sample_size`: 100
- `noise_threshold`: 0.02

Practical rules:

- Do not benchmark trivial getters, clones, or ID constructors on their own.
- If a measured op is below about 0.05 us median, fold it into a larger workflow or exclude it from the headline report.
- Use `black_box()` on inputs, but do not use it to hide fake work.
- Keep setup outside `b.iter()` or `iter_batched()`.

The benchmark summary is median-first. Mean is still recorded, but the headline tables and regression checks use median latency or throughput.

### Stress configuration (Tier 3 and 4)

Tier 3 and Tier 4 benchmarks use `cntryl-stress` and `#[stress_test]`. Configuration is passed after `--` to the bench command (see `benches/stress_config.rs`):

| Argument       | Meaning                                    | Default |
| -------------- | ------------------------------------------ | ------- |
| `--runs <N>`   | Number of measurement runs per stress test | 5       |
| `--warmup <N>` | Number of warmup runs before measurement   | 1       |

- **Install tooling:** Install the shared bench/report helpers once per environment with `cargo install --git https://github.com/cntryl/tools --locked`.
- **set_elements(N):** Set this to the logical number of operations in each `ctx.measure(|| { ... })` (e.g. 3 for begin+put+rollback, 10 for 10 puts). Throughput reported by `cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"` is elements/time, so N must match what the closure does.
- **Minimum runtime:** Aim for 5s of measured work per scenario. Runs shorter than 3s are invalid, and the summary script flags them as such because they do not provide stable enough medians.
- **Output:** Stress results are written under `target/stress/<bench_name>/` (e.g. `target/stress/tier3_system_kv/latest.json`). Run `cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"` after `cargo bench` and the stress bench binaries to produce `target/bench_summary.md`.
- **Full refresh:** Run the full tier 3 / tier 4 suites, then run `cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"` to regenerate the summary in one step.

For local PowerShell runs, use the repo helper so Tier 3 and Tier 4 benches always carry the intended stress-sampling flags:

The helper also removes the targeted raw output directories before rerun (`target/criterion/<group>` for Criterion and `target/stress/<suite>` for stress suites) so renamed or deleted cases do not survive into the next summary.

```powershell
.\scripts\refresh-benchmarks.ps1
.\scripts\refresh-benchmarks.ps1 -Tiers tier3,tier4 -StressRuns 5 -StressWarmup 1
.\scripts\refresh-benchmarks.ps1 -Tiers tier3 -BenchNames tier3_system_kv -SkipSummary
```

For CI, you can reduce total time by passing `-- --runs 5 --warmup 1` (or lower only if you are intentionally collecting provisional data) when running the full tier3/tier4 suite.

### Stress benchmark contract (Tier 3/4)

Tier 3 and Tier 4 stress tests must follow the **stress benchmark contract**: rules for what goes inside vs outside `ctx.measure`, how to implement direct/encoded/tcp/websocket/multiclient layers, use of `shared_bench_runtime()`, and real actor logic (no fake work). See **[Stress benchmark contract](stress-bench-contract.md)** for the full contract and reference examples.

### Performance targets

Numerical, testable performance targets are defined in **[Performance targets](bench-targets.md)** and mirrored in [`config/perf_targets.json`](../../config/perf_targets.json). The doc is the human-facing matrix; the JSON file is the machine-readable source used by the perf loop. Fitz currently gates on `mean_us`, with derived ops/sec shown only as a convenience. Latency percentiles remain out of scope until explicit percentile scenarios exist.

For a domain-by-domain production validation checklist that turns the current benchmark inventory into concrete benchmark and failure-mode questions, see **[Production credibility checklist](production-credibility-checklist.md)**.

The benchmark summary script validates the collected Criterion and stress outputs before generating the report. Invalid or implausible measurements are excluded from the main tables and listed separately so they do not turn into presentation-safe numbers like `0.000 us` or absurd ops/sec.

## Best Practices

### DO ✅

| Practice                     | Rationale                                         | Example                                   |
| ---------------------------- | ------------------------------------------------- | ----------------------------------------- |
| **Use `black_box()`**        | Prevents compiler from optimizing away the work   | `black_box(engine.publish(&route, body))` |
| **Pre-allocate inputs**      | Measure only the target operation, not allocation | Setup routes/messages before `b.iter()`   |
| **Use deterministic data**   | Ensures reproducible results                      | `format!("notice://realm{}/area", i)`     |
| **Warm the cache**           | Measure steady-state performance                  | Multiple iterations before measurement    |
| **Document what's measured** | Makes intent clear for reviewers                  | `// Measures notice fanout throughput`    |
| **Group related benchmarks** | Easier to compare and analyze                     | All router benchmarks in `router.rs`      |
| **Use realistic scales**     | 1K-10K for most benchmarks                        | Avoid 1M+ unless profiling                |
| **Test scheme semantics**    | Benchmark each scheme separately                  | Notice vs Stream vs Queue vs RPC          |

### DON'T ❌

| Anti-pattern                 | Problem                        | Fix                                  |
| ---------------------------- | ------------------------------ | ------------------------------------ |
| **Allocate in `b.iter()`**   | Measures allocation, not logic | Move allocation outside              |
| **Use random data**          | Results vary across runs       | Use deterministic sequences          |
| **Ignore warm-up**           | First-run effects skew results | Configure proper warm-up time        |
| **Benchmark too much**       | Slow feedback loop             | Break into smaller benchmarks        |
| **Test correctness**         | That's what tests are for      | Only measure performance             |
| **Forget `black_box()`**     | Compiler removes "dead" code   | Wrap inputs and outputs              |
| **Mix I/O unnecessarily**    | Introduces variability         | Use in-memory storage when possible  |
| **Include network overhead** | Non-deterministic latency      | Use loopback or in-process transport |

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

## CI and Local Workflows

### Local Development

#### Quick iteration during development:

```bash
# Run a single benchmark for a faster feedback loop
cargo bench --bench tier1_hotpath_routing

# Or run a single tier / benchmark
cargo bench -- tier1_hotpath
```

#### Single benchmark:

```bash
# Run specific benchmark
cargo bench --bench tier1_hotpath_matcher
# With filter (all hotpath routing)
cargo bench -- hotpath_routing
```

#### Stress (Tier 3 / Tier 4):

```bash
cargo bench --bench tier3_system_kv
cargo bench --bench tier4_integration_kv
# Optional: fewer runs for faster feedback
cargo bench --bench tier4_integration_kv -- --runs 5 --warmup 1
```

#### Watch mode for TDD:

```bash
cargo watch -x "bench --bench tier1_hotpath_routing"
```

### CI Pipeline

The repository CI includes a **benchmarks** job that installs `cntryl-tools`, runs all Criterion benches with the shared Criterion config and stress benches with `--runs 5 --warmup 1`, then runs `cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"` and uploads `target/bench_summary.md` as an artifact. Criterion output is under `target/criterion/`; stress output is under `target/stress/<bench_name>/` (e.g. `latest.json`).

#### Pull Request Checks:

```bash
# Criterion with the shared config
cargo bench --no-fail-fast

# Stress with reduced runs (optional)
cargo bench --bench tier3_system_kv -- --runs 5 --warmup 1
cargo bench --bench tier4_integration_kv -- --runs 5 --warmup 1
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

## Quick Reference

### Criterion file template (Tier 1 / Tier 2)

Use shared config from `benches/criterion_config.rs`; do not define a local `configure_criterion()`.

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};

#[path = "criterion_config.rs"]
mod criterion_config;

fn bench_my_operation(c: &mut Criterion) {
    // Arrange: all setup outside b.iter()
    let data = precompute_test_data();

    let mut group = c.benchmark_group("hotpath_my_domain");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("operation_name", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let x = &data[idx % data.len()];
            black_box(do_operation(x))
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets = bench_my_operation
}
criterion_main!(benches);
```

**Example files:** `benches/tier1_hotpath_routing.rs`, `benches/tier2_subsystem_scheduler.rs`

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
| -------------- | -------------- | ----------- | ---------------- |
| Microbenchmark | < 2 seconds    | 10          | 1 second         |
| Subsystem      | 2-5 seconds    | 20          | 2 seconds        |
| Scheme         | 5-10 seconds   | 20          | 3 seconds        |
| System         | 10-60 seconds  | 50          | 10 seconds       |

### Scale Guidelines

| Scale      | Use Case                | Message/Route Count |
| ---------- | ----------------------- | ------------------- |
| **Small**  | Quick iteration         | 100 - 1,000         |
| **Medium** | Representative workload | 1,000 - 10,000      |
| **Large**  | Stress testing          | 10,000 - 50,000     |
| **XLarge** | Release profiling only  | 50,000+             |

### Message Broker Specific Metrics

| Metric               | What It Measures                   | Key Benchmarks                                    |
| -------------------- | ---------------------------------- | ------------------------------------------------- |
| **Publish Latency**  | Time to accept and route a message | `notice_publish_latency`, `stream_append_latency` |
| **Throughput**       | Messages per second                | `notice_throughput_10k`, `queue_throughput_5k`    |
| **Fanout**           | One-to-many delivery time          | `notice_fanout_100`, `router_dispatch_fanout`     |
| **Routing Speed**    | Route matching performance         | `router_match_1k`, `router_wildcard_10k`          |
| **Session Overhead** | Session management cost            | `session_handshake_latency`, `session_concurrent` |
| **Frame Encoding**   | Protocol overhead                  | `frame_encode_pub`, `frame_decode_dat`            |
| **Authorization**    | AuthZ check cost                   | `authz_permission_check`, `authz_grant_match`     |
| **Storage Latency**  | Backend operation time             | `memstore_append`, `memstore_reserve_ack`         |

## Document History

| Date       | Version | Changes                                          |
| ---------- | ------- | ------------------------------------------------ |
| 2025-10-20 | 1.0     | Initial version tailored for Fitz message broker |

### Contributors

- Fitz development team
- Adapted from Shale benchmark guidelines

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

### Tier 4 expectations

- **Direct** (in-process) is the in-process baseline; use it to compare domains and for regression.
- **network_roundtrip** and **concurrent\_\*** scenarios are expected to be roughly **2–3 orders of magnitude** lower throughput than direct (network and concurrency overhead). Use them for **regression and relative comparison**, not for absolute ops/sec targets.

### High variance (Criterion)

The benchmark summary flags entries with relative standard deviation &gt; 10%. Some benches (e.g. matcher, send_to_self) can remain above ~15% due to CPU cache and scheduling effects. Treat those as inherently variable; for release profiling you can increase sample size for those groups only, or document them as variable in commit messages.
