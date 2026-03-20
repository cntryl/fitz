# Queue TODOs

## Current State

Queue now has the right memory shape for large backlogs:

- payloads are no longer pinned in RAM for the full queue depth
- metadata cache is bounded instead of mirroring the full backlog
- ready state is stored as compressed ranges, not one in-memory entry per ready message
- recovery can read legacy combined records and the newer split header/body format
- recovery now rebuilds ready state in numeric message-id order so restart behavior matches live queue order

These guardrails should stay in place even if they make some direct-path benches harder. Do not revert them just to chase a short-term enqueue number.

## Latest Validated Queue Baseline

These are the latest validated queue bench numbers from `target/stress`:

### Tier 3 system queue

| scenario | current `per_op_us` | target `per_op_us` | gap |
|---|---:|---:|---:|
| `cold_start_recovery` | `13.818` | `0.65` | `+2025.85%` |
| `sustained_load` | `6.318` | `1.5` | `+321.20%` |
| `mixed_workload` | `3.997` | `3.4` | `+17.56%` |
| `high_contention` | `2.226` | `1.5` | `+48.40%` |

### Tier 4 integration queue

| scenario | current `per_op_us` | target `per_op_us` | gap |
|---|---:|---:|---:|
| `enqueue / direct` | `926.0` | `350` | `+164.57%` |
| `enqueue / encoded` | `398.0` | `500` | under target |
| `network_roundtrip / tcp` | `291.9` | `210` | `+39.00%` |
| `network_roundtrip / websocket` | `734.3` | `375` | `+95.81%` |
| `concurrent_enqueues / multiclient` | `1623.51` | `1100` | `+47.59%` |

## Next Batch Priorities

### P0: Recovery Index Redesign

- Replace restart-time full header scans with a compact persisted ready/delayed index.
- Goal: `cold_start_recovery` must stop being proportional to full durable queue depth.
- Likely direction:
  - persist ready/delayed state separately from message headers
  - recover from index entries first, not by scanning every message record/header
  - keep message body hydration lazy
- Acceptance:
  - `cold_start_recovery` moves materially toward `0.65 us/op`
  - restart does not require loading or decoding the full backlog body set
  - recovery preserves current live-order semantics

### P1: Direct Enqueue Write Path

- The split header/body layout fixed the memory model, but it increased write-path cost.
- Focus on reducing per-enqueue storage overhead without undoing offload behavior.
- Inspect:
  - transaction open/commit cost
  - two-put split write overhead
  - key construction overhead
  - avoidable cache churn after send
- Acceptance:
  - `direct enqueue` trends down from `926 us`
  - `tcp enqueue` trends down from `291.9 us`
  - `ws enqueue` trends down from `734.3 us`

### P1: Contention and Multiclient Path

- `multiclient_concurrent_enqueues` is still materially over budget.
- Inspect:
  - actor serialization on the hot queue
  - transport/session lock contention
  - notice or response-path overhead after send
  - whether ID reservation block sizing is causing avoidable sync points
- Acceptance:
  - `multiclient` trends toward `1100 us/op`
  - no regression in single-client encoded path, which is already under target

### P2: Recovery and Order Correctness Hardening

- Keep the new restart-order invariant covered.
- Add or retain tests for:
  - split-format recovery
  - legacy-format recovery
  - restart order matching live delivery order
  - bounded payload and metadata residency

### P2: Queue-Specific Perf Instrumentation

- Add lightweight timing counters or scoped tracing around:
  - recovery scan/index rebuild
  - enqueue storage commit
  - receive hydration
  - redelivery update path
- Goal: next optimization rounds should be driven by measured substeps, not just whole-bench totals.

## Work Rules

- Do not revert payload offload, bounded caches, or split header/body persistence.
- Prefer structural reductions in queue-depth cost over micro-optimizing already-cheap code.
- Treat `encoded enqueue` as protected: it is currently under target and should not be regressed while fixing other paths.

## Validation Checklist

- `cargo test queue -- --nocapture`
- `cargo test --test queue_advanced -- --nocapture`
- `cargo bench --bench tier3_system_queue`
- `cargo bench --bench tier4_integration_queue`
- `python scripts/benchmark_summary.py`
- `cargo test --all` when queue actor changes touch shared runtime or transport behavior
