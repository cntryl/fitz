# Stress benchmark contract (cntryl_stress)

This document defines the contract for Tier 2 through Tier 6 benchmarks using the `cntryl_stress` framework with `#[stress]` macros. Tier 1 hotpath rows use the micro-benchmark rules in [Benchmark Guidelines](benchmarks.md). Stress benches must follow the rules for their tier.

## Tier 2 rules (subsystem benchmarks — fixed operations)

- Tier 2 rows use stress fixed-operation samples. Omit `mode = "fixed_duration"`; explicit modes must match the tier-derived `fixed_operations` mode.
- Use `ctx.measure("name", ...)` for one subsystem operation, `ctx.measure_batch("name", logical_ops, ...)` / `benches/tier2_stress.rs` helpers when each framework iteration performs a known batch size, or `ctx.record_external("name", duration, completed)` when the row owns timing.
- If a Tier 2 row owns external timing, record it with explicit completed work through `ctx.record_external` or the shared Tier 2 helper.
- Correctness counters must represent the logical work completed by the measured operation or batch, not setup work.

## Tier 3 rules (system benchmarks — no network)

- Test the domain actor **directly**, in-process, **no TCP/WS**.
- All setup (actor creation, subscriptions, pre-population, payload construction) goes **OUTSIDE** `ctx.measure`.
- Only the operation being measured goes **INSIDE** `ctx.measure`.
- Correctness counters must reflect the **actual number of meaningful operations** inside the measure block.
- Use the `cntryl_stress` default profile for Fitz documentation and CI. Local sample-count or duration overrides are diagnostic only and must not become committed workflow defaults.
- Every scenario must exercise **real actor logic** — no `black_box` on counters, no fake loops, no measuring `pending_count()`.
- If testing fanout, actually subscribe real sinks and publish real messages.
- If testing scan_and_fire, schedules must actually be due to fire — bypass or reset the dedup window so fires actually occur.
- Add structured workload parameters with `ctx.parameter("scenario", "name")` matching the scenario name in the stress results.

## Tier 4 rules (integration benchmarks — full stack)

- Four layers per domain: **direct**, **encoded**, **tcp**, **websocket**, **multiclient**.
- **direct**: `actor.receive()` (or domain handle) only, no frames, no network — establishes the in-process baseline.
- **encoded**: Build the TLV frame **and** send it through a **codec decode path** into the actor — measures serialization cost. Do **not** just build the frame and `let _ = &frame` it. If you cannot decode through the actor, this layer is invalid and should be omitted.
- **tcp**: Full roundtrip through TestServer over TCP — encode frame, send, receive response.
- **websocket**: Same as tcp but over WS.
- **multiclient**: N clients (typically 10) making requests — must use `tokio::join!` or `FuturesUnordered` for **actual concurrency**, **not** a sequential for loop. Label it concurrent only if it is actually concurrent.
- Fanout scenarios must drain every receiving client/subscriber during measurement; aggregate delivery counters are not enough if one receiver can backlog while others advance the total.
- Network tests must use a **shared runtime** (`shared_bench_runtime()`) consistently across all domains.
- Correctness `completed` = number of meaningful operations per measure iteration or sample (e.g. 2 for begin+append, 1 for single enqueue).
- **RPC tier4** must test the full **request → worker dispatch → response** cycle over the wire, not just subscribe.

## Shared Tier 2 through Tier 4 rules

- Never measure setup, teardown, or frame construction.
- Never use `black_box` as a substitute for real work.
- Never let a failure short-circuit the measured work; if setup can fail, record it and assert after the measurement block.
- Never `return` early from `ctx.measure`. If the measured work can fail, record the failure in a local flag or result and assert after the closure so the timer still covers the full intended work.
- Scenario names in `ctx.parameter("scenario", "...")` must match what appears in the stress results output.
- Measurement names are part of the artifact ID. Keep readable names stable unless the measured workload or a workload-defining parameter changes.
- Each test must be independently runnable and deterministic.

## Tier 5 and Tier 6 rules (owned active windows)

- Use `tier5_saturation` and `tier6_endurance` with `benchkit,stress-soak`.
- These long windows deliberately use
  `--profile smoke --samples 1 --warmup-samples 0 --cooldown-samples 0`. This is
  the committed exception to the shorter tiers' default profile and invocation
  counts: the default quality gate rejects a single sample. Correctness and
  liveness remain required; a full-duration pass makes no statistical
  performance-quality claim.
- The shared startup defaults missing framework settings to smoke, one sample,
  no warmup/cooldown, a 60-second progress watchdog, and a 4,500-second hard
  timeout before framework threads start. Explicit CLI/environment overrides
  remain valid and may repeat full campaigns. Workflow settings stay explicit.
- Resolve the duration from `FITZ_TIER5_DURATION_SECS` or
  `FITZ_TIER6_DURATION_SECS`, then `FITZ_STRESS_DURATION_SECS`, then 3,600 seconds.
  Accept only integer durations from 1 through 86,400 seconds. Short overrides
  are diagnostic and do not qualify the default hour.
- Tier 5 divides one total active budget across 1/2/4/8/16/32/64-lane stages.
  Explicit capacity may end a Tier 5 stage early, with the boundary, actual
  elapsed work, and successful recovery recorded. That early stop passes only
  after the stage accumulates at least 30 seconds of active batch time (or its
  whole budget, if shorter) and every active lane completes at least 10
  validated domain cycles; an earlier capacity stop fails the stage. Every lane
  must still complete a validated cycle, as in any other stage. Tier 6 sustains
  eight lanes until the full configured active batch-time budget is accumulated after setup.
- Exclude all separate verification from the configured active time, including
  periodic checks. Record its duration as `verification_elapsed_ns`, the active
  batch time as `elapsed_ns`, and wall time as `wall_elapsed_ns`. Record actual
  verification checks separately from throughput completions.
- Establish a successful baseline, perform real semantic verification, and
  probe low-load service after load subsides. Report actual verification checks.
  These probes do not establish restart recovery or stronger storage guarantees.
- Record observed logical completions using `ctx.record_external_outcome` and
  `LogicalUnit`. Do not wrap an owned window in `ctx.measure_outcome`, and do not
  infer completions from sends, iterations, or a nonzero placeholder.
- Use logical unit `accepted_domain_cycle` with
  `framework_attempt_scope=accepted_or_unexpected_failure`. Canonical attempted
  counts include validated completions plus unexpected failures to match the
  framework's closed correctness semantics. Preserve true raw attempts in JSON
  and observations; expected rejections, contention, and delivery misses never
  become completed operations.
- Keep allowlisted capacity rejections, Lease contention, and Notice
  `delivery_window_misses` separate from validated completions and correctness
  failures. A missing delivery is not proof of a broker drop; a tested load
  range does not by itself prove saturation was reached.
- Bound retained workload state and latency samples. Preserve partial artifacts
  before reporting failure. Use stable workload parameters and dynamic observations.
- Require useful progress and bounded per-client waits. Required-response
  timeouts, invalid responses, unexpected errors, and verification failures stop
  the workload and fail the run; expected saturation must not conceal a liveness
  failure. Bound complete TCP requests to the default 60-second Midge runtime
  response budget, alongside the independent per-lane progress watchdog. Missing
  Notice baseline or recovery deliveries fail verification.
- Queue RESERVE backpressure after an accepted ENQUEUE retries with exponential
  backoff within that cycle's deadline. The stage records the retries and ends at
  a capacity boundary only after every in-flight cycle completes; an exhausted
  deadline remains a failure.
- Follow the [domain scope and artifact contract](tier5-tier6-benchmarks.md),
  including the declared storage profile, Queue running-process fast ACK scope, finite Stream history,
  Schedule definition churn, legacy RPC terminal responses, received Notice
  deliveries, and process-local Lease tokens.

## Artifact and baseline contract

- Tier 1 through Tier 4 accept the `cntryl-stress` default profile for docs and
  CI. A default five-sample report may have `authoritative: false`; that flag
  alone is not a reason to change the committed profile.
- Tier 5/6 single-invocation diagnostics use the smoke owned-window exception
  above.
  Their scaling or endurance evidence does not justify refreshing the release
  baseline, IDs, or performance targets from a partial or single-sample run.
- Before full validation or baseline refresh, clear `target/stress`,
  `target/bench_results.json`, and `target/bench_summary.md`.
- Never refresh `config/bench_baseline.json`, `config/perf_targets.json`, or
  `docs/development/bench-targets.md` from targeted runs or partial
  `latest.json` artifacts.
- Regenerate release IDs only for real ID changes, such as a workload-defining
  parameter change. Do not rename readable measurements to generic
  `operation` or `workload` suffixes.
- Keep host-sensitive local-disk KV and Stream sync rows as
  `storage_characterization`. Their memory-backed TCP and WebSocket lifecycle
  counterparts are the release regression gates; paired latency records remain
  report-only.
- `high_allocations` diagnostics are advisory for rows whose measured behavior
  is construction, parsing, or allocation. Keep allocation statistics visible;
  do not switch those rows to `record_external` solely to hide allocation
  diagnostics.

### Early-return trap

Wrong:

```rust
ctx.measure("workload", || {
	let response = actor.handle(request);
	let tx_id = match response {
		Ok(tx_id) => tx_id,
		Err(_) => return,
	};
	actor.commit(tx_id);
});
```

Correct:

```rust
ctx.measure("workload", || {
	let response = actor.handle(request);
	assert!(response.is_ok(), "benchmark setup must not fail");

	let tx_id = response.unwrap();
	actor.commit(tx_id);
});
```

The wrong pattern leaves completed operations counted while the measured work disappears, which produces inflated ops/sec and breaks the report.

## Reference examples

- **Tier 3 (system, no network):** [benches/tier3_system_rpc.rs](../../benches/tier3_system_rpc.rs) — real request → router → route actor → dispatch → worker actor → response → route actor; setup outside timed sections; only the request + drain loop inside; completed-operation counts match ops; no getters or fake work.
- **Tier 4 (integration):** `benches/tier4_kv_gate.rs` — direct (actor.handle only), encoded codec path, TCP (full roundtrip via TestClient), WebSocket (TestWebSocketClient), and multiclient rows where exposed; uses `shared_bench_runtime()`; setup (server, client, frames) outside measure.
