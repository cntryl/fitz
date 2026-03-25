# Stress benchmark contract (cntryl_stress)

This document defines the contract for Tier 3 and Tier 4 benchmarks using the `cntryl_stress` framework with `#[stress_test]` macros. All stress benches must follow these rules.

## Tier 3 rules (system benchmarks — no network)

- Test the domain actor **directly**, in-process, **no TCP/WS**.
- All setup (actor creation, subscriptions, pre-population, payload construction) goes **OUTSIDE** `ctx.measure`.
- Only the operation being measured goes **INSIDE** `ctx.measure`.
- `ctx.set_elements(n)` must reflect the **actual number of meaningful operations** inside the measure block.
- Each scenario should run long enough to be statistically useful; aim for at least 3s of measured work before the report accepts it.
- Every scenario must exercise **real actor logic** — no `black_box` on counters, no fake loops, no measuring `pending_count()`.
- If testing fanout, actually subscribe real sinks and publish real messages.
- If testing scan_and_fire, schedules must actually be due to fire — bypass or reset the dedup window so fires actually occur.
- Tag with `ctx.tag("scenario", "name")` matching the scenario name in the stress results.

## Tier 4 rules (integration benchmarks — full stack)

- Four layers per domain: **direct**, **encoded**, **tcp**, **websocket**, **multiclient**.
- **direct**: `actor.receive()` (or domain handle) only, no frames, no network — establishes the in-process baseline.
- **encoded**: Build the TLV frame **and** send it through a **codec decode path** into the actor — measures serialization cost. Do **not** just build the frame and `let _ = &frame` it. If you cannot decode through the actor, this layer is invalid and should be omitted.
- **tcp**: Full roundtrip through TestServer over TCP — encode frame, send, receive response.
- **websocket**: Same as tcp but over WS.
- **multiclient**: N clients (typically 10) making requests — must use `tokio::join!` or `FuturesUnordered` for **actual concurrency**, **not** a sequential for loop. Label it concurrent only if it is actually concurrent.
- Network tests must use a **shared runtime** (`shared_bench_runtime()`) consistently across all domains.
- `ctx.set_elements(n)` = number of meaningful operations per measure iteration (e.g. 2 for begin+append, 1 for single enqueue).
- **RPC tier4** must test the full **request → worker dispatch → response** cycle over the wire, not just subscribe.

## Both tiers

- Never measure setup, teardown, or frame construction.
- Never use `black_box` as a substitute for real work.
- Never let a failure short-circuit the measured work; if setup can fail, record it and assert after the measurement block.
- Never `return` early from `ctx.measure`. If the measured work can fail, record the failure in a local flag or result and assert after the closure so the timer still covers the full intended work.
- Scenario names in `ctx.tag` must match what appears in the stress results output.
- Each test must be independently runnable and deterministic.

### Early-return trap

Wrong:

```rust
ctx.measure(|| {
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
ctx.measure(|| {
	let response = actor.handle(request);
	assert!(response.is_ok(), "benchmark setup must not fail");

	let tx_id = response.unwrap();
	actor.commit(tx_id);
});
```

The wrong pattern leaves `ctx.set_elements(n)` counted while the measured work disappears, which produces inflated ops/sec and breaks the report.

## Reference examples

- **Tier 3 (system, no network):** [benches/tier3_system_rpc.rs](../../benches/tier3_system_rpc.rs) — real request → router → route actor → dispatch → worker actor → response/ack → route actor; setup outside `ctx.measure`; only the request + drain loop inside; `ctx.set_elements(n)` matches ops; no getters or fake work.
- **Tier 4 (integration):** [benches/tier4_integration_kv.rs](../../benches/tier4_integration_kv.rs) — direct (actor.handle only), tcp (full roundtrip via TestClient), websocket (TestWebSocketClient), multiclient; uses `shared_bench_runtime()`; setup (server, client, frames) outside measure.
