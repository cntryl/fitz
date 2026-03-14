# Stress benchmark contract (cntryl_stress)

This document defines the contract for Tier 3 and Tier 4 benchmarks using the `cntryl_stress` framework with `#[stress_test]` macros. All stress benches must follow these rules.

## Tier 3 rules (system benchmarks — no network)

- Test the domain actor **directly**, in-process, **no TCP/WS**.
- All setup (actor creation, subscriptions, pre-population, payload construction) goes **OUTSIDE** `ctx.measure`.
- Only the operation being measured goes **INSIDE** `ctx.measure`.
- `ctx.set_elements(n)` must reflect the **actual number of meaningful operations** inside the measure block.
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
- Scenario names in `ctx.tag` must match what appears in the stress results output.
- Each test must be independently runnable and deterministic.

## Reference examples

- **Tier 3 (system, no network):** [benches/tier3_system_rpc.rs](../../benches/tier3_system_rpc.rs) — real request → router → route actor → dispatch → worker actor → response/ack → route actor; setup outside `ctx.measure`; only the request + drain loop inside; `ctx.set_elements(n)` matches ops; no getters or fake work.
- **Tier 4 (integration):** [benches/tier4_integration_kv.rs](../../benches/tier4_integration_kv.rs) — direct (actor.handle only), tcp (full roundtrip via TestClient), websocket (TestWebSocketClient), multiclient; uses `shared_bench_runtime()`; setup (server, client, frames) outside measure.
