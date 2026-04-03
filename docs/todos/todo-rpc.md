# RPC

- Classification: Ephemeral.
- Goal: Make worker registration and pending request loss explicit.
- Primary outcome: honest low-latency ephemeral RPC semantics with better operational clarity.
- Status: Hardening complete. Additional performance and semantics follow-up may remain.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Explicitly ephemeral domain.
- In-memory worker registration and pending-request coordination.
- Loss on disconnect or broker restart is expected behavior.

## Current Reality

- Workers and pending requests live in memory.
- Restart loses registrations and in-flight request state.
- The contract needs to be explicit so clients do not assume durable retry or recovery semantics.

## Focus

- Keep RPC honest as a low-latency ephemeral facility.
- Make worker re-registration and request-loss semantics explicit.
- Improve operational clarity without pretending the domain is durable.

## Concrete Tasks

- [x] Audit docs and comments for any durability or replay implications and remove them.
- [x] Add tests proving workers must re-register after restart.
- [x] Add tests proving pending requests are lost on restart and handled as expected.
- [x] Review backpressure and queue-full behavior so failures are explicit and observable.
- [x] Ensure admin surfaces report live in-memory state only.

## Non-Goals

- Durable request queues.
- Durable worker registration.
- Exactly-once request execution across restart.
- Crash-safe recovery of in-flight requests or worker state.

## Verification

- [x] Restart tests prove worker registrations are lost.
- [x] Restart tests prove pending requests are not durably recovered.
- [x] Queue-full and timeout behavior are covered by tests or explicit docs.

## Benchmark Findings

- 2026-04-03 refreshed tier3 results still show the in-proc RPC path materially stronger than the transport path: dispatch-only worker-pool cases measured about 296k-303k ops/s, full roundtrip worker-pool cases about 211k-222k ops/s, and pending-cardinality steady state stayed effectively flat from 1 through 1000 pending at about 221k-232k ops/s.
- Tier4 is still transport/session dominated: direct request/response measured about 213k ops/s, encoded direct about 207k ops/s, WebSocket about 10.3k ops/s, TCP about 7.5k ops/s, and multiclient request throughput stayed roughly flat at about 17.1k-18.8k ops/s as worker count moved from 1 to 8.

## Performance Follow-Up

- [ ] Reduce transport/session overhead on the tier4 request path before revisiting sink-only concurrency changes; the current multiclient transport benches remain roughly flat as worker count moves from 1 to 8.
- [ ] Use `docs/todos/make-rpc-good.md` as the deeper production/performance backlog for fairness, timeout, streaming, and operational-trust work beyond this hardening pass.

No additional pending-cardinality follow-up is opened from the refreshed summary; the 1-1000 pending sweep stayed effectively flat.

## Files To Touch First

- `src/boot/domains/rpc_sink.rs`
- `src/domains/rpc/actor.rs`
- `src/domains/rpc/mod.rs`
- `tests/rpc_advanced.rs`
- `tests/rpc_e2e.rs`
