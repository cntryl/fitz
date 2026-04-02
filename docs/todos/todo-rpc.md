### RPC

- Classification: Ephemeral.
- Goal: Make worker registration and pending request loss explicit.
- Primary outcome: honest low-latency ephemeral RPC semantics with better operational clarity.

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

- [ ] Audit docs and comments for any durability or replay implications and remove them.
- [ ] Add tests proving workers must re-register after restart.
- [ ] Add tests proving pending requests are lost on restart and handled as expected.
- [ ] Review backpressure and queue-full behavior so failures are explicit and observable.
- [ ] Ensure admin surfaces report live in-memory state only.

## Non-Goals

- Durable request queues.
- Durable worker registration.
- Exactly-once request execution across restart.

## Verification

- [ ] Restart tests prove worker registrations are lost.
- [ ] Restart tests prove pending requests are not durably recovered.
- [ ] Queue-full and timeout behavior are covered by tests or explicit docs.

## Files To Touch First

- `src/boot/domains/rpc_sink.rs`
- `src/domains/rpc/actor.rs`
- `src/domains/rpc/mod.rs`
- `tests/rpc_advanced.rs`
- `tests/rpc_e2e.rs`
