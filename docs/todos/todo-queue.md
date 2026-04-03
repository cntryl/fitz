# Queue

- Classification: Durable committed messages and indexes, ephemeral inflight coordination.
- Goal: tighten the failure model and stop memory growth from cold actor retention.
- Primary outcome: durable queue semantics that are explicit about what is and is not restart-safe.
- Status: Hardening complete. No queue-core performance follow-up is currently open.

Define a TDD-driven implementation plan for the following server correction work.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Durable domain for committed messages and indexes.
- Inflight delivery state, lease tokens, and some retry coordination remain ephemeral.
- Queue durability should stay bounded and practical; this domain does not need a rewrite.

## Current Reality

- Message bodies and core indexes are Midge-backed.
- Recovery can rebuild from storage, but cold-start rebuilds may still be expensive.
- The domain keeps warm per-queue actors in memory.
- Retry/idempotency semantics are not yet stated as precisely as the code needs.

## Focus

- Keep the durable core intact.
- Make the failure model explicit and honest.
- Stop cold actors from growing memory usage without bound.

## Concrete Tasks

- [x] Document exactly what survives restart: committed messages yes, inflight leases and tokens no.
- [x] Align idempotency and retry markers with the failure model actually promised.
- [x] Add idle eviction for cold per-queue actors in the boot sink.
- [x] Review comments and docs that overstate synchronous commit semantics when buffered writes are used.
- [x] Keep recovery paths fast when index metadata is present and well-validated when it is not.

## Non-Goals

- Exactly-once delivery guarantees.
- Durable inflight lease ownership across broker restart.
- A full queue architecture rewrite.

## Verification

- [x] Prove ack/delete success is only reported after durable commit.
- [x] Prove restart recovers committed messages and indexes.
- [x] Prove inflight tokens are treated as ephemeral across reconnect or restart.
- [x] Prove idle actor eviction does not lose committed queue state.

## Benchmark Findings

- 2026-04-03 refreshed queue numbers still show a stable core: tier3 sustained-load and high-contention cases measured about 202k-206k ops/s, ack/extend roundtrip about 71.7k-78.6k ops/s, routed enqueue/receive cleanup about 36.2k-37.5k ops/s, and the backlog-depth steady-state sweep stayed roughly flat at about 59.7k-62.5k ops/s from 1 through 1024 ready messages.
- Tier4 enqueue throughput still plateaued outside the queue core: direct and encoded enqueue both measured about 47.5k ops/s, WebSocket enqueue about 11.9k ops/s, TCP about 10.2k ops/s, and multiclient WebSocket enqueue improved from about 12.0k ops/s at 1 client to about 19.0k ops/s by 64 clients with most of the gain already present by 4-16 clients.

## Performance Follow-Up

- None in the queue core from the current snapshot. If higher end-to-end enqueue throughput becomes a requirement, target transport/session overhead before reopening queue actor design.

## Files To Touch First

- `src/domains/queue/actor.rs`
- `src/boot/domains/queue_sink.rs`
- `src/utils/idempotency.rs`
- `tests/queue_advanced.rs`
- `tests/queue_e2e.rs`
