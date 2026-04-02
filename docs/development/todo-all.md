# TODO - All Domains

## Purpose

This file is the top-level execution summary for domain hardening work. Use it to choose the next domain to work on without re-deriving the scope each time.

The per-domain working files are:

- `todo-notice.md`
- `todo-lease.md`
- `todo-rpc.md`
- `todo-kv.md`
- `todo-queue.md`
- `todo-schedule.md`
- `todo-stream.md`

## Working Assumptions

- RouteFamily assignment can remain process-local until a future control-plane implementation exists.
- Lease, Notice, and RPC are intentionally ephemeral.
- KV, Queue, Schedule, and Stream are the domains that need durable, restart-safe, bounded-memory behavior.
- Sessions are ephemeral across the system, so session-owned state is expected to disappear on disconnect unless a domain explicitly persists its own committed state.

## Complexity Ranking

From easiest to hardest:

1. Notice
2. Lease
3. RPC
4. KV
5. Queue
6. Schedule
7. Stream

## Why This Ranking

### 1. Notice

- Already an ephemeral domain.
- Main work is contract cleanup, tests, and admin-surface honesty.
- No storage redesign is required.

### 2. Lease

- Also ephemeral if treated honestly.
- Main work is clarifying single-process coordination semantics and proving disconnect and restart loss in tests.
- Slightly more nuanced than Notice because the API can sound stronger than the implementation.

### 3. RPC

- Still ephemeral, but has more moving parts than Notice or Lease.
- Worker registration, pending requests, queue-full behavior, and timeout handling all need explicit semantics.
- Still cheaper than any durable-domain redesign.

### 4. KV

- Durable committed data path already exists through Midge.
- Most remaining work is semantic clarity around open transactions and resource locks being session-local.
- This is a cleanup and hardening pass, not a storage-model rewrite.

### 5. Queue

- Durable core is already present.
- Remaining work touches real runtime behavior: idempotency, eviction of cold actors, and making the failure model explicit.
- More moving parts than KV, but still not a foundational redesign.

### 6. Schedule

- Requires real storage changes.
- TTL-backed primary rows are the wrong foundation for durable recurring schedules.
- Boot behavior and reschedule failure handling both need structural correction.

### 7. Stream

- Hardest domain.
- There is still split authority between live sink behavior and the intended actor or store model.
- Restart-safe sequencing across resource, area, and realm is an architectural problem, not a small fix.

## Recommended Execution Order

If the goal is to knock out the easiest work first and build momentum without opening the hardest redesigns too early, use this order:

1. Notice
2. Lease
3. RPC
4. KV
5. Queue
6. Schedule
7. Stream

## Domain Summary

### Notice

- Classification: Ephemeral.
- Goal: Make reconnect and restart loss explicit and keep subscription state bounded.
- Primary outcome: honest pub/sub semantics with strong cleanup tests.

### Lease

- Classification: Ephemeral.
- Goal: Make it explicit that this is an in-memory coordination primitive, not a durable lease service.
- Primary outcome: honest single-process lease semantics with cleanup and restart-loss tests.

### RPC

- Classification: Ephemeral.
- Goal: Make worker registration and pending request loss explicit.
- Primary outcome: honest low-latency ephemeral RPC semantics with better operational clarity.

### KV

- Classification: Durable committed data, ephemeral transaction state.
- Goal: Separate the durable committed-data story from the non-durable transaction story.
- Primary outcome: no more accidental implication that transactions survive disconnect or restart.

### Queue

- Classification: Durable committed messages and indexes, ephemeral inflight coordination.
- Goal: tighten the failure model and stop memory growth from cold actor retention.
- Primary outcome: durable queue semantics that are explicit about what is and is not restart-safe.

### Schedule

- Classification: Durable.
- Goal: replace the current TTL-backed storage model with something that survives real downtime.
- Primary outcome: persisted schedules that survive restart and load before traffic.

### Stream

- Classification: Durable committed events, but incomplete durable sequencing model.
- Goal: establish one authoritative sequencing path with restart-safe counters.
- Primary outcome: monotonic resource, area, and realm sequencing without in-memory truth leaks.

## Decision Rule

Use this simple rule when choosing the next task:

- Start with Notice, Lease, and RPC if you want fast clarity wins.
- Move to KV and Queue when you want hardening without foundational redesign.
- Leave Schedule and Stream for dedicated implementation rounds because they are true structural corrections.

## Next Step

Pick the first unchecked domain from the recommended execution order and work from its dedicated `todo-{domain}.md` file until its verification section is complete.