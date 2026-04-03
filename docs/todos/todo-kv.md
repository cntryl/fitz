# KV

- Classification: Durable committed data, ephemeral transaction state.
- Goal: Separate the durable committed-data story from the non-durable transaction story.
- Primary outcome: no more accidental implication that transactions survive disconnect or restart.
- Status: Hardening complete. No KV-core performance follow-up is currently open.

Define a TDD-driven implementation plan for the following server correction work.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Durable committed data.
- Open transactions and resource-lock coordination are session-local and process-local today.
- RouteFamily selection can remain a process/deployment concern for now. This pass does not add a separate persisted control-plane service that coordinates family assignment, and committed-data durability still comes from the storage engine.

## Current Reality

- KV is a thin wrapper over Midge for committed reads and writes.
- Active transaction handles live in memory.
- Session loss means transaction loss.
- The public surface can be read as stronger than the actual transaction semantics.

## Focus

- Keep the durable committed-data story solid.
- State transaction limits explicitly instead of implying broker-grade recovery.
- Preserve bounded memory by keeping active transaction state session-scoped only.

## Concrete Tasks

- [x] Document committed-data durability separately from transaction durability.
- [x] Make session-local transaction loss on disconnect explicit in code comments, docs, and tests.
- [x] Review admin and API surfaces so they do not imply durable transaction recovery.
- [x] Tighten tests around RouteFamily isolation of committed data.
- [x] Keep lock and transaction metadata bounded to active sessions only.

## Non-Goals

- Durable transaction logs.
- Cross-session transaction recovery.
- Distributed transaction coordination.

## Verification

- [x] Prove committed values survive restart.
- [x] Prove open transactions are lost on disconnect or restart.
- [x] Prove RouteFamily isolation for the same logical key across families.
- [x] Run KV-focused tests after any semantic cleanup.

## Benchmark Findings

- 2026-04-03 refreshed tier3 committed-data throughput remained strong across contention shapes: about 3.17M ops/s for the single-family intensive case, about 4.06M ops/s for dual-family interleaving, about 4.62M ops/s for the mixed read/write case, and about 5.00M ops/s for triple-family contention.
- Tier4 was transport-dominated rather than KV-core dominated: direct begin/put/rollback measured about 1.21M ops/s, encoded direct about 642k ops/s, WebSocket about 20.6k ops/s, TCP about 14.7k ops/s, and multiclient concurrent transactions about 49.5k ops/s.

## Performance Follow-Up

- None from the current snapshot. Any future KV uplift should target session/transport overhead before reopening the KV core or storage model.

## Files To Touch First

- `src/domains/kv/actor.rs`
- `src/boot/domains/kv_sink.rs`
- `tests/kv_advanced.rs`
- `tests/kv_e2e.rs`
