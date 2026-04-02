# KV

- Classification: Durable committed data, ephemeral transaction state.
- Goal: Separate the durable committed-data story from the non-durable transaction story.
- Primary outcome: no more accidental implication that transactions survive disconnect or restart.

Define a TDD-driven implementation plan for the following server correction work.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Durable committed data.
- Open transactions and resource-lock coordination are session-local and process-local today.
- RouteFamily can remain process-local for now; committed data durability comes from the storage engine.

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

## Files To Touch First

- `src/domains/kv/actor.rs`
- `src/boot/domains/kv_sink.rs`
- `tests/kv_advanced.rs`
- `tests/kv_e2e.rs`
