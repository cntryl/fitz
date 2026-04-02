# TODO - KV

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
- [ ] Document committed-data durability separately from transaction durability.
- [ ] Make session-local transaction loss on disconnect explicit in code comments, docs, and tests.
- [ ] Review admin and API surfaces so they do not imply durable transaction recovery.
- [ ] Tighten tests around RouteFamily isolation of committed data.
- [ ] Keep lock and transaction metadata bounded to active sessions only.

## Non-Goals
- Durable transaction logs.
- Cross-session transaction recovery.
- Distributed transaction coordination.

## Verification
- [ ] Prove committed values survive restart.
- [ ] Prove open transactions are lost on disconnect or restart.
- [ ] Prove RouteFamily isolation for the same logical key across families.
- [ ] Run KV-focused tests after any semantic cleanup.

## Files To Touch First
- `src/domains/kv/actor.rs`
- `src/boot/domains/kv_sink.rs`
- `tests/kv_advanced.rs`
- `tests/kv_e2e.rs`