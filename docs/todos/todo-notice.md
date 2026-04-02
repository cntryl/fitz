# Notice

- Classification: Ephemeral.
- Goal: Make reconnect and restart loss explicit and keep subscription state bounded.
- Primary outcome: honest pub/sub semantics with strong cleanup tests.
- Status: Complete.

Notice close-out completed with the following correction work verified.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Explicitly ephemeral domain.
- Session-scoped subscription system.
- Loss on disconnect or broker restart is expected behavior.

## Current Reality

- Subscription indexes are in memory.
- Clients must re-subscribe after reconnect.
- The admin and doc surface must be careful not to imply durable pub/sub semantics.

## Focus

- Preserve the simple ephemeral pub/sub contract.
- Make reconnect and restart behavior explicit.
- Keep memory bounded as subscription counts grow.

## Concrete Tasks

- [x] Audit docs and comments for any wording that implies durable subscriptions or replay.
- [x] Add tests proving subscriptions are lost on disconnect and restart.
- [x] Keep session cleanup coverage strong for subscription removal.
- [x] Review wildcard subscription cost and add guardrails or explicit limits if needed.
- [x] Ensure admin surfaces describe current in-memory state only.

## Non-Goals

- Durable fanout history.
- Replay after reconnect.
- Persistent subscriber state.

## Verification

- [x] Disconnect tests prove subscriptions are removed.
- [x] Restart tests prove clients must re-subscribe.
- [x] Docs and admin wording reflect in-memory-only semantics.

## Files To Touch First

- `src/boot/domains/notice_sink.rs`
- `src/domains/notice/actor.rs`
- `src/domains/notice/mod.rs`
- `tests/notice_advanced.rs`
- `tests/notice_e2e.rs`
