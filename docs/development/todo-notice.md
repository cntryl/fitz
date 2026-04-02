# TODO - Notice

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
- [ ] Audit docs and comments for any wording that implies durable subscriptions or replay.
- [ ] Add tests proving subscriptions are lost on disconnect and restart.
- [ ] Keep session cleanup coverage strong for subscription removal.
- [ ] Review wildcard subscription cost and add guardrails or explicit limits if needed.
- [ ] Ensure admin surfaces describe current in-memory state only.

## Non-Goals
- Durable fanout history.
- Replay after reconnect.
- Persistent subscriber state.

## Verification
- [ ] Disconnect tests prove subscriptions are removed.
- [ ] Restart tests prove clients must re-subscribe.
- [ ] Docs and admin wording reflect in-memory-only semantics.

## Files To Touch First
- `src/boot/domains/notice_sink.rs`
- `src/domains/notice/actor.rs`
- `src/domains/notice/mod.rs`
- `tests/notice_advanced.rs`
- `tests/notice_e2e.rs`