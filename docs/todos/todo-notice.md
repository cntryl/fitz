# Notice

- Classification: Ephemeral.
- Goal: Make reconnect and restart loss explicit and keep subscription state bounded.
- Primary outcome: honest pub/sub semantics with strong cleanup tests.
- Status: Hardening complete. No performance follow-up is currently open.

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
- Crash-safe recovery of subscriptions or missed deliveries.

## Verification

- [x] Disconnect tests prove subscriptions are removed.
- [x] Restart tests prove clients must re-subscribe.
- [x] Docs and admin wording reflect in-memory-only semantics.

## Benchmark Findings

- 2026-04-03 refreshed tier3 results kept the core publish path strong: sustained fanout measured about 1.04M ops/s, single-star fanout scaled from about 1.05M ops/s at 1 subscriber to about 166k at 16, about 44.6k at 64, about 11.7k at 256, and about 3.0k at 1000, while double-star fanout followed the same curve and landed at about 2.1k ops/s at 1000 subscribers.
- Tier4 remained transport-dominated rather than actor-dominated: direct publish measured about 828k ops/s, WebSocket publish about 84.1k ops/s, TCP publish about 39.9k ops/s, the multiclient fanout publish case about 83.8k ops/s, and the multiclient subscriber-scaling cases ranged from about 36.5k to about 112k publish ops/s across the 1-, 16-, and 64-subscriber runs.

## Performance Follow-Up

- None from the current snapshot. Reopen Notice performance work only if wildcard-heavy 1k+ subscriber fanout becomes a first-class product requirement.

## Files To Touch First

- `src/boot/domains/notice_sink.rs`
- `src/domains/notice/actor.rs`
- `src/domains/notice/mod.rs`
- `tests/notice_advanced.rs`
- `tests/notice_e2e.rs`
