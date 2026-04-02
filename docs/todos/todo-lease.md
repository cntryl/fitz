# Lease

- Classification: Ephemeral.
- Goal: Make it explicit that this is an in-memory coordination primitive, not a durable lease service.
- Primary outcome: honest single-process lease semantics with cleanup and restart-loss tests.
- Status: Complete.

Lease close-out completed with the following correction work verified.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Explicitly ephemeral domain.
- In-memory coordination primitive, not a durable or distributed lease service.
- Loss on disconnect or broker restart is expected behavior.

## Current Reality

- Lease state lives in memory.
- Production boot behavior and actor-path intent are not perfectly aligned.
- It is easy for comments or API expectations to sound stronger than the implementation.

## Focus

- Remove ambiguity.
- Make the ephemeral contract explicit in docs, tests, and admin surfaces.
- Keep the implementation honest about single-process coordination only.

## Concrete Tasks

- [x] Audit docs and comments for any durable or distributed lease claims and remove them.
- [x] Add tests proving leases disappear on restart and that this is expected.
- [x] Add tests proving disconnect cleanup releases session-owned lease state as intended.
- [x] Align the boot sink and actor wording around ephemeral semantics.
- [x] Decide whether fencing tokens are only meaningful within one running process and document that clearly.

## Non-Goals

- Crash-safe lease recovery.
- Cross-node fencing guarantees.
- Persistent wait queues or durable lock handoff.

## Verification

- [x] Restart tests show lease state is lost.
- [x] Disconnect tests show session-owned leases are cleaned up.
- [x] Docs and admin wording no longer imply durable recovery.

## Files To Touch First

- `src/boot/domains/lease_sink.rs`
- `src/domains/lease/actor.rs`
- `src/domains/lease/mod.rs`
- `tests/lease_advanced.rs`
- `tests/lease_e2e.rs`
