# TODO - Stream

Define a TDD-driven implementation plan for the following server correction work.

Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Durable domain.
- RouteFamily can remain process-local for now; stream durability must come from Midge-backed state, not control-plane state.
- Append sessions are ephemeral, but committed events and sequencing must remain correct across restart.

## Current Reality

- Committed event records are persisted.
- The live boot sink still owns important sequencing state in memory.
- Resource sequencing is closer to durable than area and realm sequencing.
- Actor-path intent and boot-sink production behavior are still divergent.

## Focus

- Establish one authoritative sequencing path.
- Make resource, area, and realm offsets restart-safe.
- Clean up abandoned append state on disconnect and restart.

## Concrete Tasks

- [ ] Pick one authoritative production implementation path and thin the adapter around it.
- [ ] Move area and realm sequencing out of process-local sink maps into durably reconstructable state.
- [ ] Define whether consumer cursors are client-managed only or a durable server feature.
- [ ] Abort or clean up abandoned append sessions on disconnect.
- [ ] Add restart recovery for any staged append state that must not leak or corrupt offsets.
- [ ] Prove monotonic offsets across multiple resources in the same area and realm.

## Non-Goals

- Durable consumer groups unless the cursor model is explicitly designed first.
- Multi-node stream coordination.
- Expanding the stream API surface before sequencing and recovery are correct.

## Verification

- [ ] Restart tests prove resource offsets remain monotonic.
- [ ] Restart tests prove area and realm offsets remain monotonic.
- [ ] Disconnect tests prove abandoned append sessions are cleaned up.
- [ ] Crash/restart tests prove committed events stay readable and staged writes do not corrupt future appends.

## Files To Touch First

- `src/boot/domains/stream_sink.rs`
- `src/domains/stream/store.rs`
- `src/domains/stream/actor.rs`
- `src/domains/stream/area_actor.rs`
- `src/domains/stream/realm_actor.rs`
- `tests/stream_advanced.rs`
- `tests/stream_basics.rs`
