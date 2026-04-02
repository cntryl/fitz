# Schedule

- Requires real storage changes.
- TTL-backed primary rows are the wrong foundation for durable recurring schedules.
- Boot behavior and reschedule failure handling both need structural correction.

Define a TDD-driven implementation plan for the following server correction work.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Durable domain.
- RouteFamily can remain process-local for now; schedule durability must not depend on control-plane persistence.
- Sessions are ephemeral, but persisted schedules must survive broker restart and downtime.

## Current Reality

- Primary persisted rows are TTL-backed `sched:m...` records.
- Restart recovery depends on those rows still existing when the broker comes back.
- The live sink only instantiates actors lazily per family, so persisted schedules are not loaded until that family receives traffic.
- Fire/reschedule mutates in-memory state before checking whether persistence actually succeeded.

## Focus

- Make schedule definitions survive downtime unconditionally.
- Make boot load durable schedule state before the schedule domain starts firing.
- Remove silent loss paths from fire and reschedule.

## Concrete Tasks

- [x] Split durable schedule definition storage from the next-fire index.
- [x] Make the next-fire index rebuildable instead of authoritative.
- [x] Remove TTL from the durable schedule definition row.
- [x] Fail or roll back fire/reschedule when persistence fails instead of silently advancing in memory.
- [x] Preload persisted schedules during boot instead of waiting for the first request in a family.
- [x] Keep admin schedule snapshots aligned with the preloaded actor state.
- [x] Add restart and downtime regressions proving schedules do not disappear.

## Non-Goals

- Distributed scheduler coordination across multiple brokers.
- Durable subscriber delivery or durable outbox semantics for schedule notifications.
- Control-plane persistence for RouteFamily assignment.

## Verification

- [x] Restart the broker without sending any schedule-domain traffic and prove persisted schedules still exist and fire.
- [x] Keep the broker down longer than the old grace period and prove recurring schedules survive.
- [x] Force a persistence failure on reschedule and prove the in-memory actor does not silently advance.
- [x] Run `cargo test` for schedule-focused tests after the redesign lands.

## Files To Touch First

- `src/domains/schedule/store.rs`
- `src/domains/schedule/actor.rs`
- `src/boot/domains/schedule_sink.rs`
- `tests/schedule_advanced.rs`
- `tests/schedule_e2e.rs`
