# Schedule

- Requires real storage changes.
- TTL-backed primary rows are the wrong foundation for durable recurring schedules.
- Boot behavior and reschedule failure handling both need structural correction.
- Status: Hardening complete. Single-create performance follow-up may remain if that path becomes a product requirement.

Define a TDD-driven implementation plan for the following server correction work.

### Important context:

- Do not repeat or reinvent domain-level requirement lists unless absolutely necessary.
- Your job is to turn the existing concerns and checklists into an implementation strategy and test strategy.

- When you are done this domain should be world-class

## Classification

- Durable domain.
- RouteFamily selection can remain a process/deployment concern for now. This pass does not add a separate persisted control-plane service that coordinates family assignment, and schedule durability must not depend on broker-local memory about that choice.
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
- A separate control-plane service that persists and coordinates RouteFamily assignment across brokers.

## Verification

- [x] Restart the broker without sending any schedule-domain traffic and prove persisted schedules still exist and fire.
- [x] Keep the broker down longer than the old grace period and prove recurring schedules survive.
- [x] Force a persistence failure on reschedule and prove the in-memory actor does not silently advance.
- [x] Run `cargo test` for schedule-focused tests after the redesign lands.

## Benchmark Findings

- The refreshed report separates hot in-proc schedule operations from durable create cost: tier3 create measured about 1.73M ops/s, cancel about 2.32M ops/s, list(10) about 233M ops/s, scan-and-fire measured about 258k ops/s for 1000 all-ready schedules and about 628k ops/s for 1000 partially ready schedules, while the mixed-workload scenario measured about 1.2k ops/s.
- The end-to-end create path still looks store-bound rather than transport-bound: the latest tier4 direct/TCP/WebSocket create benches cluster around about 570-622 creates/s, multiclient creates measured about 589 creates/s, and WebSocket batch create remains the throughput escape hatch at about 16.2k creates/s.
- Current Criterion `scan_and_fire` rows remain noisy/untrustworthy in `target/bench_summary.md`, so they are not strong enough to open new work by themselves.

## Performance Follow-Up

- [ ] If single-schedule create throughput becomes a product requirement, target `ScheduleStore::insert` transaction shape and persistence cost before spending time on transport framing; that same path is the likely bound on the current low mixed-workload result.

## Files To Touch First

- `src/domains/schedule/store.rs`
- `src/domains/schedule/actor.rs`
- `src/boot/domains/schedule_sink.rs`
- `tests/schedule_advanced.rs`
- `tests/schedule_e2e.rs`
