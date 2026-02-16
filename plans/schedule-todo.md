# Schedule domain — TODO

Summary
- Make the Schedule domain robust: cron parsing, next-fire calculation, subscribe/unsubscribe lifecycle, and timer firing semantics must be locked in with focused unit and integration tests and realistic benches.

Goals
- Validate cron parser edge cases and next-fire calculations.
- Ensure subscriptions and fanout from schedule fires are tested and bench‑covered.
- Add benches for timer firing hot-path and subscription fanout.

Files to inspect
- `src/domains/schedule/actor.rs`
- `src/domains/schedule/protocol.rs`
- `src/domains/schedule/store.rs`
- `src/domains/schedule/session.rs`
- Tests: `tests/schedule_e2e_basic.rs`
- Benches: `benches/tier1_hotpath_schedule.rs`, `benches/tier3_system_schedule.rs`, `benches/tier4_integration_schedule.rs`

Required unit tests (examples)
- `should_parse_cron_every_minute()` (exists — verify edge coverage)
- `should_reject_invalid_cron_range()`
- `should_calculate_next_fire_time_for_edge_cases()`
- `should_preserve_payload_in_schedule()`

Integration tests to add/verify
- `should_fire_schedule_and_deliver_notify_to_subscribers()`
- `should_handle_many_schedules_with_minimal_jitter()`

Bench targets
- Tier1: timer-firing hot-path (minimal work per tick)
- Tier2: subscription fanout per scheduled fire
- Tier3/4: system-level scheduling throughput and durability under many schedules

PR plan (2 commits)
1. Add parser edge-case tests and subscription lifecycle tests (1–2 hr).
2. Add/extend tier1/tier2 benches and documentation (1–2 hr).

Acceptance criteria
- Cron parser edge cases are covered and correct
- Timer-firing benches measure low-latency hot-path
- All schedule tests follow `should_*` naming and AAA where required
