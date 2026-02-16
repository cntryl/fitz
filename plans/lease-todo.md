# Lease domain — TODO

Summary
- Lock in correctness for acquire/renew/release semantics, fencing tokens, TTL handling, and contention behavior; lock in benches that measure contention and renew hot paths.

Goals
- Enforce monotonic fencing tokens and TTL expiry semantics.
- Add targeted tests for contention and mock-clock behavior.
- Ensure benches measure acquire/renew latency under contention.

Files to inspect
- `src/domains/lease/lease_actor.rs`
- `src/domains/lease/guard.rs`
- `src/domains/lease/protocol.rs`
- Tests: `tests/lease_*`
- Benches: `benches/tier1_hotpath_lease.rs`, `benches/tier1_hotpath_lease_queueing.rs`, `benches/tier3_system_lease.rs`

Required unit tests (examples)
- `should_issue_monotonic_fencing_tokens()` (already present — verify coverage)
- `should_handle_concurrent_acquire_race()` — single behavior focused
- `should_reject_renew_after_expiry()` — verify TTL boundary
- `should_isolate_leases_by_realm_and_area()`

Integration tests to add/verify
- `should_scale_under_high_contention_queueing()` (queueing/lease interplay)
- `should_recover_lease_state_after_restart()`

Bench targets
- Tier1: acquire/renew/release hot-path (single-op latency)
- Tier2: contention microbench (many owners competing)
- Tier3: system bench including session/timer interactions
- Tier4: integration with transport and client retry semantics

PR plan (2–3 commits)
1. Add/tests for contention and mock clock edge cases (2 hr).
2. Add contention benches and tune sampling (2–3 hr).
3. Documentation updates and acceptance tests (1 hr).

Acceptance criteria
- All lease unit tests asserting fencing/TTL semantics exist and pass
- Bench measures acquire/renew under contention and shows deterministic behavior
- No async in domain code
