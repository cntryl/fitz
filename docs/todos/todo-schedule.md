# Schedule

This file defines Schedule-specific contract detail and proof points. For Fitz-wide domain ownership, interaction rules, complexity budgets, and future feature admission, use [../development/domain-boundaries-spec.md](../development/domain-boundaries-spec.md) together with [todo-all.md](todo-all.md).

## A. Domain Purpose Statement

Schedule provides durable timing intent for future route-triggered work.

- Problem solved: durable create, upsert, cancel, and future execution of cron-based schedules.
- Optimized for: persisted schedule definitions, boot-time preload, and explicit due-time handling.
- Not trying to do: durable subscriber delivery, durable event history of every fire, or replay of every missed execution during downtime.
- Adjacent overlap: Notice may carry live schedule notifications, but Notice does not make schedules durable. Stream may store execution history if the application writes it.
- Strict boundary: Schedule owns durable timing definitions. It does not own durable downstream event recovery.

## B. Semantic Contract

Clients can rely on the following:

- Schedule definitions are identified by route.
- Create and upsert persist schedule definition state.
- Cancel is explicit and idempotent for a missing route.
- Persisted schedules are preloaded on broker start before schedule traffic is required.
- Due-time computation comes from the parsed cron definition.
- Missed executions are skipped forward to the next future fire time rather than replayed one by one after downtime.

Server guarantees:

- Schedule definitions are durable.
- Pending due fire claims are durable broker-internal state until acknowledged.
- Restart reloads persisted schedules and pending fire claims.
- Schedule notifications and schedule subscriptions remain live session-scoped delivery state only.
- Forward clock jumps do not replay every missed interval.
- Backward clock jumps do not fire schedules early.

Execution-state semantics:

- The schedule definition and next durable fire point are authoritative.
- Pending fire claims represent due work that the broker has durably claimed for publish.
- Successful publish acknowledgement clears the pending fire claim.
- Live subscriber receipt is not part of the durable schedule contract.

Replay and retry semantics:

- Fitz does not replay every missed fire after downtime.
- Fitz normalizes overdue schedules forward.
- Durable pending fire claims can survive restart until the broker finishes its internal publish-and-ack path.
- If the application needs a durable audit trail of schedule executions, it must also write to Stream.

Intentionally unsupported:

- Durable Notice-style subscriber delivery.
- Replay of all missed executions after downtime.
- Distributed multi-broker scheduler coordination.
- A separate control plane for RouteFamily assignment.

## C. Non-Negotiable Invariants

- Invariant: due work is never fired early.
	- Why it matters: timing correctness is the primary trust boundary of Schedule.
	- How it fails: clock handling or ready-scan logic publishes before due time.
	- How to test it: [tests/schedule_advanced.rs](../../tests/schedule_advanced.rs) `should_not_fire_given_backward_epoch_jump_before_due_time`.

- Invariant: missed executions are skipped forward rather than replayed implicitly.
	- Why it matters: Fitz must be explicit that Schedule is not a missed-fire replay engine.
	- How it fails: startup or clock jump replays every overdue interval.
	- How to test it: [tests/schedule_advanced.rs](../../tests/schedule_advanced.rs) `should_skip_missed_occurrences_given_forward_epoch_jump` and [src/domains/schedule/actor.rs](../../src/domains/schedule/actor.rs) unit coverage `should_skip_missed_execution_given_overdue_schedule_on_preload`.

- Invariant: persisted schedule definitions survive restart and are preloaded before schedule traffic.
	- Why it matters: durable timing intent must not depend on a later request to wake the domain up.
	- How it fails: restart hides persisted schedules until fresh traffic arrives.
	- How to test it: [tests/schedule_e2e.rs](../../tests/schedule_e2e.rs) `should_preload_persisted_schedules_before_schedule_traffic_after_restart`.

- Invariant: cancel and upsert races preserve one clear durable outcome.
	- Why it matters: route-based identity is the core mutation model.
	- How it fails: duplicate rows or stale next-fire state survive after replacement.
	- How to test it: [tests/schedule_basics.rs](../../tests/schedule_basics.rs) `should_upsert_schedule_by_route`, `should_keep_single_schedule_given_identical_create_upsert`, and [tests/schedule_advanced.rs](../../tests/schedule_advanced.rs) `should_replace_schedule_preserving_ordering`.

- Invariant: persistence failure must not silently advance live schedule state.
	- Why it matters: a failed durable mutation must not look successful.
	- How it fails: in-memory actor moves forward even though durable store write failed.
	- How to test it: required missing focused regression such as `should_not_advance_schedule_state_given_persistence_failure`.

- Invariant: restart does not silently lose durable schedules or pending fire claims.
	- Why it matters: Schedule is the durable timing-intent surface.
	- How it fails: persisted rows disappear from boot state or pending claims are dropped.
	- How to test it: [tests/schedule_e2e.rs](../../tests/schedule_e2e.rs) `should_preload_persisted_schedules_before_schedule_traffic_after_restart` and pending-fire store tests in [src/domains/schedule/store.rs](../../src/domains/schedule/store.rs).

## D. Anti-Goals / What This Domain Must Not Become

- Schedule must not become vague best-effort timers.
- Schedule must not imply durable subscriber delivery or outbox semantics.
- Schedule must not blur skipped overdue executions into replay guarantees.
- Schedule must not become a hidden history store for execution records.
- Schedule must not depend on lazy post-traffic warmup for durable correctness.

## E. Failure Semantics

- Client disconnect: live schedule subscriptions disappear; persisted schedule definitions remain.
- Server restart: persisted schedules and pending fire claims reload; live subscriptions do not.
- Storage failure on create, upsert, cancel, or reschedule: mutation fails and must not silently advance live state.
- Invalid cron: request rejected.
- Backpressure or downstream subscriber loss: durable schedule definition remains correct, but subscriber delivery remains live-only.
- Clock jump forward: missed intervals are skipped forward.
- Clock jump backward: schedules do not fire early.

## F. Observability Requirements

Operators must be able to inspect:

- persisted schedules active
- next due time
- pending fire claims
- current live schedule subscription count
- execution rate
- create, cancel, and publish-failure counters
- preload status after restart

Current surface:

- Global stats include `schedules_active`, `executions_per_minute`, `subscriptions_active`, and `pending_fires`.
- Prometheus exports `fitz_schedule_active`, `fitz_schedule_executions_per_minute`, `fitz_schedule_subscriptions_active`, and `fitz_schedule_pending_fires`.
- Admin schedule views are preloaded from persisted definitions at boot.

Current gaps to keep explicit:

- [src/api/admin/stats.rs](../../src/api/admin/stats.rs) has a stub route for the per-domain Schedule stats endpoint; it currently returns not_implemented. The domain data is not yet populated.
- Broader admin docs already note that `last_run` and `executions_total` are not fully authoritative in the current round; that caveat must remain visible until fixed.
- Metrics do not yet expose overdue-normalization count or persistence-failure counters.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/schedule_basics.rs](../../tests/schedule_basics.rs) `should_upsert_schedule_by_route`
	- [tests/schedule_basics.rs](../../tests/schedule_basics.rs) `should_cancel_schedule_by_route`
	- [tests/schedule_advanced.rs](../../tests/schedule_advanced.rs) `should_replace_schedule_preserving_ordering`
- Restart and recovery tests:
	- [tests/schedule_e2e.rs](../../tests/schedule_e2e.rs) `should_preload_persisted_schedules_before_schedule_traffic_after_restart`
- Race and timing tests:
	- [tests/schedule_advanced.rs](../../tests/schedule_advanced.rs) `should_skip_missed_occurrences_given_forward_epoch_jump`
	- [tests/schedule_advanced.rs](../../tests/schedule_advanced.rs) `should_not_fire_given_backward_epoch_jump_before_due_time`
- Integration tests:
	- [tests/schedule_e2e.rs](../../tests/schedule_e2e.rs) create, cancel, batch create, payload preservation, and transport cases
- Benchmark and stress tests:
	- tier3 Schedule create, cancel, list, and scan-and-fire benches
	- tier4 Schedule create benches

## H. Cross-Domain Boundaries

- Schedule versus Notice: Schedule definitions are durable; schedule notifications are still live-only Notice-style delivery.
- Schedule versus Stream: Stream is where execution history belongs if the application needs it.
- Schedule versus Queue: use Queue when triggered work must be durably reserved and acknowledged.
- Schedule versus Lease: use Lease when scheduled work also needs explicit local ownership coordination.

## I. Ambiguity Risks

- Users may assume overdue schedules replay all missed intervals unless the skip-forward rule is explicit.
- Schedule notifications can be misread as durable delivery if docs blur Schedule and Notice.
- Route-based upsert semantics can be misread as append-only creation unless identity-by-route is stated clearly.
- Admin `last_run` and `executions_total` fields can be overtrusted if their current non-authoritative status is hidden.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `Schedule stores durable timing intent and preloads persisted schedules on broker start. It does not guarantee durable subscriber delivery or replay every missed fire after downtime.`
- Use this sentence when comparing Schedule and Stream: `Schedule decides when work should fire. Stream records what happened if the application needs history.`
- Keep this sentence in clock-related docs: `Overdue schedules normalize forward to the next future fire time rather than replaying every missed interval.`
