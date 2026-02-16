# Queue domain — TODO

Summary
- Verify message semantics, competing consumers, visibility timeouts, ack/nack semantics, and ensure benches measure enqueue/dequeue hot paths.

Goals
- Add focused unit tests for competing consumers and persistence.
- Ensure bench coverage for dequeue throughput and ack latency.

Files to inspect
- `src/domains/queue/queue_actor.rs`
- `src/domains/queue/session.rs`
- `src/domains/queue/protocol.rs`
- Tests: `tests/queue_*`
- Benches: `benches/tier1_hotpath_queue.rs`, `benches/tier3_system_queue.rs`, `benches/tier4_integration_queue.rs`

Required unit tests (examples)
- `should_enqueue_and_dequeue_in_fifo_order()`
- `should_handle_competing_consumers_correctly()`
- `should_redeliver_on_nack_after_visibility_timeout()`

Integration tests to add/verify
- `should_preserve_messages_across_restart()`
- `should_scale_consumers_for_high_throughput()`

Bench targets
- Tier1: pure enqueue/dequeue hot-path (precomputed messages)
- Tier2: subsystem bench measuring mailbox/visibility handling
- Tier3: system end-to-end latency under load
- Tier4: integration durability + throughput

PR plan (2–3 commits)
1. Add missing unit tests + AAA fixes (1–2 hr).
2. Add/extend benches and run validation (2–3 hr).

Acceptance criteria
- Queue semantics covered by focused `should_*` tests
- Tier1 bench allocation-free and measures hot-path throughput
