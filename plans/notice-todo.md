# Notice domain — TODO

Summary
- Make sure pub/sub matching, wildcard patterns, subscription lifecycle, and fanout hot-paths are fully tested and bench‑covered (hot-path fanout must be allocation-free).

Goals
- Validate matcher correctness (wildcards, multi-segment `**`).
- Ensure fanout path is allocation-free in tier1 benches.
- Verify subscription lifecycle and realm/area isolation.

Files to inspect
- `src/domains/notice/route_actor.rs`
- `src/domains/notice/session.rs`
- `src/domains/notice/protocol.rs`
- `src/domains/notice/bench.rs`
- Tests: `tests/notice_*`
- Benches: `benches/tier1_hotpath_notice.rs`, `benches/tier2_subsytem_notice.rs` (rename check)

Required unit tests (examples)
- `should_match_wildcard_subscriptions_correctly()`
- `should_preserve_notify_order_for_single_subscriber()`
- `should_not_leak_subscriptions_across_realms()`

Integration tests to add/verify
- `should_scale_fanout_to_many_subscribers()`
- `should_handle_subscribe_unsubscribe_race()`

Bench targets
- Tier1: exact-match and wildcard-match fanout (allocation-free)
- Tier2: matcher subsystem throughput/latency
- Tier3: end-to-end notify delivery across runtime
- Tier4: large-scale integration (many subscribers)

PR plan (2 commits)
1. Fix bench filename typo and add matcher/scalability tests (1–2 hr).
2. Harden tier1/tier2 benches to be zero-allocation and add system bench (2–3 hr).

Acceptance criteria
- `src/domains/notice/bench.rs` and `benches/tier1_hotpath_notice.rs` measure the real hot-path
- Wildcard semantics covered by unit tests
- No forbidden terminology in tests/docs
