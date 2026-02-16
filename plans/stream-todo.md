# Stream domain — TODO

Summary
- Complete the Stream domain checklist: verify append/read/commit semantics, session isolation, offset management, watermark behavior, and replace bench stubs with real tier1–tier4 measurements.

Goals
- Replace stubbed stream integration benches with real append/read/commit workflows.
- Add focused unit tests for offset/gap detection and session isolation.
- Ensure hot-path (`append`) benches are allocation-free and deterministic.

Files to inspect
- `src/domains/stream/stream_actor.rs`
- `src/domains/stream/area_actor.rs`
- `src/domains/stream/realm_actor.rs`
- `src/domains/stream/storage.rs` / `store.rs`
- `src/domains/stream/session.rs`
- Tests: `tests/stream_*`, `tests/stream_e2e_basic.rs`, `tests/stream_semantics.rs`
- Benches: `benches/tier1_hotpath_stream.rs`, `benches/tier4_integration_stream.rs` (currently stubbed)

Required unit tests (examples)
- `should_assign_sequential_resource_offsets()`
- `should_advance_watermark_only_on_contiguous_commits()`
- `should_reject_commit_with_wrong_expected_offset()`
- `should_debounce_area_watermark_notifications()`

Integration tests to add/verify
- `should_complete_begin_append_commit_cycle_end_to_end()`
- `should_support_large_append_payloads_and_reads()`
- `should_enforce_realm_and_area_isolation_for_streams()`

Bench targets (high priority)
- Tier1 (hotpath): `append` latency (single-event, batch append) — precompute payloads, no allocations.
- Tier1 (read): read-after-write latency for single and batched reads.
- Tier3/4: end-to-end append → storage durability → read throughput with local midge store.
- Replace TODOs in `benches/tier4_integration_stream.rs` with measured append/read/commit scenarios.

PR plan (3 commits)
1. Implement tier1 append/read hot-path benches, and add unit tests for offsets/gaps (3–4 hr).
2. Implement tier4 integration benches (append/read/commit with local midge storage) and validate (3–5 hr).
3. Triage & fix any discovered runtime/test issues; add e2e tests as needed (2–3 hr).

Acceptance criteria
- `benches/tier4_integration_stream.rs` contains real append/read/commit benchmarks (no TODO stubs)
- Unit tests cover offset semantics, session isolation, and watermark behavior
- All stream domain source files contain only synchronous code (no `.await`, no tokio domain locks)

Notes
- Stream domain benches are highest immediate priority because a stubbed `tier4` is currently blocking full integration validation.
- Follow Fitz benchmark rules: precompute data, no allocations in hot loop, `SamplingMode::Flat`, `black_box()` for inputs.
