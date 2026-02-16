# KV domain — TODO

Summary
- Ensure `kv` domain is idiomatic, fully tested, and has tier1–tier4 benches that measure the real hot path (transactions, put/get, conflict handling).

Goals
- Verify synchronous domain invariants (no async, no tokio types).
- Ensure unit tests follow `should_*` naming and AAA structure where required.
- Expand benches so `tier1` measures pure hot-path (get/put), `tier2` subsystem cases, `tier3` system E2E, `tier4` integration with transport.
- Validate realm/area isolation and transaction scoping.

Files to inspect
- `src/domains/kv/actor.rs`
- `src/domains/kv/session.rs`
- `src/domains/kv/protocol.rs`
- `src/domains/kv/mod.rs`
- Tests: `tests/kv_*` (e.g. `kv_e2e_basic.rs`)
- Benches: `benches/tier1_hotpath_kv.rs`, `benches/tier3_system_kv.rs`, `benches/tier4_integration_kv.rs`

Required unit tests (examples to add/check)
- `should_reject_operations_from_wrong_area()` — verify area isolation
- `should_handle_concurrent_puts_with_conflict_detection()` — single behavior: conflict handling
- `should_return_error_for_invalid_txid()` — negative path
- `should_preserve_data_across_save_load()` — split serialize/deserialize into two tests if >5 lines

Integration tests to add/verify
- `should_enforce_realm_isolation_for_kv()` (e2e)
- `should_handle_high_throughput_batch_puts()` (scale test)
- `should_recover_transaction_after_wal_restart()` (durability)

Bench targets
- Tier1 (hotpath): microbench `get` and `put` latency with precomputed keys/values, no allocations.
- Tier2 (subsystem): mailbox/scheduler interactions for KV actor.
- Tier3 (system): end-to-end frame parse → KV actor → response latency.
- Tier4 (integration): real client + local storage durability + measured throughput.

PR plan (3 commits)
1. Add/adjust unit tests and AAA fixes (1–2 hr).
2. Improve/extend tier1/tier2 benches (2–3 hr).
3. Add tier4 integration bench + docs (2–4 hr).

Acceptance criteria
- `cargo test test_guidelines_compliance` passes
- `cargo clippy -D warnings` clean
- Tier1 bench for KV measures hot-path (no setup) and is allocation-free
- No `async/.await` or tokio types in `src/domains/kv`

Notes / Risks
- Watch for tests that bundle multiple `// Act` sections — split if necessary.
- Keep terminology as `realm`, `area`, `resource`, `route`.
