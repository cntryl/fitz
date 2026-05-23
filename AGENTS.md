# Fitz Agent Guide

## Repo Layout
- Root is a Rust workspace. Main code lives under `src/`.
- `ui/` is a separate Node/Vite workspace.
- `public/` contains the static SPA served by the broker.
- Canonical repo skills live under root `skills/`.

## Read First
- [docs/development/architectural-laws.md](docs/development/architectural-laws.md)
- [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md)
- [docs/development/architecture.md](docs/development/architecture.md)
- [docs/development/testing.md](docs/development/testing.md)
- [docs/development/benchmarks.md](docs/development/benchmarks.md)
- [docs/development/stress-bench-contract.md](docs/development/stress-bench-contract.md)
- [docs/development/perf-loop.md](docs/development/perf-loop.md) for performance work
- [CONTRIBUTING.md](CONTRIBUTING.md) for local setup and validation

## Hard Constraints
- Async belongs only at the transport edge in `src/api/`.
- Keep `src/session/`, `src/runtime/`, `src/protocol/`, and `src/domains/` synchronous.
- Sessions are ephemeral. Disconnect creates a new session.
- Do not imply durability, replay, exactly-once delivery, recovery, or ownership continuity unless storage and docs explicitly support it.
- Preserve the domain meanings: Notice = live ephemeral fanout, Stream = durable history/replay, KV = current authoritative state, Queue = durable work delivery, RPC = live request/response, Lease = ephemeral ownership coordination, Schedule = durable timing intent.
- If semantics change, update the relevant docs in the same change.

## Working Rules
- Keep changes small and focused.
- Do not overwrite unrelated user edits.
- Prefer the nearest file that controls the behavior over broad refactors.
- Avoid adding async constructs to core Rust code outside transport (`.await`, `tokio::spawn`, `tokio::sync`, async locks).
- When editing UI tooling or scripts, prefer ESM and `.js` over `.mjs` for repo-owned files.
- When `public/openapi.yml` or the UI client changes, regenerate adapters with `npm run gen:adapters` from `ui/`.

## Validation
- Rust: `cargo test --workspace`, `cargo test test_guidelines_compliance`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cntryl-tools validate-tests`.
- UI: run from `ui/` with `npm run test`, `npm run lint`, `npm run type-check`, and `npm run build`.
- Benchmarks: use the relevant `cargo bench` target for Criterion suites and `scripts/run_perf_loop.ps1` for perf-loop changes.

## Test Rules
- Use `should_*` names for Rust tests.
- For tests longer than 5 lines, use exact `// Arrange`, `// Act`, and `// Assert` comments.
- Keep each test focused on one behavior.
- Put unit tests near the code and integration tests in `tests/`.
