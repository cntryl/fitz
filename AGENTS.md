# Fitz Agent Guide

## Repo Layout
- Root is a Rust workspace. Main code lives under `src/`.
- `ui/` is a separate Node/Vite workspace with its own `AGENTS.md` and local `skills/` tree.
- `public/` contains `openapi.yml` for UI adapter generation and legacy static files; production SPA assets are served from `/app/public`.
- Root workspace skills live under root `skills/`.

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

## Realm vs RouteFamily
- `realm` is an opaque, application-defined namespace boundary.
- A `realm` may represent a tenant, department, cost center, user, environment, or any other developer-chosen partition.
- Fitz does not assign one business meaning to `realm`, so do not define it as "tenant" in core semantics.
- `realm` and `route_family` are orthogonal identifiers.
- `realm` is the application-visible namespace label used in Fitz routes, permissions, and admin/API payloads.
- `route_family` is a broker-internal routing and isolation key used for session assignment and delivery partitioning.
- They MUST NEVER be inferred from each other, aliased, substituted, or used as fallback values.
- If `realm` is unknown or absent, it stays unknown or absent. It is never `route_family.to_string()`.

Wrong:
- `session.realm = session.route_family.to_string()`
- describing `realm` as inherently equal to `tenant`
- treating `?realm=41` as route-family selection

Right:
- expose `realm` and `route_family` separately when both matter
- filter by `realm` using realm-bearing data only
- treat external claim names like `tenant_id` as claim-source naming, not Fitz core terminology

## Working Rules
- Keep changes small and focused.
- Do not overwrite unrelated user edits.
- Prefer the nearest file that controls the behavior over broad refactors.
- Keep files small. No file should exceed 1,000 lines, and files should be split or refactored before they approach that limit.
- Optimize for simplicity. Complexity, DRY, and clarity are mandatory design constraints.
- Avoid adding async constructs to core Rust code outside transport (`.await`, `tokio::spawn`, `tokio::sync`, async locks).
- Do not create a top-level `scripts/` directory or repo-owned standalone shell scripts. Put automation in Rust tests or tools, package scripts, or explicit workflow steps.
- When editing UI tooling, prefer ESM and `.js` over `.mjs` for repo-owned files.
- When `public/openapi.yml` or the UI client changes, regenerate adapters with `npm run gen:adapters` from `ui/`.

## Validation
- Rust: `cargo fmt --all -- --check`, `cargo test --workspace`, `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic`.
- UI: run from `ui/` with `npm run test`, `npm run lint`, `npm run type-check`, and `npm run build`.
- Benchmarks: use direct `cargo bench` commands for the relevant tier or target, then run `cntryl-tools summarize-benchmarks` when a report is needed.

## Test Rules
- Use `should_*` names for Rust tests.
- For tests longer than 5 lines, use exact `// Arrange`, `// Act`, and `// Assert` comments.
- Keep each test focused on one behavior.
- Put unit tests near the code and integration tests in `tests/`.
