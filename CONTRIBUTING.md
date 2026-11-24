# Contributing to Fitz

## Organization-wide contribution expectations

Fitz follows the [cntryl contribution standards](https://github.com/cntryl/.github/blob/main/CONTRIBUTING.md).
Substantial work requires a linked issue and maintainer agreement on scope before
implementation. Maintainers may close context-free, duplicate, speculative,
misleading, mass-produced, or unsupported submissions to protect review capacity.

## Dev Setup

- Install the Rust toolchain pinned for CI in `.github/workflows/ci.yml`.
- Install shared tooling for test conventions and module sizes:
  - `cargo install --locked --git https://github.com/cntryl/tools --rev 4fd7f318c1f8c24bdb5d4bbea1b05d85b10f4bb5 cntryl-tools`
- Clone the repo and run the workspace tests:
  - `cargo test --locked --workspace`

## Local Validation

Run these from the workspace root before opening a behavior-changing PR. The test validator discovers each Cargo member.

```sh
cntryl-tools validate-tests
for fitz_member in crates/*; do
  cntryl-tools check-module-sizes --root "$fitz_member" || exit 1
done
cargo fmt --all -- --check
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
```

## Branch Flow

- Open feature pull requests against `develop`. The `ci.yml` workflow runs backend and frontend format, lint, build, and test checks.
- Promote `develop` to `main` only when preparing a release. The same CI jobs run on the promotion pull request.
- CodeQL runs through GitHub's default setup, outside `ci.yml`.
- Squash feature pull requests into `develop`. Merge promotion pull requests with a merge commit so later promotions retain ancestry.

## Running Locally

The repository root is a virtual Cargo workspace. Its default member is the broker package, `fitz`, in `crates/fitz-server`.

```sh
cargo run --locked -p fitz --bin fitz
cargo build --locked -p fitz --release --bin fitz --bin fitz-mcp-stdio
cargo test --locked --workspace
cargo test --locked -p fitz-queue
cargo test --locked -p fitz-e2e --features benchkit,stress-soak --test queue_drain_latency_artifacts --test queue_pressure_accounting --test queue_pacing --test queue_pressure_envelopes
```

For local Cargo dependencies, point `fitz = { path = "../fitz/crates/fitz-server" }` at the broker manifest. The Rust library is still imported as `fitz::`. Broker-wide suites live in `crates/fitz-e2e/tests`, and the repository compose files publish development ports on loopback.

## Style

- Keep PRs small and focused.
- Add or update tests when changing behavior.
- Prefer the nearest module that controls the behavior over broad refactors.
- Keep async at the transport edge in `crates/fitz-server/src/api/`; core runtime, domains, protocol, and session code stay synchronous.

## Domain Changes

- Treat [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md) as the authoritative domain contract.
- Treat [docs/development/architectural-laws.md](docs/development/architectural-laws.md) as the review gate for durability, disconnect behavior, recovery, cross-domain composition, and observability semantics.
- If a change alters a domain guarantee, boundary, storage behavior, or recovery behavior, update the relevant docs in the same PR.
- Keep `realm` and `RouteFamily` separate. `realm` is application-visible and opaque; `RouteFamily` is broker-internal routing and isolation.

## Release Work

- Use [docs/operations/release-checklist.md](docs/operations/release-checklist.md).
- Use [docs/operations/migration-guide.md](docs/operations/migration-guide.md) when compatibility risk exists.
- Use [docs/development/format-compatibility.md](docs/development/format-compatibility.md) for wire, storage, and serialized format changes.
## Explain your use case

In your issue or pull request, explain how you use or plan to use this project, what problem you encountered, and how the proposed change helps.

Follow this repository’s setup and validation instructions, and report relevant checks you ran.
