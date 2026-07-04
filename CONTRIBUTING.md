# Contributing to Fitz

## Dev Setup

- Install Rust stable.
- Install shared tooling:
  - `cargo install --git https://github.com/cntryl/tools --locked`
- Clone the repo and run the workspace tests:
  - `cargo test --workspace`

## Local Validation

Run these before opening a behavior-changing PR:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo test test_guidelines_compliance
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
cntryl-tools validate-tests
```

## Running Locally

The broker code lives under `src/`. Integration tests in `tests/` spawn broker components in-process. The repository compose files are for local development and publish ports on loopback.

## Style

- Keep PRs small and focused.
- Add or update tests when changing behavior.
- Prefer the nearest module that controls the behavior over broad refactors.
- Keep async at the transport edge in `src/api/`; core runtime, domains, protocol, and session code stay synchronous.

## Domain Changes

- Treat [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md) as the authoritative domain contract.
- Treat [docs/development/architectural-laws.md](docs/development/architectural-laws.md) as the review gate for durability, disconnect behavior, recovery, cross-domain composition, and observability semantics.
- If a change alters a domain guarantee, boundary, storage behavior, or recovery behavior, update the relevant docs in the same PR.
- Keep `realm` and `RouteFamily` separate. `realm` is application-visible and opaque; `RouteFamily` is broker-internal routing and isolation.

## Release Work

- Use [docs/operations/release-checklist.md](docs/operations/release-checklist.md).
- Use [docs/operations/migration-guide.md](docs/operations/migration-guide.md) when compatibility risk exists.
- Use [docs/development/format-compatibility.md](docs/development/format-compatibility.md) for wire, storage, and serialized format changes.
