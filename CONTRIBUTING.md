# Contributing to fitz

Thanks for your interest! This project is in early development.

## Dev setup

- Install Rust stable
- Install shared tooling:
  - `cargo install --git https://github.com/cntryl/tools --locked`
- Clone the repo and run tests:
  - `cargo test`
- Lint locally:
  - `cargo fmt --all -- --check`
  - `cargo clippy -D warnings`
  - `cntryl-tools validate-tests`

## Running locally

The broker is composed of modules under `src/`. The storage default is in-memory. Integration tests in `tests/` spawn components in-process.

## Style

- Keep PRs small and focused
- Add tests when changing behavior
- Prefer trait abstractions for storage/transport

## Domain changes

- Treat [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md) as the primary architectural contract for domain responsibility, overlap, and feature placement.
- Treat [docs/todos/todo-all.md](docs/todos/todo-all.md) and the per-domain files under [docs/todos](docs/todos) as the concise canonical contract index and proof-oriented domain detail.
- If a change alters a domain guarantee, boundary, or overlap rule, update the relevant docs in the same PR.
- Use [.github/pull_request_template.md](.github/pull_request_template.md) and complete the Domain Boundary Review section whenever a PR touches domain semantics, persistence, recovery, or cross-domain composition.

## Roadmap

See `todo.md` for prioritized tasks.

