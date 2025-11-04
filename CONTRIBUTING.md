# Contributing to fitz

Thanks for your interest! This project is in early development.

## Dev setup

- Install Rust stable
- Clone the repo and run tests:
  - `cargo test`
- Lint locally:
  - `cargo fmt --all -- --check`
  - `cargo clippy -D warnings`

## Running locally

The broker is composed of modules under `src/`. The storage default is in-memory. Integration tests in `tests/` spawn components in-process.

## Style

- Keep PRs small and focused
- Add tests when changing behavior
- Prefer trait abstractions for storage/transport

## Roadmap

See `todo.md` for prioritized tasks.

