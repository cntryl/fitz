# Project Inventory — modules and test status

Generated: 2025-11-04

This document lists the project's top-level modules and the `src/core` submodules with a short description and current unit-test backfill status. The plan is to add comprehensive in-file unit tests (following the repo guidelines) for each module and validate with `tests/test_guidelines_compliance.rs` after each change.

## Summary

- lib.rs — crate root. Status: not-applicable (integration tests and unit tests live in module files).
- main.rs — binary entrypoint. Status: not-applicable (small smoke tests may be added later).

## src/core

- control.rs — control plane helpers/glue. Status: not-started
- engine.rs — core Engine implementation and command handling. Status: not-started
- kv.rs — key/value helpers. Status: not-started
- mod.rs — core module exports. Status: not-started
- notice.rs — notice (publish/subscribe) handling. Status: not-started
- queue.rs — queue implementation and configs. Status: not-started
- router.rs — subscription router and route matching. Status: done (in-file unit tests added, naming & AAA validated)
- rpc.rs — RPC helpers and handlers. Status: not-started
- stream.rs — stream append/peek/consume helpers. Status: not-started

## top-level modules (src/*)

- authn/ — authentication utilities and types. Status: not-started
- authz/ — authorization helpers and permission checks. Status: not-started
- config/ — configuration parsing and helpers. Status: not-started
- control/ — control API and handlers (folder). Status: not-started
- protocol/ — framing, route parsing, tags, etc. Status: not-started
- storage/ — storage implementations (mem, traits). Status: not-started
- transport/ — transport implementations (http, tcp, ws, mux). Status: not-started

## tests and tooling

- `tests/test_guidelines_compliance.rs` — meta-test that validates naming and AAA test guidelines.
- `testutils/validate_tests.rs` — helper used by the meta-test to parse test files.

## Next steps (short-term plan)

1. Backfill in-file unit tests for the `src/core` modules (one module at a time):
   - `engine.rs` -> add tests + run `test_guidelines_compliance`
   - `queue.rs` -> add tests + validate
   - `kv.rs`, `notice.rs`, `rpc.rs`, `stream.rs`, `control.rs`
2. After core is complete, proceed to top-level modules: `storage`, `transport`, `protocol`, `authn`, `authz`, `config`.
3. For each module:
   - add a `#[cfg(test)]` module in the same file
   - follow naming (`should_*`) and AAA (`// Arrange`, `// Act`, `// Assert`) conventions
   - run `cargo test` and the meta-test to ensure compliance

## Notes

- `router.rs` tests were added first and validated. Use it as a template for style and AAA structure.
- Keep tests small and focused. Use the `should_*` naming and split multiple behaviors into separate tests.

