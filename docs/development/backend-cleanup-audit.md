# Backend Cleanup Audit

## Scope

This audit records the structural backend cleanup that keeps tracked Rust source files below the 1,000-line ceiling without changing runtime behavior, public routes, DTO field names, OpenAPI shape, storage layout, or domain semantics.

The cleanup is intentionally mechanical:

- split large files into smaller responsibility files
- preserve existing module paths through `include!` or `mod tests`
- keep domain/session/runtime/protocol code synchronous
- move unit tests out of oversized implementation files

## Baseline Oversized Files

Initial files above the 1,000-line ceiling:

- `src/domains/queue/actor.rs`
- `src/domains/stream/store.rs`
- `src/domains/rpc/sink.rs`
- `src/api/admin/troubleshooting.rs`
- `src/api/runtime_ingress.rs`
- `src/api/admin/list.rs`
- `src/boot/runtime/config.rs`
- `src/domains/queue/sink.rs`
- `src/domains/stream/sink.rs`
- `src/domains/kv/actor.rs`
- `src/domains/stream/storage.rs`
- `src/domains/schedule/actor.rs`
- `src/domains/schedule/store.rs`
- `src/domains/kv/sink.rs`
- `src/auth/claims.rs`
- `src/domains/schedule/sink.rs`
- `src/domains/lease/sink.rs`
- `src/domains/lease/actor.rs`
- `src/domains/notice/sink.rs`
- `src/api/admin/search.rs`
- `src/runtime/subscriptions.rs`
- `src/api/admin/handlers.rs`
- `src/testkit/transport.rs`
- `src/protocol/kv_codec.rs`
- `src/boot/storage.rs`
- `src/auth/mod.rs`
- `src/api/mcp/mod.rs`

## Split Strategy

- Admin list and troubleshooting were split by DTO/model definitions, query parsing, inventory collection, read operations, details/comparisons, diagnostics, and timelines.
- Runtime ingress was split into helper/type definitions, builder/session lifecycle, auth setup, dispatch policy, authorization dispatch, and trait implementations.
- Domain actors/sinks/stores were split by state model, domain impl, mailbox impl, storage/key encoding, read/write paths, timers, admin snapshots, and tests.
- Protocol/testkit/support files were split into codec route parsing, mutation parsing, server setup, client/frame helpers, and test modules.
- Test modules were moved to sibling `tests.rs` files or smaller test part files where needed.

## Validation

Run these commands after the cleanup:

- `find src -type f -name '*.rs' -exec wc -l {} + | awk '$2 != \"total\" && $1 > 1000 { print }'`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo test test_guidelines_compliance`
- `cntryl-tools validate-tests`

Async-boundary spot check:

- `rg -n "\\.await|tokio::spawn|tokio::sync|async fn|async move" src/session src/runtime src/protocol src/domains`

Results from this cleanup:

- line-ceiling check returned no files; largest Rust source file is `src/api/admin/metrics.rs` at 998 lines
- async-boundary spot check returned no matches
- `cargo fmt --all -- --check` passed
- `cargo clippy --workspace --all-targets -- -D warnings` passed
- `cargo test --workspace` passed
- `cargo test test_guidelines_compliance` passed
- `cntryl-tools validate-tests` passed with 1171 compliant tests and 0 violations

## Remaining Risks

- The refactor uses textual includes to preserve existing private item visibility. This keeps behavior stable but should be revisited opportunistically if a future change creates clearer module APIs.
- No behavior tests were added because this change is intended to be structure-only.
