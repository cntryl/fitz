# Test Guidelines

These guidelines define how Fitz tests are written and reviewed.

## Test Organization

Use Rust's normal split between unit and integration tests.

- Unit tests live next to the module they exercise under `src/`.
- Integration tests live under `tests/`.
- Domain end-to-end flows use `tests/*_e2e.rs`.
- Public contract and architecture checks live under `tests/`.

Use unit tests for isolated parsing, state transitions, actor helpers, storage-key helpers, and protocol codecs. Use integration tests when a behavior spans transport, session state, runtime routing, storage, auth, or multiple domains.

## Naming

Rust test names must start with `should_`.

Good:

```rust
should_reject_stale_transaction_id_after_client_reconnect()
should_require_worker_reregistration_after_broker_restart_tcp()
should_skip_missed_occurrences_given_forward_epoch_jump()
```

Avoid generic names such as `test_basic`, `works`, or `queue_case_1`.

## Arrange, Act, Assert

Tests longer than five lines must use exact comments:

```rust
#[test]
fn should_reject_invalid_inflight_token() {
    // Arrange
    let mut actor = make_queue_actor();

    // Act
    let result = actor.complete("wrong-token");

    // Assert
    assert!(result.is_err());
}
```

Keep each test focused on one behavior. If a scenario needs multiple assertions, they should all support the same behavior.

## Domain Contract Coverage

Use [domain-boundaries-spec.md](domain-boundaries-spec.md) as the source of truth for what must be tested.

Required coverage patterns:

- Notice: disconnect cleanup, wildcard matching, duplicate subscription handling, RouteFamily isolation, no replay expectation.
- Stream: append sessions, commit ordering, replay from offsets, watermarks, restart recovery of committed history, no live subscription recovery.
- KV: transaction scope, commit, rollback, stale transaction rejection, RouteFamily isolation, restart recovery of committed values only.
- Queue: enqueue, reserve, complete, extend, redelivery, dead-letter handling, write-policy durability, invalid token rejection.
- RPC: worker registration, request correlation, timeout, backpressure, streaming sequence, cleanup after disconnect or restart.
- Lease: single live holder, fencing token scope, renew/release token validation, wait ordering, restart loss.
- Schedule: persisted definitions, due-time handling, skip-forward overdue normalization, pending fire claims, live-only subscriptions.

## Determinism

- Prefer deterministic clocks and explicit test runtimes.
- Keep setup outside benchmark measurement blocks.
- Do not rely on sleeps unless the behavior is explicitly time based and no deterministic clock hook exists.
- Do not use observability output as the source of correctness.

## Validation Commands

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo test test_guidelines_compliance
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
cntryl-tools validate-tests
```

Documentation-only changes should still run the relevant doc and architecture tests:

```sh
cargo test --test architecture_docs
cargo test --test documentation_hygiene
```
