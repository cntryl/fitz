# Architecture Remediation Plan

**Status**: Planned remediation only
**Scope**: Findings from
[architecture-drift-analysis.md](architecture-drift-analysis.md)

This pass does not change broker behavior, public APIs, domain semantics, or the
existing authoritative docs. Future remediation must follow TDD: write or update
the nearest focused failing test first, observe the failure, make the smallest
owning fix, then run targeted and full validation.

## TDD Rules For Architecture Remediation

1. Write or update the nearest focused test first.
2. Observe the test fail for the intended reason.
3. Fix the smallest owning module or document.
4. Rerun the focused test.
5. Run the relevant targeted domain or integration tests.
6. Finish with full validation appropriate to the touched surface.
7. If a fix changes domain guarantees, update authoritative docs in the same
   change.

Prioritization order:

1. Public guarantee mismatch
2. Session cleanup or recovery drift
3. `realm` and `RouteFamily` isolation drift
4. Async boundary violations
5. Cross-domain guarantee leakage
6. Observability-driven behavior

## Confirmed Drift Remediation

### AR-001: Fix Persistent Storage Schema For Lease And Schedule

**Finding**: AD-006

**Problem**: `docs/development/architecture.md` lists Lease under Layer 5
persistent storage and omits Schedule from the storage schema. This can imply
durable Lease ownership and hide Schedule's durable timing-intent surface.

**First failing test**: Add a focused documentation contract test, for example
`tests/architecture_docs.rs`, named
`should_keep_architecture_storage_schema_aligned_with_domain_contracts`.

The test should fail while:

- the Layer 5 storage schema lists Lease as persisted state, or
- the Layer 5 storage schema omits Schedule durable timing intent.

**Minimal fix area**: `docs/development/architecture.md`.

**Expected fix**:

- Remove any implication that Lease is Midge-backed durable state.
- Add Schedule storage wording for persisted definitions, next-fire state, and
  pending fire claims.
- Keep Lease described as ephemeral ownership coordination with process-local
  tokens.

**Docs impact**: Updates authoritative architecture prose only. No domain
semantics or public API changes.

**Validation command**:

```bash
cargo test should_keep_architecture_storage_schema_aligned_with_domain_contracts
cargo test test_guidelines_compliance
cntryl-tools validate-tests
```

## Documentation Gap Guardrails

### AR-002: Clarify Isolation Overview Wording

**Finding**: AD-008

**Problem**: The architecture overview can be read as assigning isolation to
realm alone before later sections explain `RouteFamily`.

**First failing test**: Add or extend a documentation contract test named
`should_describe_route_family_and_realm_as_separate_isolation_axes`.

**Minimal fix area**: `docs/development/architecture.md`.

**Expected fix**: Change the overview from realm-only isolation wording to the
two-axis model: hard broker isolation by `RouteFamily`, application-visible
namespace by `realm`.

**Docs impact**: Clarifies terminology without changing behavior.

**Validation command**:

```bash
cargo test should_describe_route_family_and_realm_as_separate_isolation_axes
cargo test test_guidelines_compliance
cntryl-tools validate-tests
```

### AR-003: Document Cleanup Retry Without Implying Session Recovery

**Finding**: AD-004

**Problem**: The source has a retry ticket path for failed cleanup dispatch.
Current docs emphasize immediate cleanup but do not describe the failure path.
The fix must not imply reconnect recovery or ownership continuity.

**First failing test**: Add a documentation contract test named
`should_document_cleanup_retry_without_session_recovery`.

**Minimal fix area**: `docs/development/architecture.md` or a focused cleanup
section under `docs/development/`.

**Expected fix**:

- State that normal disconnect cleanup dispatch is immediate.
- State that cleanup dispatch failures are retried as cleanup completion.
- Explicitly state that retry tickets do not restore sessions or live ownership.

**Docs impact**: Clarifies failure handling only.

**Validation command**:

```bash
cargo test should_document_cleanup_retry_without_session_recovery
cargo test test_guidelines_compliance
cntryl-tools validate-tests
```

## Source Guardrail Candidates

These are not remediation for confirmed drift, but they would make future drift
harder to introduce.

### AR-004: Guard Async Boundary

**First failing test**: Add a source-scan test named
`should_keep_async_out_of_sync_core`.

**Minimal fix area if it fails**: Remove async constructs from the offending
non-`src/api/` module or move the behavior to the transport edge.

**Validation command**:

```bash
cargo test should_keep_async_out_of_sync_core
cargo test test_guidelines_compliance
cntryl-tools validate-tests
```

### AR-005: Guard RouteFamily And Realm Separation

**First failing test**: Add a focused unit or integration test named
`should_not_default_realm_from_route_family`.

**Minimal fix area if it fails**: The nearest auth, session, admin filter, or
domain route parser module that aliases the two values.

**Validation command**:

```bash
cargo test should_not_default_realm_from_route_family
cargo test test_guidelines_compliance
cntryl-tools validate-tests
```

### AR-006: Guard Domain Durable Surfaces

**First failing test**: Add domain-specific tests only when a code change touches
durability, replay, recovery, ownership, or cross-domain composition. The test
name should describe the domain invariant, for example
`should_drop_rpc_pending_request_given_session_cleanup`.

**Minimal fix area if it fails**: The owning domain sink or actor, not a broad
runtime refactor.

**Validation command**:

```bash
cargo test <focused_test_name>
cargo test --workspace
cntryl-tools validate-tests
```

## Full Validation For Future Remediation

After any remediation change that touches Rust source:

```bash
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cntryl-tools validate-tests
```

After documentation-only remediation:

```bash
cargo test test_guidelines_compliance
cargo fmt --all -- --check
git diff --check
cntryl-tools validate-tests
```
