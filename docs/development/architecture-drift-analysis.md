# Architecture Drift Analysis

**Status**: Evidence-based review
**Scope**: Core Rust broker architecture under `src/` and
`docs/development/`
**Review date**: 2026-07-01

This analysis compares the current architecture documentation and Rust source
against:

- [architectural-laws.md](architectural-laws.md)
- [domain-boundaries-spec.md](domain-boundaries-spec.md)
- [architecture.md](architecture.md)
- current source structure under `src/`

Classification terms:

- **Confirmed Drift**: A documented or implemented behavior conflicts with an
  authoritative guarantee or with source evidence.
- **Suspected Drift**: Evidence is incomplete or ambiguous enough that a focused
  test or audit must establish whether behavior is actually wrong.
- **Documentation Gap**: Behavior appears correct, but docs are incomplete,
  stale, or easy to misread.
- **Clean**: Docs and source evidence align.

No suspected drift was found in this pass.

## Findings

### AD-001: Async Transport Boundary

**Classification**: Clean

**Expected behavior**: Async belongs at the transport edge in `src/api/`.
`src/session/`, `src/runtime/`, `src/protocol/`, and `src/domains/` remain
synchronous.

**Observed behavior**: Async ingress code is under `src/api/runtime_ingress`.
The scanned core paths did not show `.await`, `tokio::spawn`, or `tokio::sync`
usage in `src/session`, `src/runtime`, `src/protocol`, or `src/domains`. Runtime
threading uses standard threads in the scheduler.

**Evidence**:

- `docs/development/architecture.md`: transport is async; runtime and domains
  are synchronous.
- `src/api/runtime_ingress/types_and_helpers.rs`: `Ingress` is the async API
  boundary.
- `src/runtime/scheduler.rs`: uses synchronous scheduler/thread execution.
- `src/domains/*/mod.rs`: domain contracts describe sync domain behavior and
  broker-local live state where applicable.

**Impacted law or domain**: Architectural law 2; all domains.

**Remediation test target**: Keep `cargo test test_guidelines_compliance`.
Consider adding a future source-scan test that fails on async primitives outside
allowed `src/api/` modules.

### AD-002: Request Lifecycle

**Classification**: Clean

**Expected behavior**: A frame enters through async transport, is authenticated
and authorized at ingress/session, is dispatched by `(RouteFamily, Route)`, and
then reaches exactly one owning domain sink for typed handling.

**Observed behavior**: `RuntimeIngress::on_frame` authenticates the frame,
derives the route family from session state, authorizes route-shaped permissions,
then delegates to `DomainFrameDispatcher` and the router.

**Evidence**:

- `src/api/runtime_ingress/trait_impls.rs`: `on_frame` calls the session
  authenticator and domain frame dispatcher.
- `src/api/runtime_ingress/session_authenticator.rs`: CONNECT verification
  resolves route family through `RouteFamilyResolverConfig`.
- `src/api/runtime_ingress/auth_session_setup.rs`: route canonicalization
  preserves route realm as route data.
- `src/runtime/domain_manifest.rs`: domain scheme inventory and cleanup routes
  are centralized.

**Impacted law or domain**: Laws 2, 4, and 6; all domains.

**Remediation test target**: Existing authorization-route tests under
`src/api/runtime_ingress/tests/authorization_routes.rs`. Add focused coverage
there if a new domain or route shape is introduced.

### AD-003: Disconnect Cleanup Source Semantics

**Classification**: Clean

**Expected behavior**: Disconnect destroys session-owned state. Reconnect creates
a new session and must not restore subscriptions, live RPC work, open KV
transactions, stream append sessions, queue inflight ownership, leases, or
ephemeral waiters.

**Observed behavior**: `RuntimeIngress::on_close` reads the session route family,
dispatches `SessionCleanup` through `DomainRegistry::cleanup_order()`, then
finalizes the session close. Domain sinks remove their session-owned live state:
KV transactions and locks, Notice subscriptions, RPC workers and pending live
requests, Stream append sessions and live subscriptions, Schedule live
subscriptions, Lease holders and waiters, and Queue inflight ownership.

**Evidence**:

- `src/runtime/domain_manifest.rs`: cleanup order is `KV`, `Notice`, `RPC`,
  `Stream`, `Schedule`, `Lease`, `Queue`.
- `src/api/runtime_ingress/session_cleanup_coordinator.rs`: cleanup dispatch and
  retry tickets.
- `src/domains/kv/sink/domain_sink_impl.rs`: drops live KV transactions and
  resource locks.
- `src/domains/notice/sink.rs`: removes Notice subscriptions for a session.
- `src/domains/rpc/sink/domain_sink_impl.rs`: applies RPC session cleanup.
- `src/domains/stream/sink/domain_sink_impl.rs`: removes subscriptions and
  uncommitted append sessions.
- `src/domains/schedule/sink/domain_sink_impl.rs`: removes live schedule
  subscriptions.
- `src/domains/lease/sink/domain_sink_impl.rs`: releases leases and removes
  waiters.
- `src/domains/queue/sink/domain_sink_impl.rs`: releases live queue inflight
  ownership.

**Impacted law or domain**: Law 3; all session-owned domain state.

**Remediation test target**: Existing cleanup tests under
`src/api/runtime_ingress/tests/session_lifecycle_and_cleanup.rs` and
`src/api/runtime_ingress/tests/real_domain_cleanup.rs`.

### AD-004: Cleanup Retry Tickets Are Under-Documented

**Classification**: Documentation Gap

**Expected behavior**: Architecture docs should say that cleanup retry tickets
exist only to finish cleanup dispatch after a failed cleanup delivery. They must
not imply session recovery, ownership continuity, or restoration of live state.

**Observed behavior**: `RuntimeIngress::on_open`, `RuntimeIngress::on_frame`,
and `RuntimeIngress::on_close` call `retry_pending()` before normal ingress work.
`SessionCleanupCoordinator` stores `PendingSessionCleanup { route_family }` for a
failed cleanup dispatch and retries `dispatch_session_cleanup(...)` for that
same closed session. The architecture guide strongly states that sessions are
not recovered, but it does not describe this cleanup retry failure path.

**Evidence**:

- `docs/development/architecture.md`: Session Recovery Model says there is no
  server-side session recovery.
- `src/api/runtime_ingress/trait_impls.rs`: ingress invokes pending cleanup
  retries before open, frame, and close handling.
- `src/api/runtime_ingress/session_cleanup_coordinator.rs`: retry tickets store
  only session id and route family for cleanup redispatch.
- `src/api/runtime_ingress/types_and_helpers.rs`: `dispatch_session_cleanup`
  sends `SessionCleanup` to each cleanup domain.

**Impacted law or domain**: Laws 3 and 4; session cleanup documentation.

**Remediation test target**: Documentation contract test that requires cleanup
retry language to describe cleanup completion and forbid session recovery or
live ownership restoration wording.

### AD-005: Domain Semantics Boundaries

**Classification**: Clean

**Expected behavior**: Notice, RPC, and Lease expose ephemeral live semantics.
KV, Stream, Queue, and Schedule expose only their own durable surfaces. Domain
composition is explicit and must not transfer guarantees.

**Observed behavior**: Domain module contracts match the boundary spec:

- Notice is non-durable live fanout.
- RPC is live request/response with in-memory worker and pending state.
- Lease is single-broker ephemeral ownership with process-local tokens.
- KV persists committed current state while transaction handles are live state.
- Stream persists committed history while append sessions and live
  subscriptions are ephemeral.
- Queue persists backlog according to write policy while inflight ownership is
  live state.
- Schedule persists timing intent while live delivery and subscriptions remain
  ephemeral.

**Evidence**:

- `src/domains/notice/mod.rs`
- `src/domains/rpc/mod.rs`
- `src/domains/lease/mod.rs`
- `src/domains/kv/mod.rs`
- `src/domains/stream/mod.rs`
- `src/domains/queue/mod.rs`
- `src/domains/schedule/mod.rs`
- `src/boot/domains.rs`: storage is passed to KV, Queue, Stream, and Schedule;
  not to Notice, RPC, or Lease.

**Impacted law or domain**: Laws 1, 2, 4, 5, and 6; all domains.

**Remediation test target**: Domain-specific tests near each sink/actor. Add
integration tests only for cross-layer guarantee leaks.

### AD-006: Persistent Storage Schema Mentions Lease and Omits Schedule

**Classification**: Confirmed Drift

**Expected behavior**: Architecture documentation must not imply durable Lease
ownership, restart-safe Lease fencing, or persisted Lease state. It must show
Schedule as the domain that stores durable timing intent.

**Observed behavior**: `docs/development/architecture.md` includes a Layer 5
"Key-value schema for each domain" entry for Lease:
`{realm}/{area}/{resource} -> {owner, ttl, token}`. The same storage schema list
does not include Schedule. This conflicts with the domain boundary spec and
source, where Lease is intentionally ephemeral and Schedule uses storage.

**Evidence**:

- `docs/development/domain-boundaries-spec.md`: Lease does not guarantee
  crash-safe recovery, cross-restart token monotonicity, or durable wait queues;
  Schedule guarantees persisted schedule definitions and pending fire claims.
- `src/domains/lease/mod.rs`: Lease state disappears on broker restart or
  session disconnect.
- `src/domains/schedule/mod.rs`: Schedule durably stores timing intent and
  pending claimed occurrences.
- `src/boot/domains.rs`: `ScheduleDomainSink::new_with_storage(...)` receives
  storage; `LeaseDomainSink::new(...)` does not.

**Impacted law or domain**: Laws 2 and 4; Lease and Schedule.

**Remediation test target**: Add a focused documentation contract test that
fails while `architecture.md` lists Lease under persistent storage or omits
Schedule storage. Then fix `architecture.md`.

### AD-007: RouteFamily and Realm Separation

**Classification**: Clean

**Expected behavior**: `RouteFamily` is a broker-internal routing and isolation
key. `realm` is an application-visible opaque namespace inside routes,
permissions, and admin/API payloads. They must never be inferred from one
another or used as fallback values.

**Observed behavior**: Authenticated sessions resolve route family from a
configured identity claim and `FITZ_ROUTE_FAMILY_MAP`. Request routes retain
their own realm path segment. Routing uses `RouteAddress(RouteFamily, Route)`.
Source search did not find a core fallback assigning `realm =
route_family.to_string()`.

**Evidence**:

- `src/auth/claims/route_family.rs`: route-family resolver maps configured
  identity claim values to numeric route families.
- `src/api/runtime_ingress/session_authenticator.rs`: stores the resolved route
  family on the session.
- `src/api/runtime_ingress/auth_session_setup.rs`: canonicalizes domain routes
  from route strings.
- `src/runtime/routing.rs`: explicitly states realm is not `RouteFamily`.
- `docs/clients/client-spec.md`: JWTs do not carry Fitz `route_family`,
  `realm`, or `areas`.

**Impacted law or domain**: Laws 2 and 4; routing and authorization.

**Remediation test target**: Existing route-family resolver and authorization
tests. Add a focused test if future code allows a missing realm to fall back to
route family or a route-family query parameter.

### AD-008: Architecture Overview Uses Incomplete Isolation Wording

**Classification**: Documentation Gap

**Expected behavior**: High-level architecture text should make the two-axis
model explicit: `RouteFamily` is hard broker isolation and `realm` is an opaque
application-visible route namespace.

**Observed behavior**: The overview in `docs/development/architecture.md`
mentions "Isolation via realms" and "realm-based isolation" before later
sections correctly explain route-family resolution and the separation between
`realm` and RouteFamily. The later sections and source are correct, but the
opening wording can be misread as assigning all isolation to realm alone.

**Evidence**:

- `docs/development/architecture.md`: overview wording plus later corrected
  session-layer wording.
- `docs/development/domain-boundaries-spec.md`: RouteFamily is a hard isolation
  boundary; `realm` and RouteFamily are separate axes.
- `src/runtime/routing.rs`: full address is `(RouteFamilyId, Route)`.

**Impacted law or domain**: Laws 2 and 4; route isolation docs.

**Remediation test target**: Documentation contract test that requires
architecture overview text to mention both `RouteFamily` and `realm` when
describing isolation.

## Summary

The source structure aligns with the authoritative docs for the async boundary,
request lifecycle, disconnect cleanup, domain guarantees, and route isolation.
The only confirmed drift found in this pass is documentation drift in the existing
architecture guide's persistent storage schema: Lease is shown as storage-backed
and Schedule is omitted. Additional gaps are wording issues that can mislead
readers about route isolation and cleanup retry behavior, but they do not show
source behavior drift.
