# Fitz Client Requirements

**Version:** 1.0  
**Date:** March 24, 2026  
**Purpose:** Define what a world-class Fitz client looks like across all dimensions — correctness, API design, resilience, concurrency, error handling, observability, performance, testing, and developer experience. This is the definitive grading rubric.

---

## Document Relationships

```
client-spec.md                      ← Wire protocol (normative)
client-acceptance-criteria.md       ← Testable correctness criteria (normative)
client-implementation-guide.md      ← Idiomatic patterns per language
connection-flow.md                  ← Connection lifecycle state machine
cross-language-conformance-suite.yaml ← Scenario definitions
cross-language-conformance-runner.md  ← CI harness contract
client-requirements.md              ← THIS FILE: grading rubric covering all dimensions
```

This document references the above as authoritative sources. It adds requirements for dimensions that the acceptance criteria do not cover: API ergonomics, concurrency safety, resilience, observability, performance, and testing completeness.

---

## Requirement Tiers

Every requirement belongs to one of three tiers.

| Tier | Label | Meaning |
|------|-------|---------|
| **T0** | Ship Gate | Non-negotiable. A client missing any T0 requirement is broken and must not be published. |
| **T1** | Production Grade | Required for any client used in a real system. Missing T1 items are production risks. |
| **T2** | World Class | Separates a good client from a great one. These are the requirements that earn the "world-class" designation. |

RFC 2119 keywords (MUST, SHOULD, MAY) apply within each tier. A T0 requirement using MUST is a hard gate. A T2 requirement using SHOULD is still a T2 requirement — it is expected of a world-class client, even if not immediately fatal to skip.

---

## Dimension Index

1. [Protocol Correctness](#1-protocol-correctness)
2. [API Completeness](#2-api-completeness)
3. [API Ergonomics & Design](#3-api-ergonomics--design)
4. [Connection Lifecycle & Resilience](#4-connection-lifecycle--resilience)
5. [Concurrency Safety](#5-concurrency-safety)
6. [Error Handling](#6-error-handling)
7. [Observability](#7-observability)
8. [Performance](#8-performance)
9. [Test Coverage](#9-test-coverage)
10. [Documentation & Developer Experience](#10-documentation--developer-experience)

---

## 1. Protocol Correctness

The acceptance criteria in `client-acceptance-criteria.md` are the normative source for this dimension. The requirements below summarize the tier assignment of each category.

### T0 — Wire Fidelity

**REQ-PROTO-001 (T0)** All message type constants MUST match the server's canonical registry exactly:

| Domain | Range |
|--------|-------|
| Control (CONNECT) | 1 |
| KV | 100–108 |
| Queue | 200–209 |
| RPC | 300–304 |
| Lease | 400–409 |
| Notice | 500–504 |
| Stream | 600–609 |
| Schedule | 700–705 |

**REQ-PROTO-002 (T0)** TLV encoding MUST be big-endian throughout. Type field is 1 byte for values 0–254 and `0xFF` + 2-byte BE u16 for values ≥255. Length field is u16 BE. A single TLV value MUST NOT exceed 65535 bytes.

**REQ-PROTO-003 (T0)** TCP transport MUST prefix every frame with a u32 BE length. WebSocket transport MUST use binary frames only; text frames MUST be rejected or ignored.

**REQ-PROTO-004 (T0)** CONNECT frame (MessageType=1) MUST be the first outbound frame after transport open. A JWT payload of zero length is valid for anonymous mode. The client MUST NOT wait for an ACK before sending the first domain request.

**REQ-PROTO-005 (T0)** The client MUST NOT send duplicate TLV tags within a single frame. If the server closes the connection with a parse error, the client MUST log the event and not retry the same malformed frame.

**REQ-PROTO-006 (T0)** All seven domains MUST be implemented: KV, Queue, Notice, RPC, Lease, Stream, Schedule.

**REQ-PROTO-007 (T0)** Every operation listed in the acceptance criteria for each domain MUST be implemented. Acceptance criteria AC-KV-001 through AC-SCHEDULE-008 are gates; the client MUST pass all of them.

**REQ-PROTO-008 (T0)** Auth rejection (AC-CONN-003) MUST be handled: broker closes the connection, client recognizes close reason, and MUST NOT retry with the same JWT.

**REQ-PROTO-009 (T0)** The client MUST treat JWT as opaque bytes. It MUST NOT generate, sign, validate, or inspect JWT claims.

**REQ-PROTO-010 (T0)** Routes MUST be treated as opaque URI strings. The client MUST NOT parse, validate, or normalize route strings.

### T1 — Protocol Edge Cases

**REQ-PROTO-011 (T1)** The client MUST correctly handle all error code ranges and map each code to the right domain (AC-ERROR-002). Error code `XXYY` where `XX` identifies the domain and `YY` the specific error MUST NOT be confused across domains.

**REQ-PROTO-012 (T1)** The client MUST correctly categorize retryable vs. fatal error codes per the table in `client-acceptance-criteria.md` (AC-ERROR-003). Retryable codes: 1004, 2004, 4005, 5001, 6001, 6002, 6003, 6004. All Unauthorized codes and non-retryable codes MUST be treated as fatal (no retry).

**REQ-PROTO-013 (T1)** Frame size MUST be respected. Default server limit is 1 MB (configurable). Clients SHOULD expose this as a configurable option. Individual TLV values MUST NOT exceed 65535 bytes regardless of frame size setting.

**REQ-PROTO-014 (T1)** The KV `Insert` operation MUST be distinct from `Put`: Insert MUST fail with error `1006` (ERR_KEY_EXISTS) when the key already exists; Put MUST overwrite unconditionally.

**REQ-PROTO-015 (T1)** KV `Scan` end key MUST be exclusive. `DeleteRange` end key MUST be exclusive. The client MUST encode these correctly on the wire.

**REQ-PROTO-016 (T1)** Stream `Begin` MUST send `expected_offset` on every call. The client MUST NOT omit this field; the server enforces it for optimistic concurrency (AC-STREAM-013).

**REQ-PROTO-017 (T1)** RPC correlation IDs MUST be unique 16-byte UUIDs generated by the client. The client MUST match inbound RESPONSE frames to their pending call by `correlation_id`, not by arrival order.

**REQ-PROTO-018 (T1)** `Notice.Publish` produces no response frame. The client MUST NOT block waiting for one.

---

## 2. API Completeness

### T0 — Full Surface Area

**REQ-API-001 (T0)** The public API MUST expose all seven domains, accessible from a single top-level `Client` type.

**REQ-API-002 (T0)** Every domain MUST expose the full operation set defined in `client-spec.md`:

| Domain | Required Operations |
|--------|--------------------|
| KV | Begin, Get, Put, Insert, Delete, DeleteRange, Scan, Commit, Rollback |
| Queue | Enqueue, Reserve, Extend, Complete |
| Notice | Publish, Subscribe, Unsubscribe, UnsubscribeAll |
| RPC | RegisterWorker, Call |
| Lease | Acquire, Extend (Renew), Release, Query |
| Stream | Begin, Append, Commit, Rollback, Read, Peek, Metadata, Subscribe, Unsubscribe |
| Schedule | Create, Cancel, List, Subscribe, Unsubscribe |

**REQ-API-003 (T0)** The `Queue.Subscribe` / `Queue.Unsubscribe` operations for queue availability notifications (QUEUE_NOTIFY 209) MUST be exposed.

**REQ-API-004 (T0)** The `Lease.Subscribe` / `Lease.Unsubscribe` operations for lease change notifications (LEASE_NOTIFY 409) MUST be exposed.

**REQ-API-005 (T0)** The `Schedule.List` operation MUST support pagination (offset, limit) and return a total count alongside the results.

**REQ-API-006 (T0)** `Schedule.ListBySelector` (filtered listing by selector pattern) MUST be exposed.

**REQ-API-007 (T0)** The client's connection state MUST be observable. At minimum, the state machine transitions (DISCONNECTED → CONNECTING → CONNECTED → AUTHENTICATING → AUTHENTICATED → CLOSED) must be exposed as typed values or events.

### T1 — Iterator and Streaming

**REQ-API-008 (T1)** Operations that return variable-length result sets — KV Scan, Stream Read, Schedule List, and RPC chunked responses — MUST return an iterator (or language-equivalent lazy cursor) rather than a fully-buffered slice. The iterator MUST be closable to release server-side resources.

**REQ-API-009 (T1)** The `Queue.Reserve` operation MUST return a typed item object that carries its own `Extend` and `Complete` methods, encapsulating the message ID and fencing token so callers never handle raw tokens directly.

**REQ-API-010 (T1)** The `Lease.Acquire` result MUST be a typed `Lease` object that carries its own `Extend` and `Release` methods, encapsulating the opaque token.

---

## 3. API Ergonomics & Design

### T0 — No Wire Internals in Public API

**REQ-ERGON-001 (T0)** Wire-level identifiers (tx_id, session_id, subscription_id, message_id, correlation_id) MUST NOT appear in the public API. They are internal implementation details. Users MUST operate on opaque typed objects (Transaction, StreamSession, Subscription, QueueItem, Lease).

**REQ-ERGON-002 (T0)** The client MUST NOT require users to specify or track the transport-framing layer (TLV, TCP length prefix). The public API is domain operations only.

**REQ-ERGON-003 (T0)** Terminology in the public API MUST use the canonical terms from `client-spec.md`: `realm`, `area`, `resource`, `route`. Synonyms (`tenant`, `namespace`, `collection`, `endpoint`, `path`, `key` (for route), `topic`) are forbidden in all public symbols.

### T1 — Object-Oriented State Management

**REQ-ERGON-004 (T1)** KV operations MUST be expressed through a transaction object, not free functions that accept a raw tx_id. The lifecycle is: `client.KV().Begin(ctx, route) → Transaction`, then `tx.Get(ctx, key)`, `tx.Put(...)`, `tx.Commit(ctx)`, `tx.Rollback(ctx)`.

**REQ-ERGON-005 (T1)** Stream write operations MUST be expressed through a session object: `client.Stream().Begin(ctx, route, expectedOffset) → StreamSession`, then `session.Append(ctx, body)`, `session.Commit(ctx)`, `session.Rollback(ctx)`.

**REQ-ERGON-006 (T1)** Subscriptions MUST be expressed through a subscription object with a discoverable `Unsubscribe` method. The user MUST NOT be required to track a raw subscription_id.

**REQ-ERGON-007 (T1)** The KV `Get` operation MUST NOT use a nil pointer or sentinel byte slice to signal "not found". It MUST return a typed discriminated result (e.g., `GetResult` with explicit Found/NotFound variants) so callers can distinguish "missing key" (valid) from "operation error" (exceptional).

### T1 — Configuration

**REQ-ERGON-008 (T1)** Client construction MUST use a configuration options pattern (functional options or a dedicated options/config struct) rather than positional constructor arguments beyond URL and token source. Required options for T1: transport type, reconnect policy, read/write timeouts, logger, frame size limit.

**REQ-ERGON-009 (T1)** The token/auth source MUST be expressed as a callable (function or interface), not a static string, so token refresh can be implemented by the caller without rebuilding the client.

**REQ-ERGON-010 (T1)** Default values MUST be safe: KV transactions default to `ReadWrite` + `Sync` durability; stream sessions default to `Sync` durability. Buffered/ReadOnly modes MUST be opt-in.

### T2 — Developer Convenience

**REQ-ERGON-011 (T2)** Domain clients SHOULD be accessible as named fields or typed methods on the top-level `Client` (e.g., `client.KV()`, `client.Notice()`) rather than requiring separate construction.

**REQ-ERGON-012 (T2)** The public API SHOULD be expressible through interfaces (not only concrete types) so users can write testable application code against mock or stub clients.

**REQ-ERGON-013 (T2)** RPC worker registration SHOULD return a typed registration object with a `Deregister()` method, mirroring the subscription pattern used by Notice, Stream, Lease, and Schedule.

---

## 4. Connection Lifecycle & Resilience

### T0 — Basic Lifecycle

**REQ-CONN-001 (T0)** The client MUST implement the full connection state machine: DISCONNECTED → CONNECTING → CONNECTED → AUTHENTICATING → AUTHENTICATED → CLOSED (per `connection-flow.md`).

**REQ-CONN-002 (T0)** Auth rejection MUST transition the client to CLOSED (not DISCONNECTED). The client MUST NOT auto-reconnect after auth rejection.

**REQ-CONN-003 (T0)** Calling `Close()` on the client MUST cleanly shut down the transport, cancel all pending in-flight operations with a recognizable error (not a context deadline exceeded), and release all goroutines.

**REQ-CONN-004 (T0)** After the client is closed, subsequent domain calls MUST return an error immediately without panicking.

### T1 — Reconnect & Recovery

**REQ-CONN-005 (T1)** The client MUST support configurable automatic reconnection on network-level disconnects (not auth failures). Reconnect MUST use exponential backoff with configurable base delay, multiplier, and maximum attempts.

**REQ-CONN-006 (T1)** On reconnect, all active subscriptions (Notice, RPC, Stream, Lease, Schedule, Queue) MUST be automatically re-established before the client enters AUTHENTICATED state. The application MUST NOT be required to manually re-subscribe.

**REQ-CONN-007 (T1)** In-flight KV transactions and Stream sessions that were open at the time of disconnect MUST be cancelled and their pending `context.Context` values resolved with an error. They MUST NOT silently stall.

**REQ-CONN-008 (T1)** The client MUST correctly timeout connection attempts. If the broker does not respond within a configurable settle window after the CONNECT frame is sent, the connection MUST be treated as failed.

**REQ-CONN-009 (T1)** The connection state MUST be queryable at any time without blocking (e.g., `client.State() ConnectionState`).

### T2 — Advanced Resilience

**REQ-CONN-010 (T2)** The client SHOULD support a configurable reconnect backoff ceiling (max delay between attempts) to prevent retry storms.

**REQ-CONN-011 (T2)** The client SHOULD support token refresh on reconnect: the token provider callable (see REQ-ERGON-009) MUST be re-invoked for each reconnect attempt so expiring JWTs are refreshed without client restart.

---

## 5. Concurrency Safety

### T0 — No Data Races

**REQ-CONC-001 (T0)** Every public method MUST be safe to call concurrently from multiple goroutines. The client MUST pass the Go race detector (`go test -race`) at T0.

**REQ-CONC-002 (T0)** Outbound writes to the transport MUST be serialized. The multiplexer MUST acquire a write lock before writing any frame to the underlying transport.

**REQ-CONC-003 (T0)** The inbound read loop MUST run in a dedicated goroutine and dispatch to pending response channels or notification handlers without holding the write lock.

**REQ-CONC-004 (T0)** NOTIFY handler dispatch (Notice, RPC REQUEST, Lease, Queue, Stream, Schedule) MUST NOT block the read loop. Handlers MUST be dispatched asynchronously (e.g., to a goroutine or a buffered channel).

### T1 — Goroutine Lifecycle

**REQ-CONC-005 (T1)** The total number of goroutines spawned by the client MUST be bounded. The client MUST NOT spawn an unbounded goroutine per subscription or per notification.

**REQ-CONC-006 (T1)** All goroutines started by `Connect` or `Subscribe` MUST be cleanly stopped when `Close()` is called. Verification: goroutine count after `Close()` MUST return to baseline (testable via `runtime.NumGoroutine()`).

**REQ-CONC-007 (T1)** Async handler goroutines MUST have a configurable maximum concurrency limit (`WithAsyncHandlerMaxConcurrency`) and a per-handler execution timeout (`WithAsyncHandlerTimeout`) to prevent runaway handlers from exhausting resources.

### T2 — Multiplexer Correctness

**REQ-CONC-008 (T2)** Same-transaction KV operations MUST be serialized at the call site or at the multiplexer level. The concurrency spec allows Level 1 and Level 2 parallelism but forbids Level 3 (same tx_id) parallelism. The client SHOULD enforce or document this constraint.

**REQ-CONC-009 (T2)** RPC calls MUST support true per-request multiplexing: multiple simultaneous `Call` invocations on different correlation IDs MUST all be in-flight concurrently without serialization.

---

## 6. Error Handling

### T0 — Surface All Errors

**REQ-ERR-001 (T0)** Every server error response (status byte = 1) MUST be surfaced to the caller as a non-nil error. Silent discard of server errors is a critical defect.

**REQ-ERR-002 (T0)** Every error MUST carry the numeric error code and the human-readable message from the server response payload.

**REQ-ERR-003 (T0)** `context.Context` cancellation and deadline expiry MUST be correctly propagated: if the calling context is cancelled before a response arrives, the operation MUST return `ctx.Err()` (or a wrapping error), and the pending response MUST be cleaned up.

### T1 — Typed Errors

**REQ-ERR-004 (T1)** Each domain MUST have a strongly typed error type (e.g., `*KvError`, `*StreamError`, `*LeaseError`) that carries the numeric code and uses `errors.As` for inspection.

**REQ-ERR-005 (T1)** Domain error codes MUST be exported as named constants (e.g., `ErrKvKeyExists`, `ErrLeaseHeld`, `ErrRpcRouteNotRegistered`) so callers can write `errors.Is(err, fitz.ErrKvKeyExists)` without hard-coding integers.

**REQ-ERR-006 (T1)** Retryable errors (codes 1004, 2004, 4005, 5001, 6001, 6002, 6003, 6004) MUST be distinguishable from fatal errors via a type assertion or helper (`fitz.IsRetryable(err) bool`). Callers MUST NOT be required to know the numeric ranges.

**REQ-ERR-007 (T1)** Server error messages MUST be included in the `Error()` string. `fmt.Errorf("kv get: %w", err)` wrapping MUST preserve the code through the chain.

### T2 — Operational Errors

**REQ-ERR-008 (T2)** Transport-level errors (connection refused, TLS failure, unexpected close) MUST be wrapped in a typed transport error distinct from domain errors, so callers can tell the difference between "server said no" and "could not reach server".

**REQ-ERR-009 (T2)** The client SHOULD log retryable errors at DEBUG level and fatal errors at WARN level (with structured fields: domain, operation, code, route, latency) so production operators get actionable signals without noise.

---

## 7. Observability

### T1 — Structured Logging

**REQ-OBS-001 (T1)** The client MUST accept an optional structured logger. In Go, this MUST be `*slog.Logger`. If no logger is provided, the client MUST default to a no-op logger (not to the default stdlib logger).

**REQ-OBS-002 (T1)** The client MUST log the following at appropriate levels:
- Connection established (INFO)
- Auth failure + reason (WARN)
- Reconnect attempt + attempt number + backoff delay (INFO)
- Reconnect success (INFO)
- Terminal connect failure (ERROR)
- Handler timeout or panic (WARN/ERROR)

**REQ-OBS-003 (T1)** Log records MUST use structured key-value fields, not printf-style strings. Required fields on connection events: `transport`, `addr`, `state`. Required fields on operation errors: `domain`, `op`, `code`, `route`, `latency_ms`.

### T2 — OpenTelemetry

**REQ-OBS-004 (T2)** The client SHOULD accept an optional `trace.Tracer`. When provided, every domain operation MUST create a child span with the operation name as span name (e.g., `fitz.kv.begin`, `fitz.notice.publish`).

**REQ-OBS-005 (T2)** Span attributes SHOULD follow OpenTelemetry semantic conventions where applicable. Additional Fitz-specific attributes: `fitz.domain`, `fitz.route`, `fitz.op`, `fitz.tx_id` (internal, for correlation in traces).

**REQ-OBS-006 (T2)** The client SHOULD accept an optional `metric.Meter`. When provided, the client MUST record:
- `fitz.request.duration` histogram (per domain, per op, with `error` label)
- `fitz.request.errors` counter (per domain, per error code)
- `fitz.connection.state` gauge (current connection state as numeric)
- `fitz.subscriptions.active` gauge (total live subscriptions)

**REQ-OBS-007 (T2)** Tracing and metrics MUST be strictly opt-in (zero-cost when not configured). No OpenTelemetry code MUST execute unless a Tracer/Meter is injected.

---

## 8. Performance

Target baseline derived from the ecosystem performance bar (`.NET PERF_GUIDELINES.md`) adapted for Go idioms.

### T1 — Allocation Discipline

**REQ-PERF-001 (T1)** Frame encoding for fixed-size request types (KV Get, KV Put, Notice Publish, Lease Acquire/Release) MUST NOT allocate on the heap in the steady state. Use pre-allocated encode buffers or `sync.Pool`.

**REQ-PERF-002 (T1)** Response parsing for fixed-size responses MUST decode from the inbound buffer without an intermediate copy. `[]byte` slices passed to callers that come directly from the transport buffer MUST be documented as borrowed (copy if retained beyond the callback).

**REQ-PERF-003 (T1)** The client's multiplexer MUST NOT use a single global mutex held across both write serialization and response dispatch. Write serialization and handler dispatch MUST use separate locking to avoid head-of-line blocking.

### T2 — Throughput & Latency Targets

These targets apply to a loopback connection (broker and client on the same machine) with no application processing overhead:

**REQ-PERF-004 (T2)** Round-trip latency for a KV transaction (Begin + Put + Commit) SHOULD be < 500 µs at p99 on loopback.

**REQ-PERF-005 (T2)** Frame encoding latency for a single TLV frame SHOULD be < 500 ns per frame.

**REQ-PERF-006 (T2)** RPC correlation ID lookup SHOULD be < 2 µs with 1,000+ concurrent in-flight calls (hash map lookup, not linear scan).

**REQ-PERF-007 (T2)** The `Notice.Publish` hot path (fire-and-forget, no response wait) SHOULD achieve > 50,000 ops/sec on a single goroutine on loopback.

**REQ-PERF-008 (T2)** Per-operation allocations on the hot path SHOULD be measured and tracked via benchmarks. The benchmark suite MUST include at minimum: Notice Publish, KV Get, KV Put, Lease Acquire, RPC Call.

---

## 9. Test Coverage

### T0 — Protocol Unit Tests

**REQ-TEST-001 (T0)** The TLV encoder MUST have unit tests covering: u8, u16, u32, u64, string, bytes, optional present, optional absent, UUID encoding, and the escape byte (0xFF) path for MessageType ≥255.

**REQ-TEST-002 (T0)** The TLV decoder MUST have unit tests covering: happy path for every primitive type, length mismatch, duplicate tag detection (MUST return error), and truncated payload.

**REQ-TEST-003 (T0)** Error code constants MUST have a test asserting their numeric values against the canonical registry (REQ-PROTO-001 table). This prevents silent drift when the registry is updated.

### T1 — Integration Tests

**REQ-TEST-004 (T1)** Integration tests MUST exist for every domain covering the complete happy-path lifecycle:
- KV: Begin → Put → Get → Scan → Commit → verify
- Queue: Enqueue → Reserve → Extend → Complete
- Notice: Subscribe → Publish (from second client) → receive NOTIFY → Unsubscribe
- RPC: RegisterWorker → Call → receive REQUEST → send RESPONSE  → receive at caller
- Lease: Acquire → Query → Extend → Release
- Stream: Begin → Append → Commit → Read → Peek → Metadata → Subscribe → receive NOTIFY
- Schedule: Create → List → Subscribe → receive NOTIFY → Cancel

**REQ-TEST-005 (T1)** Every integration test MUST run with both TCP and WebSocket transports. A test helper (e.g., `RunWithBothTransports`) MUST make this frictionless.

**REQ-TEST-006 (T1)** Every integration test MUST use `context.WithTimeout` with a wall-clock deadline. Tests MUST NOT hang indefinitely on broker unresponsiveness.

**REQ-TEST-007 (T1)** Every integration test MUST use a unique route (e.g., via a `t.Name()`-derived prefix or `UniqueRoute` helper) to prevent state leakage between test cases.

**REQ-TEST-008 (T1)** Integration tests for error paths MUST exist for each domain: unauthorized operation (expected error code), invalid input (e.g., inverted KV range, invalid cron), and transaction/session invalidation after disconnect.

**REQ-TEST-009 (T1)** The reconnect flow MUST have an integration test: client subscribes → broker is restarted (or connection dropped) → client reconnects → subscriptions are re-established → notifications resume.

**REQ-TEST-010 (T1)** The conformance test suite (from `cross-language-conformance-suite.yaml`) MUST be implemented and MUST achieve **100% P0 pass rate** across all four CI combinations (TCP × anonymous, TCP × valid_jwt, WebSocket × anonymous, WebSocket × valid_jwt).

### T2 — Coverage & Race Detection

**REQ-TEST-011 (T2)** All tests MUST pass under the race detector (`go test -race ./...`). CI MUST run with `-race` enabled.

**REQ-TEST-012 (T2)** The conformance test suite MUST achieve **100% P1 pass rate** in addition to P0.

**REQ-TEST-013 (T2)** A benchmark suite MUST exist covering at minimum the hot paths listed in REQ-PERF-008. Benchmarks MUST be runnable independently of integration tests and MUST produce stable baselines comparable across commits.

**REQ-TEST-014 (T2)** Goroutine leak detection MUST be verified in at least one integration test: after calling `client.Close()`, the goroutine count MUST equal the pre-`Connect` count (using a goroutine leak checker or manual `runtime.NumGoroutine` assertions).

---

## 10. Documentation & Developer Experience

### T1 — Package Documentation

**REQ-DOCS-001 (T1)** The root package MUST have a package-level doc comment that explains: what Fitz is, how to create a client, and links to the full documentation.

**REQ-DOCS-002 (T1)** Every exported type, method, and function MUST have a godoc comment. Documentation MUST be accurate — it MUST NOT describe behavior the implementation does not exhibit.

**REQ-DOCS-003 (T1)** All error constants MUST be documented with their numeric code, the conditions under which the server returns them, and whether they are retryable.

**REQ-DOCS-004 (T1)** The README MUST include a working quickstart that connects to a broker, performs a KV transaction, and publishes a notice. The quickstart MUST be `go run`-able without modification after adding the module dependency.

### T2 — Examples & API Hygiene

**REQ-DOCS-005 (T2)** Runnable `Example*` functions SHOULD exist for each domain in the `_test.go` or `example_*_test.go` files so they appear in godoc.

**REQ-DOCS-006 (T2)** Misuse of the API MUST produce a clear, actionable error at call time — not a panic, not a nil pointer dereference, not a generic "connection error". For example: calling a domain method before `Connect`, or calling `Commit` after `Rollback`, MUST return a typed error with a descriptive message.

**REQ-DOCS-007 (T2)** The module MUST declare a stable `v1` API (`module github.com/cntryl/fitz-go` with no pseudo-version suffix in go.mod) before being considered world-class. Pre-v1 clients can break callers; v1 signals API stability.

**REQ-DOCS-008 (T2)** A `CHANGELOG.md` or equivalent SHOULD be maintained with a per-version summary of breaking changes, new features, and bug fixes so consumers can safely upgrade.

---

## Grading Scorecard

Use this table to grade a specific client implementation. For each row, mark:
- **Pass** — requirement met
- **Partial** — requirement partially met; note what is missing
- **Fail** — requirement not met
- **N/A** — not applicable to this language/client

**Overall grade tiers:**
- **T0 complete** = all T0 requirements Pass → client is functional
- **T1 complete** = all T0 + all T1 requirements Pass → client is production-grade
- **T2 complete** = all T0 + T1 + T2 requirements Pass → client is world-class

| Req ID | Tier | Area | Short Description |
|--------|------|------|-------------------|
| REQ-PROTO-001 | T0 | Protocol | Message type constants match canonical registry |
| REQ-PROTO-002 | T0 | Protocol | TLV encoding: big-endian, escape byte, 64 KiB value limit |
| REQ-PROTO-003 | T0 | Protocol | TCP u32 prefix; WebSocket binary only |
| REQ-PROTO-004 | T0 | Protocol | CONNECT is first frame; no ACK wait |
| REQ-PROTO-005 | T0 | Protocol | No duplicate TLV tags emitted |
| REQ-PROTO-006 | T0 | Protocol | All 7 domains implemented |
| REQ-PROTO-007 | T0 | Protocol | All acceptance criteria AC-* pass |
| REQ-PROTO-008 | T0 | Protocol | Auth rejection handled; no retry with same JWT |
| REQ-PROTO-009 | T0 | Protocol | JWT treated as opaque bytes |
| REQ-PROTO-010 | T0 | Protocol | Routes treated as opaque strings |
| REQ-PROTO-011 | T1 | Protocol | Error code domain mapping correct |
| REQ-PROTO-012 | T1 | Protocol | Retryable vs. fatal error categorization |
| REQ-PROTO-013 | T1 | Protocol | Frame size respected; configurable |
| REQ-PROTO-014 | T1 | Protocol | Insert distinct from Put |
| REQ-PROTO-015 | T1 | Protocol | Scan/DeleteRange end-key exclusive |
| REQ-PROTO-016 | T1 | Protocol | Stream Begin includes expected_offset |
| REQ-PROTO-017 | T1 | Protocol | RPC correlation IDs unique; matched by ID not order |
| REQ-PROTO-018 | T1 | Protocol | Notice Publish: no response wait |
| REQ-API-001 | T0 | API | All 7 domains on top-level Client |
| REQ-API-002 | T0 | API | Full operation set per domain |
| REQ-API-003 | T0 | API | Queue availability subscribe/unsubscribe |
| REQ-API-004 | T0 | API | Lease change subscribe/unsubscribe |
| REQ-API-005 | T0 | API | Schedule List with pagination + total count |
| REQ-API-006 | T0 | API | Schedule ListBySelector |
| REQ-API-007 | T0 | API | Connection state observable |
| REQ-API-008 | T1 | API | Iterator for variable-length results |
| REQ-API-009 | T1 | API | QueueItem carries Extend/Complete |
| REQ-API-010 | T1 | API | Lease object carries Extend/Release |
| REQ-ERGON-001 | T0 | Ergonomics | Wire IDs absent from public API |
| REQ-ERGON-002 | T0 | Ergonomics | No TLV/framing in public API |
| REQ-ERGON-003 | T0 | Ergonomics | Canonical terminology enforced |
| REQ-ERGON-004 | T1 | Ergonomics | KV via Transaction object |
| REQ-ERGON-005 | T1 | Ergonomics | Stream writes via StreamSession object |
| REQ-ERGON-006 | T1 | Ergonomics | Subscriptions via typed Subscription object |
| REQ-ERGON-007 | T1 | Ergonomics | KV Get: typed Found/NotFound result |
| REQ-ERGON-008 | T1 | Ergonomics | Options/config pattern; not positional args |
| REQ-ERGON-009 | T1 | Ergonomics | Token source is a callable, not a string |
| REQ-ERGON-010 | T1 | Ergonomics | Safe defaults (Sync durability, ReadWrite mode) |
| REQ-ERGON-011 | T2 | Ergonomics | Domain clients as typed accessors on Client |
| REQ-ERGON-012 | T2 | Ergonomics | Public API expressible as interfaces |
| REQ-ERGON-013 | T2 | Ergonomics | RPC worker registration object with Deregister |
| REQ-CONN-001 | T0 | Lifecycle | Full connection state machine implemented |
| REQ-CONN-002 | T0 | Lifecycle | Auth rejection → CLOSED; no auto-reconnect |
| REQ-CONN-003 | T0 | Lifecycle | Close() cleans up transport and goroutines |
| REQ-CONN-004 | T0 | Lifecycle | Post-close domain calls return error, not panic |
| REQ-CONN-005 | T1 | Lifecycle | Auto-reconnect with exponential backoff |
| REQ-CONN-006 | T1 | Lifecycle | Subscriptions auto-re-established on reconnect |
| REQ-CONN-007 | T1 | Lifecycle | Disconnect cancels in-flight KV/Stream ops |
| REQ-CONN-008 | T1 | Lifecycle | Connect timeout configurable |
| REQ-CONN-009 | T1 | Lifecycle | Connection state queryable without blocking |
| REQ-CONN-010 | T2 | Lifecycle | Reconnect backoff ceiling configurable |
| REQ-CONN-011 | T2 | Lifecycle | Token refresh on each reconnect attempt |
| REQ-CONC-001 | T0 | Concurrency | Public API concurrency-safe; passes -race |
| REQ-CONC-002 | T0 | Concurrency | Outbound writes serialized |
| REQ-CONC-003 | T0 | Concurrency | Read loop on dedicated goroutine |
| REQ-CONC-004 | T0 | Concurrency | NOTIFY dispatch is async (non-blocking read loop) |
| REQ-CONC-005 | T1 | Concurrency | Bounded goroutine count |
| REQ-CONC-006 | T1 | Concurrency | All goroutines exit on Close() |
| REQ-CONC-007 | T1 | Concurrency | Async handler max concurrency + timeout |
| REQ-CONC-008 | T2 | Concurrency | Same-tx operations serialized or documented |
| REQ-CONC-009 | T2 | Concurrency | RPC calls truly concurrent (by correlation ID) |
| REQ-ERR-001 | T0 | Errors | All server errors surfaced as non-nil error |
| REQ-ERR-002 | T0 | Errors | Error carries numeric code + message |
| REQ-ERR-003 | T0 | Errors | ctx cancellation propagated; pending op cleaned up |
| REQ-ERR-004 | T1 | Errors | Typed error per domain; errors.As compatible |
| REQ-ERR-005 | T1 | Errors | Error code constants exported |
| REQ-ERR-006 | T1 | Errors | IsRetryable(err) helper |
| REQ-ERR-007 | T1 | Errors | Error message preserved through wrapping |
| REQ-ERR-008 | T2 | Errors | Transport errors typed separately from domain errors |
| REQ-ERR-009 | T2 | Errors | Retryable errors logged at DEBUG; fatal at WARN |
| REQ-OBS-001 | T1 | Observability | Optional slog.Logger; defaults to no-op |
| REQ-OBS-002 | T1 | Observability | Connection events logged at correct levels |
| REQ-OBS-003 | T1 | Observability | Structured log fields (no printf) |
| REQ-OBS-004 | T2 | Observability | Optional OTel trace.Tracer; span per operation |
| REQ-OBS-005 | T2 | Observability | Span attributes: fitz.domain, fitz.route, fitz.op |
| REQ-OBS-006 | T2 | Observability | Optional OTel metric.Meter; 4 metric instruments |
| REQ-OBS-007 | T2 | Observability | OTel strictly opt-in; zero-cost when absent |
| REQ-PERF-001 | T1 | Performance | Hot path frame encoding: no heap alloc |
| REQ-PERF-002 | T1 | Performance | Response parsing: no intermediate copy |
| REQ-PERF-003 | T1 | Performance | Multiplexer: separate locks for write vs. dispatch |
| REQ-PERF-004 | T2 | Performance | KV round-trip < 500 µs p99 on loopback |
| REQ-PERF-005 | T2 | Performance | Frame encode < 500 ns per frame |
| REQ-PERF-006 | T2 | Performance | RPC correlation lookup < 2 µs at 1k+ concurrent |
| REQ-PERF-007 | T2 | Performance | Notice Publish > 50k ops/sec single goroutine |
| REQ-PERF-008 | T2 | Performance | Benchmark suite for hot paths |
| REQ-TEST-001 | T0 | Testing | TLV encoder unit tests |
| REQ-TEST-002 | T0 | Testing | TLV decoder unit tests |
| REQ-TEST-003 | T0 | Testing | Error code constant values tested against registry |
| REQ-TEST-004 | T1 | Testing | Integration tests: full lifecycle per domain |
| REQ-TEST-005 | T1 | Testing | Tests run with both TCP and WebSocket |
| REQ-TEST-006 | T1 | Testing | All tests use context.WithTimeout |
| REQ-TEST-007 | T1 | Testing | All tests use unique routes |
| REQ-TEST-008 | T1 | Testing | Error path integration tests per domain |
| REQ-TEST-009 | T1 | Testing | Reconnect + re-subscribe integration test |
| REQ-TEST-010 | T1 | Testing | Conformance suite: 100% P0 pass rate |
| REQ-TEST-011 | T2 | Testing | All tests pass under -race |
| REQ-TEST-012 | T2 | Testing | Conformance suite: 100% P1 pass rate |
| REQ-TEST-013 | T2 | Testing | Benchmark suite runnable independently |
| REQ-TEST-014 | T2 | Testing | Goroutine leak verified after Close() |
| REQ-DOCS-001 | T1 | Docs | Package-level godoc comment |
| REQ-DOCS-002 | T1 | Docs | All exported symbols documented |
| REQ-DOCS-003 | T1 | Docs | Error constants documented with codes + retryability |
| REQ-DOCS-004 | T1 | Docs | README with go-run-able quickstart |
| REQ-DOCS-005 | T2 | Docs | Example functions per domain in godoc |
| REQ-DOCS-006 | T2 | Docs | API misuse returns clear error, not panic |
| REQ-DOCS-007 | T2 | Docs | Module at stable v1 |
| REQ-DOCS-008 | T2 | Docs | CHANGELOG maintained |

---

## T0 Count Summary

| Area | T0 | T1 | T2 | Total |
|------|----|----|----|---------| 
| Protocol | 10 | 8 | 0 | 18 |
| API Completeness | 7 | 3 | 0 | 10 |
| API Ergonomics | 3 | 7 | 3 | 13 |
| Connection Lifecycle | 4 | 5 | 2 | 11 |
| Concurrency | 4 | 3 | 2 | 9 |
| Error Handling | 3 | 4 | 2 | 9 |
| Observability | 0 | 3 | 4 | 7 |
| Performance | 0 | 3 | 5 | 8 |
| Testing | 3 | 7 | 4 | 14 |
| Documentation | 0 | 4 | 4 | 8 |
| **Total** | **34** | **47** | **26** | **107** |

A world-class client passes all 107 requirements.
