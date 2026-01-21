# Fitz Implementation TODO

**Priority-ordered remaining work. Remove items from this file when completed.**

Last updated: January 21, 2026

---

## CRITICAL (Blocks Implementation)

### Permission & Authentication System

- [ ] **Verify JWT validation in Layer 2 (Session)**
  - [ ] Check `src/session/permissions.rs` validates JWT signature (use external lib, NOT manual validation)
  - [ ] Verify expiration check against `exp` claim
  - [ ] Verify JWT claims extraction: `realm`, `areas` (array), `scopes` (array)
  - [ ] Add tests: valid JWT, expired JWT, invalid signature, missing claims
  - [ ] Reference: CLIENT.md lines 619–675, SERVER.md lines 633–649

- [ ] **Verify permission check order in request pipeline**
  - [ ] Check code implements: Route validation → JWT validation → Permission enforcement → Domain dispatch
  - [ ] Verify permission check failures return domain error code `*001` (ERR_UNAUTHORIZED)
  - [ ] Verify permission checks are per-request (realm match, area match, scope match)
  - [ ] Add tests: realm mismatch, area not in JWT areas, scope not in JWT scopes
  - [ ] Reference: CLIENT.md lines 641–650, SERVER.md lines 152–156

- [ ] **Verify standard error codes across all domains**
  - [ ] [ ] 1001 = ERR_UNAUTHORIZED (KV)
  - [ ] [ ] 1002 = ERR_INVALID_SCOPE (KV)
  - [ ] [ ] 1003 = ERR_REALM_MISMATCH (KV)
  - [ ] [ ] Same codes for other domains (3001 Notice, 4001 Queue, etc.)
  - [ ] Reference: CLIENT.md lines 1786–1819

### Session Lifecycle

- [ ] **Verify session creation on successful CONNECT**
  - [ ] Check session gets unique ID
  - [ ] Verify JWT claims stored in session
  - [ ] Verify subscriptions/transactions/workers tracked per-session
  - [ ] Add test: CONNECT → session created → domain requests accepted
  - [ ] Reference: SERVER.md lines 165–170

- [ ] **Verify session cleanup on disconnect**
  - [ ] Rollback all active KV transactions
  - [ ] Drop all active Notice subscriptions
  - [ ] Abort all active Stream sessions
  - [ ] Release all held Leases
  - [ ] Unregister all RPC workers
  - [ ] Discard queued notifications
  - [ ] Add integration test: disconnect → all state cleaned up
  - [ ] Reference: SERVER.md lines 171–178

- [ ] **Verify reconnect creates NEW session**
  - [ ] New session ID assigned
  - [ ] Old session ID becomes invalid
  - [ ] Previous subscriptions NOT recovered
  - [ ] Add test: disconnect → reconnect → subscriptions lost (require re-subscribe)
  - [ ] Reference: SERVER.md lines 179–182, CLIENT.md lines 237–241

### TLS & Transport Security

> **NOTE:** TLS termination handled externally (reverse proxy/load balancer/ingress controller).
> Fitz receives already-decrypted traffic. Infrastructure concern, not code implementation.

- [x] **TLS enforcement for production** (INFRASTRUCTURE - External)
  - WebSocket: `wss://` enforced by external TLS terminator
  - TCP: TLS connection handled by external proxy
  - Certificate validation: external responsibility
  - Hostname verification: external responsibility
  - Reference: CLIENT.md lines 277–305, SERVER.md lines 655–670

- [x] **Certificate validation** (INFRASTRUCTURE - External)
  - Certificate chain validation: external responsibility
  - Hostname verification: external responsibility
  - Self-signed certs: external configuration
  - Reference: CLIENT.md lines 286–298

---

## HIGH (Blocks Interop Tests)

### RPC Domain Implementation

- [x] **Verify RPC wire format matches spec exactly**
  - [x] SUBSCRIBE_WORKER request format (lines ~1055 in CLIENT.md)
  - [x] REQUEST wire format with correlation_id (exactly 16 bytes)
  - [x] RESPONSE with sequence and stream_end flag
  - [x] ACK wire format
  - [x] Add codec tests: encode/decode all message types
  - [x] Reference: CLIENT.md lines 1055–1108
  - ✅ Tests: tests/rpc_spec_validation.rs (27 tests)

- [x] **Verify RPC error codes match spec**
  - [x] 6001 = ERR_RPC_TIMEOUT
  - [x] 6002 = ERR_WORKER_NOT_FOUND
  - [x] 6003 = ERR_RPC_BACKPRESSURE
  - [x] 6004 = ERR_ROUTE_NOT_REGISTERED
  - [x] Reference: CLIENT.md line 1103

- [x] **Verify RPC acceptance tests pass**
  - [x] Single request/response cycle
  - [x] Streaming response reassembled in order
  - [x] Request timeout returns error
  - [x] Multiple workers on same route handle requests
  - [x] Response with wrong correlation_id rejected
  - [x] Backpressure error when buffer full
  - [x] Reference: CLIENT.md lines 1105–1108

### Queue Domain Implementation

- [x] **Verify Queue wire format matches spec exactly**
  - [x] ENQUEUE request/response (lines ~1015 in CLIENT.md)
  - [x] RESERVE request with batch_size (lines ~1025)
  - [x] EXTEND request format
  - [x] COMPLETE request with message_id + lease_token
  - [x] Add codec tests: encode/decode all message types
  - [x] Reference: CLIENT.md lines 1001–1052
  - ✅ Tests: tests/queue_spec_validation.rs (36 tests)

- [x] **Verify Queue acceptance tests pass**
  - [x] Enqueue/reserve/complete cycle
  - [x] Lease expiry returns message to ready queue
  - [x] Extend lease delays expiry
  - [x] Complete with wrong token fails
  - [x] Reserve with batch_size returns up to that many
  - [x] Multiple consumers can reserve from same queue
  - [x] Reference: CLIENT.md lines 1049–1052

### Request/Response Correlation

- [x] **Verify synchronous request/response model**
  - [x] Client sends request, blocks waiting for response
  - [x] Broker sends exactly one response per request
  - [x] No pipelining (no multiple requests in flight)
  - [x] Add test: verify async frames don't break sync model
  - [x] Reference: CLIENT.md lines 849–874
  - ✅ Tests: tests/request_response_correlation.rs (32 tests)

- [x] **Verify streaming/fanout exceptions work correctly**
  - [x] Notice SUBSCRIBE: first response (subscription ID), then async NOTIFYs
  - [x] RPC REQUEST: first response (accepted), then async RPC responses with sequence
  - [x] Stream READ: may return multiple frames for large result sets
  - [x] Add integration test: subscribe → async notifications arrive correctly
  - [x] Reference: CLIENT.md lines 859–878
  - ✅ Tests: tests/streaming_fanout_exceptions.rs (34 tests)

- [x] **Verify asynchronous frame handling**
  - [x] Client buffers async frames while waiting for next response
  - [x] Async frames dispatch to correct handlers
  - [x] No frame loss or reordering
  - [x] Add test: send request, receive async notification, send next request
  - [x] Reference: CLIENT.md lines 882–886
  - ✅ Tests: tests/request_response_correlation.rs (32 tests)

### Idempotency & Retry

- [x] **Verify idempotency classification is enforced** ⏳ FAILING TESTS
  - [x] Idempotent ops (GET, SCAN, READ, LAST, QUERY, RESERVE): safe to retry 
  - [x] NOT idempotent ops: PUT, INSERT, DELETE, APPEND, BEGIN, COMMIT, PUBLISH, ENQUEUE, etc.
  - [x] Context-dependent ops (COMPLETE, REQUEST): require deduplication
  - [x] Add tests: verify retry behavior matches classification per domain
  - [x] Reference: CLIENT.md lines 892–950
  - 📋 Tests (FAILING): tests/idempotency_classification.rs (33 tests)

- [x] **Verify deduplication for context-dependent operations** ⏳ FAILING TESTS
  - [x] Queue COMPLETE: track message_id+token to avoid duplicate completion
  - [x] RPC REQUEST: track correlation_id to avoid duplicate processing
  - [x] Add test: retry COMPLETE with same token → idempotent (same result)
  - 📋 Tests (FAILING): tests/idempotency_classification.rs (tests 19-20, 24-25)
  - [ ] Reference: CLIENT.md lines 930–935

---

## MEDIUM (For Comprehensive Coverage)

### Error Handling & Recovery

- [x] **Verify transport error handling** ⏳ FAILING TESTS
  - [x] Connection refused → retry with backoff
  - [x] Connection reset → reconnect gracefully
  - [x] Frame too large → close connection, raise error
  - [x] Invalid UTF-8 → close connection, raise error
  - [x] TLV decode error → close connection, raise error
  - [x] Add integration tests for each error type
  - [x] Reference: CLIENT.md lines 811–825
  - 📋 Tests (FAILING): tests/error_handling_recovery.rs (28 tests)

- [x] **Verify domain error codes are complete** ⏳ FAILING TESTS
  - [x] Verify all domains define error codes in 100-block ranges
  - [x] Verify error codes can be extended without collision
  - [x] Add test: unknown error code handled gracefully
  - [x] Reference: CLIENT.md lines 1786–1819
  - 📋 Tests (FAILING): tests/error_handling_recovery.rs (28 tests)

- [x] **Verify error code allocation in all domains** ⏳ FAILING TESTS
  - [x] KV: 1000–1099 (verify no gaps, no collisions)
  - [x] Stream: 2000–2099
  - [x] Notice: 3000–3099
  - [x] Queue: 4000–4099
  - [x] Lease: 5000–5099
  - [x] RPC: 6000–6099
  - [x] Schedule: 7000–7099
  - 📋 Tests (FAILING): tests/error_handling_recovery.rs (28 tests)

### All 7 Domain Implementations

- [x] **KV Domain: Verify all operations** ⏳ FAILING TESTS
  - [x] Verify error codes: 1001–1005 defined and used correctly
  - [x] Verify transactions (BEGIN/COMMIT/ROLLBACK) work per spec
  - [x] Verify isolation modes (ReadOnly/ReadWrite)
  - [x] Add acceptance tests from CLIENT.md (KV section)
  - [x] Reference: CLIENT.md lines 1205–1365
  - 📋 Tests (FAILING): tests/full_domain_implementations.rs (13 tests for KV)

- [x] **Stream Domain: Verify all operations** ⏳ FAILING TESTS
  - [x] Verify error codes: 2001–2005 defined
  - [x] Verify watermarks protect uncommitted data
  - [x] Verify optimistic concurrency (expected_offset)
  - [x] Add acceptance tests from CLIENT.md (Stream section)
  - [x] Reference: CLIENT.md lines 1000–1052
  - 📋 Tests (FAILING): tests/full_domain_implementations.rs (12 tests for Stream)

- [ ] **Notice Domain: Verify all operations**
  - [ ] Verify error codes: 3001–3004 defined
  - [ ] Verify wildcard pattern matching: `*` and `**`
  - [ ] Verify fanout to all subscribers
  - [ ] Add acceptance tests from CLIENT.md (Notice section)
  - [ ] Reference: CLIENT.md lines 959–1000

- [ ] **Queue Domain: Verify all operations** (listed above, but verify completely)
  - [ ] Verify error codes: 4001–4004 defined
  - [ ] Verify leasing model and visibility timeout
  - [ ] Verify token binding for COMPLETE/EXTEND
  - [ ] Add acceptance tests from CLIENT.md
  - [ ] Reference: CLIENT.md lines 1001–1052

- [x] **RPC Domain: Verify all operations** (listed above, but verify completely)
  - [x] Verify error codes: 6001–6004 defined
  - [x] Verify correlation_id exactly 16 bytes
  - [x] Verify streaming response reassembly
  - [x] Add acceptance tests from CLIENT.md
  - [x] Reference: CLIENT.md lines 1055–1108
  - ✅ Tests: tests/rpc_spec_validation.rs (27 tests)

- [ ] **Lease Domain: Verify all operations**
  - [ ] Verify error codes: 5001–5004 defined
  - [ ] Verify mutual exclusion (only one owner)
  - [ ] Verify fencing tokens prevent stale commands
  - [ ] Verify TTL-based expiry
  - [ ] Add acceptance tests from CLIENT.md (Lease section)
  - [ ] Reference: CLIENT.md lines 1366–1465

- [ ] **Schedule Domain: Verify all operations**
  - [ ] Verify error codes: 7001–7004 defined
  - [ ] Verify durable persistence across restart
  - [ ] Verify cron syntax support (5-field format)
  - [ ] Verify nested TLV payload parsing
  - [ ] Verify LIST streaming response format
  - [ ] Add acceptance tests from CLIENT.md (Schedule section)
  - [ ] Reference: CLIENT.md lines 1466–1550

### Acceptance Test Suite

- [ ] **Transport-level tests (from CLIENT.md)**
  - [ ] WebSocket connect with CONNECT frame
  - [ ] TCP connect with CONNECT frame
  - [ ] Frame size enforcement (>max_frame_size closes connection)
  - [ ] Reconnect creates new session
  - [ ] Reference: CLIENT.md lines 1953–1960

- [ ] **Permission/Auth tests**
  - [ ] Realm mismatch → ERR_UNAUTHORIZED
  - [ ] Area not in JWT areas → ERR_UNAUTHORIZED
  - [ ] Scope not in JWT scopes → ERR_UNAUTHORIZED
  - [ ] Valid JWT → request succeeds
  - [ ] Expired JWT → connection rejected

- [ ] **Multi-realm isolation tests**
  - [ ] Client A (realm=prod) cannot see Client B (realm=staging) resources
  - [ ] Subscriptions isolated per realm
  - [ ] Transactions isolated per realm
  - [ ] Cross-realm operations rejected

- [ ] **Interoperability tests (from CLIENT.md)**
  - [ ] All 7 domain happy-path tests
  - [ ] All 7 domain error-path tests
  - [ ] Multi-client concurrent operations
  - [ ] Fanout scale (1000+ subscribers receive all publishes)
  - [ ] Reference: CLIENT.md lines 1953–2020

---

## LOW (Nice-to-Have / Future)

### Edge Cases & Boundary Conditions

- [x] **Verify edge case handling** ⏳ FAILING TESTS
  - [x] Zero-length keys, values, events
  - [x] Maximum size enforcement (keys, values, events)
  - [x] Transaction/offset wraparound
  - [x] Realm/area limits
  - [x] Connection limits
  - [x] Add comprehensive edge case tests
  - [x] Reference: CLIENT.md (all domains)
  - 📋 Tests (FAILING): tests/edge_cases_recovery.rs (34 tests)

- [x] **Verify timeout and expiration handling** ⏳ FAILING TESTS
  - [x] Transaction timeout (idle expiry)
  - [x] Session timeout (idle expiry)
  - [x] Subscription timeout (long-lived)
  - [x] Lease expiration (TTL enforcement)
  - [x] Reference: CLIENT.md, SERVER.md
  - 📋 Tests (FAILING): tests/edge_cases_recovery.rs (34 tests)

- [x] **Verify recovery scenarios** ⏳ FAILING TESTS
  - [x] Partial commit recovery
  - [x] Incomplete append recovery
  - [x] Broker restart during operation
  - [x] Network partition handling
  - [x] Reference: SERVER.md lines 179–189
  - 📋 Tests (FAILING): tests/edge_cases_recovery.rs (34 tests)

- [x] **Verify data integrity** ⏳ FAILING TESTS
  - [x] Key order consistency in KV scans
  - [x] Event order persistence in streams
  - [x] Data corruption detection
  - [x] Duplicate operation handling
  - [x] Reference: CLIENT.md (per domain)
  - 📋 Tests (FAILING): tests/edge_cases_recovery.rs (34 tests)

### Performance & Scale

- [ ] **Benchmark idempotent operations**
  - [ ] GET/SCAN/READ performance (should be fast, no locks)
  - [ ] Compare with write operations (should be slower due to locks)
  - [ ] Target: <1ms for GET, <10ms for SCAN

- [ ] **Benchmark fanout**
  - [ ] Single PUBLISH to 1000 subscribers
  - [ ] Verify all clients receive NOTIFY
  - [ ] Measure latency (target: <100ms)

- [ ] **Scale test: large state**
  - [ ] KV: 1M+ keys
  - [ ] Notice: 10k+ subscriptions
  - [ ] Queue: 100k+ pending messages
  - [ ] Verify no performance degradation

### Documentation & Implementation Notes

- [ ] **Add implementation notes for broker maintainers**
  - [ ] Session ID generation strategy
  - [ ] Lease expiry check strategy (background task vs. lazy evaluation)
  - [ ] Notification fanout batching strategy
  - [ ] Performance tuning tips

- [ ] **Add SDK implementation notes**
  - [ ] How to implement connection retry with backoff
  - [ ] How to handle reconnect and state restoration
  - [ ] How to implement deduplication for idempotent retries
  - [ ] Common pitfalls and how to avoid them

- [ ] **Update domain-specific documentation**
  - [ ] Add KV transaction isolation levels to docs
  - [ ] Add Stream watermark semantics to docs
  - [ ] Add Notice pattern matching examples to docs
  - [ ] Add Queue leasing model to docs

### Future Protocol Extensions

- [ ] **Version negotiation (for future compatibility)**
  - [ ] Design protocol version handshake (if needed)
  - [ ] Plan backward compatibility strategy
  - [ ] Plan rollout strategy for new verbs

- [ ] **MessageType range expansion**
  - [ ] Define process for expanding error code ranges
  - [ ] Document when to use 1100–1199 vs. 1000–1099
  - [ ] Update spec with expansion rules

---

## VALIDATION CHECKLIST

Use this checklist to verify all remaining work is complete:

### Spec Compliance
- [ ] All error codes defined per CLIENT.md (lines 1786–1819)
- [ ] All wire formats match spec exactly (per domain)
- [ ] All acceptance tests pass (per CLIENT.md)
- [ ] Permission model matches spec (CLIENT.md lines 619–675)
- [ ] Session lifecycle matches spec (SERVER.md lines 165–182)
- [ ] TLS requirements enforced (SERVER.md lines 655–670)

### Security
- [ ] JWT signature validation implemented (not manual)
- [ ] JWT expiration checked
- [ ] Permission checks per-request
- [ ] TLS certificate validation enforced
- [ ] Self-signed certs only with explicit flag

### Functionality
- [ ] All 7 domains fully implemented
- [ ] Idempotency classification enforced
- [ ] Retry strategy working correctly
- [ ] Deduplication for context-dependent ops
- [ ] Error handling covers all cases

### Testing
- [ ] Transport-level tests pass (WebSocket, TCP, reconnect)
- [ ] Domain-level tests pass (all 7 domains)
- [ ] Acceptance tests pass (CLIENT.md)
- [ ] Interoperability tests pass (multi-client, multi-realm)
- [ ] Scale tests pass (fanout, large state)

---

## Notes

- **Items are ordered by impact and dependencies.** Complete CRITICAL items first, then HIGH, then MEDIUM.
- **Remove completed items from this file** to maintain focus on remaining work.
- **For each completed item, verify against spec** (CLIENT.md or SERVER.md) before removing.
- **When items are completed, commit with message:** `[DONE] {item_name}` for easy tracking.
- **Estimated effort:** 
  - CRITICAL: 4–6 weeks (auth, permissions, session lifecycle, TLS)
  - HIGH: 2–3 weeks (RPC, Queue, request/response, idempotency)
  - MEDIUM: 2–3 weeks (error handling, all domains, acceptance tests)
  - LOW: 1–2 weeks (perf, docs, future extensions)
  - **Total: ~10–14 weeks for full completion**

