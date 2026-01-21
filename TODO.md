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

- [ ] **Verify TLS enforcement for production**
  - [ ] WebSocket: require `wss://` (reject `ws://` in production)
  - [ ] TCP: require TLS connection
  - [ ] Certificate validation: chain validation against system CA roots
  - [ ] Hostname verification implemented (CN or SAN match)
  - [ ] Add configuration flag: allow self-signed only with explicit opt-in
  - [ ] Reference: CLIENT.md lines 277–305, SERVER.md lines 655–670

- [ ] **Verify certificate validation in code**
  - [ ] Check `src/api/ws.rs` validates certificate chain
  - [ ] Check `src/api/tcp.rs` validates certificate chain
  - [ ] Verify hostname verification (not just name existence, but correct match)
  - [ ] Add tests: valid cert, self-signed (should fail), expired cert (should fail)
  - [ ] Reference: CLIENT.md lines 286–298

---

## HIGH (Blocks Interop Tests)

### RPC Domain Implementation

- [ ] **Verify RPC wire format matches spec exactly**
  - [ ] SUBSCRIBE_WORKER request format (lines ~1055 in CLIENT.md)
  - [ ] REQUEST wire format with correlation_id (exactly 16 bytes)
  - [ ] RESPONSE with sequence and stream_end flag
  - [ ] ACK wire format
  - [ ] Add codec tests: encode/decode all message types
  - [ ] Reference: CLIENT.md lines 1055–1108

- [ ] **Verify RPC error codes match spec**
  - [ ] 6001 = ERR_RPC_TIMEOUT
  - [ ] 6002 = ERR_WORKER_NOT_FOUND
  - [ ] 6003 = ERR_RPC_BACKPRESSURE
  - [ ] 6004 = ERR_ROUTE_NOT_REGISTERED
  - [ ] Reference: CLIENT.md line 1103

- [ ] **Verify RPC acceptance tests pass**
  - [ ] Single request/response cycle
  - [ ] Streaming response reassembled in order
  - [ ] Request timeout returns error
  - [ ] Multiple workers on same route handle requests
  - [ ] Response with wrong correlation_id rejected
  - [ ] Backpressure error when buffer full
  - [ ] Reference: CLIENT.md lines 1105–1108

### Queue Domain Implementation

- [ ] **Verify Queue wire format matches spec exactly**
  - [ ] ENQUEUE request/response (lines ~1015 in CLIENT.md)
  - [ ] RESERVE request with batch_size (lines ~1025)
  - [ ] EXTEND request format
  - [ ] COMPLETE request with message_id + lease_token
  - [ ] Add codec tests: encode/decode all message types
  - [ ] Reference: CLIENT.md lines 1001–1052

- [ ] **Verify Queue acceptance tests pass**
  - [ ] Enqueue/reserve/complete cycle
  - [ ] Lease expiry returns message to ready queue
  - [ ] Extend lease delays expiry
  - [ ] Complete with wrong token fails
  - [ ] Reserve with batch_size returns up to that many
  - [ ] Multiple consumers can reserve from same queue
  - [ ] Reference: CLIENT.md lines 1049–1052

### Request/Response Correlation

- [ ] **Verify synchronous request/response model**
  - [ ] Client sends request, blocks waiting for response
  - [ ] Broker sends exactly one response per request
  - [ ] No pipelining (no multiple requests in flight)
  - [ ] Add test: verify async frames don't break sync model
  - [ ] Reference: CLIENT.md lines 849–874

- [ ] **Verify streaming/fanout exceptions work correctly**
  - [ ] Notice SUBSCRIBE: first response (subscription ID), then async NOTIFYs
  - [ ] RPC REQUEST: first response (accepted), then async RPC responses with sequence
  - [ ] Stream READ: may return multiple frames for large result sets
  - [ ] Add integration test: subscribe → async notifications arrive correctly
  - [ ] Reference: CLIENT.md lines 859–878

- [ ] **Verify asynchronous frame handling**
  - [ ] Client buffers async frames while waiting for next response
  - [ ] Async frames dispatch to correct handlers
  - [ ] No frame loss or reordering
  - [ ] Add test: send request, receive async notification, send next request
  - [ ] Reference: CLIENT.md lines 882–886

### Idempotency & Retry

- [ ] **Verify idempotency classification is enforced**
  - [ ] Idempotent ops (GET, SCAN, READ, LAST, QUERY, RESERVE): safe to retry 
  - [ ] NOT idempotent ops: PUT, INSERT, DELETE, APPEND, BEGIN, COMMIT, PUBLISH, ENQUEUE, etc.
  - [ ] Context-dependent ops (COMPLETE, REQUEST): require deduplication
  - [ ] Add tests: verify retry behavior matches classification per domain
  - [ ] Reference: CLIENT.md lines 892–950

- [ ] **Verify deduplication for context-dependent operations**
  - [ ] Queue COMPLETE: track message_id+token to avoid duplicate completion
  - [ ] RPC REQUEST: track correlation_id to avoid duplicate processing
  - [ ] Add test: retry COMPLETE with same token → idempotent (same result)
  - [ ] Reference: CLIENT.md lines 930–935

---

## MEDIUM (For Comprehensive Coverage)

### Error Handling & Recovery

- [ ] **Verify transport error handling**
  - [ ] Connection refused → retry with backoff
  - [ ] Connection reset → reconnect gracefully
  - [ ] Frame too large → close connection, raise error
  - [ ] Invalid UTF-8 → close connection, raise error
  - [ ] TLV decode error → close connection, raise error
  - [ ] Add integration tests for each error type
  - [ ] Reference: CLIENT.md lines 811–825

- [ ] **Verify domain error codes are complete**
  - [ ] Verify all domains define error codes in 100-block ranges
  - [ ] Verify error codes can be extended without collision
  - [ ] Add test: unknown error code handled gracefully
  - [ ] Reference: CLIENT.md lines 1786–1819

- [ ] **Verify error code allocation in all domains**
  - [ ] KV: 1000–1099 (verify no gaps, no collisions)
  - [ ] Stream: 2000–2099
  - [ ] Notice: 3000–3099
  - [ ] Queue: 4000–4099
  - [ ] Lease: 5000–5099
  - [ ] RPC: 6000–6099
  - [ ] Schedule: 7000–7099

### All 7 Domain Implementations

- [ ] **KV Domain: Verify all operations**
  - [ ] Verify error codes: 1001–1005 defined and used correctly
  - [ ] Verify transactions (BEGIN/COMMIT/ROLLBACK) work per spec
  - [ ] Verify isolation modes (ReadOnly/ReadWrite)
  - [ ] Add acceptance tests from CLIENT.md (KV section)
  - [ ] Reference: CLIENT.md lines 1205–1365

- [ ] **Stream Domain: Verify all operations**
  - [ ] Verify error codes: 2001–2005 defined
  - [ ] Verify watermarks protect uncommitted data
  - [ ] Verify optimistic concurrency (expected_offset)
  - [ ] Add acceptance tests from CLIENT.md (Stream section)
  - [ ] Reference: CLIENT.md lines 1000–1052

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

- [ ] **RPC Domain: Verify all operations** (listed above, but verify completely)
  - [ ] Verify error codes: 6001–6004 defined
  - [ ] Verify correlation_id exactly 16 bytes
  - [ ] Verify streaming response reassembly
  - [ ] Add acceptance tests from CLIENT.md
  - [ ] Reference: CLIENT.md lines 1055–1108

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

