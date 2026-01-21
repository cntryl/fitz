# Fitz Client Specification

## Purpose
This document defines the client contract, wire protocol expectations, and acceptance criteria for any Fitz client implementation. It is language-agnostic and focuses solely on behaviour the broker requires over supported transports (WebSocket and TCP).

Normative language: **MUST**, **MUST NOT**, **SHOULD**, **MAY**.

---

## Terminology (Strict)
Use these exact terms in implementations and docs: **realm**, **area**, **resource**, **operation**, **route**. Avoid forbidden terminology; use the Fitz terms above exclusively.

---

## Supported Transports
- WebSocket (binary frames) - recommended for browsers and bidirectional usage.
- Raw TCP (length-prefixed frames) - identical payload semantics; clients must implement u32 big-endian length framing.

Clients MUST behave identically across both transports - framing differs only by transport encapsulation.

---

## Wire Protocol Summary (Transport Frame + TLV Records)
Fitz transports carry a **frame payload** that is a concatenation of 1+ TLV records.

### Transport framing (authoritative)

**WebSocket:** each WebSocket *binary message* is a frame payload (raw bytes).

**TCP:** each frame payload is prefixed by a 4-byte length:

- `[u32 BE length][payload]`
- `length` counts only the payload bytes (does not include the 4-byte prefix).

> Source: `src/api/tcp.rs` and `src/api/ws.rs`.

### TLV record encoding (authoritative)

Each record is:

- **Type**: `MessageType` (u16) encoded as:
  - single byte if `type <= 0xFE`
  - else `0xFF` escape marker + 2-byte big-endian u16
- **Length**: 4-byte big-endian u32
- **Value**: `length` bytes

Records are concatenated back-to-back; a single transport frame MAY contain multiple records.

### Payload field encoding (authoritative)
All primitive fields in message payloads use **big-endian** encoding.

Common conventions (used by `src/protocol/tlv_codec.rs` and most domain codecs):
- **String**: `[u32 BE length][UTF-8 bytes]`
- **Bytes**: `[u32 BE length][raw bytes]`
- **Optional field**: `[u8 flag][value]` where flag `0` = absent, `1` = present
- **UUID** (RPC correlation ID): 16 raw bytes (standard UUID byte order, no hyphens)

Some domains embed **nested TLV** payloads (Schedule); see that domain section.

### Identifier canonical forms
Unless explicitly stated otherwise for a domain:
- **route_family**: u64 (big-endian)
- **session_id** (transport session): u64 (big-endian) and **NOT** encoded in TLV frames unless explicitly required by a domain
- **tx_id**: u64 (big-endian)
- **message_id**: u64 (big-endian)
- **lease_token / fencing_token**: u64 (big-endian) in request payloads
- **correlation_id**: exactly 16 raw bytes (UUID); length prefix MUST be 16
- **route**: UTF-8 string with u32 length prefix

NOTE: Some responses include optional string tokens. These are opaque and broker-specific unless explicitly documented in this spec.

> Source: `src/protocol/tlv.rs` (`MessageType::{ESCAPE_MARKER=0xFF, MAX_SINGLE_BYTE=0xFE}`; length is u32 BE).

---

## Connection Lifecycle & Handshake
1. Open transport (WebSocket/TCP) to broker.
2. Client MUST send a **CONNECT** record as the first message:
  - `MessageType = 1` (`MessageType::CONNECT`)
  - `Value =` the compact JWT string bytes (UTF-8), **no length prefix**
3. If the CONNECT is missing/invalid, the broker will close the connection.
4. After CONNECT succeeds, the client may send domain requests.
5. Close: perform transport close.

> Source: `src/session/manager.rs` enforces "unauthenticated: connect required" unless `(channel=Control, msg_type=CONNECT)`.

---

## Authentication & Security
- JWT tokens are the primary mechanism: the CONNECT record's value is the compact JWT string.
- Always use TLS: `wss://` for WebSocket, TLS for TCP where available. Clients MUST validate server certificates.
- Authorization is route-based (claims contain allowed route patterns). The broker enforces this.

---

## Heartbeats & Idle Timeouts
- The broker may drop idle sessions. Clients SHOULD ensure periodic activity.
- There is currently no standardized "heartbeat message type" defined in the protocol layer; clients MAY use transport-level keepalives (WebSocket ping/pong) and/or send an application-level no-op appropriate to their deployment.
- Clients must be prepared to reconnect and re-establish session-scoped state.

---

## Routing & Message Semantics
- Routes are URIs with the form: `{scheme}://{realm}/{area}/{resource}/{operation}`.
  - **scheme** indicates interaction pattern (e.g., `rpc`, `notify`, `queue`, `stream`, `lease`, `kv`)
  - **realm** is a string inside the route; it is **not** the same as `route_family`
- **Route family** is a required numeric isolation key (`u64`). It is **not** embedded in the route string.
- For the current Rust broker, **Notice/RPC/Stream/Lease** payloads include `family_id`. **KV/Queue** routing is derived from the routing envelope and is not embedded in their payloads.
- Fitz multiplexes traffic internally by mapping `MessageType` ranges to logical channels (see Constants). Clients do **not** send a channel identifier on the wire.
- Domain semantics (ordering, delivery guarantees) are per-domain. Clients MUST follow domain-specific contracts (see domain specs).

**MessageType overlap rule (authoritative):** MessageType values overlap across domains. A client MUST disambiguate by **route scheme** and **broker mapping**. A client MUST NOT assume that MessageType alone selects a domain.

**Channel IDs (authoritative):** Channel IDs are broker-internal only. Clients MUST NOT encode, infer, multiplex, or depend on Channel IDs on the wire.

---

## Error Handling & Retries
- Transport-level errors are signaled by connection close (e.g., frame too large, unauthenticated).
- Domain-level errors are encoded per-domain (many domain codecs use a leading `u8` status flag + an error string; others use domain-specific response layouts).
- Retry policy: clients SHOULD use exponential backoff. For non-idempotent operations, do not retry unless the client can guarantee idempotency.

---

## Flow Control & Backpressure
- The broker may apply per-session or per-channel quotas. Clients MUST handle `ERR` frames or ACKs that indicate backpressure and throttle sending.
- Avoid unbounded in-memory buffering of outbound messages. Provide a configurable write queue limit; on backpressure return errors to callers rather than silently dropping.

---

## Extensions & Versioning
- The TLV codec allows extension tags; clients SHOULD ignore unknown tags and preserve them when acting as proxies.
- If a broker introduces an incompatible wire change, a new documented version will be published. Clients SHOULD expose configuration to pin protocol behaviors where relevant.

---

## Acceptance Criteria / Test Suite (must pass)
Each client implementation MUST include an automated test suite that validates the following minimal cases against a reference broker instance.

1. Basic connect (WebSocket) - send a single TLV record `MessageType=CONNECT(1)` containing a valid compact JWT; connection remains open.
2. Basic connect (TCP) - same as above over `[u32 BE length][payload]` framing.
3. Frame size enforcement - send a frame payload exceeding broker `max_frame_size` and assert the broker closes the connection.
4. Reconnect - drop transport, reconnect, re-send CONNECT, and validate session-scoped state is re-established by the client as needed.

Domain-level acceptance tests (notice/stream/queue/rpc/kv/lease) are REQUIRED once those domains are wired end-to-end for the broker build you are targeting.

---

## Implementation Tips (Language-agnostic)
- Provide both synchronous and asynchronous APIs depending on language conventions.
- Make the TLV encoder/decoder (MessageType + u32 length + bytes) a first-class, well-tested module.
- Keep transport framing (WS vs TCP) isolated from the TLV codec.

---

## Known Gaps / Broker-Specific Behaviors
These items are **not yet fully standardized** in the current Rust broker and require broker-specific handling:

1. **Session ID exposure**: Notice subscribe/unsubscribe payloads include `session_id`, but there is no standard server-to-client session identifier message yet. If a broker build does not expose session IDs, these operations are not externally usable without a broker-specific extension.
2. **KV/Queue routing envelope**: KV and Queue payloads do not include the route. The broker currently derives the route from the envelope/destination routing (connection context or broker-side routing configuration).
3. **Stream response `data` encoding**: Stream responses carry opaque `data` bytes. The serialization format is broker-defined; check the target broker build before implementing decoding.
4. **MessageType overlaps**: Several domains reuse the same MessageType values (e.g., 100–108 and 200–206). Disambiguation depends on broker routing configuration and route scheme.

---

## Next Steps / Proposal
- Add a machine-readable TLV type/tag registry to the specs directory.
- Publish canonical acceptance tests (playbook) and a lightweight test harness the community can run against any broker.

---

## Domains & Client Methods (API surface)
Below are the canonical client-facing methods for each Fitz domain.

**Important (definitiveness rule):** domain *semantics* are canonical in `docs/specs/domains/*.md`, but domain *wire encodings* are currently converging between those docs and the Rust protocol codecs in `src/protocol/*_codec.rs`. A client MUST pick a single broker version/commit and implement the encodings that broker actually accepts.

### Notice Domain (Fire-and-forget pub/sub)
Purpose: fast, session-scoped fanout for notifications with wildcard pattern matching.

#### Message Types (u16)
- PUBLISH = 100
- SUBSCRIBE = 101
- UNSUBSCRIBE = 102
- UNSUBSCRIBE_ALL = 103
- NOTIFY = 104 (server-to-client delivery)

#### Message Format (payload bytes)

**Publish Request (MessageType=100):**
```
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route (UTF-8)
  [u32 BE] payload_len
  [bytes]  payload

Response:
  [u8]     status (0=ok, 1=error)
  [if status=0]
    [u8]     has_subscription_id (0 or 1)
    [u64 BE] subscription_id (if has_subscription_id=1)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Subscribe Request (MessageType=101):**
```
  [u64 BE] family_id
  [u32 BE] route_pattern_len  (supports * and ** wildcards)
  [bytes]  route_pattern
  [u64 BE] session_id
  [u32 BE] subscriber_route_len
  [bytes]  subscriber_route

Response:
  [u8]     status (0=ok, 1=error)
  [if status=0]
    [u8]     has_subscription_id (0 or 1)
    [u64 BE] subscription_id (if has_subscription_id=1)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Unsubscribe Request (MessageType=102):**
```
  [u64 BE] family_id
  [u32 BE] route_pattern_len
  [bytes]  route_pattern
  [u64 BE] session_id
  [u32 BE] subscriber_route_len
  [bytes]  subscriber_route

Response:
  [u8]     status (0=ok, 1=error)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Unsubscribe All Request (MessageType=103):**
```
  [u64 BE] session_id
  [u64 BE] family_id
  [u32 BE] subscriber_route_len
  [bytes]  subscriber_route

Response:
  [u8]     status (0=ok, 1=error)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Notify (Server → Client, MessageType=104):**
```
  [u32 BE] route_len
  [bytes]  route
  [u32 BE] payload_len
  [bytes]  payload
```

#### Pattern Matching Rules
- `*` matches a single route segment (e.g., `notice://realm/*/events` matches `notice://realm/orders/events`)
- `**` matches zero or more segments (e.g., `notice://realm/**` matches all routes in realm)
- Exact routes (no wildcards) also supported
- Patterns are matched at subscription time; published routes must exactly match to be delivered

#### Error Codes (Notice Domain - 3xxx)
- 3001 = ERR_INVALID_ROUTE
- 3002 = ERR_INVALID_PATTERN
- 3003 = ERR_SUBSCRIPTION_LIMIT
- 3004 = ERR_TRANSPORT_CLOSED

#### Client Methods (Derived)
```
subscribe(family_id: u64, route_pattern: string, session_id: u64, subscriber_route: string)
  -> subscription_id? | error

unsubscribe(family_id: u64, route_pattern: string, session_id: u64, subscriber_route: string)
  -> ok | error

unsubscribe_all(session_id: u64, family_id: u64, subscriber_route: string) -> ok | error

publish(family_id: u64, route: string, payload: bytes) -> ok | error

on_notify(route, payload) -> void  (callback/event handler)
```

#### Semantics & Guarantees
- **Delivery**: Best-effort; under backpressure, server MAY drop notifications and emit metrics
- **Ordering**: Notifications delivered in publish order per subscription
- **Fanout**: Single publish reaches all matching subscriptions (may be 0 to many)
- **Session-scoped**: Subscriptions are tied to connection; lost on disconnect

#### Success vs Error Encoding (Notice)
- All notice responses begin with a **status** byte: 0 = ok, 1 = error.
- On error, the payload MUST be `[u32 BE error_len][bytes error_msg]`.

#### Retry / Idempotency (Notice)
- `publish` is **not idempotent**; clients MUST NOT retry unless they accept duplicates.
- `subscribe` and `unsubscribe` MAY be retried if the client can tolerate duplicate server-side state or the broker guarantees idempotency.

#### Disconnect Behavior (Notice)
- On disconnect, all subscriptions are dropped. Clients MUST re-subscribe after reconnect.

#### Acceptance Tests
- subscribe to pattern and receive matching publications
- multiple subscriptions on same pattern both receive publications
- publish with no subscribers returns ok (no error)
- unsubscribe stops delivery
- wildcard patterns match correctly
- exact routes take precedence over patterns

NOTE: Field ordering for UNSUBSCRIBE and UNSUBSCRIBE_ALL differs intentionally and is authoritative as documented. Clients MUST NOT reorder or normalize field order.

### Stream Domain (Durable append-only logs)
Purpose: durable, strictly ordered append/read operations with watermark tracking.

#### Message Types (u16)
- BEGIN = 200
- APPEND = 201
- COMMIT = 202
- ROLLBACK = 203
- READ = 204
- LAST = 205
- GET_METADATA = 206

#### Session Lifecycle

**Begin Session (MessageType=200):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route
  [u64 BE] expected_offset
  [u8]     has_ingest_metadata (0 or 1)
  [if has_ingest_metadata=1]
    [u32 BE] ingest_metadata_len
    [bytes]  ingest_metadata

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

**Append (MessageType=201):**
```
Request:
  [u32 BE] session_id_len
  [bytes]  session_id (UTF-8)
  [u32 BE] body_len
  [bytes]  body
  [u8]     has_metadata (0 or 1)
  [if has_metadata=1]
    [u32 BE] metadata_len
    [bytes]  metadata

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

**Commit (MessageType=202):**
```
Request:
  [u32 BE] session_id_len
  [bytes]  session_id
  [u32 BE] mode_len
  [bytes]  mode ("Buffered" or "Sync")

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

**Rollback (MessageType=203):**
```
Request:
  [u32 BE] session_id_len
  [bytes]  session_id

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

**Read Range (MessageType=204):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route
  [u64 BE] from_offset
  [u64 BE] limit
  [u8]     has_max_bytes (0 or 1)
  [u64 BE] max_bytes (if has_max_bytes=1)

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

**Get Last Record (MessageType=205):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

**Get Metadata (MessageType=206):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_session_id (0 or 1)
  [u64 BE] session_id (if has_session_id=1)
  [u32 BE] data_len
  [bytes]  data (operation-specific)
```

#### Error Codes (Stream Domain - 2xxx)
- 2001 = ERR_CONCURRENCY_CONFLICT (expected_offset mismatch)
- 2002 = ERR_OFFSET_TOO_FAR_AHEAD
- 2003 = ERR_INVALID_READ_BOUND
- 2004 = ERR_READ_BEYOND_WATERMARK
- 2005 = ERR_RESOURCE_NOT_FOUND

#### Client Methods (Derived)
```
begin(family_id: u64, route: string, expected_offset: u64, ingest_metadata?: bytes)
  -> {session_id?: u64, data: bytes} | error

append(session_id: string, body: bytes, metadata?: bytes)
  -> {session_id?: u64, data: bytes} | error

commit(session_id: string, mode: "Buffered" | "Sync") -> {data: bytes} | error

rollback(session_id: string) -> {data: bytes} | error

read(family_id: u64, route: string, from_offset: u64, limit: u64, max_bytes?: u64)
  -> {data: bytes} | error

last(family_id: u64, route: string) -> {data: bytes} | error

get_metadata(family_id: u64, route: string) -> {data: bytes} | error
```

NOTE: Stream response `data` is broker-defined. Portable clients MUST treat Stream as broker-specific unless the broker documents the `data` encoding.

#### Semantics & Guarantees
- **Atomicity**: Append is atomic; partial writes are never visible
- **Ordering**: Records are strictly ordered by offset within resource
- **Watermarks**: Reads cannot advance beyond watermark (protects uncommitted data)
- **Optimistic concurrency**: `expected_offset` on append prevents lost updates
- **Durability**: All committed data survives broker restart
- **Isolation**: Stream sessions are isolated per resource

#### Success vs Error Encoding (Stream)
- Stream responses begin with a **status** byte: 0 = ok, 1 = error.
- When status = ok, the payload is `[u8 has_session_id][u64 session_id (if present)][u32 data_len][bytes data]`.
- When status = error, the payload is `[u32 error_len][bytes error_msg]`.

NOTE: The optional `session_id` in responses is **not** the same as the UTF-8 `session_id` required in request payloads. The response `session_id` is an opaque handle unless the broker documents it.

NOTE: The UTF-8 `session_id` used in Stream request payloads is a broker-issued opaque identifier and MUST be treated as an uninterpreted byte string by clients. The numeric `session_id` (u64) optionally returned in Stream responses is a separate identifier. Clients MUST NOT assume equivalence between request and response session IDs.

#### Retry / Idempotency (Stream)
- `begin` and `append` are **not idempotent**; clients MUST NOT retry unless they accept duplicates or enforce concurrency with `expected_offset` semantics.
- `read`, `last`, `get_metadata` MAY be retried safely.

#### Disconnect Behavior (Stream)
- On disconnect, active stream sessions are abandoned. Clients MUST begin a new session after reconnect.

#### Acceptance Tests
- begin/append/commit cycle
- read returns records in offset order
- read beyond watermark returns only watermarked records
- append with mismatched expected_offset returns ERR_CONCURRENCY_CONFLICT
- rollback discards uncommitted appends
- multiple sessions can read same resource concurrently
- only one write session per resource at a time

### Queue Domain (Durable at-least-once delivery)
Purpose: durable FIFO-ish message queues with leasing and visibility timeouts.

> Note: Queue payloads do not include the route or family. The broker derives
> the route from the routing envelope/connection context.

#### Message Types (u16)
- ENQUEUE = 200
- RESERVE = 202
- EXTEND = 203
- COMPLETE = 204

#### Message Format (payload bytes)

**Enqueue Request (MessageType=200):**
```
  [u32 BE] body_len
  [bytes]  body
  [u8]     has_delay (0 or 1)
  [u64 BE] delay_seconds (if has_delay=1)

Response:
  [u64 BE] message_id
```

**Reserve Request (MessageType=202):**
```
  [u64 BE] lease_seconds
  [u8]     has_batch_size (0 or 1)
  [u32 BE] batch_size (if has_batch_size=1)
  [u8]     has_wait_seconds (0 or 1)
  [u64 BE] wait_seconds (if has_wait_seconds=1)

Response:
  [u32 BE] lease_count
  [repeat for each lease]
    [u64 BE] message_id
    [u64 BE] lease_token
    [u32 BE] body_len
    [bytes]  body
```

**Extend Lease Request (MessageType=203):**
```
  [u64 BE] message_id
  [u64 BE] lease_token
  [u64 BE] lease_seconds

Response:
  (empty on success)
```

**Complete Message Request (MessageType=204):**
```
  [u64 BE] message_id
  [u64 BE] lease_token

Response:
  (empty on success)
```

#### Error Codes (Queue Domain - current Rust broker)
- 0x01 = INVALID_TOKEN
- 0x02 = LEASE_EXPIRED
- 0x03 = NOT_FOUND
- 0x04 = QUEUE_NOT_FOUND

#### Error Response Encoding (Queue)
- InvalidToken: single byte `0x01`
- LeaseExpired: single byte `0x02`
- NotFound: single byte `0x03`
- QueueNotFound: single byte `0x04`
- BadRequest / Error: `[u32 BE error_len][bytes error_msg]`

Queue responses do **not** include a unified status byte. Clients must interpret
errors based on the expected response shape for the operation.

#### Client Methods (Derived)
```
enqueue(body: bytes, delay_seconds?: u64) -> message_id | error

reserve(lease_seconds: u64, batch_size?: u32, wait_seconds?: u64)
  -> [{message_id, lease_token, body}, ...] | error

extend(message_id: u64, lease_token: u64, lease_seconds: u64) -> ok | error

complete(message_id: u64, lease_token: u64) -> ok | error
```

#### Semantics & Guarantees
- **At-Least-Once**: Messages are delivered until completed; lease expiry puts them back on queue
- **FIFO-ish**: Messages generally delivered in enqueue order, but leasing can cause out-of-order
- **Visibility Timeout**: Leased messages are invisible to other consumers until lease expires
- **Token Binding**: Complete/Extend require both message_id and lease_token (prevents replay)
- **Durability**: All enqueued messages survive broker restart

#### Success vs Error Encoding (Queue)
- Success responses for `enqueue`, `reserve`, `extend`, `complete` are as defined in the payload layouts.
- Errors are encoded as single-byte codes or error strings (see Error Response Encoding).

#### Retry / Idempotency (Queue)
- `enqueue` is **not idempotent**; clients MUST NOT retry unless they accept duplicate messages.
- `reserve` MAY be retried; it is expected to be repeatable.
- `extend` and `complete` MAY be retried if the client can tolerate `NOT_FOUND` or `LEASE_EXPIRED` errors.

#### Disconnect Behavior (Queue)
- On disconnect, leases continue to expire server-side. Clients MUST assume in-flight messages may be redelivered to other consumers after lease expiry.

#### Acceptance Tests
- enqueue/reserve/complete cycle
- lease expiry returns message to ready queue
- extend lease delays expiry
- complete with wrong token fails
- reserve with batch_size returns up to that many messages
- multiple consumers can reserve from same queue

### RPC Domain (Request/Response & Streaming)
Purpose: low-latency request/response with reply inbox semantics and optional streaming replies.

#### Message Types (u16)
- SUBSCRIBE_WORKER = 300
- UNSUBSCRIBE_WORKER = 301
- REQUEST = 302
- RESPONSE = 303
- ACK = 304

#### Message Format (payload bytes)

**Subscribe Worker Request (MessageType=300):**
```
Request:
  [u64 BE] family_id
  [u32 BE] worker_route_len
  [bytes]  worker_route

Response:
  [u8]     status (0=ok, 1=error)
  [u32 BE] data_len
  [bytes]  data (empty on success)
```

**Unsubscribe Worker Request (MessageType=301):**
```
Request:
  [u64 BE] family_id
  [u32 BE] worker_route_len
  [bytes]  worker_route

Response:
  [u8]     status (0=ok, 1=error)
  [u32 BE] data_len
  [bytes]  data (empty on success)
```

**Request (MessageType=302):**
```
Request:
  [u64 BE] family_id
  [u32 BE] correlation_id_len
  [bytes]  correlation_id (16 bytes UUID)
  [u32 BE] route_len
  [bytes]  route
  [u32 BE] reply_route_len
  [bytes]  reply_route
  [u32 BE] body_len
  [bytes]  body

Response (from broker to caller):
  [u8]     status (0=ok, 1=error)
  [u32 BE] data_len
  [bytes]  data (empty on success)
```

**Response from Worker (MessageType=303):**
```
Request (from worker, responding to previous REQUEST):
  [u32 BE] correlation_id_len
  [bytes]  correlation_id (16 bytes UUID)
  [u64 BE] sequence
  [u32 BE] body_len
  [bytes]  body
  [u8]     stream_end (0=more, 1=end)

Response:
  [u8]     status (0=ok, 1=error)
  [u32 BE] data_len
  [bytes]  data (empty on success)
```

**ACK (MessageType=304):**
```
Request:
  [u32 BE] correlation_id_len
  [bytes]  correlation_id (16 bytes UUID)

Response:
  [u8]     status (0=ok, 1=error)
  [u32 BE] data_len
  [bytes]  data (empty on success)
```

NOTE: `correlation_id_len` MUST be exactly 16. Any other value MUST be treated as a protocol error.

#### Error Codes (RPC Domain - 3xxx)
- 3001 = ERR_RPC_TIMEOUT
- 3002 = ERR_RPC_WORKER_NOT_FOUND
- 3003 = ERR_RPC_BACKPRESSURE
- 3004 = ERR_RPC_ROUTE_NOT_REGISTERED

#### Client Methods (Derived)
```
subscribe_worker(family_id: u64, worker_route: string) -> ok | error

unsubscribe_worker(family_id: u64, worker_route: string) -> ok | error

request(family_id: u64, route: string, reply_route: string, correlation_id: UUID, body: bytes)
  -> ok | error

on_response(correlation_id: UUID, seq: u64, body: bytes, stream_end: bool) -> void

ack(correlation_id: UUID) -> ok | error

send_response(correlation_id: UUID, seq: u64, body: bytes, stream_end: bool) -> ok | error
```

#### Semantics & Guarantees
- **Request/Reply Correlation**: correlation_id (UUID) links request to responses (client-generated)
- **Streaming**: Multi-frame responses include an incrementing `sequence` and `stream_end` flag
- **Backpressure**: RPC_BACKPRESSURE error if outbound queue is full
- **Ordering**: Responses delivered in sequence order (if streaming)
- **Exactly-Once Request Delivery**: Each request reaches worker once

#### Success vs Error Encoding (RPC)
- All RPC responses begin with a **status** byte: 0 = ok, 1 = error.
- On error, the payload is `[u32 error_len][bytes error_msg]`.

#### Retry / Idempotency (RPC)
- `request` is **not idempotent** unless the application enforces idempotency via correlation_id semantics. Clients MUST NOT retry without such guarantees.
- `ack` MAY be retried safely.

#### Disconnect Behavior (RPC)
- On disconnect, outstanding requests MAY be lost. Clients MUST treat responses as non-guaranteed and SHOULD re-issue requests only if idempotent.

#### Acceptance Tests
- single request/response cycle
- streaming response reassembled in order
- request timeout returns ERR_RPC_TIMEOUT
- multiple workers on same route can handle requests
- response with wrong correlation_id is rejected
- backpressure error when buffer is full

### KV Domain (Durable key-value)
Purpose: transaction-based CRUD and range operations with isolation.

**Transactions are REQUIRED:** All KV operations occur within a transaction (Begin/Commit/Rollback).

#### Message Types (u16)
- BEGIN = 100
- COMMIT = 101  
- ROLLBACK = 102
- GET = 103
- PUT = 104
- INSERT = 105
- DELETE = 106
- DELETE_RANGE = 107
- SCAN = 108

#### Transaction Lifecycle

**1. Begin Transaction (MessageType=100)**
```
Request:
  [u32 BE] resource_len
  [bytes]  resource_name
  [u8]     mode (0=ReadOnly, 1=ReadWrite)
  [u8]     durability (0=sync, 1=buffered)

Response (success):
  [u64 BE] tx_id

Response (error):
  [u32 BE] error_len
  [bytes]  error_msg
```

**2. Put/Insert/Delete within transaction**
```
PUT Request (MessageType=104):
  [u64 BE] tx_id
  [u32 BE] resource_len
  [bytes]  resource_name
  [u32 BE] key_len
  [bytes]  key
  [u32 BE] value_len
  [bytes]  value

GET Request (MessageType=103):
  [u64 BE] tx_id
  [u32 BE] resource_len
  [bytes]  resource_name
  [u32 BE] key_len
  [bytes]  key

DELETE Request (MessageType=106):
  [u64 BE] tx_id
  [u32 BE] resource_len
  [bytes]  resource_name
  [u32 BE] key_len
  [bytes]  key

INSERT Request (MessageType=105):
  [u64 BE] tx_id
  [u32 BE] resource_len
  [bytes]  resource_name
  [u32 BE] key_len
  [bytes]  key
  [u32 BE] value_len
  [bytes]  value

DELETE_RANGE Request (MessageType=107):
  [u64 BE] tx_id
  [u32 BE] resource_len
  [bytes]  resource_name
  [u32 BE] start_key_len
  [bytes]  start_key
  [u32 BE] end_key_len
  [bytes]  end_key

SCAN Request (MessageType=108):
  [u64 BE] tx_id
  [u32 BE] resource_len
  [bytes]  resource_name
  [u8]     has_start (0 or 1)
  [u32 BE] start_key_len (if has_start=1)
  [bytes]  start_key (if has_start=1)
  [u8]     has_end (0 or 1)
  [u32 BE] end_key_len (if has_end=1)
  [bytes]  end_key (if has_end=1)
  [u8]     has_limit (0 or 1)
  [u32 BE] limit (if has_limit=1)
  [u8]     reverse (0 or 1)

Responses:
  PUT/INSERT/DELETE/DELETE_RANGE: empty payload on success
  GET:
    [u8] found (0=not_found, 1=found)
    [u32 BE] value_len
    [bytes]  value (empty if not found)
  SCAN:
    [u32 BE] item_count
    [repeat]
      [u32 BE] key_len
      [bytes]  key
      [u32 BE] value_len
      [bytes]  value
    [u8] has_more (0 or 1)
  Error (all ops):
    [u32 BE] error_len
    [bytes]  error_msg
```

**3. Commit/Rollback Transaction**
```
COMMIT Request (MessageType=101):
  [u64 BE] tx_id

ROLLBACK Request (MessageType=102):
  [u64 BE] tx_id

Response (success):
  (empty payload)

Response (error):
  [u32 BE] error_len
  [bytes]  error_msg
```

#### Required Fields

KV operations are scoped by **route_family** and **route** at the routing layer.
The current Rust broker derives `route_family`, `realm`, and `area` from the
routing envelope, not from the KV payload itself. Clients should treat KV
payloads as **resource-only** and rely on the route address configured for the
connection or request context.

#### Error Codes (KV Domain - 1xxx)
- 1001 = ERR_TRANSACTION_NOT_FOUND
- 1002 = ERR_INVALID_MODE
- 1003 = ERR_KEY_NOT_FOUND
- 1004 = ERR_ISOLATION_CONFLICT
- 1005 = ERR_WRITE_IN_READONLY

#### Error Response Encoding (KV)
KV responses do **not** include a status byte. Errors are encoded as:
```
  [u32 BE] error_len
  [bytes]  error_msg
```
Clients must interpret errors based on the expected response shape for the
operation.

#### Semantics & Guarantees
- **Isolation**: Transactions are isolated by realm + area
- **Durability**: Sync mode guarantees WAL; buffered is best-effort
- **Read modes**: ReadOnly allows multi-reader; ReadWrite is exclusive per resource
- **Persistence**: All committed data survives broker restart

#### Success vs Error Encoding (KV)
- Success responses are **operation-specific**; many have empty payloads.
- Errors are encoded as `[u32 BE error_len][bytes error_msg]` with no status byte.

#### Retry / Idempotency (KV)
- `begin` is **not idempotent**; clients MUST NOT retry unless they accept duplicate transactions.
- `get`, `scan` MAY be retried safely.
- `put`, `insert`, `delete`, `delete_range` are **not idempotent** unless the application enforces idempotency.
- `commit` MUST be assumed non-idempotent unless explicitly documented by a broker build and MUST NOT be retried by default.

#### Disconnect Behavior (KV)
- On disconnect, in-flight transactions MAY be abandoned or rolled back by the broker. Clients MUST begin a new transaction after reconnect.

#### Client Methods (Derived)
```
begin(resource: string, mode: 'ReadOnly' | 'ReadWrite', durability: 'sync' | 'buffered')
  -> tx_id | error

put(tx_id: u64, resource: string, key: bytes, value: bytes) -> ok | error

get(tx_id: u64, resource: string, key: bytes) -> value | not_found | error

insert(tx_id: u64, resource: string, key: bytes, value: bytes) -> ok | error

delete(tx_id: u64, resource: string, key: bytes) -> ok | error

delete_range(tx_id: u64, resource: string, start_key: bytes, end_key: bytes) -> ok | error

scan(tx_id: u64, resource: string, start_key?: bytes, end_key?: bytes, limit?: u32, reverse?: bool)
  -> [(key, value), ...] | error

commit(tx_id: u64) -> ok | error

rollback(tx_id: u64) -> ok | error
```

#### Acceptance Tests
- begin/put/commit cycle
- begin/get on non-existent key
- begin with ReadOnly mode rejects put
- two transactions on same resource conflict (2nd waits/fails)
- rollback discards all changes
- scan returns lexicographically ordered pairs

### Lease Domain (Ephemeral coordination)
Purpose: in-memory exclusive leases for coordination and distributed locking.

#### Message Types (u16)
- ACQUIRE = 400
- RENEW = 401
- RELEASE = 402
- QUERY = 403

#### Message Format (payload bytes)

**Acquire Lease Request (MessageType=400):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route
  [u32 BE] owner_id_len
  [bytes]  owner_id
  [u64 BE] ttl_secs

Response:
  [u8]     status (0=ok, 1=error)
  [if status=0]
    [u8]     has_token (0 or 1)
    [u32 BE] token_len
    [bytes]  token (if has_token=1)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Renew Lease Request (MessageType=401):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route
  [u32 BE] owner_id_len
  [bytes]  owner_id
  [u64 BE] fencing_token
  [u64 BE] ttl_secs

Response:
  [u8]     status (0=ok, 1=error)
  [if status=0]
    [u8]     has_token (0 or 1)
    [u32 BE] token_len
    [bytes]  token (if has_token=1)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Release Lease Request (MessageType=402):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route
  [u32 BE] owner_id_len
  [bytes]  owner_id
  [u64 BE] fencing_token

Response:
  [u8]     status (0=ok, 1=error)
  [if status=0]
    [u8]     has_token (0 or 1)
    [u32 BE] token_len
    [bytes]  token (if has_token=1)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

**Query Lease Request (MessageType=403):**
```
Request:
  [u64 BE] family_id
  [u32 BE] route_len
  [bytes]  route

Response:
  [u8]     status (0=ok, 1=error)
  [if status=0]
    [u8]     has_token (0 or 1)
    [u32 BE] token_len
    [bytes]  token (if has_token=1)
  [if status=1]
    [u32 BE] error_len
    [bytes]  error_msg
```

#### Error Codes (Lease Domain - 4xxx)
- 4001 = ERR_LEASE_HELD
- 4002 = ERR_LEASE_INVALID_TOKEN
- 4003 = ERR_LEASE_EXPIRED
- 4004 = ERR_LEASE_NOT_FOUND

#### Client Methods (Derived)
```
acquire(family_id: u64, route: string, owner_id: string, ttl_secs: u64)
  -> {token?: string} | error

renew(family_id: u64, route: string, owner_id: string, fencing_token: u64, ttl_secs: u64)
  -> {token?: string} | error

release(family_id: u64, route: string, owner_id: string, fencing_token: u64)
  -> {token?: string} | error

query(family_id: u64, route: string) -> {token?: string} | error
```

#### Semantics & Guarantees
- **Mutual Exclusion**: Only one owner can hold a lease at a time
- **Fencing Token**: Prevents stale commands from releasing a new holder's lease
- **TTL-based Expiry**: Expired leases are automatically released; queried as free
- **Route Partitioned**: Different routes have independent leases
- **In-Memory**: Not persisted; lost on broker restart (use for coordination, not durability)

#### Success vs Error Encoding (Lease)
- Lease responses begin with a **status** byte: 0 = ok, 1 = error.
- On error, the payload is `[u32 error_len][bytes error_msg]`.
- On success, the optional token is an **opaque string**. Clients MUST NOT interpret it unless broker documentation defines its meaning.

#### Retry / Idempotency (Lease)
- `acquire` is **not idempotent**; clients SHOULD NOT retry unless they accept acquiring a newer fencing token.
- `renew` and `release` MAY be retried but can return errors if the token is stale.
- `query` MAY be retried safely.

#### Disconnect Behavior (Lease)
- On disconnect, leases continue to expire server-side. Clients MUST re-acquire leases after reconnect.

#### Acceptance Tests
- acquire succeeds when free, fails with ERR_LEASE_HELD when held
- renew with valid token extends TTL
- renew with invalid token fails
- release with valid token releases lease
- release with invalid token fails
- expired lease can be acquired by new owner
- two acquires with same route conflict

### Schedule Domain (Delayed/Recurring Tasks)
Purpose: durable scheduling of delayed tasks and recurring jobs.

#### Message Types (u16)
- CREATE = 500
- CANCEL = 501
- LIST = 502

#### Message Format (payload bytes)

**Create Schedule Request (MessageType=500):**
```
Request:
  [u32 BE] schedule_payload_len
  [bytes]  schedule_payload (nested TLV, see below)

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_schedule_id (0 or 1)
  [u32 BE] schedule_id_len
  [bytes]  schedule_id (if has_schedule_id=1)
```

**Cancel Schedule Request (MessageType=501):**
```
Request:
  [u32 BE] schedule_id_len
  [bytes]  schedule_id

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_schedule_id (0 or 1)
  [u32 BE] schedule_id_len
  [bytes]  schedule_id (if has_schedule_id=1)
```

**List Schedules Request (MessageType=502):**
```
Request:
  (empty payload)

Response:
  [u8]     status (0=ok, 1=error)
  [u8]     has_schedule_id (0 or 1)
  [u32 BE] schedule_id_len
  [bytes]  schedule_id (if has_schedule_id=1)
```

NOTE: LIST returns a single response per invocation. If no schedules are present, the response MUST indicate status=ok with `has_schedule_id = 0`.

**Schedule Payload (nested TLV within Create):**
Each field is a TLV record using the top-level TLV rules:
- Type 1: cron (UTF-8 bytes)
- Type 2: target_resource (UTF-8 bytes)
- Type 3: target_operation (UTF-8 bytes)

#### Error Codes (Schedule Domain - 5xxx)
- 5001 = ERR_SCHEDULE_NOT_FOUND
- 5002 = ERR_SCHEDULE_INVALID_TIME
- 5003 = ERR_SCHEDULE_LIMIT_EXCEEDED
- 5004 = ERR_SCHEDULE_PARSE_ERROR

#### Client Methods (Derived)
```
create(payload: SchedulePayload) -> schedule_id? | error

cancel(schedule_id: string) -> ok | error

list() -> ok | error
```

#### Semantics & Guarantees
- **Durability**: All schedules persist across broker restarts
- **Strict Timing**: Tasks execute at designated times (best-effort, no guarantee < tolerance)
- **Recurring**: Interval-based recurring tasks (cron-like, but fixed intervals)
- **Cancellation**: Cancels future runs; already-running tasks may not be aborted
- **Route Scoped**: Schedules are created per route; different routes have independent schedules

#### Success vs Error Encoding (Schedule)
- Schedule responses begin with a **status** byte: 0 = ok, 1 = error.
- On error, the payload is `[u32 error_len][bytes error_msg]`.

#### Retry / Idempotency (Schedule)
- `create` is **not idempotent**; clients MUST NOT retry unless they accept duplicate schedules.
- `cancel` MAY be retried and SHOULD be treated as idempotent by clients.
- `list` MAY be retried safely.

#### Disconnect Behavior (Schedule)
- On disconnect, schedules remain active on the broker. Clients MUST assume schedules continue to run.

#### Acceptance Tests
- create_once schedules task and executes at delay
- create_recurring executes at intervals
- cancel prevents future runs
- list returns created schedules
- max_runs respected for recurring tasks
- schedule persists across restart

---

## Domain-level Acceptance Tests (additions)
Client implementations MUST include automated tests for each domain covering the bullets above. Tests must be runnable over WebSocket and TCP transports and included in the canonical test harness.

---

## Constants & TLV Registry (canonical)
This section collects the canonical numeric constants clients must implement.

### 1) Channel IDs (u8)
| ChannelId | Value | Purpose |
|---|---:|---|
| Control | 0 | Control/handshake/connection messages |
| Pub     | 1 | Publishing/notice traffic |
| Sub     | 2 | Subscriptions / delivery traffic |
| Rpc     | 3 | RPC request/response traffic |
| Lease   | 4 | Lease domain messages |
| Internal| 5 | Internal/engine-only messages |

> Source: `src/protocol/frame.rs`.

**Default MessageType → ChannelId mapping (Rust broker):**
- `0..=99` → Control
- `100..=199` → Pub
- `200..=299` → Sub
- `300..=399` → Rpc
- `400..=499` → Lease

### 2) MessageType (u16)
- `MessageType` is a u16.
- Encoding:
  - `0x00..=0xFE` encoded as a single byte
  - `0xFF` is reserved as an escape marker; values `> 0xFE` are encoded as `0xFF` + `u16 BE`
- Each TLV record also includes a `u32 BE` length.

#### Canonical Control Messages
- CONNECT = 1 (`MessageType::CONNECT`)

#### Domain Message Types (u16)

**Note:** These ranges **overlap across domains** in the current broker. The effective domain is resolved by routing configuration (route scheme and broker mapping), not by MessageType alone.

**KV Domain (100-108):**
| MessageType | Name | Purpose |
|---|---:|---|
| 100 | BEGIN | Start transaction |
| 101 | COMMIT | Finalize transaction |
| 102 | ROLLBACK | Abort transaction |
| 103 | GET | Read key |
| 104 | PUT | Write key |
| 105 | INSERT | Insert new key (fail if exists) |
| 106 | DELETE | Delete key |
| 107 | DELETE_RANGE | Delete key range |
| 108 | SCAN | Scan keys in range |

**Stream Domain (200-206):**
| MessageType | Name | Purpose |
|---|---:|---|
| 200 | BEGIN | Start stream session |
| 201 | APPEND | Append record to stream |
| 202 | COMMIT | Finalize stream session |
| 203 | ROLLBACK | Abort stream session |
| 204 | READ | Read range of records |
| 205 | LAST | Get last record |
| 206 | GET_METADATA | Get stream metadata |

**Notice Domain (100-104):**
| MessageType | Name | Purpose |
|---|---:|---|
| 100 | PUBLISH | Publish message to route |
| 101 | SUBSCRIBE | Subscribe to route pattern |
| 102 | UNSUBSCRIBE | Unsubscribe from subscription |
| 103 | UNSUBSCRIBE_ALL | Unsubscribe from all |
| 104 | NOTIFY | Delivery (server->client) |

**Queue Domain (200-204):**
| MessageType | Name | Purpose |
|---|---:|---|
| 200 | ENQUEUE | Add message to queue |
| 202 | RESERVE | Lease message(s) |
| 203 | EXTEND | Extend lease TTL |
| 204 | COMPLETE | Mark message complete |

**RPC Domain (300-304):**
| MessageType | Name | Purpose |
|---|---:|---|
| 300 | SUBSCRIBE_WORKER | Register as RPC worker |
| 301 | UNSUBSCRIBE_WORKER | Unregister from RPC |
| 302 | REQUEST | Send RPC request |
| 303 | RESPONSE | Send RPC response |
| 304 | ACK | Acknowledge receipt |

**Lease Domain (400-403):**
| MessageType | Name | Purpose |
|---|---:|---|
| 400 | ACQUIRE | Acquire distributed lease |
| 401 | RENEW | Extend lease TTL |
| 402 | RELEASE | Release owned lease |
| 403 | QUERY | Query lease status |

**Schedule Domain (500-502):**
| MessageType | Name | Purpose |
|---|---:|---|
| 500 | CREATE | Create schedule |
| 501 | CANCEL | Cancel schedule |
| 502 | LIST | List schedules |

### 3) Domain Registries
Domain-level tag/type registries (route/id/body, domain-specific error codes, etc.) are specified in:

- `docs/specs/domains/*.md` (semantic contract)
- `src/protocol/*_codec.rs` (current Rust codec behavior)

Until the registry is centralized, clients MUST treat the broker build they target as authoritative and align to its published registry.

### 4) Error Code Ranges & Examples

Domains use numeric ranges to reduce collisions, but overlaps exist. Response payloads are **domain-specific**: some include a leading status byte, others use empty payloads on success and error strings or numeric codes on failure. Numeric codes are not always emitted on the wire.

#### KV Domain Error Codes (1xxx)
| Code | Name | Cause |
|---:|---|---|
| 1001 | `ERR_TRANSACTION_NOT_FOUND` | Received message for unknown tx_id |
| 1002 | `ERR_INVALID_MODE` | Invalid TxMode value or operation conflict (e.g., Write in ReadOnly) |
| 1003 | `ERR_KEY_NOT_FOUND` | Get operation on non-existent key |
| 1004 | `ERR_ISOLATION_CONFLICT` | Serializable isolation violated; retry with fresh Begin |
| 1005 | `ERR_WRITE_IN_READONLY` | Put/Insert/Delete attempted during ReadOnly transaction |

#### Stream Domain Error Codes (2xxx)
| Code | Name | Cause |
|---:|---|---|
| 2001 | `ERR_CONCURRENCY_CONFLICT` | Append expected_offset did not match current offset (lost update) |
| 2002 | `ERR_OFFSET_TOO_FAR_AHEAD` | Requested read offset beyond stream extent |
| 2003 | `ERR_INVALID_READ_BOUND` | Read start_offset > end_offset |
| 2004 | `ERR_READ_BEYOND_WATERMARK` | Requested offset beyond current watermark |
| 2005 | `ERR_STREAM_NOT_FOUND` | Stream resource does not exist |

#### Notice Domain Error Codes (3001-3004)
| Code | Name | Cause |
|---:|---|---|
| 3001 | `ERR_INVALID_NOTICE_ROUTE` | Route format invalid or resource not found |
| 3002 | `ERR_INVALID_PATTERN` | Wildcard pattern syntax invalid or exceeds depth limit |
| 3003 | `ERR_SUBSCRIPTION_LIMIT` | Client subscription count exceeds realm limit |
| 3004 | `ERR_TRANSPORT_CLOSED` | Connection closed by broker |

#### Queue Domain Error Codes (wire-level bytes)
| Code | Name | Cause |
|---:|---|---|
| 0x01 | `INVALID_TOKEN` | lease_token not found or does not match any active reservation |
| 0x02 | `LEASE_EXPIRED` | Attempted extend/complete on expired lease; message returned to queue |
| 0x03 | `NOT_FOUND` | Message ID does not exist in queue |
| 0x04 | `QUEUE_NOT_FOUND` | Queue route does not exist |

#### RPC Domain Error Codes (3xxx, disambiguated from Notice by context)
| Code | Name | Cause |
|---:|---|---|
| 3001 | `ERR_RPC_TIMEOUT` | Request exceeded worker response deadline |
| 3002 | `ERR_WORKER_NOT_FOUND` | No worker subscribed to RPC route |
| 3003 | `ERR_RPC_BACKPRESSURE` | Broker queue for route at capacity |
| 3004 | `ERR_ROUTE_NOT_REGISTERED` | RPC route has no handler registered |

#### Lease Domain Error Codes (4xxx, disambiguated from Queue by context)
| Code | Name | Cause |
|---:|---|---|
| 4001 | `ERR_LEASE_HELD` | Lease is currently held by another owner (Acquire conflict) |
| 4002 | `ERR_INVALID_FENCE` | Fence token does not match current lease holder (stale Release) |
| 4003 | `ERR_LEASE_EXPIRED` | Lease TTL expired; must re-acquire |
| 4004 | `ERR_LEASE_NOT_FOUND` | Lease resource does not exist |

#### Schedule Domain Error Codes (5xxx)
| Code | Name | Cause |
|---:|---|---|
| 5001 | `ERR_SCHEDULE_NOT_FOUND` | Schedule ID does not exist |
| 5002 | `ERR_INVALID_TIME` | Scheduled time is in past or exceeds max duration |
| 5003 | `ERR_SCHEDULE_LIMIT` | Client schedule count exceeds realm limit |
| 5004 | `ERR_PARSE_ERROR` | Cron expression or interval syntax invalid |

#### Control Plane Error Codes (5xxx, disambiguation context-dependent)
| Code | Name | Cause |
|---:|---|---|
| 5001 | `ERR_INVALID_HEARTBEAT` | Heartbeat interval or format invalid |
| 5002 | `ERR_METRICS_TOO_LARGE` | Metrics payload exceeds max size |
| 5003 | `ERR_INVALID_CONFIG` | Configuration parameter invalid or missing |
| 5004 | `ERR_SHUTDOWN_IN_PROGRESS` | Broker shutting down; no new connections accepted |
| 5005 | `ERR_CONTROL_PLANE_UNAVAILABLE` | Control plane replica unavailable |

> **Note on Error Code Disambiguation:**
> - Some error code ranges overlap (Notice/RPC both use 3xxx; Queue/Lease both use 4xxx; Schedule/Control both use 5xxx)
> - Disambiguation depends on **MessageType context**: if MessageType is Notice (100-104), then 3001 -> ERR_INVALID_NOTICE_ROUTE; if MessageType is RPC (300-304), then 3001 -> ERR_RPC_TIMEOUT
> - Clients SHOULD include MessageType in error logs to ensure correct interpretation
> - Brokers MUST ensure error codes are consistent within their domain codec
### 5) Formatting & Conventions
- All constants are documented in the TLV registry and in code as named constants/enums.
- Clients MUST parse unknown numeric values robustly: unknown MessageType -> treat as opaque field; unknown TAG -> ignore but preserve if proxying.
- Where domains include both numeric and symbolic error information, clients SHOULD preserve both for logging and debugging.

---

End of specification.


