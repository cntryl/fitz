# Fitz Client Specification

## Purpose ✅
This document defines the client contract, wire protocol expectations, and acceptance criteria for any Fitz client implementation. It is language-agnostic and focuses solely on behaviour the broker requires over supported transports (WebSocket and TCP).

---

## Terminology (Strict) ⚠️
Use these exact terms in implementations and docs: **realm**, **area**, **resource**, **operation**, **route**. Never use forbidden terms (e.g., tenant, namespace, endpoint).

---

## Supported Transports 🔌
- WebSocket (binary frames) — recommended for browsers and bidirectional usage.
- Raw TCP (length-prefixed frames) — identical payload semantics; clients must implement u32 big-endian length framing.

Clients MUST behave identically across both transports—framing differs only by transport encapsulation.

---

## Wire Protocol Summary (Transport Frame + TLV Records) 🔧
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

Records are simply concatenated back-to-back; a single transport frame MAY contain multiple records.

> Source: `src/protocol/tlv.rs` (`MessageType::{ESCAPE_MARKER=0xFF, MAX_SINGLE_BYTE=0xFE}`; length is u32 BE).

---

## Connection Lifecycle & Handshake 🔄
1. Open transport (WebSocket/TCP) to broker.
2. Client MUST send a **CONNECT** record as the first message:
  - `MessageType = 1` (`MessageType::CONNECT`)
  - `Value =` the compact JWT string bytes (UTF-8)
3. If the CONNECT is missing/invalid, the broker will close the connection.
4. After CONNECT succeeds, the client may send domain requests.
5. Close: perform transport close.

> Source: `src/session/manager.rs` enforces “unauthenticated: connect required” unless `(channel=Control, msg_type=CONNECT)`.

---

## Authentication & Security 🔒
- JWT tokens are the primary mechanism: the CONNECT record’s value is the compact JWT string.
- Always use TLS: `wss://` for WebSocket, TLS for TCP where available. Clients MUST validate server certificates.
- Authorization is route-based (claims contain allowed route patterns). The broker enforces this.

---

## Heartbeats & Idle Timeouts ❤️
- The broker may drop idle sessions. Clients SHOULD ensure periodic activity.
- There is currently no standardized “heartbeat message type” defined in the protocol layer; clients MAY use transport-level keepalives (WebSocket ping/pong) and/or send an application-level no-op appropriate to their deployment.
- Clients must be prepared to reconnect and re-establish session-scoped state.

---

## Routing & Message Semantics 🧭
- Routes are URIs with the form: `<domain>://<realm>/<area>/<resource>/<operation>`.
- Fitz multiplexes traffic internally by mapping `MessageType` ranges to logical channels (see Constants). Clients do **not** send a channel identifier on the wire.
- Domain semantics (ordering, at-least-once/at-most-once) are per-domain. Clients MUST follow domain-specific contracts (see domain specs).

---

## Error Handling & Retries ⚠️
- Transport-level errors are signaled by connection close (e.g., frame too large, unauthenticated).
- Domain-level errors are encoded per-domain (many domain codecs use a leading `u8` status flag + an error string; others use domain-specific response layouts).
- Retry policy: clients SHOULD use exponential backoff. For non-idempotent operations, do not retry unless the client can guarantee idempotency.

---

## Flow Control & Backpressure 🧰
- The broker may apply per-session or per-channel quotas. Clients MUST handle `ERR` frames or ACKs that indicate backpressure and throttle sending.
- Avoid unbounded in-memory buffering of outbound messages. Provide a configurable write queue limit; on backpressure return errors to callers rather than silently dropping.

---

## Extensions & Versioning ✨
- The TLV codec allows extension tags; clients SHOULD ignore unknown tags and preserve them when acting as proxies.
- If a broker introduces an incompatible wire change, a new documented version will be published. Clients SHOULD expose configuration to pin protocol behaviors where relevant.

---

## Acceptance Criteria / Test Suite (must pass) ✅
Each client implementation MUST include an automated test suite that validates the following minimal cases against a reference broker instance.

1. Basic connect (WebSocket) — send a single TLV record `MessageType=CONNECT(1)` containing a valid compact JWT; connection remains open. ✅
2. Basic connect (TCP) — same as above over `[u32 BE length][payload]` framing. ✅
3. Frame size enforcement — send a frame payload exceeding broker `max_frame_size` and assert the broker closes the connection. ✅
4. Reconnect — drop transport, reconnect, re-send CONNECT, and validate session-scoped state is re-established by the client as needed. ✅

Domain-level acceptance tests (notice/stream/queue/rpc/kv/lease) are REQUIRED once those domains are wired end-to-end for the broker build you are targeting.

---

## Implementation Tips (Language-agnostic) 💡
- Provide both synchronous and asynchronous APIs depending on language conventions.
- Make the TLV encoder/decoder (MessageType + u32 length + bytes) a first-class, well-tested module.
- Keep transport framing (WS vs TCP) isolated from the TLV codec.

---

## Next Steps / Proposal ✍️
- Add a machine-readable TLV type/tag registry to the specs directory.
- Publish canonical acceptance tests (playbook) and a lightweight test harness the community can run against any broker.

---

## Domains & Client Methods (API surface) 📚
Below are the canonical client-facing methods for each Fitz domain.

**Important (definitiveness rule):** domain *semantics* are canonical in `docs/specs/domains/*.md`, but domain *wire encodings* are currently converging between those docs and the Rust protocol codecs in `src/protocol/*_codec.rs`. A client MUST pick a single broker version/commit and implement the encodings that broker actually accepts.

### Notice Domain (Fire-and-forget) 🔔
Purpose: fast, session-scoped fanout (notifications).

Client methods:
- subscribe(route: string) -> ack
- unsubscribe(route: string) -> ack
- publish(route: string, body: bytes) -> ok (optional ack)

Semantics/acceptance tests:
- subscribe → receive published DAT frames matching pattern
- publish → best-effort delivery; under backpressure, server may drop and emit metrics

### Stream Domain (Durable append-only logs) 📜
Purpose: durable, strictly ordered append/read with watermarks.

Client methods:
- append(route: string, body: bytes, expected_offset?: u64) -> AppendResult{resource_offset, area_offset, realm_offset}
- read_resource(route: string, from: u64, limit: u32) -> [StreamRecord]
- read_area(route_pattern: string, from_area_offset: u64, limit: u32) -> [StreamRecord]

Semantics/acceptance tests:
- append with expected_offset enforces optimistic concurrency (ERR_CONCURRENCY_CONFLICT on mismatch)
- reads obey watermarks (no records beyond watermark)
- durable replay after restart

### Queue Domain (Durable at-least-once) 📦
Purpose: durable FIFO-ish leases with visibility timeouts.

Client methods:
- enqueue(route: string, body: bytes) -> message_id
- reserve(route: string, lease_secs: u32, batch_size?: u32) -> List<Lease{ id, body, token, lease_secs }>
- extend(route: string, id: string, token: u64, lease_secs: u32) -> ok
- complete(route: string, id: string, token: u64) -> ok
- peek(route: string, limit?: u32) -> [Message] (optional)

Semantics/acceptance tests:
- enqueue → reserve → complete cycle
- lease expiry puts messages back onto ready queue
- token mismatch yields `QUEUE_INVALID_TOKEN`

### RPC Domain (Request/Response & Streaming) 🧩
Purpose: low-latency request/response with reply inbox semantics and streaming replies.

Client methods:
- request(route: string, body: bytes, timeout?: Duration) -> Response (sync)/Future<Response>
- request_stream(route: string, body: bytes) -> Stream<ResponseChunk>
- register_worker(route: string, handler) -> subscription ack (server-side worker API)

Semantics/acceptance tests:
- single-request → single-reply correlation via TAG_ID
- streaming responses reassembled and ordered by TAG_SEQ
- backpressure & RPC_BACKPRESSURE behavior

### KV Domain (Durable key-value) 🗂️
Purpose: simple durable CRUD and range operations.

Client methods:
- put(route: string, key: string, value: bytes) -> ok
- get(route: string, key: string) -> Option<bytes>
- delete(route: string, key: string) -> ok
- scan(route_prefix: string, start_key?: string, end_key?: string, limit?: u32) -> List<(key,value)>
- delete_range(route_prefix: string, start_key: string, end_key: string) -> count

Semantics/acceptance tests:
- put/get/delete correctness and persistence across restarts
- scan returns lexicographically ordered pairs

### Lease Domain (Ephemeral coordination) 🔐
Purpose: in-memory exclusive leases (acquire/renew/release).

Client methods:
- acquire(route: string, ttl_secs: u32) -> { token: bytes, expires_at: timestamp } | LEASE_HELD
- renew(route: string, token: bytes, ttl_secs: u32) -> { expires_at } | INVALID_TOKEN
- release(route: string, token: bytes) -> ok | INVALID_TOKEN

Semantics/acceptance tests:
- acquire grants token when free; held returns LEASE_HELD
- renew extends expiry only with valid token
- release removes ownership

---

## Domain-level Acceptance Tests (additions) ✅
Client implementations MUST include automated tests for each domain covering the bullets above. Tests must be runnable over WebSocket and TCP transports and included in the canonical test harness.

---

## Constants & TLV Registry (canonical) 🧾
This section collects the canonical numeric constants clients must implement.

### 1) Channel IDs (u8) 🔢
| ChannelId | Value | Purpose |
|---|---:|---|
| Control | 0 | Control/handshake/connection messages |
| Pub     | 1 | Publishing/notice traffic |
| Sub     | 2 | Subscriptions / delivery traffic |
| Rpc     | 3 | RPC request/response traffic |
| Lease   | 4 | Lease domain messages |
| Internal| 5 | Internal/engine-only messages |

> Source: `src/protocol/frame.rs`.

### 2) MessageType (u16) ✉️
- `MessageType` is a u16.
- Encoding:
  - `0x00..=0xFE` encoded as a single byte
  - `0xFF` is reserved as an escape marker; values `> 0xFE` are encoded as `0xFF` + `u16 BE`
- Each TLV record also includes a `u32 BE` length.

Canonical control message:
- CONNECT = 1 (`MessageType::CONNECT`)

Per-domain operation and payload mappings are broker-version-specific and must be treated as a single, published registry.

### 3) Domain Registries 🏷️
Domain-level tag/type registries (route/id/body, domain-specific error codes, etc.) are specified in:

- `docs/specs/domains/*.md` (semantic contract)
- `src/protocol/*_codec.rs` (current Rust codec behavior)

Until the registry is centralized, clients MUST treat the broker build they target as authoritative and align to its published registry.

### 4) Error Code Ranges & Examples 🚨
Domains use numeric ranges to avoid collisions. Known canonical values:
- Stream domain (2xxx):
  - 2001 = ERR_CONCURRENCY_CONFLICT
  - 2002 = ERR_OFFSET_TOO_FAR_AHEAD
  - 2003 = ERR_INVALID_READ_BOUND
  - 2004 = ERR_READ_BEYOND_WATERMARK
- Notice domain (3xxx):
  - 3001 = ERR_INVALID_NOTICE_ROUTE
  - 3002 = ERR_INVALID_NOTICE_PATTERN
  - 3003 = ERR_SUBSCRIPTION_LIMIT
  - 3004 = ERR_TRANSPORT_CLOSED
- Control domain (5xxx):
  - 5001 = ERR_INVALID_HEARTBEAT
  - 5002 = ERR_METRICS_TOO_LARGE
  - 5003 = ERR_INVALID_CONFIG
  - 5004 = ERR_SHUTDOWN_IN_PROGRESS
  - 5005 = ERR_CONTROL_PLANE_UNAVAILABLE

> Note: Some domains (e.g., queue) currently encode failure as structured response variants instead of numeric error codes; align to the broker’s domain codec/registry.

> Action: We'll add a definitive mapping file (`docs/specs/tlv_registry.toml`) and a generated header for client languages so these codes are authoritative and single-sourced.

### 5) Formatting & Conventions ✅
- All constants are documented in the TLV registry and in code as named constants/enums.
- Clients MUST parse unknown numeric values robustly: unknown MessageType → treat as opaque field; unknown TAG → ignore but preserve if proxying.
- Where domains include both numeric and symbolic error information, clients SHOULD preserve both for logging and debugging.

---

> Next action options: (1) add concrete TLV frame byte-level examples per method, (2) draft the acceptance test harness (examples + runner), or (3) add concise pseudo-code quickstarts for each domain. Which should I do next? 🚀
