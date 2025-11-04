# fitz — Design Document (v0.1)

**Owner:** Jeff (fitz)

**Last Updated:** 2025‑10‑10

## 0. Purpose & Scope

A broker that routes messages across multiple semantic routes — `notice://`, `stream://`, `rpc://`, `inbox://`, and `queue://` — with:

- A **core engine** that is **transport‑agnostic** (initial transport: WebSocket; later: HTTP/2, QUIC/GRPC, NATS‑like adapters, etc.).
- **Storage plug‑ins** for durable routes (`stream://`, `queue://`) built initially on a Pebble‑like KV (Rocks/Pebble semantics), but fully **swappable** (e.g., Redis, Disk LSM, Azure Table/Blob, ClickHouse append tables).
- A **control plane** that manages identity, authorization, topology, configuration distribution, and broker health.
- A **tenant‑aware** model where multi‑tenant hosting sits **above** realm/area routing (no tenant baked into route URI); authorization binds a client to a tenant and to allowed routes.

Non‑goals (v0.1): cross‑broker federation, multi‑AZ replication, exactly‑once semantics. These can come later.

## 1. Core Concepts & Glossary

- **Tenant:** Billing/ownership boundary. Not encoded in route URIs; enforced via JWT claims and control plane policy.
- **Realm/Area/Resource/Op:** Logical routing segments (e.g., `realm.area/resource/op`).
- **Route URI:** `scheme://realm/area/resource/op?kv...` where `scheme ∈ {notice, stream, rpc, inbox, queue}`.
- **Session:** Authenticated client connection with capabilities (claims+policy). Multiple subscriptions under one session.
- **Envelope:** Canonical message wrapper used across all transports.
- **Broker Node:** A single running broker instance.
- **Control Plane:** Service that issues policy snapshots, knows brokers, gathers heartbeats/metrics, manages keys.

## 2. Route Design

### 2.1 URI & Matching

```
<scheme>://<realm>/<area>/<resource>/<operation>
```

- **Normalization:** Lowercase segments; RFC3986 encoding; reject ".." or empty segments.
- **Wildcards:** Supported for subscriptions only (`*` single segment, `**` multi) — never permitted in publish.
- **Params:** Optional query string for hints (`shard`, `partition`, `ttl`, `priority`, etc.).

### 2.2 Authorization Model

- Clients present a **JWT** with **tenant id** (`tid`), **subject** (`sub`), and **route claims** (`allow`, `deny`).
- Control plane materializes **policy snapshots**: `{tenant, subject, route_acl, quotas, rate limits}` — pushed to broker and cached.
- Broker enforces **publish** and **subscribe** ACLs separately, with **scope resolution**:
  - Exact route > wildcard allow/deny > default (deny).
  - Tenant isolation: sessions tagged with `tenant_id`; route ACLs evaluated within tenant scope; storage namespaces per tenant.

### 2.3 QoS & Ordering

- **notice://**: best‑effort, in‑order _per session_, drop‑on‑backpressure (or bounded buffer + LIFO drop). No durability.
- **stream://**: append‑only log with **partitioned ordering**; per‑partition offsets; at‑least‑once delivery to consumers.
- **queue://**: traditional work queue with **visibility timeouts**, at‑least‑once, optional **dead‑letter**.
- **rpc://**: request/response with correlation id; timeouts; responses typically via `inbox://` temp routes.
- **inbox://**: ephemeral, scoped to session or subject; auto‑GC with session close or TTL.

## 3. Message Envelope & Framing

### 3.1 Canonical Envelope (transport‑agnostic)

```json
{
  "id": "uuidv7",
  "ts": "2025-10-10T13:21:42.123Z",
  "route": "stream://realm/area/resource/op",
  "tenant": "T-12345",
  "headers": {
    "contentType": "application/json",
    "encoding": "snappy|lz4|none",
    "key": "optional-partition-key",
    "partition": 0
  },
  "correlationId": "uuidv7-req",
  "replyTo": "inbox://realm/area/$session/auto",
  "ttlMs": 0,
  "body": "<opaque bytes/base64 or framed binary>"
}
```

### 3.2 Framing over Transports

- **WebSocket (initial):** text frames for JSON control, binary frames for data (envelope as header + payload slice).
  - Flow control via **credit‑based** window: server advertises `windowSize`; client sends `CREDIT(n)` frames when ready.
- **Abstract framing API:** `FrameEncoder`, `FrameDecoder`, `Ping/Pong`, `Credit`, `Ack`, `Nack`, `Err` control frames — shared across transport implementations.

## 4. Transport Architecture (Swappable)

### 4.1 Interfaces

```csharp
// Pseudocode, neutral style
interface ITransportServer {
  Task Start(Endpoint endpoint, IConnectionHandler handler, CancellationToken ct);
}

interface IConnectionHandler {
  Task OnOpen(ISession session);
  Task OnFrame(ISession session, ReadOnlyMemory<byte> frame);
  Task OnClose(ISession session, CloseReason reason);
}

interface ITransportClient { /* for broker->broker, tests, RPC callbacks */ }

interface ISession {
  string SessionId { get; }
  string TenantId { get; }
  Claims Claims { get; }
  Task SendAsync(ReadOnlyMemory<byte> frame, CancellationToken ct);
}
```

- **WebSocketTransport** implements `ITransportServer` (Kestrel/Hyper with WS upgrade).
- Future transports (HTTP/2, QUIC) plug in without touching the core engine.

### 4.2 Session Lifecycle

1. Connect → `HELLO` (client metadata, sdkVersion, desired inbox behavior)
2. Auth → `AUTH {jwt}`
3. `POLICY_SYNC` (from broker cache)
4. Subscriptions → `SUB` frames with route filters
5. Publish → `PUB` frames with envelopes
6. Heartbeat/Ping; graceful `CLOSE`.

## 5. Storage Architecture (Swappable)

### 5.1 Abstractions

```go
// Go-ish pseudocode
// Namespaces are tenant-aware: <tenant>/<kind>/<routeKey>/...

// Append-only log for stream://
type LogStore interface {
  Append(partition int, key []byte, value []byte) (offset int64, err error)
  Read(partition int, fromOffset int64, maxBytes int) (records []Record, next int64, err error)
  Commit(consumerGroup string, partition int, offset int64) error
  Partitions(routeKey string) ([]int, error)
}

// Visibility-lease queue for queue://
type QueueStore interface {
  Enqueue(priority int, key []byte, value []byte) (id string, err error)
  Lease(consumer string, leaseMs int, max int) ([]Lease, error)
  Ack(id string) error
  Nack(id string, delayMs int) error
  DeadLetter(id string, reason string) error
}
```

- **Default implementation:** **Pebble‑like LSM** library (or real Pebble via cgo/CGO equivalent) with:
  - **CF/Prefix namespaces**: `ten/<tid>/str/<route>/<part>/...` and `ten/<tid>/que/<route>/...`.
  - **Compaction and retention**: size/time policies per stream; queue DLQ thresholds.
  - **Indexes**: consumer offsets (stream), visibility wheels (queue), priority queues.

### 5.2 Swap Strategy

- Each store behind a **factory + interface**, selected by policy or config: `storage.stream.driver=pebble|redis|azuretable`.
- **No route semantics in store**: engine keeps semantics; store is primitive KV/index layer.
- **Pluggable compression** (none/lz4/snappy) on value; configured per route.

## 6. Route Semantics (Detailed)

### 6.1 `notice://` — Ephemeral Fan‑out

- **Use:** UI signals, presence, lightweight state.
- **Delivery:** best‑effort only to **currently subscribed sessions**. No persistence.
- **Backpressure:** drop oldest per‑session or per‑topic buffer. Optional coalescing by `key`.
- **Security:** publish/subscribe ACLs; rate limits per subject.

### 6.2 `stream://` — Durable Append Log

- **Use:** audit trails, analytics, event‑sourcing, CDC.
- **Partitioning:** by `headers.key` hash, or explicit `partition` query param.
- **Offset Model:** each partition is a monotonically increasing 64‑bit offset.
- **Consumption:**
  - **Direct**: client keeps its own offset.
  - **Groups**: broker tracks `consumerGroup → {partition → committedOffset}`.
- **Retention:** time/size; optional compaction by key (keep latest per key).
- **Replay:** subscribe from explicit offset or timestamp (`fromTs=...`).
- **Acks:** client commits offsets (`COMMIT` frames) — at‑least‑once.

### 6.3 `queue://` — Work Queue with Visibility

- **Use:** background jobs, integrations.
- **Lease:** `Lease(consumer, leaseMs, maxN)` pops messages and assigns visibility.
- **Retry:** `Nack` returns to ready queue (optionally with delay, exponential backoff).
- **DLQ:** after `maxAttempts`, move to `queue://.../dlq`.
- **Ordering:** best‑effort; can add priority (min‑heap or bucketed priorities).
- **Acks:** `Ack(id)` removes permanently.

### 6.4 `rpc://` — Request/Response

- **Use:** low‑latency service calls over the broker transport.
- **Pattern:** `PUB rpc://server/endpoint` with `replyTo: inbox://realm/.../$session/*` and `correlationId`.
- **Timeouts:** client specifies `ttlMs`; broker routes timeout error if no response.
- **Load‑balancing:** competing consumers on same route.

### 6.5 `inbox://` — Ephemeral Mailbox

- **Use:** per‑session responses, direct messages.
- **Lifecycle:** created implicitly on session open (auto route: `inbox://<realm>/.../$session/<id>`); GC on close or TTL.
- **Security:** only owner can subscribe; others may publish if ACL allows.

## 7. Control Plane

### 7.1 Responsibilities

- **Identity & Keys**: JWKS (or external IdP) distribution, token validation hints (issuer, audience, claim mapping).
- **Policy Snapshots**: per tenant/subject route ACLs, quotas, per‑route config (partitions, retention, store driver, compression).
- **Topology**: directory of broker nodes (host, version), **heartbeats** (every 10s), last‑seen map.
- **Stats & Audit**: receive counters (PUB/SUB, acks, storage bytes, consumer lag, errors).
- **Leases**: optional central lease host for scheduled jobs (future).

### 7.2 Data Model (examples)

```json
// Policy snapshot
{
  "tenant": "T-12345",
  "subjects": {
    "user:abc": {
      "allow": ["stream://realm/**", "rpc://realm/service/*"],
      "deny": ["queue://realm/secret/**"],
      "limits": { "pubRps": 500, "subRps": 1000 }
    }
  },
  "routes": {
    "stream://realm/ops/audit/*": {
      "partitions": 6,
      "retentionHours": 168,
      "compression": "lz4"
    },
    "queue://realm/jobs/email": { "maxAttempts": 10, "visibilityMs": 300000 }
  },
  "storage": { "stream.driver": "pebble", "queue.driver": "pebble" }
}
```

### 7.3 Broker ↔ Control Plane Protocol

- **Bootstrap:** broker starts with control plane host+creds (env).
- **Subscribe:** broker subscribes to `policy@tenant` topics; receives **versioned** snapshots.
- **Heartbeat:** broker sends `{nodeId, uptime, build, sessions, cpu/mem, storeSizes}`.
- **Failure mode:** if control plane is down, brokers operate with **last known policy** (stale‑tolerant) and **local caching**.

### 7.4 JWT & Permissions

This section describes the JWT claims and the scope grammar. Scopes are intentionally aligned with route URIs so the mental model matches routing (`scheme://realm/...`). Examples:

- `stream://realm/**` — publish or read to any stream under `realm`
- `rpc://realm/service/**` — subscribe/receive RPCs for `service` in `realm`

Goals:

- Keep scope checks simple and route-aligned so `scope` entries map directly to route patterns.
- Support quick offline validation (signature + claims + scope match) with optional control-plane overrides.

Token types

- Access Token: short‑lived bearer token used for connection/authN and per-request authZ. TTL = 5–60m.
- Service Token: long‑lived but tightly scoped (rotateable, revocable). Use for broker↔broker or server clients.

Required claims

- `iss` — issuer
- `sub` — subject (user or service id)
- `aud` — audience (must include `fitz-broker` or a tenant‑scoped entry; see below)
- `exp`, `iat` — expiry / issued at
- `jti` — JWT id (recommended for revocation)
- `scope` — space separated scope atoms (grammar below)

Tenant via audience

- Prefer conveying tenant via `aud` rather than a custom `tid` claim so external IdPs don't need custom claims.
- Convention: include a tenant-scoped audience entry like `fitz-broker:<tenant-id>` (for example `fitz-broker:T-12345`) or an `aud` array containing that entry. Brokers extract the tenant id from that audience entry for tenant-scoped policy lookup.

Scope grammar (tenant‑prefixed, operation-as-last-segment)

Simpler rule: scopes do not include an explicit action prefix. The operation (op) is the last segment of the route URI and is used to determine the requested action (for example `publish`, `subscribe`, `ack`, etc.). The compact scope format is:

`<tenant>::<scheme>://<realm>/<path...>`

Where:

- `<tenant>` is the fitz tenant id (for example `T-12345`).
- `<scheme>` is one of the route schemes (`stream`, `queue`, `rpc`, `notice`, `inbox`).
- Wildcards in the `<path...>` follow the same rules: `*` single segment, `**` multi‑segment.

Examples:

- `T-12345::stream://acme/ingest/publish` — allow the `publish` op in `acme/ingest`.
- `T-12345::stream://acme/ingest/**` — allow any op under `acme/ingest` (including `publish`, `commit`, ...).
- `T-12345::rpc://acme/payments/execute` — allow `execute` op on the payments RPC.

Matching rules

- A request carries a concrete route URI (e.g., `stream://acme/ingest/publish`) and the token scopes.
- Authorization succeeds if any scope in the token matches all of the following:
  1. The `<tenant>` in the scope must match the tenant used for policy lookup (extracted from `aud` or resolved via control plane mapping).
  2. The `<scheme>://<realm>/<path...>` pattern must match the request route using `*`/`**` wildcard rules. The last segment of the matched route is interpreted as the operation (op) the client is attempting.
- If a scope pattern uses `**` or a wildcard that covers the final segment, it grants permissions for all ops in that subtree; if a scope specifies the op explicitly (final segment), it grants only that specific op.
- Control plane policy snapshots still take precedence: explicit deny or allow in a snapshot overrides token scopes according to the decision flow.

Notes

- Embedding tenant in scopes is useful when tokens may be issued by many IdPs or when scopes must be self‑describing (no extra lookups). Prefer carrying tenant via `aud` when possible and use tenant in scope as an additional check when helpful.
- Because the op is the last segment of the route, scope authors can be very granular (specify `.../publish`) or broad (`.../**`) depending on desired least-privilege.

Verification & validation (broker-side)

1. Parse JWT and select key via `kid` header; fetch key from JWKS cache for `iss`.
2. Verify signature (only accept strong algs like RS256/ES256).
3. Validate `iss`, `aud` (contains broker), `exp/iat` within allowed clock skew.
4. Extract tenant from `aud` (expect `fitz-broker:<tenant-id>` or an `aud` array containing it) and use it for policy lookup; avoid requiring a custom `tid` claim.
5. Check `jti` against local revocation cache (if present).
6. Extract scopes and perform action+URI pattern match.

JWKS & key rotation

- Brokers cache JWKS per issuer and refresh periodically (e.g., every 5 minutes) and on unknown `kid`.
- Issuers should publish new keys alongside old keys in JWKS to allow in-flight tokens to validate during rotation window.
- Control plane may push JWKS snapshots as part of policy distribution for lower latency and tighter control.

Revocation

- Prefer short access token TTLs to reduce reliance on revocation.
- For service tokens and long-lived credentials, use `jti` and a revocation list pushed by control plane; brokers cache revocations with TTL equal to remaining token lifetime.
- For critical revocations, control plane can push immediate revoke events which brokers apply atomically to their local cache.

Policy interplay

- Broker decision order (fast path):
  1. If local policy snapshot denies subject/jti → deny.
  2. If local policy snapshot explicitly allows subject/action/route → allow.
  3. Else, check token scopes for matching action+URI pattern → allow if matched.
  4. Else, deny.

Note: Keeping the scope grammar aligned with route URIs keeps checks simple and intuitive for developers and policy authors.

### Refresh tokens & token lifecycle

Although access tokens should be short‑lived for security and performance, practical client applications need a smooth long‑running session experience. Follow these recommendations for refresh tokens and lifecycle management.

High level

- Refresh tokens are issued by the identity provider (control plane / auth server), not by the broker. Brokers remain resource servers and should accept access tokens only.
- The broker should never accept a refresh token in place of an access token. Any refresh exchange must happen at the auth server/introspection endpoint.

Refresh token characteristics

- Long‑lived and confidential: refresh tokens are typically issued only to confidential clients (server apps) or to public clients with PKCE and short lifetimes.
- One‑time rotation: use rotating refresh tokens (each refresh returns a new refresh token and invalidates the old one). This prevents reuse if a token is stolen.
- Bind to client: associate refresh tokens with the client id, client session, and optionally client attributes (IP, device id). Reject use from different clients.

Flows (recommended)

1. Initial auth: client authenticates with auth server (OAuth2/OIDC) and receives: access_token (short TTL), refresh_token, optional id_token.
2. Use: client calls broker with access_token for connections/requests.
3. Refresh: when access_token is near expiry, client calls auth server with refresh_token to obtain a new access_token (+ rotated refresh_token).
4. Rotate: auth server rotates refresh_token; old refresh_token is invalidated; if old token is used later, treat as compromise and revoke the whole session.

Security & revocation

- Short access token TTL (5–15m) minimizes the window for leaked access tokens.
- Maintain a revocation mechanism for refresh tokens on the auth server; brokers rely on short access TTL + optional introspection caching for critical routes.
- For high‑sensitivity operations, require additional checks (token introspection or step‑up authentication) instead of relying solely on long‑lived tokens.

Broker responsibilities

- Validate only access tokens (signature, exp, aud, scope). Do not accept refresh tokens.
- Optionally support token introspection for long‑lived access tokens or to validate unusual claims; cache introspection results with a short TTL (e.g., 30s).
- Subscribe to control plane pushes for critical revocations (session termination, forced logout) so brokers can immediately drop sessions.

Client guidance

- Confidential clients: store refresh tokens securely on the server; use rotating refresh tokens.
- Public/native clients: use short‑lived access tokens + refresh tokens with PKCE and short refresh TTL, and consider device code flow.
- Revoke and rotate refresh tokens on logout, credential change, or suspicious activity.

Operational notes

- Logging: do not log refresh tokens; log refresh events (timestamps, client id, outcome) for audit.
- Metrics: track refresh success/failure rates and detect spikes indicating abuse.
- Edge cases: support forced global logout by pushing a revocation event (jti list) to brokers, which then terminate affected sessions.

## 8. Engine Internals

### 8.1 Components

- **Router:** matches route → scheme handler; enforces ACL; resolves storage handlers.
- **Scheme Handlers:** `NoticeHandler`, `StreamHandler`, `QueueHandler`, `RpcHandler`, `InboxHandler` — stateless; orchestrate store + sessions.
- **Subscription Registry:** per session, wildcard matching, interest indexes.
- **Delivery Pipeline:** batching, flow control (credits), per‑session buffers with backpressure policy.
- **Ack Tracker:** offsets (stream) and lease ids (queue).
- **Metrics:** per route and per tenant counters + histograms; structured logs (`slog` style).

### 8.2 Backpressure & Flow

- **Credit‑based receive** (consumer controls rate).
- **Publisher limits** via token bucket; on exceed → `Nack(TooManyRequests)`.
- **Store pressure**: when write stalls, slow‑path with `Nack(Retry)`; optionally drop `notice://`.

### 8.3 Concurrency Model

- Per‑route **shards** (e.g., per partition) with single threaded append; async fan‑out to subscribers.
- Queue leases handled by dedicated workers; timers wheel for visibility expirations.

## 9. Security

- **AuthN:** JWT (pluggable validator; offline JWKS cache; clock skew tolerance).
- **AuthZ:** control plane snapshots + route evaluation; audit log on denial.
- **Multi‑tenant Isolation:** storage prefixes per tenant; session scoping; quotas per tenant.
- **Transport Security:** TLS required; mTLS optional for service accounts/broker↔broker.

## 10. Observability

- **Logs:** structured (traceId, sessionId, tenantId, route, scheme, latencyMs, sizeBytes).
- **Metrics:** Prometheus/OpenTelemetry: `pub_total`, `sub_total`, `deliver_latency_ms`, `store_write_bytes`, `consumer_lag`, `queue_visible`, `queue_in_flight`, `dlq_total`.
- **Traces:** OpenTelemetry spans for publish, store append, delivery, ack.

## 11. Configuration

- **Broker config:**
  - `transport: websocket`
  - `listen: :8080`
  - `compression: none|lz4|snappy`
  - `stream: { driver: pebble, partitionsDefault: 1, retentionHoursDefault: 168 }`
  - `queue:  { driver: pebble, visibilityMsDefault: 300000, maxAttemptsDefault: 5 }`
  - `limits: { pubRpsPerSession: 1000, maxSubscriptions: 1000 }`
- **Hot‑reloadable** via control plane snapshot (versioned; atomic swap).

## 12. WebSocket Wire Frames (v0)

```
## 12. Wire Protocol — WebSocket (fitz.v1)

This section specifies the initial wire protocol used by clients and brokers over WebSocket. It defines the subprotocol, frame set, encoding rules, a small JSON schema for control frames, the binary envelope framing used for payloads, application error codes, and common sequences (connect/auth, publish→store→deliver).

This protocol is intentionally small and extensible. Control frames are JSON text frames. Large payloads (message bodies) are sent as binary frames using a small header + payload framing so brokers can stream without re-parsing JSON for every message body.

### 12.1 Subprotocol and Negotiation

- WebSocket subprotocol: `fitz.v1` (clients MUST include it in the `Sec-WebSocket-Protocol` header).
- Transport security: TLS required. Use of client certificates (mTLS) is optional and orthogonal to the subprotocol.

Handshake example (client HTTP Upgrade):

  GET /connect HTTP/1.1
  Host: broker.example
  Upgrade: websocket
  Connection: Upgrade
  Sec-WebSocket-Protocol: fitz.v1
  Sec-WebSocket-Key: ...

Server responds with `101` and accepts the same subprotocol.

### 12.2 Frame types (overview)

Control frames are encoded as UTF-8 JSON in WebSocket text frames. Binary frames carry message envelopes where the header is JSON and the body is opaque bytes (optionally compressed). The canonical control frame names are:

- HELLO — initial client metadata
- AUTH — bearer access token
- POLICY_SYNC — control plane snapshot pointer (broker→client for policy hints; rarely used by clients)
- SUB — subscribe to a route/filter
- UNSUB — cancel a subscription
- PUB — publish an envelope (may be sent in binary with header+payload)
- CREDIT — client indicates readiness to receive n messages
- ACK — acknowledge processing of an item (queue id or stream offset)
- NACK — negative ack or request for retry/delay
- PING / PONG — keepalive
- CLOSE — graceful close intent
- ERR — asynchronous error notification

Section names in examples below are upper-case for readability.

### 12.3 Control frame JSON schema (informal)

All control frames share the following envelope when sent as text frames. The `type` field selects the concrete shape.

Common fields:
- `type` (string, required) — one of the frame types above
- `id` (string, optional) — client-supplied frame id for tracing
- `ts` (string, optional) — ISO8601 timestamp for diagnostic use

Example minimal schemas (informal JSON):

- HELLO

  {"type": "HELLO", "clientId": "sdk-1.2.3", "sdk": "rust/0.1.0", "wantsInbox": true }

- AUTH

  {"type":"AUTH", "token":"<access_token_bearer>" }

- SUB

  {"type":"SUB", "subId":"s-1", "filter":"stream://realm/area/resource/*", "credit":100 }

- UNSUB

  {"type":"UNSUB", "subId":"s-1" }

- PUB (control-only, small payloads)

  {"type":"PUB", "envelope": { /* canonical envelope JSON (see section 3.1) */ } }

- CREDIT

  {"type":"CREDIT", "subId":"s-1", "n":50 }

- ACK / NACK

  {"type":"ACK", "route":"queue://realm/area/resource/ack", "id":"<msg-id>" }
  {"type":"NACK", "route":"stream://realm/area/resource/publish", "offset":12345, "code":"RETRY" }

- ERR

  {"type":"ERR", "code":4003, "message":"unauthorized to publish to route" }

All frames MUST be valid JSON when sent as text and MUST not exceed reasonable control frame sizes (recommendation: < 16KB). Large message bodies should use binary `PUB` frames (see below).

### 12.4 Binary envelope framing (recommended for PUB with body)

For large payloads or when efficiency matters, the client SHOULD send a WebSocket binary frame using this framing layout: a fixed 4‑byte big-endian header length followed by a UTF‑8 JSON header and then the raw body bytes.

Layout:

- 4 bytes: header_len (uint32 big-endian)
- header_len bytes: header JSON (UTF-8) — a subset of the canonical envelope without the `body` field, for example:

  {
    "id":"uuidv7",
  "ts":"2025-10-10T13:21:42.123Z",
  "route":"stream://acme/ingest/publish",
    "tenant":"T-12345",
    "headers": {"contentType":"application/json","encoding":"none"},
    "correlationId":"...",
    "ttlMs":0
  }

- remaining bytes: raw body — opaque bytes (binary) as encoded by `headers.encoding` (e.g., `none`, `lz4`, `snappy`). When `contentType` is `application/json`, the body is raw JSON bytes.

When the broker receives such a binary frame it must parse the first 4 bytes, decode the header JSON, then hand the header+payload to the engine. The broker SHOULD not allocate additional copies when streaming to the store when possible.

Note: text-mode `PUB` (JSON with base64 body) is allowed for small messages and debugging but is less efficient.

### 12.5 Frame semantics and flow control

- Credits: subscribers receive data only when they have outstanding credit. A `SUB` frame may include initial `credit`. The server will decrement credit for each delivered envelope and the client sends `CREDIT` frames to replenish.
- Server to client ordering: order is preserved per subscription/partition as described in route semantics. If multiple subscriptions map to the same logical delivery stream the server preserves each stream's ordering guarantees.
- Backpressure: when store writes are slow the broker may stop delivering and may send `NACK` or `ERR` to publishers depending on route QoS.

### 12.6 Error codes (application-level)

These are application-level numeric codes carried in `ERR` frames or in `CLOSE` payloads. They are distinct from WebSocket close codes and are used for programmatic handling.

- 1000 — OK / Normal (not an error; used in CLOSE semantics)
- 4000 — ProtocolError (malformed frame / invalid JSON)
- 4001 — Unauthorized (auth failed or missing)
- 4002 — Forbidden (auth succeeded but not authorized)
- 4003 — InvalidRoute (route URI malformed or unknown scheme)
- 4004 — RateLimited (publish or subscribe rate limit exceeded)
- 4005 — Backpressure (store busy / resource constrained)
- 4006 — NotFound (e.g., ack target not found)
- 4007 — Conflict (duplicate id / duplicate subscription)
- 4100 — Internal (server internal error)

Clients SHOULD treat 4000–4099 as client-level errors (fix request) and 4100 as transient server error. For critical errors the server MAY issue a WebSocket CLOSE after sending an `ERR` frame.

### 12.7 Example sequences

Connect → Auth → Subscribe (happy path)

1. Client: WebSocket Upgrade with `Sec-WebSocket-Protocol: fitz.v1`
2. Client -> Text: HELLO
   {"type":"HELLO","clientId":"web-sdk-1","sdk":"js/0.2.0","wantsInbox":true}
3. Client -> Text: AUTH
   {"type":"AUTH","token":"eyJ..."}
4. Server -> Text: AUTH (ack success) or ERR
   {"type":"AUTH_OK","ts":"...","sessionId":"sess-xxx"}
5. Client -> Text: SUB
  {"type":"SUB","subId":"s-1","filter":"stream://acme/ingest/*","credit":100}
6. Server -> Text: SUB_OK
   {"type":"SUB_OK","subId":"s-1","partitions":[0]}
7. Server -> Binary: (header_len + header_json + body) — delivers envelopes while credit > 0
8. Client -> Text: CREDIT
  {"type":"CREDIT","subId":"s-1","n":100}
9. Client -> Text: ACK
  {"type":"ACK","route":"stream://acme/ingest/publish","offset":12345}

Publish (binary payload) → store → deliver to subscribers

1. Client -> Binary: PUB (header_len + header_json + body)
  header_json.route = "stream://acme/ingest/publish"; header_json.id = "m-1"
2. Broker receives PUB, returns immediate control ack (optionally)
   {"type":"PUB_OK","id":"m-1","ts":"...","storeOffset":12345}
3. Broker appends to store and fans out to subscriptions matching the route; delivery is credit‑gated.
4. Subscriber receives binary envelope and when processing finishes sends ACK/NACK. For queue:// ACK is by message id; for stream:// ACK is commit/commit intent (COMMIT frames or ACK with offset).

Queue lease and ack flow (consumer with lease)

1. Consumer SUBscribes to a `queue://` route with credit `n=1`.
2. Broker issues a delivery (binary envelope) and marks the item in-flight with a visibility deadline.
3. Consumer processes and sends ACK:
  {"type":"ACK","route":"queue://realm/area/resource/ack","id":"<msg-id>"}
4. If ACK not received before visibility, broker returns item to ready queue (and increments attempts), or moves to DLQ on attempts>max.

RPC request/response pattern

1. Caller publishes to an `rpc://` route with `replyTo` set to its `inbox://.../$session/<id>` and `correlationId` set.
2. Broker routes the request to a subscriber (service). The service replies by publishing a message to the caller's inbox route with the same `correlationId`.
3. Caller receives response on its `inbox` subscription and matches `correlationId`.

### 12.8 Extensibility and versioning

- The `type` field and `id` allow extension without breaking older clients. Servers MUST ignore unknown optional fields in control frames.
- New frame types should use namespaced `type` values (for example `X.Y.NEW_TYPE`) during experimental rollout.
- Subprotocol version bumps (e.g., `fitz.v2`) indicate incompatible wire changes; servers MAY accept multiple subprotocols concurrently.

### 12.9 Diagnostics and best practices

- Keep control frames small and use binary PUB for heavy payloads.
- Use `CREDIT` to ensure consumers are not overwhelmed; set sensible initial credit based on message sizes.
- Log `id` and `correlationId` for traces; propagate them in spans.
- For replay and debugging, small `PUB` messages can be sent as text JSON (base64 body), but production clients should use binary framing.

---


## 13. Failure Modes & Guarantees

- **notice://**: may drop under pressure; no retry; no durability.
- **stream://**: at‑least‑once; order within partition; duplicates possible on reconnect; client or group commits.
- **queue://**: at‑least‑once; redelivery after visibility; DLQ on attempts.
- **rpc://**: request may time out; user retriable; idempotency via `correlationId`.

## 14. Migration & Extensibility

- Add new schemes by implementing `ISchemeHandler` and adding store mappings.
- Add new transports by implementing `ITransportServer` and frame codecs.
- Swap storage by implementing `LogStore`/`QueueStore` with the same keys and indexes (keep envelope stable).

## 15. Minimal MVP Checklist

- [ ] WebSocket transport with auth, subs, pub, credit.
- [ ] `notice://` in‑memory fan‑out.
- [ ] `stream://` with single partition (Pebble‑like store), group commits.
- [ ] `queue://` with leases & ack/Nack & DLQ.
- [ ] `rpc://` using `inbox://` auto mailbox.
- [ ] Control plane shim: static policy snapshot service.
- [ ] Metrics + structured logs.

## 16. Open Questions

- Should `stream://` compaction by key be a per‑route toggle with a side index?
- Do we want consumer **epochs** for fence‑off old sessions in groups?
- Broker federation: what’s the minimal routing table we would exchange?
- Rate limit primitives per tenant vs per subject — both?

## 17. Appendix — Storage Keys (Pebble‑like)

```
// All keys are big‑endian sortable; UTF‑8 segments; 0x1F unit separator
// Tenant prefix
T:<tid>

// stream data: offset-ordered per partition
T:<tid>\x1FSTR\x1F<routeKey>\x1F<P#:u32>\x1F<O#:u64> -> value(compressed envelope)

// stream commits per consumer group
T:<tid>\x1FSCM\x1F<routeKey>\x1F<G:group>\x1F<P#:u32> -> <O#:u64>

// queue ready list by priority & enqueue time
T:<tid>\x1FQUE\x1F<routeKey>\x1F<QPRIO:u8>\x1F<TS:u64>\x1F<ID:uuid> -> payload

// queue in‑flight (visibility deadlines)
T:<tid>\x1FQIN\x1F<routeKey>\x1F<DEADLINE:u64>\x1F<ID> -> payload

// attempts
T:<tid>\x1FQAT\x1F<routeKey>\x1F<ID> -> u16

// DLQ
T:<tid>\x1FDLQ\x1F<routeKey>\x1F<TS:u64>\x1F<ID> -> payload
```

## 18. Appendix — Example Policy for a Tenant

```yaml
tenant: T-12345
subjects:
  svc:ingest
    allow:
      - "stream://realm/ingest/**"
      - "rpc://realm/schema/*"
    deny:
      - "queue://realm/payment/**"
    limits: { pubRps: 5000 }
routes:
  stream://realm/audit/*: { partitions: 4, retentionHours: 720, compression: lz4 }
  queue://realm/email: { maxAttempts: 10, visibilityMs: 300000 }
storage:
  stream.driver: pebble
  queue.driver: pebble
```
