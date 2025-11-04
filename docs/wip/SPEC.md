# Mesh Broker Specification

**Version:** Draft 0.5  
**Status:** Work in Progress (implementation‑guiding)  
**Last Updated:** 2025‑10‑12  

---

## 0. Document Conventions

- **Keywords:** MUST, SHOULD, MAY follow RFC 2119 semantics.  
- **Routes:** `scheme://realm/area/resource[/operation]`.  
- **Frames:** Binary TLV envelopes over a transport (WebSocket now; others later).  
- **Time:** Timestamps are ISO‑8601 UTC unless stated.  
- **IDs:** All IDs are opaque bytes unless specified (UUIDv7 RECOMMENDED for monotonicity).

---

## 1. Purpose and Scope

The Mesh Broker is a transport‑agnostic, multi‑tenant message broker providing unified **notices**, **streams**, **queues**, **RPC**, **inboxes**, and **control‑plane coordination** via typed routes.  
This specification defines the **architecture**, **route semantics**, **wire protocol**, **persistence model**, **security**, **operational behavior**, and **error handling** such that independent teams can implement interoperable brokers and clients.

### 1.1 Design Goals

- **Transport‑agnostic core:** WebSocket today; future QUIC/gRPC/NATS via adapters.  
- **Deterministic routing:** URI routes with strict semantics per scheme.  
- **Tenant‑aware security:** JWT claims enforce realm isolation and route permissions.  
- **Swappable persistence:** Streams & queues pluggable (Local kv‑like, Azure Table/Blob, AWS Kinesis/S3, etc.).  
- **Crash‑safe durability:** WAL + manifested stores; deterministic recovery.  
- **Low‑latency ephemeral paths:** `notice://` and `inbox://`.  
- **Operability:** Rich metrics; heartbeats via `control://`; straightforward config.  
- **Simplicity over clustering:** Single‑broker operation; replication/Raft **out of scope**.

---

## 2. System Architecture

### 2.1 Diagram

```
          +-------------+
          |   Clients   |
          +------+------+            (WebSocket binary frames)
                 |
                 v
        +--------+--------+
        |   Transport     |   ← abstracts framing
        +--------+--------+
                 |
                 v
        +--------+--------+
        |   Router Core   |   ← route lookup, acks, fanout, backpressure
        +--------+--------+
          |    |      |
          |    |      +-------------------+
          |    |                          |
          v    v                          v
   +--------+  +---------+        +---------------+
   | Streams |  | Queues |        | Notice Router |
   +--------+  +---------+        +---------------+
          |
          v
     +------------------+
     | Storage Provider |  ← pluggable (local/cloud)
     +------------------+

            |
            v
     +---------------+
     | Control Plane |
     +---------------+   ← registration, heartbeat, config, leases
```

### 2.2 Core Components & Responsibilities

| Component | Responsibilities |
|---|---|
| **Transport** | WebSocket binding, frame I/O, ping/pong. |
| **Router Core** | Route parse/validate, ACLs, delivery, ack, flow control, retries. |
| **Session Manager** | JWT validation, session state, client caps, heartbeat. |
| **Streams Subsystem** | Append/Read/Peek/Consume(prefix) with monotonic sequences. |
| **Queues Subsystem** | Enqueue/Lease/Complete with visibility & dedupe. |
| **Storage Provider** | WAL + StreamStore + QueueStore implementations (local/cloud). |
| **Control Link** | `control://` routes for register, heartbeat, config, leases. |

### 2.3 Data Model

- **Tenant (realm):** Required claim on client JWT; prefixes all tenant‑visible routes.  
- **Route:** `scheme://realm/area/resource[/operation]` (`control://` is system scope).  
- **Sequence IDs:** Monotonic per‑stream (u64 or UUIDv7).  
- **Message:** Opaque payload + headers (ct, encoding, correlation‑id, etc.).

---

## 3. Route Semantics

### 3.1 Route Table

| Scheme | Example | Persistence | Delivery | Notes |
|---|---|---|---|---|
| `notice://` | `notice://acme/alerts/system` | None | Best‑effort broadcast | No replay. |
| `stream://` | `stream://acme/orders/events` | Durable (local/cloud) | Ordered, replayable | `Append`, `Read`, `Peek`, `Consume(prefix)`. |
| `queue://` | `queue://acme/jobs/thumbnail` | Durable (local/cloud) | At‑least‑once via leases | Visibility timeout, `Complete`. |
| `rpc://` | `rpc://acme/auth/verify` | Transient | Req/Rep | Uses `inbox://` when needed. |
| `inbox://` | `inbox://client/abcd1234` | Transient | Direct | Session‑ or client‑scoped. |
| `control://` | `control://broker/heartbeat` | Transient (small durable topics MAY exist) | System directional | Reserved namespace, realm bypass. |

### 3.2 Enforcement

- Tenant routes MUST include realm and MUST match JWT `realm`.  
- Permissions via claims/tokens from control plane (`pub:`, `read:`, `consume:`, `peek:`).  
- `control://` reserved to broker↔control plane.

### 3.3 Streams

- **Append(route, payload) → seq**: durable append; returns assigned seq.  
- **Read(route, fromSeq, limit) → [records]**: forward scan by seq.  
- **Peek(route) → record**: last (highest seq) record; **requires fully‑qualified route**.  
- **Consume(prefixRoute, fromSeq, limit) → [records]**: hierarchical consumption; merges descendants by deterministic order `(ts, route, seq)`; backend MAY optimize.

### 3.4 Queues

- **Enqueue(route, message, [dedupeKey]) → msgID**.  
- **Lease(route, visibilityMs[, maxBatch]) → messages**.  
- **Complete(route, msgID)** to acknowledge.  
- MAY support **DLQ** via `queue://.../dlq`.

### 3.5 RPC

- **Call(route, payload, timeout, replyTo?)**; if omitted, broker allocates `inbox://session/...`.  
- **Reply(route=replyTo, correlationId, payload)**.

### 3.6 Control

- `control://broker/{register|heartbeat|shutdown}`.  
- `control://config/update`, `control://lease/{acquire|release}`, `control://command/*`.

---

## 4. Wire Protocol (Frames)

### 4.1 Framing (TLV)

```
+----------------+------------+------------+----------------+
| Length (u32)   | Type (u8)  | Flags (u8) | Channel (u32)  |
+----------------+------------+------------+----------------+
|     TLV Payload (repeating [Tag u8][Len u16][Value...])    |
+------------------------------------------------------------+
```
- Integers are big‑endian. Length includes the 4‑byte prefix.  
- Flags: `COMPRESSED=1<<0`, `ENCRYPTED=1<<1`, `ACK_REQUIRED=1<<2`, `FINAL=1<<3`.  
- Multiplexing: Channel identifies a logical stream; current implementation uses a single default channel.

Common TLV tags:
- `TAG_ROUTE (0x20)`: UTF‑8 route string (e.g., `notice://acme/alerts/system`).
- `TAG_ID (0x21)`: message id or correlation id.
- `TAG_BODY (0x22)`: opaque payload bytes.
- `TAG_ROUTE_REPLY (0x23)`: reply route for RPC.
- `TAG_SEQ (0x24)`: u32 sequence for streaming replies.
- `TAG_STREAM_END (0x25)`: empty value; marks end of stream.
- `TAG_TTL_SECS (0x70)`: u64 TTL override.
- `TAG_LEASE (0x76)`: u32 visibility seconds.
- `TAG_DELIVERY_TOKEN (0x77)`: opaque token for queue delivery.
- `TAG_SUBSCRIBE (0x90)`: empty value; subscribe intent.
- `TAG_UNSUBSCRIBE (0x91)`: empty value; unsubscribe intent.
- `TAG_NOTIFICATION (0x92)`: empty value; present on delivered DAT.
- `TAG_TOKEN (0x10)`: auth token (`mock:*` in dev).
- `TAG_ERR_CODE (0x40)`, `TAG_ERR_MSG (0x41)`: error reporting.

### 4.2 Types (TLV)

| Type | Name | Purpose |
|---|---|---|
| `0x01` | CONN_OPEN | Client hello/auth start (includes TAG_TOKEN in this implementation) |
| `0x02` | CONN_CLOSE | Close or secondary auth; broker treats TAG_TOKEN as auth as well |
| `0x03` | ACK | Generic acknowledgment |
| `0x05` | REG | Subscribe/Unsubscribe (TAG_ROUTE + TAG_SUBSCRIBE/UNSUBSCRIBE) |
| `0x06` | REQ | Queue and other request verbs |
| `0x07` | PUB | Publish to a route (TAG_ROUTE + TAG_ID + TAG_BODY + optional tags) |
| `0x08` | DAT | Broker → client delivery/notification |
| `0x0B` | ERR | Error frame (TAG_ERR_CODE + TAG_ERR_MSG) |

Notes:
- Heartbeats are broker‑initiated DAT frames that may include `TAG_NOTIFICATION` without body.
- RPC uses PUB with `TAG_ROUTE_REPLY`; replies are PUB to `replyTo` and delivered as DAT to the subscriber.

### 4.3 Connection Lifecycle (TLV mapping)

1. Client sends `CONN_OPEN` (optionally with `TAG_TOKEN`).
2. Broker responds `ACK` if accepted; else `ERR` with code/message.
3. Client may also send `CONN_CLOSE` with `TAG_TOKEN` (alt auth path in this implementation).
4. Heartbeats: broker emits periodic `DAT` with `TAG_NOTIFICATION`.
5. Close: client/broker may send `CONN_CLOSE`; no specific reason TLV yet.

### 4.4 Flow Control & Backpressure

- Client proposes `ack_window` (default 128). In‑flight `ACK_REQUIRED` frames counted; broker pauses when exceeded.  
- Consumers MAY set `max_records`/`max_bytes` hints.

### 4.5 Optional End‑to‑End Encryption

- Ephemeral pubkey in HELLO → broker derives symmetric keys; `ENCRYPTED` flag set for encrypted payloads.  
- Routes/headers remain clear for routing.

---

## 5. Persistence and Recovery

### 5.1 Storage Provider Abstractions

```
interface StreamStore {
  Append(streamID, payload) -> seqID
  Read(streamID, fromSeq, limit) -> [Record]
  Peek(streamID) -> Record                       // fully‑qualified only
  Consume(prefixID, fromSeq, limit) -> [Record]  // hierarchical interleave
  Snapshot(streamID) -> BlobRef?                 // optional
}

interface QueueStore {
  Enqueue(queueID, message, dedupeKey?) -> msgID
  Lease(queueID, visibilityMs, maxBatch?) -> [Message]
  Complete(queueID, msgID) -> void
  Abandon(queueID, msgID) -> void               // optional early release
}
```
**Record:** `{ seq:u64, ts:time, route:string, payload:bytes, headers:map }`  
**Message:** `{ id:string, ts:time, route:string, payload:bytes, headers:map, leaseUntil:time }`

### 5.2 Implementations

- **Local:** kv‑like KV + WAL + SST/manifest.  
- **Azure:** Blob for chunked stream segments + Table for index; Queue/Table for queues.  
- **AWS:** Kinesis (hot tail) + S3 (SST‑like cold segments); SQS/DynamoDB for queues.  
All MUST preserve monotonic sequences and at‑least‑once queue semantics.

### 5.3 WAL

- Append‑only segments with header `{lsn, crc, ts, routeID, type}`.  
- `fsync` batch/interval (`WAL_FSYNC_INTERVAL_MS`, default 5ms).  
- Startup: scan last segment(s), verify CRC, rebuild tails/indexes.

### 5.4 Stream Manifest & Compaction (Local)

- Immutable SSTs; manifest lists active runs by route/prefix.  
- Background compaction merges small SSTs; tombstones removed.  
- `Peek`: memtable tail → WAL tail → SST index top entry.

### 5.5 Recovery Sequence

1. Initialize selected storage providers; discover capabilities.  
2. WAL replay → rebuild indexes & open manifests.  
3. Reconstruct unacked queue leases if persisted.  
4. Send `control://broker/register`; begin `control://broker/heartbeat` when healthy.

---

## 6. Security

### 6.1 Authentication

- JWT in HELLO headers (`Authorization: Bearer ...`).  
- Validate signature (JWKS), `exp`, `aud`.  
- `realm` claim REQUIRED for non‑control routes.

### 6.2 Authorization

- Claims include route grants, e.g.:  
  - `pub:stream://acme/orders/*`  
  - `read:queue://acme/jobs/*`  
  - `consume:stream://acme/*`  
  - `peek:stream://acme/*`  
- Broker enforces per frame at dispatch.

### 6.3 Multi‑Tenant Isolation

- Route.realm MUST equal JWT.realm.  
- Optional **ownership**: `owner={tenantId}` claim; broker checks against control‑plane registry.

### 6.4 Auditing

- Security events SHOULD be emitted to `stream://{realm}/audit/broker` (or system control topic).

---

## 7. Errors & Retry Semantics

### 7.1 Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 1001 | ERR_UNAUTHORIZED | JWT invalid/expired | Refresh token; reconnect |
| 1002 | ERR_FORBIDDEN | Missing route permission | Fix grants |
| 1003 | ERR_BAD_ROUTE | Route parse/realm mismatch | Correct route |
| 1004 | ERR_NOT_FOUND | Route/resource missing | Verify existence |
| 1005 | ERR_CONFLICT | Dedupe/idempotency conflict | Retry/backoff or change key |
| 1006 | ERR_FLOW_CONTROL | Ack window exceeded | Drain ACKs; increase window |
| 1007 | ERR_BACKEND_UNAVAILABLE | Storage provider offline | Exponential backoff |
| 1008 | ERR_TIMEOUT | Operation timeout | Retry |
| 1009 | ERR_PAYLOAD_TOO_LARGE | Exceeds limits | Split payload |
| 1010 | ERR_INTERNAL | Unhandled broker error | Retry/backoff; report |

### 7.2 Retry Guidance

- Provide idempotency (`dedupeKey` or `cid`) for safe retries.  
- Backoff: `50ms → 100 → 200 → 400 → 800` (cap 2s) with jitter; max 10 attempts (suggested).  
- On `ERR_FLOW_CONTROL`, halt `ACK_REQUIRED` sends until ACKs reduce in‑flight.

---

## 8. Router & Storage Pseudocode

### 8.1 Routing Loop (receive → deliver)

```pseudo
loop:
  frame = transport.recv()
  route = parse(frame.route)

  if !authorize(session.jwt, route, frame.type):
    sendError(frame, ERR_FORBIDDEN); continue

  switch frame.type:
    case NOTICE:
      deliverNotice(route, frame.payload)
      ackIfRequested(frame)

    case STREAM_APPEND:
      seq = streamStore.Append(route.id, frame.payload)
      emitAck(frame, headers={ "seq": seq })

    case RPC_CALL:
      dst = resolveRpcTarget(route)
      forwardRpc(dst, frame, replyTo=session.inbox)

    case QUEUE_PUSH:
      id = queueStore.Enqueue(route.id, frame.payload, frame.headers["dedupeKey"]?)
      emitAck(frame, headers={ "id": id })

    case CONTROL:
      handleControl(frame)  // broker↔control only

    default:
      sendError(frame, ERR_BAD_ROUTE)
```

### 8.2 Stream Consume (hierarchical)

```pseudo
function Consume(prefix, fromSeq, limit):
  iters = []
  for child in listDescendantStreams(prefix):
    iters.append(streamIter(child, fromSeq))
  heap = minheap(by = record.seq, then = record.ts, then = record.route)
  for it in iters:
    if r := it.next(): heap.push((r, it))
  out = []
  while heap not empty and len(out) < limit:
    (rec, it) = heap.pop()
    out.append(rec)
    if next := it.next(): heap.push((next, it))
  return out
```

### 8.3 Queue Lease Cycle

```pseudo
function WorkerLoop(queue, visMs):
  while True:
    msgs = queueStore.Lease(queue, visMs, maxBatch=16)
    for msg in msgs:
      try:
        process(msg)
        queueStore.Complete(queue, msg.id)
      except Exception:
        // allow lease to expire or Abandon(queue, msg.id) if supported
        continue
```

---

## 9. Control Plane Integration

### 9.1 Registration

- On startup, broker sends `control://broker/register` with:  
  `brokerId`, `version`, `realmSpan`, `endpoints`, `capabilities` (example below).  
```json
{
  "stream_backend": "azure",
  "queue_backend": "kv",
  "supports_peek": true,
  "supports_consume_prefix": true
}
```
- Control plane MAY respond with configuration deltas.

### 9.2 Heartbeats

- Default 30s interval (configurable): `control://broker/heartbeat` with:  
  `uptime`, `clients`, `streams_appended`, `queue_depth`, `wal_lag_ms`, `errors_last_min`, `backend_status`.

### 9.3 Leases & Config

- Control plane MAY grant **work leases** to coordinate external workers.  
- Live config updates via `control://config/update` (e.g., change `ack_window`, limits).

**Sequence:**

```
Broker → Control: CONTROL register
Control → Broker: ACK with config
Broker ↔ Control: CONTROL heartbeat (periodic)
```

---

## 10. Operational Model

### 10.1 Configuration

**Environment:**

```
BROKER_LISTEN=:8080
BROKER_REALM=dev1
BROKER_CONTROL_PLANE=wss://control.dev1.mesh.local
BROKER_ACK_WINDOW=256
BROKER_WAL_DIR=/var/lib/mesh/wal
BROKER_STREAM_BACKEND=azure   # kv|azure|aws
BROKER_QUEUE_BACKEND=kv   # kv|azure|aws
BROKER_HEARTBEAT_INTERVAL=30s
```

**Manifest (YAML):**
```yaml
version: 1
routes:
  - scheme: stream
    prefix: stream://dev1/orders
    retention_days: 14
  - scheme: queue
    prefix: queue://dev1/jobs
    max_visibility_ms: 600000
limits:
  max_payload_bytes: 1048576
  max_inflight: 512
security:
  jwks_url: https://auth.dev1/jwks.json
```

### 10.2 Startup

1. Load config/manifest.  
2. Open WAL and storage backend(s).  
3. WAL recovery & index rebuild.  
4. Register with control plane; begin heartbeats.  
5. Accept client connections.

### 10.3 Shutdown

1. Stop accepting new connections.  
2. Flush WAL & stores.  
3. Send `control://broker/shutdown`.  
4. Close transports.

### 10.4 Metrics

- `broker_clients{realm}`  
- `stream_appends_total{route}`  
- `stream_peek_latency_ms{route}`  
- `stream_consume_records_total{prefix}`  
- `queue_enqueued_total{route}`  
- `queue_lease_latency_ms{route}`  
- `queue_inflight{route}`  
- `wal_lag_ms`  
- `errors_total{code}`

---

## 11. Limits and Defaults

| Setting | Default | Notes |
|---|---|---|
| Max payload | 1 MiB | Configurable |
| Ack window | 128 | Per‑session |
| Heartbeat interval | 30s | Control link |
| WAL fsync | 5ms batch | 0 = every write |
| Queue lease | 120s | Client‑chosen |
| Max consume batch | 1,000 records or 4 MiB | whichever first |

---

## 12. Examples

### 12.1 Frame Trace

```
C → B: HELLO{clientId, jwt, ack_window=256, ephemeralPubKey}
B → C: AUTH{ok, sessionId}
C → B: STREAM_APPEND route="stream://acme/orders/events", payload=...
B → C: ACK{seq=42}
C → B: RPC_CALL route="rpc://acme/auth/verify", payload=...
B → C: RPC_REPLY{correlationId=..., payload=...}
B → CP: CONTROL route="control://broker/heartbeat", payload=metrics
```

### 12.2 Stream Peek & Consume

```
Peek("stream://acme/orders/events") -> last record only (no offset advance)

Consume("stream://acme/orders", fromSeq=0, limit=100)
 -> interleaved records from orders/{created,updated,deleted,...}
```

---

## 13. Compliance Checklist

- [ ] Enforce realm = JWT.realm for all non‑control routes.  
- [ ] Implement frame TLV with CRC32.  
- [ ] Support NOTICE, STREAM_APPEND, QUEUE_PUSH, RPC_CALL/REPLY, CONTROL, ACK, ERROR, HEARTBEAT, CLOSE.  
- [ ] Provide StreamStore: Append/Read/Peek/Consume(prefix).  
- [ ] Provide QueueStore: Enqueue/Lease/Complete.  
- [ ] WAL with crash‑safe replay.  
- [ ] Heartbeat via `control://broker/heartbeat`.  
- [ ] Error codes + retry semantics as defined.  
- [ ] Configurable limits; metrics emission.

---

## 14. Glossary

**Realm** tenant identifier; **Route** URI identifying resource; **WAL** write‑ahead log; **SST** immutable sorted segment; **Lease** timed claim on queue message; **Capability** feature of a broker/storage backend announced to control plane.

---

*End of Specification.*