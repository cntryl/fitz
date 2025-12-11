# Fitz RPC v2 — Domain Specification

**Version:** 2.0  
**Status:** Ready for implementation  
**Durability:** None (ephemeral)  
**Transport:** TLV (binary, actor-first)  
**Execution Model:** Actor-based with inbox routing

---

# 1. Overview

Fitz RPC v2 provides ultra-low-latency request/response messaging with:

* Exactly-one worker processing per request
* Strict correlation for replies
* Native streaming responses
* Fully in-memory dispatch (no persistence)
* Actor-model internal routing
* Natural backpressure via per-client inboxes
* No shared locks → zero contention
* Horizontal worker scaling without coordination

RPC is intentionally not durable. If a client disconnects or crashes, their pending RPCs evaporate, making RPC v2 a perfect fit for microservice calls, control-plane traffic, user-facing requests, analytics queries, and streaming workloads such as AI inference or large reports.

---

# 2. Route Format

```
rpc://{realm}/{area}/{resource}/{operation}
```

Examples:

* rpc://acme/auth/user/create
* rpc://acme/inventory/item/update
* rpc://acme/reports/monthly/run
* rpc://cntryl/ai/embedding/generate

All RPC requests MUST specify a reply route (inbox).

---

# 3. Core Concepts

## 3.1 Actor Participants

| Actor               | Purpose                                              |
| ------------------- | ---------------------------------------------------- |
| RpcRouteActor       | Owns a single RPC route; receives requests and assigns workers |
| WorkerActor         | Handles business logic; subscribes to RPC routes     |
| ReplyInboxActor     | Per-client inbox that serializes replies to the transport |
| TransportActor      | Moves TLV frames to/from connections                  |

Workers do not communicate directly—every hand-off flows through the engine-level actor graph.

---

## 3.2 Request Lifecycle

```
Client → RpcRouteActor → WorkerActor → ReplyInboxActor → Client
```

Each stage is non-blocking and actor-serialized.

---

## 3.3 In-Memory Semantics

RPC v2 guarantees:

* No durability
* No persistence
* No rewinds
* No duplicate replies

It is similar to a NATS Request but without lock contention, routing tables, subscription work stealing, or shard coordination.

---

# 4. RPC Request Model

## 4.1 TLV Frame

```
TAG_ROUTE        = "rpc://acme/auth/user/create"
TAG_ID           = correlation_id
TAG_BODY         = request_bytes
TAG_ROUTE_REPLY  = "inbox://session/123"
TAG_HINT         = optional reply-mode hint
TAG_CONTENT_TYPE = optional
```

## 4.2 Required Fields

| Tag              | Meaning                           |
| ---------------- | --------------------------------- |
| TAG_ROUTE        | Fully qualified RPC route         |
| TAG_ID           | Client-generated correlation ID   |
| TAG_ROUTE_REPLY  | Client inbox route                |
| TAG_BODY         | Request payload                   |

---

## 4.3 Correlation Semantics

* Correlation ID is unique per client session.
* Every response chunk must echo the same ID.
* Workers use it to track streaming boundaries.

---

# 5. RPC Response Model

## 5.1 Response Frame

```
TAG_ID         = correlation_id
TAG_BODY       = response_bytes
TAG_SEQ        = sequence number (optional)
TAG_STREAM_END = optional terminal marker
TAG_CONTENT_TYPE = optional
```

## 5.2 Streaming Rules

* `TAG_SEQ` starts at 0 and must increment by 1.
* `TAG_STREAM_END` finalizes the response and releases inbox state.
* ReplyInboxActor enforces ordering, buffering ahead-of-time chunks, and dropping duplicates.

Streaming is push-based: chunks flow as soon as they are ready.

---

# 6. RpcRouteActor Specification

Each route uses a dedicated RpcRouteActor to queue inbound RPCs, assign workers, issue ephemeral leases, handle backpressure, and drop stale requests when clients disconnect.

## 6.1 State

```rust
struct RpcRouteState {
    route: Route,
    pending: VecDeque<RpcRequest>,
    workers: Vec<WorkerRegistration>,
    capacity: usize,
}
```

## 6.2 Behavior

### On request arrival

* If queue is full, reply with `RPC_BACKPRESSURE`.
* Otherwise enqueue and attempt an immediate hand-off to a worker.

### Worker assignment

* Policy: round-robin or least-busy selection.
* The worker receives a `RpcWorkItem` plus lease expiration metadata.
* Worker must reply (or ack) before lease expiry.

### Lease expiration

* If a worker crashes or hangs, lease expiration re-enqueues the request.
* The next worker receives the request.
* Inbox-layer correlation ensures the client never sees duplicate replies.

---

# 7. WorkerActor Specification

Workers subscribe to RPC routes via `subscribe rpc://{realm}/{area}/{resource}/{operation}`.

## 7.1 Work item

```rust
struct RpcWorkItem {
    correlation_id: String,
    reply_route: String,
    body: Vec<u8>,
    lease_expiration: u64,
}
```

## 7.2 Responsibilities

1. Process the request synchronously (single or streaming responses).
2. Send replies to `reply_route` using TLV frames.
3. Emit `RPC_ACK(correlation_id)` when work is complete.

## 7.3 Failure handling

* Worker crashes: lease expires and the route actor re-enqueues the request.
* Client observes only the committed replies since ReplyInboxActor serializes output.

---

# 8. ReplyInboxActor Specification

Each client transport owns `inbox://session/{session_id}`—a single-threaded actor that orders replies per correlation ID, handles slow transports, and drops state when the session ends.

## 8.1 State

```rust
struct InboxState {
    pending: HashMap<CorrelationId, ReplyAccumulator>,
    transport_sink: Sender<TlvFrame>,
}
```

## 8.2 Streaming enforcement

* If `seq == expected`, forward immediately.
* If `seq > expected`, buffer until missing chunks arrive.
* If `seq < expected`, drop as duplicate.
* On `TAG_STREAM_END`, finalize and clear state.

---

# 9. TLV Wire Summary

## 9.1 Request

| Tag          | Value          |
| ------------ | -------------- |
| ROUTE        | RPC route      |
| ID           | correlation ID |
| BODY         | request bytes  |
| ROUTE_REPLY  | reply inbox    |
| HINT         | optional       |
| CONTENT_TYPE | optional       |

## 9.2 Response chunk

| Tag        | Value           |
| ---------- | --------------- |
| ID         | correlation ID  |
| SEQ        | sequence number |
| BODY       | chunk bytes     |
| STREAM_END | optional        |

## 9.3 Ack (Worker → Route)

| Tag | Value          |
| --- | -------------- |
| ID  | correlation ID |

---

# 10. Backpressure Model

## 10.1 Route-level capacity

* Full queue → return `RPC_BACKPRESSURE` so clients can retry with jitter.

## 10.2 Inbox-level pressure

* The inbox buffers up to a configurable limit.
* Exceeding it disconnects the session and frees state.
* Worker replies remain in memory until the inbox flushes or session ends.

## 10.3 Worker flow control

* Workers process one request by default (configurable concurrency per worker).

---

# 11. Error Model

| Code                      | Meaning                     |
| ------------------------- | --------------------------- |
| RPC_TIMEOUT               | Worker didn't reply in time |
| RPC_BACKPRESSURE          | Route queue full            |
| RPC_UNAUTHORIZED          | Missing permission          |
| RPC_INVALID_ROUTE         | Route parsing failure       |
| RPC_STREAM_GAP            | Out-of-order chunk          |
| RPC_CLIENT_DISCONNECTED   | Inbox vanished mid-request  |
| RPC_WORKER_CRASHED        | Lease expired without reply |

Errors travel to the reply inbox under the same correlation ID.

---

# 12. Performance Expectations

* < 150µs p50 latency for small RPCs
* < 1.0ms p99 under load
* Millions of RPC/sec per node
* Zero hot-path allocations (actor-local queues)
* Streaming with 64KB chunks limited only by transport bandwidth

---

# 13. Testing Requirements

## 13.1 Unit tests

* TLV parsing and validation
* Inbox reassembly and ordering
* Worker lease expiration and requeue
* RpcRouteActor load balancing
* Streaming order enforcement
* Error delivery to inbox

## 13.2 Integration tests

* Full round-trip RPC
* Multi-worker concurrency
* Worker crash recovery
* Client disconnect mid-stream
* Backpressure scenarios
* High-volume streaming calls

## 13.3 Performance tests

* Latency
* Throughput
* Worker scaling
* Streaming bandwidth
* Load shedding/backpressure effectiveness

---

# 14. Why This Is World Class

* Actor-driven, non-blocking, zero-durable state machines
* Single-owner queues (no locks) and perfect backpressure
* Streaming-first while remaining correlation-safe
* Multi-worker and multi-route optimized
* Designed like a modern fabric rather than a pub/sub relic
