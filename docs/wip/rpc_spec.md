# RPC specification

This document describes the RPC model for Fitz, including TLV conventions, reply-routing options (reply-queue vs direct transport), an efficient hybrid worker-selection workflow (signal + reserve), recommended client/worker helpers, error model, and the test matrix.

## Goals
- Support simple request/response with clear correlation semantics.
- Reuse existing engine primitives (Publish, Subscribe, Reserve/Consume) where possible.
- Provide a robust reply-queue pattern with optional streaming responses.
- Offer a hybrid signal+reserve workflow for multi-worker services (single responder, minimal fanout).
- Document an advanced direct-transport reply path option for lower-latency workflows.

Important: RPC requests are ephemeral and non-durable by design. The broker keeps RPC requests only in memory to minimize latency and avoid disk I/O. Do not rely on RPC for guaranteed delivery or persistence. If you need durability, use the regular queue subsystem (separate concern) described in `docs/queue_spec.md`.

## TLV and correlation conventions
- TAG_ROUTE: logical route (request route or reply route)
- TAG_BODY: opaque request/response bytes
- TAG_ID: correlation id (client-supplied or server-assigned). Must be echoed in responses
- TAG_PROTOCOL_VERSION: optional per-call version hint
- TAG_ROUTE_REPLY or TAG_REPLY_HINT: reply route or direct-reply hint
- TAG_SEQ: optional u32 sequence number for streaming responses; starts at 1 and increments per chunk
- TAG_STREAM_END: optional empty tag indicating the final response message in a stream (end-of-stream)

## Reply-routing options
## Backpressure and flow control
- RPC queues are memory-only and subject to in-broker caps to prevent overload (e.g., max total bytes for rpc:// routes). When limits are exceeded, the broker rejects publishes with an error on the same channel. This naturally backpressures clients so they throttle or retry with jitter. Workers should also process promptly and ack to free memory.


### Option A — Reply-queue (baseline)
- Client-side flow:
  1. Client creates a reply route (e.g. `rpc/reply/<client-id>`) and subscribes/reserves on it.
  2. Client publishes the request to `rpc://realm/service` with:
     - TAG_ID = `cid` (correlation id)
     - TAG_ROUTE_REPLY = `rpc/reply/<client-id>` (where worker should publish responses)
  3. Worker reserves/consumes the request from `rpc://realm/service`, processes it, and publishes one or more responses to `rpc/reply/<client-id>` with TAG_ID=`cid`. For streaming, include TAG_SEQ per message and TAG_STREAM_END on the final message.
  4. Client receives responses on its reply route, matches by TAG_ID, and orders by TAG_SEQ until TAG_STREAM_END.

- Advantages: simple, robust, no engine-level per-connection state.
- Notes: ensure Publish -> notify subscribers so Subscribe-based workers can receive signals.

### Option A2 — Hybrid: signal + reserve (recommended for multi-worker services)
- Motivation: Avoid pushing full request bodies to all workers while still selecting exactly one responder via lease.
- Flow:
  1. Client publishes request to `rpc://realm/service` with TAG_ID and TAG_ROUTE_REPLY (request body is stored in-memory on the broker; non-durable).
  2. Engine enqueues the request, then fans out a lightweight notification to all subscribers of `rpc://realm/service` containing TAG_NOTIFICATION + TAG_ROUTE + TAG_ID + TAG_ROUTE_REPLY (omit TAG_BODY).
  3. Each worker that receives the signal immediately calls `reserve("rpc://realm/service", lease_secs)` to pull the full request. The engine grants a lease to exactly one worker and returns (TAG_ID, TAG_BODY, TAG_DELIVERY_TOKEN).
  4. The winning worker processes the request and publishes responses to the client’s reply route using the same TAG_ID; include TAG_SEQ for streaming and TAG_STREAM_END on the final message.
  5. Worker acknowledges/consumes the request using the delivery token.
- Advantages: single responder by lease, efficient signal fanout (tiny), natural backpressure and redelivery on crash (lease expiry).

### Option B — Direct-transport replies (advanced)
- Engine maintains per-connection reply channels and maps a `reply_hint` (opaque) included with a request to the originating session. Workers publish responses with the `reply_hint` and the engine routes the response across the originating transport.
- Advantages: lower latency, simpler client API (no reply route creation).
- Disadvantages: engine/transport coupling, complex reconnection semantics.

## Recommended client API (pseudocode / Rust-like)

- Client helper (reply-queue):
```rust
async fn rpc_call(engine: &EngineHandle, route: &str, body: &[u8], timeout: Duration) -> Result<Vec<u8>, RpcError> {
    // 1. create or reuse reply route `rpc/reply/<client-id>`
    // 2. ensure we are subscribed/reserving on reply route
    // 3. generate correlation id `cid`
    // 4. publish request including TAG_ROUTE_REPLY
    // 5. wait for the response on reply route with matching TAG_ID within timeout
}
```

- Streaming helper:
```rust
async fn rpc_call_stream(engine: &EngineHandle, route: &str, body: &[u8], timeout: Duration) -> Result<impl Stream<Item = Vec<u8>>, RpcError> {
    // same as rpc_call, but return a stream of chunks ordered by TAG_SEQ
    // complete when a message with TAG_STREAM_END arrives or timeout occurs
}
```

- Worker pattern (queue-backed):
```rust
// Reserve next request (in-memory RPC queue)
let (id, body, token) = engine.reserve("rpc://realm/service", 30).await?;
// process request
engine.publish(reply_route, id.clone(), response_body).await?;
// acknowledge (consume) the reserved request using the delivery token to remove it from the in-memory queue
// (API name may vary; call the engine/store consume/ack method with route, id, and delivery token)
```

- Worker pattern (hybrid signal + reserve):
```rust
// 1) Subscribe to rpc://realm/service to receive tiny notifications (no body)
// 2) On notification, immediately call reserve to pull full request and obtain delivery token
// 3) Process and publish responses to TAG_ROUTE_REPLY with the same TAG_ID (use TAG_SEQ for streams; TAG_STREAM_END on last)
// 4) Ack (consume) using the delivery token
```

## Error model
- RpcError::Timeout
- RpcError::NotFound
- RpcError::PermissionDenied
- RpcError::Backpressure
- RpcError::InvalidToken

## Tests
- Simple RPC: client rpc_call -> worker reserves -> worker publishes response -> client receives
- Streaming RPC: ensure responses carry TAG_SEQ and final response has TAG_STREAM_END
- Hybrid signal + reserve: engine emits signal; multiple workers race to reserve; only one wins; worker replies; client receives
- Timeout: no response within timeout -> RpcError::Timeout
- Worker crash: worker reserves and dies before responding -> client times out; depending on lease, request may be redelivered

## Decision guidance
- Start with Option A (reply-queue) or A2 (hybrid signal + reserve) for multi-worker services.
- After implementing Publish->notify and queue semantics, add an ergonomic rpc_call helper that manages reply route lifecycle.
- Optionally implement Option B later if you need lower-latency direct replies.

---

End of RPC spec.
