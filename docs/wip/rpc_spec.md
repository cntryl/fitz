Excellent — that’s a good addition, and it should be made explicit in the spec so implementers don’t improvise route formats.
Here’s the **cleaned-up version** of the earlier spec incorporating your canonical RPC route shape:

---

# **RPC Specification**

This document defines the **Remote Procedure Call (RPC)** subsystem for Fitz — including TLV format, routing conventions, reply-routing modes, hybrid worker selection, flow-control semantics, and test coverage expectations.

---

## **1. Goals**

- Support low-latency, in-memory request/response with strong correlation semantics.
- Reuse core Fitz primitives (`Publish`, `Subscribe`, `Reserve`, `Consume`).
- Provide both simple reply-queue and hybrid signal+reserve models.
- Enable streaming responses with proper sequencing and end-of-stream signaling.
- Keep all RPCs ephemeral — never persisted to disk.

> **Note:** RPC messages are **ephemeral**. For durable semantics, use queues (see `docs/queue_spec.md`).

---

## **2. Route Format**

All RPC routes follow the canonical structure:

```
rpc://{realm}/{area}/{resource}/{operation}
```

| Segment     | Description                                        |
| ----------- | -------------------------------------------------- |
| `realm`     | Logical tenant or namespace                        |
| `area`      | Functional subsystem (e.g. auth, compute, storage) |
| `resource`  | Entity or service name                             |
| `operation` | Specific callable action or method                 |

Examples:

- `rpc://acme/auth/user/create`
- `rpc://acme/inventory/item/update`
- `rpc://cntryl/analytics/query/run`

---

## **3. TLV and Correlation Conventions**

| Tag                                  | Description                                |
| ------------------------------------ | ------------------------------------------ |
| `TAG_ROUTE`                          | RPC request or reply route                 |
| `TAG_BODY`                           | Request or response payload                |
| `TAG_ID`                             | Correlation ID (must be echoed in replies) |
| `TAG_PROTOCOL_VERSION`               | Optional per-call version hint             |
| `TAG_ROUTE_REPLY` / `TAG_REPLY_HINT` | Reply route or direct reply token          |
| `TAG_SEQ`                            | Sequence number for streaming responses    |
| `TAG_STREAM_END`                     | Empty tag marking end of stream            |

---

## **4. Reply-Routing Modes**

### **A. Reply Queue (Baseline)**

1. Client creates a reply route: `rpc/reply/<client-id>`.
2. Subscribes or reserves on that route.
3. Publishes a request to `rpc://realm/area/resource/operation` with:

   - `TAG_ID`
   - `TAG_ROUTE_REPLY`

4. Worker consumes the request, processes it, and publishes replies to the specified reply route.
5. Client matches replies by `TAG_ID`, ordering by `TAG_SEQ` until `TAG_STREAM_END`.

**Pros:** Simple, transport-agnostic, ideal for most RPCs.
**Cons:** Requires one per-client reply route.

---

### **A2. Hybrid Signal + Reserve (Recommended for Multi-Worker Services)**

1. Client publishes request to `rpc://realm/area/resource/operation` with `TAG_ID` and `TAG_ROUTE_REPLY`.
   The body is stored in-memory (not broadcast).
2. Broker emits a **lightweight signal** to all subscribers:

   - `TAG_NOTIFICATION`, `TAG_ROUTE`, `TAG_ID`, `TAG_ROUTE_REPLY`
   - **No** `TAG_BODY`

3. Workers receiving the signal immediately call:

   ```rust
   reserve("rpc://realm/area/resource/operation", lease_secs)
   ```

4. Broker grants a lease to **one worker**, returning `(TAG_ID, TAG_BODY, TAG_DELIVERY_TOKEN)`.
5. Worker processes, publishes replies, and acknowledges the delivery token.

**Pros:**

- Exactly-one responder
- Minimal network fanout
- Natural backpressure and redelivery via lease expiry

---

### **B. Direct Transport Reply (Advanced)**

Replies are routed directly via the originating connection when a `TAG_REPLY_HINT` is present.

**Pros:** Lowest latency
**Cons:** Requires engine-level mapping and complex reconnection semantics

---

## **5. Flow Control**

- RPC queues are **bounded, memory-only**.
- Exceeding broker limits returns a `RpcError::Backpressure`.
- Clients must throttle or retry with jitter.
- Workers must ack promptly to release capacity.

---

## **6. Client and Worker Patterns**

### **Client Helper**

```rust
async fn rpc_call(engine: &EngineHandle, route: &str, body: &[u8], timeout: Duration)
    -> Result<Vec<u8>, RpcError> {
    // 1. Create or reuse reply route
    // 2. Subscribe/reserve
    // 3. Generate correlation ID
    // 4. Publish with TAG_ROUTE_REPLY
    // 5. Await matching TAG_ID reply
}
```

### **Streaming Helper**

```rust
async fn rpc_call_stream(engine: &EngineHandle, route: &str, body: &[u8], timeout: Duration)
    -> Result<impl Stream<Item = Vec<u8>>, RpcError> {
    // Yield chunks ordered by TAG_SEQ until TAG_STREAM_END
}
```

### **Worker (Queue Mode)**

```rust
let (id, body, token) = engine.reserve("rpc://realm/area/resource/operation", 30).await?;
engine.publish(reply_route, id.clone(), response).await?;
engine.ack("rpc://realm/area/resource/operation", token).await?;
```

### **Worker (Signal + Reserve)**

```rust
// 1. Subscribe to rpc://realm/area/resource/operation
// 2. On signal, reserve() to claim
// 3. Publish reply (TAG_ID, TAG_SEQ, TAG_STREAM_END)
// 4. Ack using token
```

---

## **7. Error Model**

| Error              | Description                |
| ------------------ | -------------------------- |
| `Timeout`          | No reply within timeout    |
| `NotFound`         | No matching route          |
| `PermissionDenied` | Unauthorized route access  |
| `Backpressure`     | Broker memory cap hit      |
| `InvalidToken`     | Bad or expired lease token |

---

## **8. Test Matrix (Summary)**

- **Basic RPC:** end-to-end single call
- **Streaming:** ordered chunks + stream termination
- **Inbox Lifecycle:** create, secure, cleanup
- **Concurrency:** isolation by correlation ID
- **Error Handling:** invalid route, timeout, crash recovery
- **Large Payloads:** multi-MB body handling
- **Load Balancing:** ensure single worker wins per call
- **Idempotency:** deduplication via TAG_ID

---

## **9. Implementation Roadmap**

1. Implement `RpcDomain::handle()` for TLV parsing and routing.
2. Add bounded in-memory queues per RPC route.
3. Integrate Notice-based signaling for hybrid dispatch.
4. Implement `rpc_call()` and streaming helpers.
5. Enforce inbox auth and route permissions.
6. Add optional direct-transport reply optimization.

---

## **10. Design Principles**

- Stateless at edges — all state is lease-scoped.
- Predictable latency — no disk I/O.
- Extensible TLV schema for forward compatibility.
- Shared primitives across Notice, Queue, and RPC domains.
- Backpressure > failure — graceful degradation under load.

---

Would you like me to add a **route schema diagram** (showing realm/area/resource/operation hierarchy + reply route) as a companion SVG in `docs/rpc_route_structure.svg`? It helps visually unify how RPC and Notice routes align.

## Test Inventory (48 tests)

### Basic RPC (3 tests)

- ✅ `should_deliver_rpc_request_to_handler`
- ✅ `should_deliver_reply_to_specified_reply_route`
- ✅ `should_correlate_reply_with_request_id`

### Inbox Management (12 tests)

- ✅ `should_allocate_inbox_when_reply_route_omitted`
- ✅ `should_generate_cryptographically_secure_inbox_routes`
- ✅ `should_prevent_inbox_route_collision`
- ✅ `should_prevent_unauthorized_inbox_subscription`
- ✅ `should_allow_owner_to_receive_on_inbox`
- ✅ `should_isolate_inbox_from_other_sessions`
- ✅ `should_reject_unauthorized_inbox_publish`
- ✅ `should_prevent_delivery_from_unauthorized_sender`
- ✅ `should_allow_handler_to_publish_to_reply_inbox`
- ✅ `should_deliver_handler_reply_to_client`
- ✅ `should_prevent_inbox_access_after_session_ends`
- ✅ `should_cleanup_allocated_inboxes_after_session_close`

### Streaming Responses (4 tests)

- ✅ `should_deliver_streaming_rpc_responses_in_order`
- ✅ `should_mark_end_of_stream_with_stream_end_tag`
- ✅ `should_handle_multiple_chunks_in_streaming_response`
- ✅ `should_stream_large_response_in_chunks`

### Concurrency (2 tests)

- ✅ `should_handle_concurrent_rpc_calls`
- ✅ `should_isolate_replies_by_correlation_id`

### RPC Client (3 tests)

- ✅ `should_use_rpc_client_for_call_stream`
- ✅ `should_manage_reply_route_subscription_automatically`
- ✅ (client wrapper tests)

### Error Handling (9 tests)

- ✅ `should_handle_rpc_request_when_no_handler_subscribed`
- ✅ `should_timeout_when_no_reply_received`
- ✅ `should_reject_rpc_to_invalid_route`
- ✅ `should_reject_reply_without_correlation_id`
- ✅ `should_handle_out_of_order_sequence_numbers`
- ✅ `should_handle_missing_sequence_number`
- ✅ `should_propagate_application_errors_in_reply`
- ✅ `should_handle_handler_crash_during_request_processing`
- ✅ (various error modes)

### Custom Configuration (3 tests)

- ✅ `should_support_custom_inbox_reply_routes`
- ✅ `should_respect_client_specified_timeout`
- ✅ `should_use_default_timeout_when_not_specified`

### Large Payloads (2 tests)

- ✅ `should_handle_large_rpc_request_payload`
- ✅ `should_handle_large_rpc_reply_payload`

### Load Balancing (2 tests)

- ✅ `should_distribute_requests_across_multiple_handlers`
- ✅ `should_ensure_single_handler_receives_each_request`

### Cancellation & Idempotency (4 tests)

- ✅ `should_support_request_cancellation`
- ✅ `should_not_deliver_reply_after_cancellation`
- ✅ `should_support_idempotent_request_ids`
- ✅ `should_deduplicate_requests_by_id`

## Implementation Status

- **Total Tests**: 48
- **Passing**: 0 (domain handler stubbed with panic!)
- **Blocked**: All tests blocked on domain implementation

## Special Considerations

- RPC requires coordination with notice domain for subscriptions
- Inbox lifecycle tied to session/channel cleanup
- Security critical: inbox authorization must be enforced

## Next Steps

1. Implement RpcDomain::handle() to parse TLV and route to operations
2. Integrate with Router for pub/sub mechanics
3. Implement inbox security model
4. Update tests to work with new architecture
