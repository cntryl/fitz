# Queue Domain Specification (v2 — Actor Model MVP)

**Version:** 2.0  
**Status:** MVP Specification  
**Durability:** Durable (backed by Midge)  
**Last Updated:** December 11, 2025

---

# 1. Overview

The Fitz Queue domain provides a **durable, actor-driven, at-least-once message queue** with:

* fast in-memory scheduling
* durable message persistence via Midge
* visibility-timeout semantics
* redelivery after expiration
* single-node exclusivity
* zero lock contention
* microsecond-range dequeue latency

This is the Fitz drop-in equivalent for **SQS / Rabbit / Azure Queue**, but drastically faster because:

* the QueueActor owns all queue state
* Midge provides durable append + delete
* operations require no distributed coordination

---

# 2. Route Format

```
queue://{realm}/{area}/{resource}/{operation}
```

Examples:

```
queue://acme/jobs/thumbnail/enqueue
queue://acme/payments/refund/reserve
queue://acme/orders/pick/complete
```

---

# 3. Actor Model Architecture

Each physical queue is represented by **one QueueActor**.

It contains **in-memory state**:

```rust
struct QueueActor {
    ready: VecDeque<MessageId>,            // Message IDs ready for delivery
    inflight: HashMap<MessageId, Inflight>,// Active leases
    timers: MinHeap<LeaseExpiry>,          // Expiry scheduling
    store: MidgeHandle,                    // Durable storage
}
```

### Inflight entry:

```rust
struct Inflight {
    token: u64,        // random u64, actor-generated
    expires_at: Instant,
}
```

No HMAC.  
No external crypto.  
Just a random u64 owned by the actor.

---

# 4. Core Operations

## 4.1 Enqueue

**Route:**
`queue://{realm}/{area}/{resource}/enqueue`

**Request:**
`TAG_BODY` → message payload

**Behavior:**

* Writes message to Midge (append)
* Pushes message ID into `ready` queue
* Returns message ID

**Response:**
`TAG_ID` = message ID

---

## 4.2 Reserve (Lease)

**Route:**
`queue://{realm}/{area}/{resource}/reserve`

**Request:**

* TAG_LEASE = seconds
* TAG_BATCH_SIZE (optional)

**Behavior:**

* Pops N messages from `ready`
* Creates inflight entries
* Schedules expiry timers
* Returns (id, body, token) for each leased message

**Response:**
For each message:
`TAG_ID`, `TAG_BODY`, `TAG_DELIVERY_TOKEN`, `TAG_LEASE`

---

## 4.3 Extend (Lease Extension)

**Route:**
`queue://{realm}/{area}/{resource}/extend`

**Request:**
`TAG_ID`, `TAG_DELIVERY_TOKEN`, `TAG_LEASE`

**Behavior:**

* Validates inflight token
* Extends expiry
* Updates timer

**Response:**
`status = ok`

---

## 4.4 Complete (Acknowledge)

**Route:**
`queue://{realm}/{area}/{resource}/complete`

**Request:**
`TAG_ID`, `TAG_DELIVERY_TOKEN`

**Behavior:**

* Validates token
* Removes from inflight
* Deletes durable record from Midge
* Removes timer entry

**Response:**
`status = ok`

---

## 4.5 Expiration (Actor-internal)

When a timer fires:

* Lease is expired
* Inflight entry removed
* Message ID reinserted into `ready`
* Redelivery count incremented

This requires **zero external coordination**.

---

## 4.6 Peek (Optional MVP)

Return the next visible message **without leasing**.

---

# 5. Data Model (MVP)

### Message stored in Midge:

```rust
struct QueueRecord {
    body: Bytes,
    attempts: u32,
}
```

### In-memory scheduling state (actor-owned):

```
ready: VecDeque<MessageId>
inflight: HashMap<MessageId, Inflight>
timers: MinHeap<LeaseExpiry>
```

**No HMAC delivery tokens.**  
**No metadata blobs.**  
**No lease_until fields persisted to disk.**  
Only the QueueActor tracks lease state.

---

# 6. Removed From MVP (Important)

The following v1-style features are **removed** because actor-based design eliminates their need:

❌ Dead Letter Queue (DLQ) as built-in routing (can be added later)  
❌ Deduplication keys  
❌ HMAC delivery tokens  
❌ Global admin listing APIs  
❌ Multi-node coordination  
❌ Peek + metadata inspection beyond basics  
❌ Complex storage interfaces  
❌ Backpressure errors (actor mailboxes provide backpressure naturally)

This preserves the blazing-fast, actor-model queue.

---

# 7. Error Codes (Simplified)

| Code                | Meaning                           |
| ------------------- | --------------------------------- |
| QUEUE_INVALID_TOKEN | Token mismatch                     |
| QUEUE_LEASE_EXPIRED | Lease expired before operation     |
| QUEUE_NOT_FOUND     | Message vanished or already acked |
| QUEUE_BAD_REQUEST   | Malformed input                    |

All error semantics are single-node and deterministic.

---

# 8. Observability

Metrics:

* `queue_enqueue_total`
* `queue_lease_total`
* `queue_ack_total`
* `queue_retry_total`
* `queue_depth_gauge`
* `queue_inflight_gauge`

Logs:

* enqueue
* lease
* expire
* ack

---

# 9. Testing Requirements (Actor Model)

### Unit

* enqueue → reserve → complete
* reserve after empty → returns none
* lease extension updates expiry
* expiry returns to ready queue
* token mismatch

### Integration

* Restart durability (Midge persists messages)
* High-volume enqueue
* High-volume reserve with batch sizes
* Concurrency with multiple workers

### Performance

* target: 200k–1M msg/sec enqueue on local machine
* reserve latency < 10µs
* ack latency < 5µs

---

# 10. Usage Patterns

## Worker Loop

```rust
loop {
    let msgs = client.reserve("queue://acme/jobs/process", 30, 10).await?;

    for msg in msgs {
        if process(msg.body).await {
            client.complete(msg.id, msg.token).await?;
        }
        // else let it expire for redelivery
    }
}
```

## Long-running tasks (with extension)

```rust
let lease = client.reserve(route, 300, 1).await?;
while !task_done {
    sleep(60);
    client.extend(route, lease.id, lease.token, 300).await?;
}
client.complete(route, lease.id, lease.token).await?;
```

---

# ⭐ FINAL SUMMARY

Here is what Fitz Queue **v2 (Actor Model)** truly is:

> A **single-node, actor-scheduled, durable message queue** that uses Midge as its persistent backend and maintains all lease & visibility state in-memory for microsecond-level performance.

No HMAC tokens.  
No remote coordination.  
No distributed consistency.  
No heavyweight features that compromise throughput.  
Just a **world-class fast local queue**.

---

If you want, next I can produce:

* **Stream Domain Specification (v2)**
* **Notice Domain Specification (v2)**
* **RPC Domain Specification (v2)**

Just say **“next domain: stream”** or whichever domain you want next.
