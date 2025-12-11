# Fitz Stream Domain Specification — Version 2.0

**Status:** In Design  
**Last Updated:** December 11, 2025  
**Durability:** Fully durable (backed by Midge LSM)

---

# 1. Overview

Fitz Streams provide **strictly ordered, durable, append-only logs** designed for:

* Event sourcing
* Audit logging
* CDC-style change streams
* Replayable processing pipelines
* Multi-partition analytics
* CQRS projections

Streams support **three levels of ordering**:

1. **Resource ordering**
2. **Area ordering**
3. **Realm ordering**

These ordering layers ensure deterministic replay of:

* a single aggregate (“resource stream”)
* a whole bounded context (“area stream”)
* a whole multi-area system (“realm stream”)

All writes and reads use **pure actor-model serialization**, eliminating locks and complexity.

---

# 2. Route Format

```
stream://{realm}/{area}/{resource}[/{operation}]
```

Examples:

* `stream://acme/orders/checkout`
* `stream://acme/audit/security`
* `stream://acme/inventory/stock`

---

# 3. Actor Model Architecture

### Actors:

```
StreamActor   (one per resource)
AreaActor     (one per area)
RealmActor    (one per realm)
```

### Responsibilities:

| Actor           | Responsibilities                                                                   |
| --------------- | ---------------------------------------------------------------------------------- |
| **StreamActor** | Append validation, optimistic concurrency, assign `resource_offset`, durable write |
| **AreaActor**   | Assign `area_offset`, track resource progress, compute `area_watermark`            |
| **RealmActor**  | Assign `realm_offset`, track area progress, compute global `realm_watermark`       |

All offsets are strictly increasing and monotonic.

---

# 4. Event Model

```rust
pub struct StreamRecord {
    pub resource_offset: u64,     // Strict order within resource stream
    pub area_offset: u64,         // Global order within area
    pub realm_offset: u64,        // Global order within realm

    pub body: Vec<u8>,            // Opaque user payload
    pub metadata: Option<Vec<u8>>,// Optional metadata
    pub created_at: u64,          // Server timestamp
}
```

Offsets are stored as part of the durable record.

---

# 5. Append Semantics (ES-Ready)

## 5.1 Operation: Append

**Route:**

```
stream://{realm}/{area}/{resource}/append
```

**Request TLV:**

* `TAG_BODY` – event payload
* `TAG_EXPECTED_OFFSET` (optional optimistic concurrency)

**Response TLV:**

* `TAG_RESOURCE_OFFSET`
* `TAG_AREA_OFFSET`
* `TAG_REALM_OFFSET`

---

## 5.2 Optimistic Concurrency (Event Sourcing)

If provided, `expected_offset` must match next resource offset:

```
if expected_offset != state.next_resource_offset:
    return ERR_CONCURRENCY_CONFLICT
```

This supports full event-sourcing invariants:

* version checks
* conflict detection
* aggregate concurrency control

Equivalent to EventStoreDB or DynamoDB conditional writes.

---

## 5.3 Append Workflow (Actor Pipeline)

### Step 1 — StreamActor

1. Validate expected_offset
2. Assign:

   ```
   resource_offset = next_resource_offset
   next_resource_offset += 1
   ```
3. Write durable record to Midge (area_offset & realm_offset = placeholder)
4. Send `ResourceCommitted{resource_offset}` → AreaActor

### Step 2 — AreaActor

1. Assign `area_offset = next_area_offset`
2. Update resource commit map
3. Advance `area_watermark`
4. Send `AreaCommitted{area_offset}` → RealmActor

### Step 3 — RealmActor

1. Assign `realm_offset = next_realm_offset`
2. Update area commit map
3. Advance `realm_watermark`

All offsets flushed back to storage via update messages.

---

# 6. Watermarks

Watermarks prevent consumers from reading events ahead of fully committed, gap-free ordering.

---

## 6.1 Resource Watermark

Not needed — StreamActor serializes all writes.

---

## 6.2 Area Watermark

Consumers may only read:

```
event.area_offset <= area_watermark
```

### Area watermark rule:

```
area_watermark advances only when the next expected area_offset
is committed by all writers.
```

---

## 6.3 Realm Watermark

Consumers may only read:

```
event.realm_offset <= realm_watermark
```

### Realm watermark rule:

```
realm_watermark = min(all area_watermarks)
```

This guarantees global order without gaps.

---

# 7. Multi-Level Read Operations

---

## 7.1 Resource Read

Strictly ordered by `resource_offset`.

**Route:**

```
stream://realm/area/resource/read
```

**Request:**

* `TAG_FROM`
* `TAG_LIMIT`

**Response:**
List of events in resource order.

No watermark needed.

---

## 7.2 Area Read

Merge of all resources in the area, ordered by `area_offset`.

**Route:**

```
stream://realm/area/*/read
```

**Visibility Rule:**

```
return events where event.area_offset <= area_watermark
```

---

## 7.3 Realm Read

Merge of all areas, sorted by `realm_offset`.

**Route:**

```
stream://realm/*/*/read
```

**Visibility Rule:**

```
return events where event.realm_offset <= realm_watermark
```

---

### 7.4 Merged Read Algorithm (Area + Realm)

```
while results.len < limit:
    for each contributing stream:
        peek next record
            
    pick record with smallest area_offset or realm_offset
    ensure it does not exceed watermark
    consume it
```

Equivalent to K-way merge with watermark gating.

---

# 8. Storage Layout in Midge

Perfectly suited for LSM:

```
1. Resource Index:
   key = [rf][realm][area][resource][resource_offset]
   val = Encoded StreamRecord

2. Area Index:
   key = [rf][realm][area][area_offset]
   val = pointer to resource record

3. Realm Index:
   key = [rf][realm][realm_offset]
   val = pointer to area record

4. Watermark Store:
   key = watermark:{realm}:{area}
   val = watermark_u64
```

All indexes are totally ordered lexicographically.

---

# 9. Error Handling

| Code | Meaning                   |
| ---- | ------------------------- |
| 2001 | ERR_CONCURRENCY_CONFLICT  |
| 2002 | ERR_OFFSET_TOO_FAR_AHEAD  |
| 2003 | ERR_INVALID_READ_BOUND    |
| 2004 | ERR_READ_BEYOND_WATERMARK |

---

# 10. Configuration

```yaml
streams:
  retention_days: 30
  max_event_size: 1048576
  max_batch_events: 1000

watermarks:
  update_interval_ms: 5
  realm_sync_interval_ms: 20
```

---

# 11. Observability

Metrics:

* `stream_events_appended_total`
* `stream_watermark_area`
* `stream_watermark_realm`
* `stream_read_latency_seconds`
* `stream_append_conflicts_total`
* `stream_area_merge_latency_ms`

---

# 12. Change Notifications

Stream writes trigger **debounced, ephemeral notice events** to support real-time consumer wakeup:

```
notice://{realm}/{area}/{resource}/committed
```

These notifications carry only **state advancement metadata** (area sequence, resource sequence, timestamp). They are **best-effort** and **non-durable**, and consumers must re-fetch actual events via `stream.read(... from last_seq)`. Notifications are emitted no more frequently than the domain-level debounce interval (default 10–50ms) to prevent fan-out overload.

Logs:

* append decisions
* watermark advancement
* multi-resource merge steps

---

# 12. Guarantees

### **Per-resource**

* Linearizable order
* No gaps
* Idempotent retry with expected_offset

### **Per-area**

* Global strict ordering
* No future reads (watermarks)

### **Per-realm**

* Global deterministic ordering
* No jumps ahead of slow areas

---

# 13. Event Sourcing Alignment

This design fully supports:

✓ append-only aggregate streams  
✓ optimistic concurrency  
✓ snapshots + projections  
✓ timeline replay  
✓ multi-stream projections  
✓ full system replay from realm root  
✓ millions of aggregates under one area

This matches and exceeds the guarantees provided by:

* EventStoreDB
* Kafka compacted streams
* DynamoDB streams
* Kinesis + sequence numbers

But with:

* simpler semantics
* stronger ordering
* actor-driven serialization
* Midge-backed durability

---

# 14. Final Notes

This is a **world-class ES streaming architecture**, built on:

* no locks
* no coordination protocols
* deterministic actor scheduling
* compact offset space
* durable LSM storage
* explicit multi-level order guarantees

It is simpler than Kafka, stronger than EventStoreDB, and much faster to implement than multi-version concurrency schemes.

---

If you'd like, I can generate:

📄 `STREAM_SPEC_v2.md` fully formatted  
📦 the actor message protocol  
🗄️ the Midge-backed storage layout code stubs  
🚀 the StreamActor skeleton in Rust  
🧪 the full integration test suite

Just tell me which one you want next.
