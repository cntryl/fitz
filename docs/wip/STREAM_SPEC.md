# Stream Domain Specification

**Version:** 1.0  
**Status:** Implementation in Progress  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz Streams provide ordered, append-only event logs with strict consistency guarantees. Streams are designed for event sourcing, audit logging, and any scenario requiring durable, ordered message sequences with replay capabilities.

### Key Features

- **Client-controlled sequences**: Producers specify explicit resource sequences for idempotency
- **Dual-index storage**: Events stored in both resource-specific and area-wide indexes
- **Strict ordering guarantees**: Watermark-based visibility ensures no out-of-order reads
- **Concurrent batch appends**: Multiple producers can write without blocking each other
- **Transaction semantics**: Explicit begin/append/commit/rollback lifecycle
- **Resume and replay**: Clients can resume from any sequence number within retention

### Differences from Queues

| Feature | Streams | Queues |
|---------|--------|---------|
| Persistence | Append-only, replayable | Consumed and removed |
| Ordering | Strict sequence numbers | Best-effort ordering |
| Consumption | Multiple readers, non-destructive | Single consumer, destructive |
| Use Cases | Event sourcing, audit logs | Task distribution, work queues |

---

## Route Format

Stream routes follow the standard Fitz format:

```
stream://{realm}/{area}/{resource}[/{operation}]
```

### Examples
- `stream://acme/orders/events` - Order events stream
- `stream://acme/audit/security` - Security audit log
- `stream://acme/metrics/system` - System metrics stream

---

## Core Operations

### 1. Begin Append (Transaction Start)

**Route Operation:** `stream://{realm}/{area}/{resource}/begin`  
**TLV Tags:** `TAG_ROUTE`

**Behavior:**
- Allocates transaction ID and reserves initial sequence range
- Returns transaction handle for subsequent appends
- No events are visible until commit

**Response TLV:** `TAG_ID` (transaction ID)

### 2. Append Event (Within Transaction)

**Route Operation:** `stream://{realm}/{area}/{resource}/append`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (transaction), `TAG_BODY`, `TAG_SEQ` (resource sequence), `TAG_STREAM_END` (optional)

**Behavior:**
- Appends event to transaction with client-provided resource sequence
- Validates sequence continuity (no gaps)
- Events written immediately but not visible until commit

**Response TLV:** Success acknowledgment

### 3. Commit Append (Transaction Complete)

**Route Operation:** `stream://{realm}/{area}/{resource}/commit`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (transaction)

**Behavior:**
- Makes all transaction events visible atomically
- Advances area watermark to highest contiguous sequence
- Returns commit statistics (first/last sequences, event count)

**Response TLV:** `TAG_SEQ` (first), `TAG_SEQ` (last), event count

### 4. Rollback Append (Transaction Abort)

**Route Operation:** `stream://{realm}/{area}/{resource}/rollback`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (transaction)

**Behavior:**
- Discards all transaction events
- Releases reserved sequences
- No events become visible

**Response TLV:** Success acknowledgment

### 5. Read Events (Replay)

**Route Operation:** `stream://{realm}/{area}/{resource}/read`  
**TLV Tags:** `TAG_ROUTE`, `TAG_SEQ` (from), `TAG_LIMIT` (optional)

**Behavior:**
- Returns events starting from specified sequence
- Respects watermark (only committed events visible)
- Supports pagination with limit

**Response TLV:** Multiple `TAG_SEQ`, `TAG_BODY`, `TAG_STREAM_END`

### 6. Peek Latest (Inspect Head)

**Route Operation:** `stream://{realm}/{area}/{resource}/peek`  
**TLV Tags:** `TAG_ROUTE`

**Behavior:**
- Returns most recent committed event
- Non-destructive read operation
- Useful for monitoring and debugging

**Response TLV:** `TAG_SEQ`, `TAG_BODY`

### 7. Consume Area (Hierarchical Read)

**Route Operation:** `stream://{realm}/{area}/consume`  
**TLV Tags:** `TAG_ROUTE` (area prefix), `TAG_SEQ` (from), `TAG_LIMIT` (optional)

**Behavior:**
- Reads across all resources in area, ordered by timestamp then sequence
- Merges multiple resource streams deterministically
- Enables area-wide event processing

**Response TLV:** Multiple events with `TAG_SEQ`, `TAG_BODY`, `TAG_ROUTE`

---

## Data Model

### StreamEvent Structure

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    // Identity (resource_seq is the idempotency key)
    pub sequence: u64,              // Client-controlled resource_seq
    pub resource: String,           // Resource name (last segment of route)

    // Server-assigned (set during append)
    pub area_seq: Option<u64>,      // Global ordering within area

    // Content
    pub body: Vec<u8>,              // Event payload (opaque bytes)
    pub metadata: Option<Vec<u8>>,  // Optional metadata (CBOR/JSON)

    // Lifecycle
    pub is_end: bool,               // Stream completion marker
    pub created_at: u64,            // Unix timestamp (seconds)
}
```

### ActiveTransaction Structure

```rust
#[derive(Debug, Clone)]
struct ActiveTransaction {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub transaction_id: String,         // Unique transaction identifier
    pub first_area_seq: u64,           // Server-assigned starting area_seq
    pub event_count: u32,              // Number of events in transaction
    pub created_at: u64,               // Transaction start time
}
```

---

## Sequence Management

### Client-Controlled Resource Sequences

Producers provide explicit `resource_seq` for each event:

- **Idempotency**: Retry with same `(route, resource_seq, body)` is safe
- **Gap Detection**: Server rejects gaps (e.g., seq 0→2 without seq 1)
- **No Overwrites**: Cannot replace existing sequence with different content
- **Monotonic**: Sequences must start at 0 and increment by 1

```rust
// Example: Producer controls resource_seq explicitly
let event = StreamEvent {
    sequence: 0,          // Client-provided resource_seq
    resource: "batch_123".to_string(),
    body: b"event data".to_vec(),
    is_end: false,
    // ...
};

stream.append_event(txn_id, route_family, event).await?;
// → Event stored with resource_seq=0, server assigns area_seq
```

### Server-Assigned Area Sequences

The server assigns global `area_seq` for area-wide ordering:

- **Monotonic**: Strictly increasing per (realm, area)
- **Deterministic**: No timestamp ambiguity
- **Dual-Indexed**: Same event stored in both resource and area indexes

```rust
// Server assigns area_seq atomically during commit
area_states["payments"]["transactions"].next_seq = 1000
→ Increments to 1001, 1002, 1003...
```

---

## Transaction Lifecycle

All appends require explicit transaction lifecycle:

### Phase 1: Begin
```rust
let txn_id = stream.begin_append(realm, area, resource).await?;
// → Creates ActiveTransaction with reserved sequence range
```

### Phase 2: Append
```rust
stream.append_event(txn_id, event1).await?;
stream.append_event(txn_id, event2).await?;
// → Events written to storage but not visible
// → Sequences validated for continuity
```

### Phase 3: Commit or Rollback
```rust
// Commit: Make events visible and advance watermark
let (first_seq, last_seq, count) = stream.commit_append(txn_id).await?;

// OR Rollback: Discard all events
stream.rollback_append(txn_id).await?;
```

**Key Properties:**
- Single resource per transaction (enforced)
- Events written immediately (no buffering)
- Watermark advances only on commit
- Rollback clears reservations but leaves orphaned data

---

## Concurrency Control

### Reserve-Then-Commit Pattern

The core challenge with concurrent batch appends:

```
Timeline without coordination:
T0: Producer A starts batch (100 events) → writes area_seq [1000..1100]
T1: Producer B starts batch (1 event)    → writes area_seq [1100..1101]
T2: Producer B commits (fast)            → area_seq 1100 visible
T3: Consumer reads from 1000             → GAP! [1000..1100] not committed yet
T4: Producer A commits (slow)            → [1000..1100] now visible
```

**Solution:** Reserve sequences before writing, advance watermark only after commit:

```rust
#[derive(Debug, Default)]
struct AreaStreamState {
    next_seq: u64,                                    // Next sequence to allocate
    watermark: u64,                                   // Highest contiguous committed seq
    reserved_ranges: BTreeMap<u64, ReservationStatus>,  // Pending commits
}

#[derive(Debug, Clone, Copy)]
enum ReservationStatus {
    Reserved,    // Allocated but not committed
    Committed,   // Written and committed
}
```

### Watermark Advancement Logic

```rust
// After marking sequences as Committed:
let mut new_watermark = area_state.watermark;

// Scan from current watermark
loop {
    if let Some(status) = reserved_ranges.get(&scan_seq) {
        if matches!(status, ReservationStatus::Committed) {
            reserved_ranges.remove(&scan_seq);  // Clean up
            highest_committed = scan_seq;
            scan_seq += 1;
        } else {
            break;  // Hit Reserved (uncommitted), stop
        }
    } else {
        break;  // No more reservations
    }
}

area_state.watermark = highest_committed;
```

---

## Storage Architecture

### Dual Indexing Strategy

Streams maintain two indexes for optimal access patterns:

#### Resource Index
```
Key: 0x01 0x01 {rf} {realm} {area} {resource} {resource_seq}
Val: Encoded StreamEvent
```
- Enables resource-specific reads and replay
- Supports sequence validation and gap detection
- Optimized for single-resource consumers

#### Area Index
```
Key: 0x01 0x02 {rf} {realm} {area} {area_seq}
Val: Encoded StreamEvent
```
- Enables area-wide consumption and ordering
- Supports hierarchical merging across resources
- Optimized for multi-resource processors

#### Watermark Index
```
Key: 0x01 0x03 {rf} {realm} {area}
Val: u64 (watermark)
```
- Tracks highest contiguous committed sequence
- Enables efficient visibility queries
- Supports resumable consumption

---

## Consumption Patterns

### Resource-Specific Reading

```rust
// Read events for specific resource from sequence 100
let events = stream.read_resource("acme/orders/events", 100, 50).await?;
// → Returns events [100..149] for orders/events resource
```

### Area-Wide Consumption

```rust
// Consume across all resources in orders area
let events = stream.consume_area("acme/orders", 1000, 100).await?;
// → Returns merged events from orders/* resources
// → Ordered by (timestamp, area_seq, resource)
```

### Monitoring and Debugging

```rust
// Peek at latest event
let latest = stream.peek_resource("acme/orders/events").await?;
// → Returns most recent committed event without advancing cursor
```

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 2001 | ERR_SEQUENCE_GAP | Missing sequence in resource stream | Check client sequence generation |
| 2002 | ERR_SEQUENCE_CONFLICT | Sequence already exists with different content | Use different sequence or check idempotency |
| 2003 | ERR_TRANSACTION_NOT_FOUND | Invalid transaction ID | Begin new transaction |
| 2004 | ERR_TRANSACTION_EXPIRED | Transaction timed out | Begin new transaction |
| 2005 | ERR_SEQUENCE_OUT_OF_RANGE | Sequence too far ahead | Check resource sequence continuity |
| 2006 | ERR_WATERMARK_NOT_REACHED | Reading ahead of committed watermark | Wait for commits or use lower sequence |

### Sequence Validation

**Gap Detection:**
```rust
// When appending sequence N to resource
if let Some(last_seq) = get_last_sequence(resource) {
    if sequence != last_seq + 1 {
        return Err(SequenceGap { expected: last_seq + 1, got: sequence });
    }
} else if sequence != 0 {
    return Err(SequenceGap { expected: 0, got: sequence });
}
```

**Idempotency:**
```rust
// Check for exact duplicate (route, sequence, body)
if let Some(existing) = get_existing_event(route, sequence) {
    if existing.body == new_event.body {
        return Ok(existing.area_seq); // Idempotent success
    } else {
        return Err(SequenceConflict);
    }
}
```

---

## Configuration

### Stream-Level Settings

```yaml
streams:
  # Realm-level defaults
  "stream://acme/**":
    retention_days: 30
    max_transaction_events: 1000
    transaction_timeout_seconds: 300

  # Area-specific overrides
  "stream://acme/orders/**":
    retention_days: 90
    max_transaction_events: 5000

  # Stream-specific settings
  "stream://acme/audit/security":
    retention_days: 365
    max_transaction_events: 100
```

### Global Limits

```yaml
limits:
  max_event_size: 1048576           # 1MB per event
  max_transaction_events: 10000     # Events per transaction
  transaction_timeout_seconds: 600  # 10 minutes
  max_concurrent_transactions: 100  # Per area
```

---

## Observability

### Metrics

- `stream_events_appended_total{route}`
- `stream_transactions_started_total{area}`
- `stream_transactions_committed_total{area}`
- `stream_transactions_rolled_back_total{area}`
- `stream_watermark{area}`
- `stream_read_latency_seconds{operation}`
- `stream_sequence_gaps_total{route}`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "transaction_committed",
  "area": "acme/orders",
  "resource": "events",
  "transaction_id": "txn_12345",
  "first_seq": 1000,
  "last_seq": 1100,
  "event_count": 101,
  "duration_ms": 250
}
```

---

## Implementation Status

### ✅ Completed
- Stream event data structures
- Transaction lifecycle management
- Sequence validation and gap detection
- Dual indexing storage design
- Watermark advancement logic
- Basic read/peek operations

### 🚧 In Progress
- Transaction timeout handling
- Concurrent transaction management
- Area-wide consumption (hierarchical merge)
- Cloud storage backend integration
- Retention policy enforcement

### 📋 TODO
- Stream compaction and cleanup
- Consumer group support
- Event time vs ingest time ordering
- Cross-area transactions
- Stream schema validation

---

## Testing Requirements

### Unit Tests
- Happy path: begin → append → commit
- Sequence validation: gap detection and conflict handling
- Transaction lifecycle: commit vs rollback
- Watermark advancement: concurrent transaction ordering
- Idempotency: duplicate append handling
- Resource isolation: transactions don't interfere

### Integration Tests
- End-to-end event publishing and consumption
- Concurrent producers with sequence management
- Consumer resume from arbitrary sequence
- Area-wide consumption across multiple resources
- Transaction timeout and cleanup
- Storage backend durability

### Performance Benchmarks
- Append throughput (events/second)
- Read latency for different sequence ranges
- Concurrent transaction scaling
- Storage backend comparison
- Area consumption merge performance

---

## Usage Patterns

### Event Sourcing

```rust
// Order service publishes events
async fn publish_order_event(order_id: &str, event: OrderEvent) {
    let route = format!("stream://acme/orders/events");
    let txn_id = stream.begin_append("acme", "orders", "events").await?;

    let stream_event = StreamEvent {
        sequence: get_next_sequence(order_id),
        resource: order_id.to_string(),
        body: serde_json::to_vec(&event)?,
        is_end: false,
        created_at: now(),
    };

    stream.append_event(txn_id, stream_event).await?;
    stream.commit_append(txn_id).await?;
}
```

### Audit Logging

```rust
// Security service logs all access
async fn log_security_event(event: SecurityEvent) {
    let route = "stream://acme/audit/security".to_string();
    let txn_id = stream.begin_append("acme", "audit", "security").await?;

    let stream_event = StreamEvent {
        sequence: get_next_audit_sequence(),
        resource: "access".to_string(),
        body: serde_json::to_vec(&event)?,
        is_end: false,
        created_at: now(),
    };

    stream.append_event(txn_id, stream_event).await?;
    stream.commit_append(txn_id).await?;
}
```

### Stream Processing

```rust
// Consumer processes events in order
async fn process_order_events() {
    let mut last_seq = get_checkpoint("orders");

    loop {
        let events = stream.read_resource("acme/orders/events", last_seq, 100).await?;

        for event in events {
            process_event(event).await?;
            last_seq = event.sequence + 1;
        }

        save_checkpoint("orders", last_seq);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
```

---

*See OVERVIEW.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\STREAM_SPEC.md