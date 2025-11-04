# Fitz Stream Architecture - Final Design

## Overview

Fitz Streams provide ordered, append-only event logs with:
- **Client-controlled sequences** (idempotency + gap detection)
- **Dual-index storage** (resource-level + area-wide interleaving)
- **Strict ordering guarantees** via low watermark visibility control
- **Concurrent batch appends** without blocking

---

## Core Principles

### 1. Client-Controlled Resource Sequences

Producers provide explicit `resource_seq` for each event:
- **Idempotency**: Retry with same `(route, resource_seq, body)` is safe
- **Gap Detection**: Server rejects gaps (e.g., seq 0→2 without seq 1)
- **No Overwrites**: Cannot replace existing sequence with different content
- **Monotonic**: Sequences must start at 0 and increment by 1

```rust
// Producer controls resource_seq explicitly
stream_append(
    route: "stream://payments/transactions/batch_123",
    resource_seq: 0,  // Client-provided
    body: b"event data",
    is_end: false
) → Ok(AppendResult { resource_seq: 0, area_seq: 1000 })
```

### 2. Server-Assigned Area Sequences

Server assigns global `area_seq` for area-wide ordering:
- **Monotonic**: Strictly increasing per (realm, area)
- **Deterministic**: No timestamp ambiguity
- **Dual-Indexed**: Same event stored in both resource and area indexes

```rust
// Server assigns area_seq atomically
area_seq_counter["payments"]["transactions"] = 1000
→ Increments to 1001, 1002, 1003...
```

### 3. Dual-Index Storage Model

```rust
#[derive(Debug, Clone)]
pub struct StreamEvent {
    // Identity
    pub route: String,              // "stream://payments/transactions/batch_123"
    
    // Dual sequences
    pub resource_seq: u64,          // Client-controlled: 0, 1, 2, 3...
    pub area_seq: u64,              // Server-assigned: 1000, 1001, 1002...
    
    // Content
    pub body: Arc<Vec<u8>>,         // Shared to avoid duplication
    pub metadata: Option<Arc<Vec<u8>>>,
    pub is_end: bool,               // Stream completion marker
    pub created_at: u64,
}

// Storage indexes
struct StreamStorage {
    // Index 1: Resource streams (client sequences)
    resource_streams: HashMap<String, Vec<StreamEvent>>,
    //   Key: "stream://payments/transactions/batch_123"
    //   Value: [event@resource_seq=0, event@resource_seq=1, ...]
    
    // Index 2: Area streams (global sequences)
    area_streams: HashMap<(String, String), Vec<StreamEvent>>,
    //   Key: ("payments", "transactions")
    //   Value: [event@area_seq=1000, event@area_seq=1001, ...]
    
    // Area sequence counters
    area_seq_counters: HashMap<(String, String), u64>,
    
    // Visibility control (watermarks)
    area_states: HashMap<(String, String), AreaStreamState>,
}
```

---

## Concurrent Batch Append Strategy

### The Challenge

```
Timeline:
T0: Producer A starts batch (100 events) → reserves area_seq [1000..1100]
T1: Producer B starts batch (1 event)    → reserves area_seq [1100..1101]
T2: Producer B commits (fast)            → area_seq 1100 visible
T3: Consumer reads from 1000             → GAP! [1000..1100] not committed
T4: Producer A commits (slow)            → [1000..1100] now visible
```

**Problem:** Without coordination, consumer sees event 1100 before [1000..1100], violating ordering.

### The Solution: Reserve-Then-Commit with Low Watermark

```rust
#[derive(Debug)]
struct AreaStreamState {
    next_seq: u64,                      // Next sequence to allocate
    low_watermark: u64,                 // All seq < watermark are committed
    reserved_ranges: BTreeMap<u64, ReservationStatus>,
}

#[derive(Debug)]
enum ReservationStatus {
    Reserved,    // Reserved, not yet committed
    Committed,   // Committed, visible to consumers
}
```

### Append Flow (3-Phase Commit)

```rust
pub async fn stream_append_batch(
    route: String,
    events: Vec<(u64, Vec<u8>, Option<Vec<u8>>, bool)>,
) -> Result<AppendResult, StreamError> {
    
    // PHASE 1: RESERVE (fast, no I/O)
    // --------------------------------
    // Atomically allocate area_seq range
    let area_seq_range = {
        let mut state = area_states.lock().await;
        let area_state = state.entry((realm, area)).or_default();
        
        let start = area_state.next_seq;
        let end = start + events.len() as u64;
        
        // Mark as reserved (not yet visible)
        for seq in start..end {
            area_state.reserved_ranges.insert(seq, ReservationStatus::Reserved);
        }
        
        area_state.next_seq = end;
        start..end
    };
    
    // PHASE 2: WRITE (slow, I/O intensive)
    // -------------------------------------
    // Write events to both indexes
    // This can take time, other batches proceed concurrently
    let batch_events = build_events(route, events, area_seq_range);
    write_to_resource_index(batch_events.clone()).await?;
    write_to_area_index(batch_events).await?;
    
    // PHASE 3: COMMIT (fast, atomic)
    // --------------------------------
    // Mark sequences as committed, advance watermark
    {
        let mut state = area_states.lock().await;
        let area_state = state.get_mut(&(realm, area)).unwrap();
        
        // Mark range as committed
        for seq in area_seq_range.clone() {
            area_state.reserved_ranges.insert(seq, ReservationStatus::Committed);
        }
        
        // Advance watermark to first uncommitted sequence
        while let Some((&seq, status)) = area_state.reserved_ranges
            .range(area_state.low_watermark..)
            .next() 
        {
            if seq != area_state.low_watermark {
                break; // Gap found
            }
            if matches!(status, ReservationStatus::Committed) {
                area_state.low_watermark += 1;
                area_state.reserved_ranges.remove(&seq);
            } else {
                break; // Hit reserved (uncommitted)
            }
        }
    }
    
    Ok(AppendResult {
        resource_seq_range,
        area_seq_range,
    })
}
```

### Watermark Advancement Examples

```rust
// Example 1: In-order commits
// ---------------------------
Reserve: A[0..100], B[100..110]
Commit:  A → watermark advances 0→100
Commit:  B → watermark advances 100→110

// Example 2: Out-of-order commits
// --------------------------------
Reserve: A[0..100], B[100..110], C[110..150]
Commit:  B → watermark stays at 0 (A blocks)
Commit:  C → watermark stays at 0 (A still blocks)
Commit:  A → watermark jumps 0→150 (all committed)

// Example 3: Interleaved commits
// --------------------------------
Reserve: A[0..50], B[50..100], C[100..120]
Commit:  A → watermark 0→50
Commit:  C → watermark stays at 50 (B blocks)
Commit:  B → watermark 50→120 (unblocked)
```

---

## Read Paths

### Resource-Specific Read (No Watermark)

```rust
pub async fn stream_read(
    route: String,                  // "stream://payments/transactions/batch_123"
    from_resource_seq: u64,
    limit: usize,
) -> Result<Vec<StreamEvent>, StreamError> {
    // Direct index lookup, no watermark
    let events = resource_streams.get(route)?;
    Ok(events.range(from_resource_seq..)
        .take(limit)
        .cloned()
        .collect())
}
```

**Use Cases:**
- Read specific batch/session
- Replay events for one resource
- Not affected by other resources' commit state

### Area-Wide Read (Watermark-Controlled)

```rust
pub async fn stream_read_area(
    realm: String,
    area: String,
    from_area_seq: u64,
    limit: usize,
) -> Result<AreaReadResponse, StreamError> {
    // Get current watermark
    let watermark = area_states.lock().await
        .get(&(realm.clone(), area.clone()))
        .map(|s| s.low_watermark)
        .unwrap_or(0);
    
    // Only return events < watermark (visible range)
    let events = area_streams.get(&(realm, area))?;
    let visible_events = events.iter()
        .filter(|e| e.area_seq >= from_area_seq && e.area_seq < watermark)
        .take(limit)
        .cloned()
        .collect();
    
    Ok(AreaReadResponse {
        events: visible_events,
        current_watermark: watermark,
        has_more: watermark < area_seq_counters[&(realm, area)],
    })
}

#[derive(Debug)]
pub struct AreaReadResponse {
    pub events: Vec<StreamEvent>,
    pub current_watermark: u64,     // Consumer can resume from here
    pub has_more: bool,             // Are there uncommitted events beyond?
}
```

**Guarantees:**
- **Strict Ordering**: Never skips uncommitted sequences
- **No Gaps**: Events returned are contiguous in area_seq space
- **Deterministic**: Same watermark = same results

**Use Cases:**
- Interleaved event processing across all resources
- Audit logs requiring strict chronological order
- Event sourcing with global consistency

---

## Error Handling

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum StreamError {
    // Client sequence errors
    SequenceGap { 
        route: String,
        expected: u64, 
        received: u64 
    },
    SequenceConflict { 
        route: String,
        seq: u64 
    },
    SequenceMustStartAtZero { 
        route: String,
        received: u64 
    },
    
    // Stream lifecycle errors
    StreamClosed { route: String },
    InvalidRoute(String),
    
    // Validation errors
    PayloadTooLarge { size: usize, max: usize },
    EmptyBatch,
    DuplicateSequenceInBatch { seq: u64 },
    
    // Not found
    RouteNotFound(String),
    AreaNotFound(String, String),
    
    // Internal
    Internal(String),
}
```

---

## API Summary

```rust
// ============================================================================
// APPEND - Single Event
// ============================================================================

pub async fn stream_append(
    route: String,              // "stream://{realm}/{area}/{resource}"
    resource_seq: u64,          // Client-controlled (idempotency key)
    body: Vec<u8>,
    metadata: Option<Vec<u8>>,
    is_end: bool,               // Mark stream complete
) -> Result<AppendResult, StreamError>

// ============================================================================
// APPEND - Batch (Atomic)
// ============================================================================

pub async fn stream_append_batch(
    route: String,
    events: Vec<BatchEvent>,
) -> Result<AppendResult, StreamError>

#[derive(Debug)]
pub struct BatchEvent {
    pub resource_seq: u64,
    pub body: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub is_end: bool,
}

#[derive(Debug)]
pub struct AppendResult {
    pub resource_seq_range: Range<u64>,  // [start, end)
    pub area_seq_range: Range<u64>,      // [start, end)
}

// ============================================================================
// READ - Resource-Specific
// ============================================================================

pub async fn stream_read(
    route: String,
    from_resource_seq: u64,
    limit: usize,
) -> Result<Vec<StreamEvent>, StreamError>

// ============================================================================
// READ - Area-Wide (Interleaved)
// ============================================================================

pub async fn stream_read_area(
    realm: String,
    area: String,
    from_area_seq: u64,
    limit: usize,
) -> Result<AreaReadResponse, StreamError>

// Or prefix-based:
pub async fn stream_read_prefix(
    prefix: String,             // "stream://payments/transactions"
    from_area_seq: u64,
    limit: usize,
) -> Result<AreaReadResponse, StreamError>

// ============================================================================
// METADATA
// ============================================================================

pub async fn stream_info(
    route: String,
) -> Result<StreamInfo, StreamError>

#[derive(Debug)]
pub struct StreamInfo {
    pub route: String,
    pub head_resource_seq: Option<u64>,
    pub event_count: u64,
    pub is_closed: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

pub async fn area_info(
    realm: String,
    area: String,
) -> Result<AreaInfo, StreamError>

#[derive(Debug)]
pub struct AreaInfo {
    pub realm: String,
    pub area: String,
    pub next_area_seq: u64,
    pub low_watermark: u64,
    pub resource_count: usize,
    pub total_events: u64,
}
```

---

## Route Structure

Streams MUST follow 3-level hierarchy:

```
stream://{realm}/{area}/{resource}
         ^^^^^^   ^^^^   ^^^^^^^^
         Tenant   Topic  Session/Batch
```

**Examples:**
```
stream://payments/transactions/stripe_batch_20250104_001
stream://audit/api_calls/session_abc123
stream://orders/created/batch_producer_1_seq_50
```

**Validation:**
- Reject routes with != 3 levels
- Each segment must be non-empty
- Use URL-safe characters (alphanumeric, dash, underscore)

---

## Implementation Roadmap

### Phase 1: Core Dual-Index Storage ✅
- [x] Design document (this file)
- [ ] Implement `StreamEvent` with dual sequences
- [ ] Implement route parsing and validation
- [ ] Implement dual storage indexes
- [ ] Add area sequence counter management

### Phase 2: Append with Watermark Control
- [ ] Implement `AreaStreamState` with reservation tracking
- [ ] Implement 3-phase append (reserve → write → commit)
- [ ] Implement watermark advancement logic
- [ ] Add idempotency checking
- [ ] Add gap detection

### Phase 3: Read APIs
- [ ] Implement `stream_read()` (resource-specific)
- [ ] Implement `stream_read_area()` (watermark-controlled)
- [ ] Add `AreaReadResponse` with watermark metadata
- [ ] Implement prefix-based read variant

### Phase 4: Error Handling
- [ ] Define complete `StreamError` enum
- [ ] Update all error returns to use structured errors
- [ ] Add error serialization for protocol layer

### Phase 5: Testing
- [ ] Implement all stubbed tests in `tests/stream.rs`
- [ ] Add concurrent append stress tests
- [ ] Add watermark advancement tests
- [ ] Add integration tests for dual-index consistency

### Phase 6: Protocol Integration
- [ ] Add TLV tags for `resource_seq`, `area_seq`, `is_end`
- [ ] Update frame encoding/decoding
- [ ] Add watermark to response frames
- [ ] Document protocol changes

### Phase 7: Observability
- [ ] Add metrics (appends/sec, watermark lag, reservation queue depth)
- [ ] Add structured logging
- [ ] Add admin API for stream/area inspection

---

## Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Sequence control | Client-controlled resource_seq | Idempotency + gap detection |
| Global ordering | Server-assigned area_seq | Deterministic, no timestamp ambiguity |
| Storage model | Dual-index | Efficient for both query patterns |
| Concurrency | Reserve-then-commit | Allow concurrent writes without blocking |
| Visibility | Low watermark | Strict ordering guarantee, no gaps |
| Sequence start | Must start at 0 | Simplicity, clear semantics |
| Route structure | 3-level mandatory | Clear hierarchy, predictable parsing |
| Batch atomicity | All-or-nothing per resource | Strong consistency per stream |
| Idempotency | (route, resource_seq) → dedupe | Safe retries |
| Stream closure | `is_end=true` hard close | Prevents accidental re-opens |

---

## Open Questions / Future Enhancements

1. **Retention policies**: Time-based or size-based cleanup
2. **Compaction**: For high-volume streams, compact old events
3. **Snapshots**: Periodic checkpoints for fast recovery
4. **Sharding**: Partition large areas across multiple nodes
5. **Live subscriptions**: Push notifications on new appends
6. **Cross-area queries**: Federated reads across multiple areas
7. **Encryption**: At-rest and in-transit encryption
8. **Compression**: Reduce storage footprint for large batches

---

End of design document.
