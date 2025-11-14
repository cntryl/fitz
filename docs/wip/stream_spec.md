# Fitz Stream Service - Unified Specification

**Version:** 1.0  
**Last Updated:** November 14, 2025  
**Status:** Implemented (Core Features)

---

## Table of Contents

1. [Overview](#overview)
2. [Core Principles](#core-principles)
3. [Architecture](#architecture)
4. [Data Model](#data-model)
5. [Storage Implementation](#storage-implementation)
6. [API Reference](#api-reference)
7. [Concurrency Control](#concurrency-control)
8. [Error Handling](#error-handling)
9. [Protocol Integration](#protocol-integration)
10. [Testing](#testing)
11. [Implementation Status](#implementation-status)
12. [Future Enhancements](#future-enhancements)

---

## Overview

Fitz Streams provide ordered, append-only event logs with strict consistency guarantees. Streams are designed for event sourcing, audit logging, and any scenario requiring durable, ordered message sequences with replay capabilities.

### Key Features

- **Client-controlled sequences**: Producers specify explicit `resource_seq` for idempotency and gap detection
- **Dual-index storage**: Events stored in both resource-specific and area-wide indexes
- **Strict ordering guarantees**: Watermark-based visibility ensures no out-of-order reads
- **Concurrent batch appends**: Multiple producers can write without blocking each other
- **Optimistic concurrency control**: Conditional appends with expected revision checking
- **Transaction semantics**: Explicit begin/append/commit/rollback lifecycle
- **Resume and replay**: Clients can resume from any sequence number within retention

### Differences from Queues

| Feature | Streams | Queues |
|---------|---------|--------|
| Persistence | Append-only, replayable | Consumed and removed |
| Ordering | Strict sequence numbers | Best-effort ordering |
| Consumption | Multiple readers, non-destructive | Single consumer, destructive |
| Use Cases | Event sourcing, audit logs | Task distribution, work queues |

---

## Core Principles

### 1. Client-Controlled Resource Sequences

Producers provide explicit `resource_seq` for each event, enabling:

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

### 2. Server-Assigned Area Sequences

The server assigns global `area_seq` for area-wide ordering:

- **Monotonic**: Strictly increasing per (realm, area)
- **Deterministic**: No timestamp ambiguity
- **Dual-Indexed**: Same event stored in both resource and area indexes

```rust
// Server assigns area_seq atomically during commit
area_states["payments"]["transactions"].next_seq = 1000
→ Increments to 1001, 1002, 1003...
```

### 3. Transaction-Based Appends

All appends require explicit transaction lifecycle:

```rust
// Phase 1: Begin transaction
let txn_id = stream.begin_append(rf, realm, area, resource).await?;

// Phase 2: Append events (direct writes to storage)
stream.append_event(txn_id, rf, event1).await?;
stream.append_event(txn_id, rf, event2).await?;

// Phase 3: Commit (advances watermark) or Rollback
let (first_seq, last_seq, count) = stream.commit_append(txn_id, rf).await?;
// OR: stream.rollback_append(txn_id, rf).await?;
```

**Key Properties:**
- Single resource per transaction (enforced)
- Events written immediately (no buffering)
- Watermark advances only on commit
- Rollback clears reservations but leaves orphaned data

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                        Stream Service                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐      ┌──────────────────┐            │
│  │  Active Txns     │      │  Area States     │            │
│  │  ──────────      │      │  ──────────      │            │
│  │  txn_id → {      │      │  (rf,area) → {   │            │
│  │    realm,        │      │    next_seq,     │            │
│  │    area,         │      │    watermark,    │            │
│  │    resource,     │      │    reserved: {   │            │
│  │    first_seq,    │      │      seq→status  │            │
│  │    event_count   │      │    }             │            │
│  │  }               │      │  }               │            │
│  └──────────────────┘      └──────────────────┘            │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                      KvStore (Midge)                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Index 1: Resource Events                                   │
│  ─────────────────────────                                  │
│  Key: 0x01 0x01 {rf} {realm} {area} {resource} {rsrc_seq}  │
│  Val: Encoded StreamEvent                                   │
│                                                              │
│  Index 2: Area Events                                       │
│  ─────────────────────                                      │
│  Key: 0x01 0x02 {rf} {realm} {area} {area_seq}             │
│  Val: Encoded StreamEvent                                   │
│                                                              │
│  Index 3: Watermarks                                        │
│  ─────────────────────                                      │
│  Key: 0x01 0x03 {rf} {realm} {area}                        │
│  Val: u64 (watermark)                                       │
│                                                              │
│  Index 4: Discovery (Area)                                  │
│  Key: 0x01 0x04 {rf} {realm} {area}                        │
│  Val: Marker byte                                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Concurrency Strategy: Reserve-Then-Commit

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

### Append Flow (3-Phase Commit)

```rust
// PHASE 1: BEGIN (allocate transaction, peek next_seq)
let txn_id = service.begin_append(rf, realm, area, resource).await?;
// → Creates ActiveTransaction with first_area_seq = area_states[area].next_seq

// PHASE 2: APPEND (reserve + write)
service.append_event(txn_id, rf, event).await?;
// → For each event:
//    1. Reserve area_seq in reserved_ranges (Reserved status)
//    2. Write to both resource and area indexes
//    3. Increment event_count

// PHASE 3: COMMIT (mark committed + advance watermark)
let result = service.commit_append(txn_id, rf).await?;
// → Mark all sequences in range as Committed
// → Advance watermark to first uncommitted sequence (skip gaps)
// → Clean up committed entries from reserved_ranges
```

### Watermark Advancement Logic

```rust
// After marking sequences as Committed:
let mut new_watermark = area_state.watermark;

// Special case: first commit to area starting at seq 0
if area_state.watermark == 0 && first_seq == 0 {
    scan_seq = 0;
}

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

**Examples:**

```
Example 1: In-order commits
----------------------------
Reserve: A[0..100], B[100..110]
Commit:  A → watermark: 0→99 (highest committed seq)
Commit:  B → watermark: 99→109

Example 2: Out-of-order commits
--------------------------------
Reserve: A[0..100], B[100..110], C[110..150]
Commit:  B → watermark stays at 0 (A blocks)
Commit:  C → watermark stays at 0 (A still blocks)
Commit:  A → watermark jumps 0→149 (all unblocked)

Example 3: Partial rollback
----------------------------
Reserve: A[0..100], B[100..110]
Commit:  B → watermark stays at 0 (A blocks)
Rollback: A → removes Reserved entries for [0..100]
Result:  Watermark advances 0→109 (gap skipped)
```

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
    pub first_area_seq: u64,      // Server-assigned starting area_seq
    pub event_count: usize,       // Number of events appended
}
```

### Route Structure

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

**Validation Rules:**
- Must have exactly 3 path segments after scheme
- Each segment must be non-empty
- Use URL-safe characters (alphanumeric, `-`, `_`, `.`)
- Case-sensitive

---

## Storage Implementation

### Key Schema

All keys use lexicographic encoding via `lexkey::LexKey`:

```rust
// Index 1: Resource event index (client sequences)
// Enables: "Give me all events for stream X starting at resource_seq Y"
Key: [DOMAIN_PREFIX] [IDX_RESOURCE_EVENT] [rf] [realm] [area] [resource] [resource_seq]
     0x01            0x01                  u32  bytes   bytes  bytes     u64(BE)

// Index 2: Area event index (server sequences)
// Enables: "Give me all events in area (realm, area) starting at area_seq Y"
Key: [DOMAIN_PREFIX] [IDX_AREA_EVENT] [rf] [realm] [area] [area_seq]
     0x01            0x02              u32  bytes   bytes  u64(BE)

// Index 3: Watermark storage
// Enables: "What is the highest visible area_seq for this area?"
Key: [DOMAIN_PREFIX] [IDX_WATERMARK] [rf] [realm] [area]
     0x01            0x03             u32  bytes   bytes
Val: u64(BE) watermark

// Index 4: Area discovery
// Enables: "Which areas exist in this realm?"
Key: [DOMAIN_PREFIX] [IDX_AREA_DISCOVERY] [rf] [realm] [area]
     0x01            0x04                  u32  bytes   bytes
Val: 0x01 (marker)
```

### Event Encoding

Events stored as CBOR-encoded bytes:

```rust
pub fn encode_event(event: &StreamEvent) -> Vec<u8> {
    serde_cbor::to_vec(event).expect("Encode event")
}

pub fn decode_event(bytes: &[u8]) -> Result<StreamEvent, String> {
    serde_cbor::from_slice(bytes)
        .map_err(|e| format!("Decode error: {:?}", e))
}
```

### Dual-Index Writes

Every event is written to **both** indexes:

```rust
// Write to resource index (client sequence space)
let resource_key = key_resource_event(&realm, &area, &resource, event.sequence);
kv_store.put(cf, &resource_key, &encoded)?;

// Write to area index (server sequence space)
let area_key = key_area_event(&realm, &area, area_seq);
kv_store.put(cf, &area_key, &encoded)?;
```

**Consistency Note:** Both writes happen in `append_event()` before commit. If a transaction rolls back, events remain in storage as "orphaned" data but are invisible to readers (watermark won't advance past them).

---

## API Reference

### Transaction Lifecycle

#### `begin_append`

Starts a new append transaction for a specific resource.

```rust
pub async fn begin_append(
    &self,
    rf: RouteFamilyId,
    realm: &str,
    area: &str,
    resource: &str,
) -> Result<u64, String>
```

**Returns:** Transaction ID (u64)

**Behavior:**
- Allocates next transaction ID
- Peeks at `area_states` to get starting `area_seq` for this transaction
- Creates `ActiveTransaction` entry
- **Does not allocate area sequences yet** (happens in append_event)

**Enforces:**
- Single resource per transaction

---

#### `append_event`

Appends a single event to an active transaction.

```rust
pub async fn append_event(
    &self,
    txn_id: u64,
    rf: RouteFamilyId,
    event: StreamEvent,
) -> Result<(), String>
```

**Behavior:**
1. **Reserve area_seq**: Insert into `reserved_ranges` with `ReservationStatus::Reserved`
2. **Write event**: Immediately write to both resource and area indexes
3. **Increment counter**: Update transaction's `event_count`

**Validations:**
- Transaction must exist
- Resource sequence must match expected (no gaps, no conflicts)

**Error Cases:**
- `Transaction not found`
- Sequence gap/conflict (future enhancement)

---

#### `commit_append`

Commits the transaction, making events visible.

```rust
pub async fn commit_append(
    &self,
    txn_id: u64,
    rf: RouteFamilyId,
) -> Result<(u64, u64, usize), String>
```

**Returns:** `(first_area_seq, last_area_seq, event_count)`

**Behavior:**
1. **Mark committed**: Change all reserved sequences to `ReservationStatus::Committed`
2. **Advance watermark**: Scan from current watermark, advance until hitting Reserved or gap
3. **Persist watermark**: Write to KvStore
4. **Mark area discovered**: Write area discovery marker
5. **Cleanup**: Remove transaction from active set

**Validations:**
- Transaction must exist
- Transaction must have at least 1 event

**Error Cases:**
- `Transaction not found`
- `Transaction is empty`

---

#### `rollback_append`

Rolls back the transaction, discarding events.

```rust
pub async fn rollback_append(
    &self,
    txn_id: u64,
    rf: RouteFamilyId,
) -> Result<(), String>
```

**Behavior:**
1. **Remove reservations**: Delete all Reserved entries for this transaction's area_seq range
2. **Remove transaction**: Delete from active transactions

**Note:** Events already written to storage remain as orphaned data. They are invisible to readers because the watermark won't advance past them. Future compaction can remove orphaned data.

---

### Read Operations

#### `read` (Resource-Specific)

Reads events from a specific resource stream.

```rust
pub async fn read(
    &self,
    rf: RouteFamilyId,
    route: &str,
    from_seq: u64,
    limit: usize,
) -> Result<Vec<StreamEvent>, String>
```

**Behavior:**
- Parses route to extract realm, area, resource
- Queries resource index starting at `from_seq`
- Returns up to `limit` events
- **Does NOT check watermark** (committed events visible immediately to resource readers)

**Use Cases:**
- Read specific batch/session
- Replay events for one resource
- Not affected by other resources' commit state

---

#### `read_area` (Area-Wide, Watermark-Controlled)

Reads events from all resources in an area, respecting watermark.

```rust
pub async fn read_area(
    &self,
    rf: RouteFamilyId,
    realm: &str,
    area: &str,
    from_seq: u64,
    limit: usize,
) -> Result<Vec<StreamEvent>, String>
```

**Behavior:**
- Gets current watermark for area
- Queries area index starting at `from_seq`
- Filters events where `area_seq < watermark` (only committed, no gaps)
- Returns up to `limit` events

**Guarantees:**
- **Strict ordering**: Never skips uncommitted sequences
- **No gaps**: Events returned are contiguous in area_seq space
- **Deterministic**: Same watermark = same results

**Use Cases:**
- Interleaved event processing across all resources
- Audit logs requiring strict chronological order
- Event sourcing with global consistency

---

#### `get_watermark`

Gets current watermark for an area.

```rust
pub async fn get_watermark(
    &self,
    rf: RouteFamilyId,
    realm: &str,
    area: &str,
) -> Result<u64, String>
```

**Returns:** Current watermark (u64)

**Behavior:**
- Reads watermark from KvStore
- Returns 0 if area has no events

---

### Utility Methods

#### `parse_route`

Parses a stream route into components.

```rust
pub fn parse_route(route: &str) -> Result<(String, String, String), String>
```

**Returns:** `(realm, area, resource)`

**Validation:**
- Must start with `stream://`
- Must have exactly 3 path segments
- Each segment must be non-empty

---

## Concurrency Control

### Optimistic Concurrency (Future Enhancement)

Streams support conditional appends using expected revision checking (similar to EventStoreDB):

```rust
pub enum ExpectedRevision {
    Any,             // Accept regardless of current state
    NoStream,        // Only if stream does not exist
    StreamExists,    // Only if stream exists (at any revision)
    Exact(u64),      // Only if current head == this value
}

pub struct AppendResult {
    pub first_assigned: u64,
    pub last_assigned: u64,
}
```

**Protocol TLVs:**
- `TAG_EXPECTED_REV (0xA0)`: Carries expected revision
  - `0xFFFFFFFFFFFFFFFF`: Any
  - `0xFFFFFFFFFFFFFFFE`: NoStream
  - `0xFFFFFFFFFFFFFFFD`: StreamExists
  - Otherwise: u64 exact revision
- `TAG_ASSIGNED_REV (0xA1)`: Server echoes assigned revision on success
- `TAG_FIRST_ASSIGNED_REV (0xA2)`: First revision in batch append

**Error Cases:**
```rust
StreamError::WrongExpectedVersion {
    expected: ExpectedRevision,
    actual: Option<u64>,
}
```

**Status:** Not yet implemented. Current implementation supports basic append without OCC.

---

## Error Handling

### Error Types

```rust
// Future: Full error enum (currently returns String errors)
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
    
    // Transaction errors
    TransactionNotFound(u64),
    TransactionEmpty(u64),
    
    // Not found
    RouteNotFound(String),
    AreaNotFound(String, String),
    
    // OCC errors (future)
    WrongExpectedVersion {
        expected: ExpectedRevision,
        actual: Option<u64>,
    },
    
    // Internal
    Internal(String),
}
```

### Current Error Returns

Most methods currently return `Result<T, String>` with descriptive error messages:

```rust
"Transaction not found or already committed"
"Transaction is empty"
"Failed to write area event: {:?}"
"Invalid route format"
```

**Future:** Migrate to structured `StreamError` enum for better error handling and protocol mapping.

---

## Protocol Integration

### TLV Conventions

```rust
// Core tags
TAG_ROUTE           = 0x01   // Stream route
TAG_BODY            = 0x02   // Event payload
TAG_SEQ             = 0x03   // Sequence number (resource or area)
TAG_NOTIFICATION    = 0x04   // Server push of events
TAG_ERROR           = 0x05   // Error frames

// Stream-specific tags
TAG_RESOURCE_SEQ    = 0x10   // Client-controlled sequence
TAG_AREA_SEQ        = 0x11   // Server-assigned sequence
TAG_WATERMARK       = 0x12   // Current area watermark
TAG_IS_END          = 0x13   // Stream end marker
TAG_METADATA        = 0x14   // Event metadata (CBOR)

// Transaction tags
TAG_TXN_ID          = 0x20   // Transaction identifier
TAG_TXN_BEGIN       = 0x21   // Begin transaction
TAG_TXN_COMMIT      = 0x22   // Commit transaction
TAG_TXN_ROLLBACK    = 0x23   // Rollback transaction

// OCC tags (future)
TAG_EXPECTED_REV    = 0xA0   // Expected revision
TAG_ASSIGNED_REV    = 0xA1   // Assigned revision
TAG_FIRST_ASSIGNED  = 0xA2   // First revision in batch
```

### Frame Encoding Examples

**Begin Append Request:**
```
[TAG_TXN_BEGIN][route: "stream://realm/area/resource"]
```

**Append Event Request:**
```
[TAG_TXN_ID][txn_id: 123]
[TAG_RESOURCE_SEQ][seq: 0]
[TAG_BODY][data: bytes]
[TAG_IS_END][false]
```

**Commit Response:**
```
[TAG_TXN_ID][txn_id: 123]
[TAG_AREA_SEQ][first_seq: 1000]
[TAG_AREA_SEQ][last_seq: 1002]
[TAG_SEQ][event_count: 3]
```

**Read Response:**
```
[TAG_WATERMARK][watermark: 1050]
[TAG_NOTIFICATION][
  [TAG_RESOURCE_SEQ][0]
  [TAG_AREA_SEQ][1000]
  [TAG_BODY][data]
]
[TAG_NOTIFICATION][
  [TAG_RESOURCE_SEQ][1]
  [TAG_AREA_SEQ][1001]
  [TAG_BODY][data]
]
```

---

## Testing

### Test Organization

Tests are located in `src/core/stream/service.rs` under the `#[cfg(test)]` module.

### Test Categories

#### Basic Operations (12 tests) ✅

All passing as of November 14, 2025:

- `should_parse_route_correctly`
- `should_reject_invalid_route_format`
- `should_append_single_event_successfully`
- `should_maintain_monotonic_area_sequences`
- `should_get_watermark_after_commit`
- `should_return_zero_watermark_when_no_events`
- `should_read_events_from_resource_stream`
- `should_read_all_committed_events_from_area`
- `should_return_empty_when_reading_ahead_of_watermark`
- `should_respect_limit_when_reading_events`
- `should_reject_append_to_unknown_transaction`
- `should_rollback_transaction_discards_events`

#### Watermark Behavior (Tested)

Current tests validate:
- Watermark starts at 0
- Watermark advances to highest committed sequence after commit
- Watermark blocks visibility of uncommitted events
- Area reads respect watermark, resource reads do not

**Future tests needed:**
- Concurrent append with out-of-order commits
- Watermark advancement with gaps
- Rollback clearing reservations properly
- Large batch blocking small batches
- Multiple concurrent writers to same area

#### Error Handling (Future)

**Tests needed:**
- Sequence gap detection
- Sequence conflict detection (same seq, different body)
- Payload size limits
- Stream closure enforcement (`is_end=true`)
- Empty batch rejection
- Duplicate sequences in batch

#### Concurrency (Future)

**Tests needed:**
- Concurrent writers to same area with interleaved commits
- Concurrent writers to different areas (isolation)
- Watermark advancement under load
- Reservation queue depth and cleanup
- Rollback while other transactions pending

#### Optimistic Concurrency Control (Future)

**Tests needed:**
- Append with ExpectedRevision::NoStream
- Append with ExpectedRevision::Exact(N)
- Append with ExpectedRevision::StreamExists
- Concurrent OCC conflicts
- WrongExpectedVersion error handling

---

## Implementation Status

### ✅ Completed (Phase 1-2)

- [x] Route parsing and validation
- [x] Dual-index storage model (resource + area)
- [x] Transaction lifecycle (begin/append/commit/rollback)
- [x] Area sequence counter management
- [x] Reservation tracking with BTreeMap
- [x] Watermark advancement with gap detection
- [x] Direct-write semantics (no buffering)
- [x] Basic read operations (resource and area)
- [x] get_watermark API
- [x] 12 passing unit tests
- [x] Integration with Midge KvStore
- [x] CBOR encoding/decoding
- [x] Area discovery markers

### 🚧 In Progress (Phase 3)

- [ ] Idempotency checking (sequence conflict detection)
- [ ] Sequence gap validation
- [ ] Stream closure (`is_end` enforcement)
- [ ] Structured error types (StreamError enum)

### 📋 Planned (Phase 4-7)

**Phase 4: Advanced Features**
- [ ] Optimistic concurrency control (ExpectedRevision)
- [ ] Batch append API (atomic multi-event)
- [ ] Payload size limits
- [ ] Metadata support in protocol
- [ ] Duplicate detection within batch

**Phase 5: Protocol Integration**
- [ ] Stream handler implementation (currently stubbed with panic!)
- [ ] TLV encoding/decoding for all operations
- [ ] Frame builders for responses
- [ ] Error mapping to protocol layer
- [ ] Transaction lifecycle in protocol

**Phase 6: Observability**
- [ ] Metrics (appends/sec, watermark lag, reservation depth)
- [ ] Structured logging with tracing
- [ ] Admin APIs (stream_info, area_info)
- [ ] List streams/areas endpoints
- [ ] Health checks and diagnostics

**Phase 7: Advanced Features**
- [ ] Live subscriptions (push notifications on append)
- [ ] Retention policies (time/size-based)
- [ ] Compaction (remove orphaned data from rollbacks)
- [ ] Snapshots for fast recovery
- [ ] Cross-area federated reads
- [ ] Sharding for horizontal scale

---

## Future Enhancements

### Retention Policies

```rust
pub enum RetentionPolicy {
    Time { max_age_secs: u64 },
    Size { max_bytes: u64 },
    Count { max_events: u64 },
    Composite { time: u64, size: u64 },
}

// Per-area configuration
pub struct AreaConfig {
    pub retention: RetentionPolicy,
    pub compact_threshold: f64,  // Trigger compaction at 80% of limit
}
```

### Compaction

Remove orphaned data (rolled-back transactions) and apply retention policies:

```rust
pub async fn compact_area(
    realm: &str,
    area: &str,
    retention: &RetentionPolicy,
) -> Result<CompactionStats, String>

pub struct CompactionStats {
    pub events_removed: u64,
    pub bytes_reclaimed: u64,
    pub duration_ms: u64,
}
```

### Live Subscriptions

Push notifications for new appends:

```rust
pub async fn subscribe(
    &self,
    rf: RouteFamilyId,
    route_or_prefix: &str,
    from_seq: u64,
) -> Result<Subscription, String>

pub struct Subscription {
    pub id: u64,
    pub rx: mpsc::Receiver<StreamEvent>,
}
```

### Sharding

Partition large areas across multiple storage backends:

```rust
pub struct AreaShardConfig {
    pub shard_count: usize,
    pub shard_key: ShardKeyFn,  // resource → shard_id
}

type ShardKeyFn = fn(&str) -> usize;
```

### Snapshots

Periodic checkpoints for fast recovery:

```rust
pub struct Snapshot {
    pub area_seq: u64,
    pub timestamp: u64,
    pub state: Vec<u8>,  // Compressed area state
}

pub async fn create_snapshot(
    realm: &str,
    area: &str,
) -> Result<Snapshot, String>
```

---

## Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Sequence Control** | Client-controlled `resource_seq` | Idempotency + gap detection |
| **Global Ordering** | Server-assigned `area_seq` | Deterministic, no timestamp ambiguity |
| **Storage Model** | Dual-index (resource + area) | Efficient for both query patterns |
| **Concurrency** | Reserve-then-commit with watermark | Allow concurrent writes without blocking |
| **Visibility** | Low watermark (highest committed) | Strict ordering guarantee, no gaps |
| **Sequence Start** | Must start at 0 | Simplicity, clear semantics |
| **Route Structure** | 3-level mandatory | Clear hierarchy, predictable parsing |
| **Transaction Model** | Explicit begin/commit/rollback | Clear lifecycle, rollback support |
| **Write Semantics** | Direct writes (no buffering) | Minimize memory footprint |
| **Idempotency** | `(route, resource_seq, body)` | Safe retries without overwrites |
| **Stream Closure** | `is_end=true` hard close | Prevents accidental re-opens |
| **Reservation Tracking** | BTreeMap (ordered) | Efficient range queries for watermark |

---

## References

### Related Documents

- `docs/wip/SPEC.md` - Overall Fitz architecture
- `docs/wip/kv_spec.md` - KV service specification
- `docs/wip/queue_spec.md` - Queue service specification
- `docs/wip/rpc_spec.md` - RPC service specification
- `docs/dev/test_guidelines.md` - Test writing guidelines

### External Inspirations

- **EventStoreDB**: Optimistic concurrency, event sourcing patterns
- **Apache Kafka**: Log-based storage, offset tracking
- **NATS JetStream**: Durable streams, watermarking
- **Amazon Kinesis**: Shard-based partitioning, sequence numbers

---

**End of Specification**
