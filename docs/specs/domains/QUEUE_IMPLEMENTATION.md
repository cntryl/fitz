# Fitz Queue Domain - Current Implementation

## Overview

The Queue domain provides single-node, durable FIFO message queues with at-least-once delivery semantics. Each queue is managed by a dedicated `QueueActor` that handles in-memory scheduling (ready queue, inflight leases, timers) while persisting messages to Midge for crash recovery. Leases are ephemeral and lost on restart.

## Actor Model and Ownership

- **One actor per queue**: Each `(RouteFamily, realm, area, resource)` tuple maps to exactly one `QueueActor` instance
- **Synchronous execution**: All operations (`enqueue`, `reserve`, `extend`, `complete`) execute synchronously in the actor's receive loop
- **Never blocks**: The actor always returns immediately; no waiter queues or blocking operations
- **Single-threaded**: All queue state mutations happen sequentially within the actor

## Durable Data Model (Midge)

### Persisted State

1. **Messages**: `queue:{family}:{realm}:{area}:{resource}:msg:{id}`
   - Format: `[attempts:4][visible_at_ms:8][body_len:4][body:N]`
   - Fields:
     - `attempts`: u32, number of times lease has expired (starts at 0)
     - `visible_at_ms`: u64, absolute timestamp when message becomes visible
     - `body`: variable-length byte array
   
2. **Next ID counter**: `queue:{family}:{realm}:{area}:{resource}:next_id`
   - Format: `[next_id:8]` (u64, little-endian)
   - Incremented on each enqueue, persisted immediately

### Not Persisted

- Ready queue order
- Inflight leases (token, expiration time)
- Timer heap
- Delayed visibility heap

## In-Memory Scheduling State

The `QueueActor` maintains four core data structures:

1. **`ready: VecDeque<MessageId>`**
   - FIFO queue of messages available for reservation
   - Rebuilt from storage scan on restart (MVP: manual recovery)

2. **`inflight: HashMap<MessageId, Inflight>`**
   - Currently leased messages
   - `Inflight { token: u64, expires_at: Instant }`
   - Lost on restart; leased messages become available again

3. **`timers: BinaryHeap<Reverse<LeaseExpiry>>`**
   - Min-heap of lease expiration events
   - `LeaseExpiry { id: MessageId, expires_at: Instant }`
   - Processed on every actor receive (eager expiration)

4. **`delayed: BinaryHeap<Reverse<DelayedMessage>>`**
   - Min-heap of messages awaiting visibility
   - `DelayedMessage { id: MessageId, visible_at: Instant }`
   - Moved to `ready` when `visible_at` passes

## Core Operations

### Enqueue

```rust
pub fn handle_enqueue(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse
```

**Behavior:**
1. Allocate monotonic `MessageId` from `next_id` counter
2. Persist counter update to Midge
3. Calculate `visible_at = now + delay_seconds`
4. Create `QueueRecord { body, attempts: 0, visible_at_ms }`
5. Persist record to Midge at message key
6. If `visible_at <= now`: add to `ready` queue
7. Else: add to `delayed` heap
8. Return `QueueResponse::Enqueued { id }`

**Durability:** Message survives restart after Midge write completes

**Notice emission:** TODO - should emit `notice://{realm}/{area}/{resource}/available` when message becomes ready (enables long polling wakeup)

### Reserve

```rust
pub fn handle_reserve(&mut self, lease_seconds: u64, batch_size: Option<usize>) -> QueueResponse
```

**Behavior:**
1. Pop up to `batch_size` (default 1) message IDs from `ready` queue
2. For each ID:
   - Load `QueueRecord` from Midge
   - Generate random `token` (u64)
   - Create `Inflight` entry with `expires_at = now + lease_seconds`
   - Insert timer into heap
   - Build `ReservedMessage { id, body, token, lease_seconds, attempts: record.attempts + 1 }`
3. Return `QueueResponse::Reserved { messages: Vec<ReservedMessage> }`

**Empty queue:** Returns `Reserved { messages: [] }` immediately (never blocks)

**Display attempts:** Client sees `attempts + 1` because attempt counting starts at 0 internally

**Long polling:** `wait_seconds` parameter is **handled by RPC layer**, not by `QueueActor` (see Long Polling section)

### Extend

```rust
pub fn handle_extend(&mut self, id: MessageId, token: u64, lease_seconds: u64) -> QueueResponse
```

**Behavior:**
1. Lookup `id` in `inflight` map
2. If not found: return `QueueResponse::NotFound`
3. If `inflight.token != token`: return `QueueResponse::InvalidToken`
4. Update `inflight.expires_at = now + lease_seconds`
5. Insert new timer (old timer becomes stale, ignored on fire)
6. Return `QueueResponse::Extended`

**Stale timers:** Old timers fire but check current `expires_at`; if extended, no action taken

### Complete

```rust
pub fn handle_complete(&mut self, id: MessageId, token: u64) -> QueueResponse
```

**Behavior:**
1. Lookup `id` in `inflight` map
2. If not found: return `QueueResponse::NotFound`
3. If `inflight.token != token`: return `QueueResponse::InvalidToken`
4. Remove from `inflight` map
5. Delete message from Midge storage
6. Return `QueueResponse::Completed`

**Idempotency:** Completing twice returns `NotFound` (not an error in at-least-once semantics)

## Delayed Enqueue Behavior

**Enqueue with `delay_seconds`:**
- Message written to Midge with `visible_at_ms = (now + delay_seconds)` as epoch millis
- Message added to `delayed` heap (not `ready`)
- Not visible to `reserve` operations

**Delayed message processing:**
- On every `receive()`, actor calls `process_delayed_messages()`
- Pops messages from `delayed` heap where `visible_at <= now`
- Moves message IDs to `ready` queue (back of queue, FIFO)

**Crash behavior:**
- Delayed messages persist in Midge with `visible_at_ms` timestamp
- On restart, manual recovery scan must check `visible_at_ms` and rebuild `delayed` heap
- MVP: Delayed messages may become immediately visible on restart (recovery not fully implemented)

## Lease Expiration and Retries

**Expiration timer fires:**
1. Actor calls `handle_lease_expired(id)`
2. Verify message still in `inflight` (may have been completed)
3. Verify current `expires_at` matches timer (may have been extended)
4. If expired: remove from `inflight`, increment `attempts` in Midge, check DLQ threshold
5. If not DLQ: add `id` to back of `ready` queue

**Attempts counter:**
- Starts at 0 on enqueue
- Incremented in storage on each lease expiration
- Displayed as `attempts + 1` to client

**Redelivery order:**
- Expired messages added to **back** of `ready` queue (not front)
- Maintains FIFO ordering with newly enqueued messages

## DLQ Policy Behavior

**Configuration:** `QueueActor::new(..., max_attempts: Option<u32>)`

**When `max_attempts = Some(n)`:**
- On lease expiration: increment `attempts` in storage
- If `attempts >= n`: message is DLQ'd
  - Delete message from Midge
  - Log: `DLQ: queue={...} message_id={...} attempts={...}` to stderr
  - **Do NOT re-enqueue** to ready queue
  - **Do NOT emit notice** (QueueActor never routes to other queues)
- If `attempts < n`: normal retry (increment, persist, re-enqueue)

**When `max_attempts = None` (default):**
- Messages retry indefinitely on lease expiration
- No DLQ behavior

**DLQ handling:**
- QueueActor only logs DLQ events
- External systems monitor logs/metrics to detect DLQ
- External systems emit notices (e.g., `notice://{realm}/{area}/dead`) if needed
- QueueActor never auto-enqueues to another queue

**Example:**
```rust
let actor = QueueActor::new(family, queue_key, store, Some(3)); // Max 3 attempts
// Enqueue: attempts=0
// 1st Reserve: client sees attempts=1, expires → storage attempts=1
// 2nd Reserve: client sees attempts=2, expires → storage attempts=2
// 3rd Reserve: client sees attempts=3, expires → storage attempts=3, DLQ'd
```

## Long Polling Behavior

**Purpose:** Reduce polling overhead for idle queues

**Implementation:** RPC layer only (not in QueueActor)

**Flow:**
1. RPC receives `Reserve { ..., wait_seconds: Some(60) }`
2. RPC calls `actor.handle_reserve()` **synchronously**
3. If `Reserved { messages: [] }` and `wait_seconds > 0`:
   - RPC subscribes to `notice://{realm}/{area}/{resource}/available`
   - RPC waits up to `wait_seconds` for notice or timeout
   - On notice or timeout: retry `actor.handle_reserve()`
4. If `Reserved { messages: [...] }`: return immediately

**QueueActor behavior:**
- Always returns immediately (never blocks)
- `wait_seconds` parameter unused by actor (logged, ignored)
- TODO: Emit `notice://{realm}/{area}/{resource}/available` on enqueue

**Notice semantics:**
- Best-effort hints (at-most-once delivery)
- No guarantee message still available when woken
- Thundering herd possible if many waiters

## Crash and Restart Semantics

### State Recovery

**On restart:**
1. `next_id` recovered from Midge (or defaults to 1)
2. Ready queue is **empty** (no automatic scan)
3. Inflight map is **empty** (all leases lost)
4. Delayed heap is **empty** (visibility timestamps lost)

**Message recovery:**
- Messages persist in Midge with `attempts`, `visible_at_ms`, `body`
- MVP: Manual recovery required (scan storage, rebuild `ready`/`delayed`)
- Leased messages become available again (at-least-once guarantee)

### Lease Loss on Restart

**Scenario:**
1. Client A reserves message ID 1 with token `abc123`
2. Server crashes before completion
3. Server restarts
4. Client A calls `complete(id=1, token=abc123)`
5. Response: `NotFound` (inflight map empty, token invalid)

**Client behavior:**
- Treat `NotFound` as "already completed or server restarted"
- Idempotent at-least-once processing must handle duplicate delivery

### Delayed Message Visibility on Restart

**Current behavior:**
- `visible_at_ms` persists in Midge
- On restart, delayed heap not rebuilt (MVP gap)
- Delayed messages may become immediately visible if manually added to `ready`

**Correct behavior (not implemented):**
- Scan Midge on startup
- Compare `visible_at_ms` to current time
- Rebuild `delayed` heap for future messages
- Add immediately visible messages to `ready`

## Error Handling

### QueueResponse Error Cases

| Operation | Error | Cause |
|-----------|-------|-------|
| Enqueue | `Error { message }` | Midge write failure |
| Reserve | `Reserved { messages: [] }` | Empty queue (not an error) |
| Extend | `NotFound` | Message not in inflight (completed or expired) |
| Extend | `InvalidToken` | Token mismatch |
| Complete | `NotFound` | Message not in inflight |
| Complete | `InvalidToken` | Token mismatch |

### Storage Errors

**On Midge failure:**
- Enqueue: returns `Error` response
- Reserve: logs warning, skips corrupted message, continues batch
- Expiration: logs warning, message lost (no re-enqueue)

### Authorization

**SessionActor enforcement:**
- `enqueue`: requires `Write` access
- `reserve`: requires `Read` access
- `extend`: requires `Write` access
- `complete`: requires `Write` access

**Pattern:**
```rust
if !session.permissions.allows(&route, Access::Write) {
    return Err("unauthorized: enqueue".to_string());
}
```

## Observability

### Logging

**Current implementation:**
- DLQ events: `eprintln!("DLQ: queue={:?} message_id={} attempts={} - Message moved to dead letter queue")`
- Storage warnings: `eprintln!("WARN: Failed to persist message: {:?}")`
- Corruption: `eprintln!("WARN: Failed to decode message {}: {}")`
- Redelivery errors: `eprintln!("WARN: Failed to increment attempts for message {}: {:?}")`

### Metrics

**Not implemented:**
- Enqueue rate
- Reserve rate
- Completion rate
- DLQ rate
- Queue depth
- Lease duration histograms
- Message age

### Notices

**Planned (not implemented):**
- `notice://{realm}/{area}/{resource}/available` on enqueue (enables long polling)
- External emission of `notice://{realm}/{area}/dead` on DLQ (outside QueueActor)

## Explicit Non-Features

### Not Implemented

- ❌ Multi-node queues (single-node only)
- ❌ Distributed coordination (no Raft, no leader election)
- ❌ Persisted lease state (leases are ephemeral)
- ❌ Priority queues (FIFO only)
- ❌ Message deduplication (clients must handle idempotency)
- ❌ Automatic recovery scan on startup (manual `ready` queue rebuild)
- ❌ Delayed message heap rebuild on restart
- ❌ Transaction support (operations are atomic but independent)
- ❌ Queue-to-queue routing (QueueActor never enqueues to other queues)
- ❌ Backpressure (no flow control, unlimited enqueue rate)
- ❌ Message size limits (limited only by Midge capacity)
- ❌ TTL / expiration for messages (only lease expiration)
- ❌ Metrics emission (only stderr logging)

### Intentional Design Choices

- **QueueActor never blocks:** Long polling handled by RPC layer
- **Leases are ephemeral:** Simplifies crash recovery, clients retry on restart
- **Notices are hints:** No guarantee of message availability
- **No persisted ready queue:** Rebuilt on restart (MVP: manual)
- **No cross-queue routing:** DLQ handling is external, explicit
- **At-least-once only:** Exactly-once requires client-side deduplication
- **Single-node only:** Horizontal scaling via queue sharding (separate actors)

## Performance Characteristics

### Operation Costs

| Operation | In-Memory | Midge I/O | Complexity |
|-----------|-----------|-----------|------------|
| Enqueue | O(log n) delayed heap insert | 2 writes (counter + message) | O(1) ready insert |
| Reserve | O(1) pop front | 1 read per message | O(log n) timer insert per message |
| Extend | O(1) map update | None | O(log n) timer insert |
| Complete | O(1) map remove | 1 delete | O(1) |
| Timer expiration | O(log n) heap pop | 1 read + 1 write (attempts) | O(1) re-enqueue |

### Latency Targets

- **Enqueue:** <10µs in-memory + Midge write (typically <1ms)
- **Reserve:** <100µs in-memory + Midge read per message
- **Complete:** <5µs in-memory + Midge delete

### Scalability Limits

- **Single queue throughput:** ~10K-100K ops/sec (Midge-limited)
- **Queue depth:** Unlimited (Midge capacity)
- **Concurrent workers:** Unlimited (FIFO reservation, no contention)
- **Lease count:** Limited by memory (ephemeral inflight map)

## Testing

### Unit Tests (14 tests)

- `should_enqueue_and_reserve_message`
- `should_return_empty_when_reserving_empty_queue`
- `should_complete_message_with_valid_token`
- `should_reject_complete_with_invalid_token`
- `should_extend_lease_with_valid_token`
- `should_reject_extend_with_invalid_token`
- `should_redelivery_message_when_lease_expires`
- `should_reserve_multiple_messages_in_batch`
- `should_ignore_stale_timer_after_extend`
- `should_reject_operations_on_expired_lease`
- `should_return_not_found_for_nonexistent_message`
- `should_delay_message_visibility`
- `should_move_to_dlq_after_max_attempts`
- `should_allow_unlimited_retries_when_max_attempts_is_none`

### Integration Tests

- `should_recover_messages_after_restart` (manual recovery)
- Performance tests (marked `#[ignore]` - slow):
  - `should_handle_high_volume_enqueue`
  - `should_handle_concurrent_workers`
  - `should_have_low_reserve_latency`
  - `should_have_low_complete_latency`

### Authorization Tests (7 tests)

- `should_reject_unauthenticated_enqueue`
- `should_allow_authorized_enqueue`
- `should_reject_unauthorized_reserve`
- `should_allow_authorized_reserve_with_read_permission`
- `should_reject_unauthorized_extend`
- `should_reject_unauthorized_complete`
- `should_allow_authorized_complete_with_write_permission`

## Summary

The Fitz Queue domain provides single-node, durable, FIFO queues with at-least-once delivery. Messages are persisted to Midge for crash recovery, while scheduling state (ready queue, leases, timers) is ephemeral. The `QueueActor` never blocks, handling all operations synchronously. Long polling is delegated to the RPC layer via notice subscriptions. DLQ behavior is configurable, with external systems responsible for monitoring and routing dead-lettered messages. The implementation prioritizes simplicity and deterministic behavior over distributed coordination.
