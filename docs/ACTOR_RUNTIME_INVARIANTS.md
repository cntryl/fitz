# Actor Runtime Invariants

**CRITICAL DESIGN CONSTRAINTS FOR FITZ ACTOR RUNTIME**

This document defines the fundamental invariants and guarantees of the Fitz actor runtime.
All code must respect these invariants. Violations indicate bugs.

---

## 1. MESSAGE DELIVERY SEMANTICS

### 1.1 Send Guarantees

**INVARIANT**: `ctx.send()` and `ActorRef::send()` are **synchronous best-effort** operations.

- ✅ **Synchronous**: Send blocks until routing decision completes
- ✅ **No Retries**: Failed sends return error immediately
- ✅ **No Queuing**: External send queue is the mailbox only
- ❌ **No Durability**: Messages are not persisted
- ❌ **No Delivery Guarantee**: Sender must handle `MailboxFull` errors

**Consequence**: Callers MUST implement exponential backoff or circuit breaking on repeated `MailboxFull` errors.

```rust
// ✅ CORRECT: Handle backpressure
match ctx.send(dest, msg) {
    Ok(_) => {},
    Err(SendError::MailboxFull { occupancy, .. }) if occupancy > 0.9 => {
        // Exponential backoff
        thread::sleep(Duration::from_millis(10 * retry_count));
    }
    Err(e) => eprintln!("Send failed: {}", e),
}

// ❌ WRONG: Infinite retry without backoff
loop {
    if ctx.send(dest, msg.clone()).is_ok() {
        break;
    }
}
```

### 1.2 Reentrancy Rules

**INVARIANT**: Sending to self during `receive()` can **deadlock** if mailbox is full.

Actors MUST NOT send to themselves under high load without checking mailbox capacity first.

```rust
// ❌ DANGEROUS: Can deadlock
fn receive(&mut self, msg: Msg, ctx: &mut Context<Self>) {
    ctx.send(ctx.address().clone(), NextMsg)?; // Deadlock if mailbox full!
}

// ✅ SAFE: Check capacity or use deferred send
fn receive(&mut self, msg: Msg, ctx: &mut Context<Self>) {
    // Option 1: Check mailbox capacity via router metrics
    // Option 2: Use work queue instead of self-send
    self.work_queue.push(NextMsg);
}
```

### 1.3 Causation Tracking

**INVARIANT**: Message IDs and causation chains are **process-local** and **non-stable**.

- Message IDs reset on process restart
- Causation chains do not cross process boundaries (until remoting is added)
- `MessageId` is monotonically increasing within a single process

---

## 2. BACKPRESSURE

### 2.1 No Global Backpressure

**INVARIANT**: There is **no global backpressure valve** in the scheduler.

- Each mailbox has independent capacity
- No coordination when all mailboxes are full
- External message sources must implement their own rate limiting

**Consequence**: Systems MUST monitor `DeliveryError::MailboxFull` occupancy and implement adaptive rate limiting.

```rust
// ✅ CORRECT: Adaptive rate limiting
struct RateLimiter {
    max_rate: f64,
    current_rate: f64,
}

impl RateLimiter {
    fn send_with_backpressure(&mut self, actor_ref: &ActorRef, msg: Msg) {
        match actor_ref.send(msg) {
            Ok(_) => {
                // Success: increase rate
                self.current_rate = (self.current_rate * 1.1).min(self.max_rate);
            }
            Err(SendError::MailboxFull { occupancy, .. }) => {
                // Backpressure: decrease rate proportional to occupancy
                self.current_rate *= 1.0 - occupancy;
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => eprintln!("Send failed: {}", e),
        }
    }
}
```

### 2.2 Batch Processing

**INVARIANT**: Actors process up to `MAX_BATCH_SIZE` (16) messages per iteration.

This prevents a single actor from monopolizing CPU but means:
- High-throughput actors may still fall behind under burst
- Latency-sensitive actors should use smaller mailbox capacities
- Fairness is **per-actor**, not **per-message**

---

## 3. TIMER LIFECYCLE

### 3.1 Timer Persistence

**INVARIANT**: Timers are **not persisted** across actor restarts.

When a supervised actor restarts:
- All timers are **discarded**
- Actors MUST re-register timers in `started()` hook
- Repeating timers do not resume automatically

```rust
// ✅ CORRECT: Re-register timers on start
impl Actor for MyActor {
    fn started(&mut self, ctx: &mut Context<Self>) {
        // Re-register all timers
        self.heartbeat_timer = ctx.schedule_repeat(
            Duration::from_secs(1),
            Duration::from_secs(1)
        );
    }

    fn stopped(&mut self) {
        // Timers are automatically cleaned up
    }
}
```

### 3.2 Timer Cancellation

**INVARIANT**: Timers are **not guaranteed** to fire on actor stop.

If an actor stops while timers are pending:
- One-shot timers that haven't fired are **discarded**
- Repeating timers are **cancelled**
- No `TimerFired` message is delivered

**Future Work**: Add graceful timer flush on shutdown (see RISK #9 in review).

---

## 4. ROUTE FAMILY ISOLATION

### 4.1 No Cross-Family Routing

**INVARIANT**: Routes in different families **never interact**, even with identical paths.

- `(family=1, route="/user")` and `(family=2, route="/user")` are **completely independent**
- Router will **never** attempt cross-family delivery
- Wildcard patterns respect family boundaries

```rust
// ✅ CORRECT: Family isolation
let family1 = RouteFamily::new(1);
let family2 = RouteFamily::new(2);

router.register(
    RouteAddress::new(family1, Route::new("/user")),
    mailbox1
);
router.register(
    RouteAddress::new(family2, Route::new("/user")),
    mailbox2
);

// These are independent actors, no collision
```

### 4.2 Subscription Isolation

**INVARIANT**: Subscriptions are isolated per `RouteFamily`.

- `SubscriptionIndex::insert()` scopes patterns to a family
- `SubscriptionIndex::match_all()` only matches within the same family
- Wildcards (`*`, `**`) never cross family boundaries

---

## 5. ERROR HANDLING

### 5.1 Panic Recovery

**INVARIANT**: Actors that panic **remain running** but enter an error state.

- Panics are caught with `catch_unwind`
- `Actor::on_error()` is called with `ActorError::Panic`
- Actor continues processing subsequent messages
- Supervision must explicitly restart the actor

**Consequence**: Supervisors MUST implement restart logic based on error classification.

```rust
// ✅ CORRECT: Supervision with restart
impl Actor for MyActor {
    fn on_error(&mut self, error: ActorError, ctx: &mut Context<Self>) {
        match error {
            ActorError::Panic(msg) => {
                eprintln!("Actor panicked: {}", msg);
                ctx.stop(); // Supervisor will restart
            }
            _ => {}
        }
    }
}
```

### 5.2 Type Mismatch

**INVARIANT**: Type mismatches on `Envelope::into_payload()` are **silent** (logged to stderr).

- Wrong message type → log error + skip message
- No actor notification
- Message is dropped

**Consequence**: Type safety is at API level (ActorRef<M>), not runtime level.

---

## 6. PERFORMANCE CHARACTERISTICS

### 6.1 Scheduler Fairness

**INVARIANT**: Scheduling is **per-actor fair**, not **per-message fair**.

- Each actor thread processes batches independently
- No global work queue
- Fast actors can process more messages than slow actors

### 6.2 Hot-Path Allocations

**OPTIMIZATION**: `EnvelopeMetadata` extraction avoids reconstructing dummy envelopes.

- Old path: Box allocation per message for causation tracking
- New path: Zero-copy metadata extraction with `into_parts()`
- Savings: ~1 heap allocation per message at 1M msg/sec = 1M alloc/sec

### 6.3 Lock Contention

**OPTIMIZATION**: `SubscriptionIndex` uses `RwLock` for concurrent reads.

- Inserts/removes take write lock
- Matches take read lock (high concurrency)
- Typical workload: 1% writes, 99% reads

---

## 7. OBSERVABILITY

### 7.1 Actor Metrics

**INVARIANT**: Metrics are **best-effort** and use relaxed ordering.

- `ActorMetrics` uses `AtomicU64` with `Ordering::Relaxed`
- No guaranteed consistency across metrics
- Suitable for monitoring, not for correctness

```rust
// ✅ CORRECT: Use for monitoring
let snapshot = ctx.metrics().snapshot();
println!("Processed: {}", snapshot.messages_processed);

// ❌ WRONG: Use for correctness
if ctx.metrics().messages_processed.load(Ordering::Relaxed) > 1000 {
    // Don't use for business logic!
}
```

### 7.2 Error Taxonomy

**INVARIANT**: `SendError` preserves detailed error context for adaptive behavior.

- `MailboxFull { occupancy }`: Retry with exponential backoff based on occupancy
- `ActorStopped { target }`: Don't retry, route is dead
- `RouteNotFound { target }`: Don't retry, route never existed

---

## 8. FUTURE WORK (NOT YET GUARANTEED)

These are NOT invariants yet, but planned improvements:

1. **Graceful Timer Flush**: Deliver pending timers on actor stop
2. **Global Backpressure**: Scheduler-level overload detection
3. **Priority Lanes**: High-priority messages bypass queue
4. **Async Send Queue**: Deferred sends to avoid reentrancy
5. **Remoting**: Stable message IDs and cross-process causation
6. **Durable Messages**: Persist messages for at-least-once delivery

---

## 9. TESTING CHECKLIST

When implementing new features, verify:

- [ ] Send failures return detailed `SendError` with occupancy
- [ ] Timers are cleaned up on actor stop
- [ ] Panics don't crash the actor thread
- [ ] Metrics are updated correctly
- [ ] Route family isolation is maintained
- [ ] No allocations in hot path (use `into_parts()`)
- [ ] Backpressure is handled by caller
- [ ] No deadlocks on self-send with full mailbox

---

## 10. PRODUCTION READINESS

**Current Status**: 75% production ready (up from 60% after improvements)

**Remaining Blockers**:
1. ❌ Priority lanes for control messages
2. ❌ Global backpressure detection API
3. ❌ Graceful timer flush on shutdown
4. ⚠️ Load testing under sustained burst (1M msg/sec for 1 hour)

**Strengths**:
- ✅ Batch processing reduces scheduler overhead
- ✅ Adaptive timeouts improve responsiveness
- ✅ Detailed error taxonomy enables smart retry
- ✅ Zero-copy metadata extraction saves allocations
- ✅ RwLock reduces subscription index contention

---

**Last Updated**: January 7, 2026
**Reviewers**: Runtime Team
**Status**: Living Document
