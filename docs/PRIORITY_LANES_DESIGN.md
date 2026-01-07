# Priority Lanes Design for Fitz Actor Runtime

**Status**: Design Review  
**Date**: January 7, 2026  
**Reviewers**: Runtime Team

---

## Motivation

**Problem**: Under sustained data-plane load (e.g., 1M user messages/sec), control-plane messages experience unbounded latency:
- Timer expirations delayed by queue depth
- Supervision signals (restart, stop) blocked
- Session/auth renewals timeout
- Lease heartbeats miss deadlines

**Example Failure Mode**:
```
T=0s:   Lease heartbeat enqueued (mailbox has 50K user messages ahead)
T=0.5s: Heartbeat should fire, but still in queue
T=1s:   Lease expires server-side
T=2s:   Heartbeat finally processed → "lease not found" error
```

**Solution**: Separate high-priority lane for runtime-internal messages.

---

## Design Principles

1. **Simplicity over Flexibility**: Exactly two lanes, no configurability
2. **Explicit Priority**: Priority is set at enqueue time, never changes
3. **Strict Access Control**: High lane is runtime-internal only
4. **Fairness**: Normal lane guaranteed forward progress
5. **Predictability**: Bounded high-priority processing per tick
6. **Fail Fast**: High lane overflow returns error immediately
7. **Benchmarkable**: Design enables proving no Normal-lane regression

---

## Architecture

### Lane Semantics

| Property | High Lane | Normal Lane |
|----------|-----------|-------------|
| **Purpose** | Control plane | Data plane |
| **Access** | Runtime internal only | Public API (`ActorRef::send`) |
| **Capacity** | Same as mailbox | Same as mailbox |
| **Overflow** | Return error | Return error |
| **Ordering** | FIFO within lane | FIFO within lane |
| **Priority** | Serviced first (capped) | Serviced after high (guaranteed) |

### Message Classification

**High Priority** (Runtime Internal):
```rust
enum HighPriorityMessage {
    TimerFired(TimerId),
    SupervisionCommand(SupervisionCommand),
    LeaseHeartbeat(LeaseId),
    SessionRefresh(SessionId),
    ActorStopping(ActorId),
}
```

**Normal Priority** (User Messages):
- All user-defined actor messages
- All domain handler messages
- All external messages

### Invariants

#### INV-1: High-Lane Cap
**INVARIANT**: Scheduler processes at most `MAX_HIGH_PER_TICK` high-priority messages per tick.

```
MAX_HIGH_PER_TICK = 4
```

**Rationale**:
- Small enough to guarantee Normal-lane latency
- Large enough for typical control-plane bursts
- Conservative: Control messages are rare

**Consequence**: If 10 timers fire simultaneously:
- Tick 1: Process 4 timer messages
- Tick 2: Process 4 timer messages
- Tick 3: Process 2 timer messages + Normal messages

#### INV-2: Forward Progress
**INVARIANT**: Normal lane processes at least 1 message per tick if non-empty.

**Proof**:
```
per_tick_work = HIGH (capped at 4) + NORMAL (at least 1)
∴ Normal lane cannot starve
```

#### INV-3: Ordering Within Lane
**INVARIANT**: Messages within the same lane are processed in FIFO order.

**Consequence**: If High lane has messages [H1, H2, H3, H4, H5]:
- Tick 1: Process H1, H2, H3, H4 (in order)
- Tick 2: Process H5

#### INV-4: Overflow Semantics
**INVARIANT**: Enqueueing to a full lane fails immediately with detailed error.

```rust
Err(SendError::HighLaneFull { 
    target: RouteAddress,
    capacity: usize,
})
```

**No degradation**: Overflow high-priority messages are NOT moved to Normal lane.

---

## Scheduler Loop Pseudocode

```rust
// Constants
const MAX_HIGH_PER_TICK: usize = 4;
const MAX_NORMAL_PER_TICK: usize = 12; // 16 total - 4 high
const POLL_TIMEOUT_MS: u64 = 100;

// Per-tick processing
loop {
    if !ctx.is_running() {
        break;
    }

    let mut processed_high = 0;
    let mut processed_normal = 0;

    // Phase 1: High-priority (capped)
    while processed_high < MAX_HIGH_PER_TICK {
        match high_receiver.try_recv() {
            Ok(envelope) => {
                process_envelope(envelope, &mut actor, &mut ctx);
                processed_high += 1;
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                ctx.stop();
                break;
            }
        }
    }

    // Phase 2: Normal-priority (remaining budget OR at least 1)
    let normal_budget = if processed_high == 0 {
        // If no high messages, use full batch size
        MAX_HIGH_PER_TICK + MAX_NORMAL_PER_TICK
    } else {
        // If we processed high messages, use remaining budget
        MAX_NORMAL_PER_TICK
    };

    while processed_normal < normal_budget {
        let envelope = if processed_high == 0 && processed_normal == 0 {
            // First message overall: blocking receive
            match normal_receiver.recv_timeout(Duration::from_millis(POLL_TIMEOUT_MS)) {
                Ok(env) => env,
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    ctx.stop();
                    break;
                }
            }
        } else {
            // Subsequent messages: non-blocking
            match normal_receiver.try_recv() {
                Ok(env) => env,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    ctx.stop();
                    break;
                }
            }
        };

        process_envelope(envelope, &mut actor, &mut ctx);
        processed_normal += 1;
    }

    // If we processed nothing in both phases, we already waited in blocking receive
}
```

### Key Properties

1. **High messages process first**: Try high channel before normal
2. **Strict cap**: Stop at MAX_HIGH_PER_TICK regardless of queue depth
3. **Normal guaranteed**: Always try normal channel after high
4. **Adaptive budget**: Normal gets more budget if high was idle
5. **No starvation**: Normal always gets at least 1 message (if available)

---

## Mailbox Changes

### New Structure

```rust
pub struct Mailbox {
    high_priority: Sender<Envelope>,
    high_receiver: Receiver<Envelope>,
    normal: Sender<Envelope>,
    normal_receiver: Receiver<Envelope>,
    capacity: usize,
}

impl Mailbox {
    pub fn new(capacity: usize) -> Self {
        let (high_tx, high_rx) = bounded(capacity);
        let (normal_tx, normal_rx) = bounded(capacity);
        Self {
            high_priority: high_tx,
            high_receiver: high_rx,
            normal: normal_tx,
            normal_receiver: normal_rx,
            capacity,
        }
    }

    /// Get sender for high-priority messages (runtime-internal only)
    pub(crate) fn high_priority_sender(&self) -> Sender<Envelope> {
        self.high_priority.clone()
    }

    /// Get receiver for high-priority messages (scheduler only)
    pub(crate) fn high_priority_receiver(&self) -> &Receiver<Envelope> {
        &self.high_receiver
    }

    /// Get sender for normal-priority messages (public API)
    pub fn sender(&self) -> Sender<Envelope> {
        self.normal.clone()
    }

    /// Get receiver for normal-priority messages (scheduler only)
    pub fn receiver(&self) -> &Receiver<Envelope> {
        &self.normal_receiver
    }

    /// Check if high-priority lane is empty
    pub fn high_priority_is_empty(&self) -> bool {
        self.high_receiver.is_empty()
    }

    /// Get high-priority lane depth
    pub fn high_priority_len(&self) -> usize {
        self.high_receiver.len()
    }
}
```

### MailboxSink Implementation

```rust
impl MailboxSink for Mailbox {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // Always use normal lane for external delivery
        self.normal.try_send(envelope).map_err(|e| match e {
            TrySendError::Full(_) => DeliveryError::MailboxFull {
                capacity: self.capacity,
                current_len: self.normal_receiver.len(),
            },
            TrySendError::Disconnected(_) => DeliveryError::ActorStopped,
        })
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // High-priority delivery (runtime-internal only)
        self.high_priority.try_send(envelope).map_err(|e| match e {
            TrySendError::Full(_) => DeliveryError::HighLaneFull {
                capacity: self.capacity,
                current_len: self.high_receiver.len(),
            },
            TrySendError::Disconnected(_) => DeliveryError::ActorStopped,
        })
    }
}
```

### New DeliveryError Variant

```rust
pub enum DeliveryError {
    MailboxFull { capacity: usize, current_len: usize },
    HighLaneFull { capacity: usize, current_len: usize },
    ActorStopped,
}
```

---

## Router Changes

### Internal High-Priority Routing

```rust
impl Router {
    /// Route a high-priority envelope (runtime-internal only)
    pub(crate) fn route_high_priority(&self, envelope: Envelope) -> Result<(), RouteError> {
        let dest = envelope.destination().clone();

        let sink = self
            .registry
            .get(&dest)
            .ok_or_else(|| RouteError::RouteNotFound(dest.clone()))?;

        sink.deliver_high_priority(envelope)
            .map_err(|e| RouteError::DeliveryFailed(dest, e))
    }
}
```

### MailboxSink Trait Update

```rust
pub trait MailboxSink: Send + Sync {
    /// Deliver to normal lane
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError>;

    /// Deliver to high-priority lane (runtime-internal only)
    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError>;
}
```

---

## Context Changes

### Internal Send Method

```rust
impl<A: Actor + ?Sized> Context<A> {
    /// Send high-priority message (runtime-internal only)
    pub(crate) fn send_high_priority<M>(&self, dest: RouteAddress, msg: M) -> Result<(), SendError>
    where
        M: Send + Sync + 'static,
    {
        let envelope = Envelope::from_route(self.address.clone(), dest.clone(), msg);
        
        self.router.route_high_priority(envelope).map_err(|e| match e {
            RouteError::RouteNotFound(target) => SendError::RouteNotFound { target },
            RouteError::DeliveryFailed(target, delivery_err) => match delivery_err {
                DeliveryError::HighLaneFull { capacity, current_len } => {
                    SendError::HighLaneFull {
                        target,
                        occupancy: current_len as f64 / capacity as f64,
                    }
                }
                DeliveryError::ActorStopped => SendError::ActorStopped { target },
                _ => SendError::RouteNotFound { target },
            },
        })
    }
}
```

### SendError Update

```rust
pub enum SendError {
    MailboxFull { target: RouteAddress, occupancy: f64 },
    HighLaneFull { target: RouteAddress, occupancy: f64 },
    ActorStopped { target: RouteAddress },
    RouteNotFound { target: RouteAddress },
}
```

---

## Usage Examples

### Timer System (High Priority)

```rust
impl TimerManager {
    fn fire_timer(&mut self, ctx: &Context<A>, timer_id: TimerId) {
        // Send timer fired message via high-priority lane
        let msg = InternalMessage::TimerFired(timer_id);
        if let Err(e) = ctx.send_high_priority(ctx.address().clone(), msg) {
            match e {
                SendError::HighLaneFull { .. } => {
                    // Critical: timer system backlogged
                    eprintln!("CRITICAL: Timer high-priority lane full!");
                    // Could cancel timer or implement overflow buffer
                }
                _ => {}
            }
        }
    }
}
```

### Supervision (High Priority)

```rust
impl Supervisor {
    fn restart_actor(&self, actor_ref: &ActorRef<A>) {
        let msg = InternalMessage::SupervisionRestart;
        // Use router's high-priority path
        if let Err(e) = self.router.route_high_priority(
            Envelope::new(actor_ref.address().clone(), msg)
        ) {
            // Supervision command failed - critical error
            panic!("Cannot deliver supervision command: {}", e);
        }
    }
}
```

### User Messages (Normal Priority)

```rust
// Users only have access to normal-priority send
impl Actor for MyActor {
    fn receive(&mut self, msg: Msg, ctx: &mut Context<Self>) {
        // ctx.send() always uses normal priority
        ctx.send(other_actor, UserMessage::Data).ok();
    }
}

// External sends also normal priority
actor_ref.send(UserMessage::Data)?;
```

---

## Fairness Analysis

### Worst-Case Normal-Lane Latency

**Scenario**: High lane continuously full

```
Tick 1: Process 4 high + 12 normal
Tick 2: Process 4 high + 12 normal
Tick 3: Process 4 high + 12 normal
...

Normal lane throughput = 12 messages per tick
High lane throughput = 4 messages per tick
Ratio = 12:4 = 3:1

∴ Normal lane gets 75% of processing capacity even under max high-priority load
```

**Guarantee**: Normal-lane message processes within `(mailbox_depth / 12) + 1` ticks.

Example with 100-message backlog:
- Max latency = ceil(100 / 12) + 1 = 9 ticks
- At 10ms per tick = 90ms maximum latency

### High-Lane Starvation Prevention

High lane cannot starve because:
1. Cap of 4 messages per tick → always yields to normal
2. If high lane is empty, normal gets full 16-message batch

---

## Benchmark Strategy

### Goal: Prove No Regression in Normal Lane

#### Benchmark 1: Baseline (High Lane Idle)
```rust
#[bench]
fn normal_lane_latency_baseline(b: &mut Bencher) {
    // Setup: Actor with empty high lane
    // Measure: End-to-end latency for normal messages
    // Expected: Same as current performance (~10µs)
}
```

#### Benchmark 2: High Lane Saturated
```rust
#[bench]
fn normal_lane_latency_under_high_pressure(b: &mut Bencher) {
    // Setup: High lane continuously full (e.g., timers firing)
    // Measure: Normal message latency
    // Expected: <2x baseline (20µs), verifies 3:1 ratio
}
```

#### Benchmark 3: Mixed Workload
```rust
#[bench]
fn mixed_workload(b: &mut Bencher) {
    // Setup: 10% high, 90% normal (realistic ratio)
    // Measure: P50, P99 latency for both lanes
    // Expected: Normal lane P99 < 15µs
}
```

#### Benchmark 4: High Lane Overflow
```rust
#[bench]
fn high_lane_overflow_handling(b: &mut Bencher) {
    // Setup: Fill high lane to capacity
    // Measure: Overflow error return time
    // Expected: Immediate (< 1µs, no blocking)
}
```

### Regression Criteria

**MUST NOT REGRESS**:
- Normal lane baseline latency (±5% acceptable)
- Normal lane throughput under idle high lane

**MUST IMPROVE**:
- Control message latency under normal load (target: <50µs P99)

**NEW GUARANTEES**:
- Normal lane latency bounded even under high saturation
- High-priority overflow fails fast (no cascading delays)

---

## Migration Plan

### Phase 1: Add Dual Channels to Mailbox
```diff
pub struct Mailbox {
+   high_priority: Sender<Envelope>,
+   high_receiver: Receiver<Envelope>,
    normal: Sender<Envelope>,
-   receiver: Receiver<Envelope>,
+   normal_receiver: Receiver<Envelope>,
    capacity: usize,
}
```

### Phase 2: Update Scheduler Loop
- Add high-priority processing phase
- Enforce MAX_HIGH_PER_TICK cap
- Adjust normal budget based on high usage

### Phase 3: Add Internal APIs
- `Context::send_high_priority()`
- `Router::route_high_priority()`
- `MailboxSink::deliver_high_priority()`

### Phase 4: Migrate Control Plane
- Timer fired messages → high lane
- Supervision commands → high lane
- Lease heartbeats → high lane
- Session refresh → high lane

### Phase 5: Benchmark & Validate
- Run all benchmarks
- Verify no normal-lane regression
- Validate high-lane latency improvements
- Load test at 1M msg/sec

---

## Testing Strategy

### Unit Tests

```rust
#[test]
fn should_process_high_before_normal() {
    // Enqueue 1 high, 1 normal
    // Verify high processes first
}

#[test]
fn should_cap_high_processing_per_tick() {
    // Enqueue 10 high messages
    // Verify only 4 process per tick
}

#[test]
fn should_guarantee_normal_forward_progress() {
    // Fill high lane, enqueue 1 normal
    // Verify normal processes within N ticks
}

#[test]
fn should_fail_fast_on_high_overflow() {
    // Fill high lane to capacity
    // Verify next enqueue returns error immediately
}

#[test]
fn should_reject_high_priority_from_user_code() {
    // Verify ActorRef::send() cannot access high lane
    // Verify Context::send() uses normal lane
}
```

### Integration Tests

```rust
#[test]
fn should_deliver_timer_under_load() {
    // Setup: Actor processing 10K normal messages
    // Action: Schedule timer
    // Verify: Timer fires within 100ms
}

#[test]
fn should_handle_supervision_under_saturation() {
    // Setup: Actor with full normal mailbox
    // Action: Supervisor sends restart command
    // Verify: Restart processes quickly
}
```

---

## Failure Modes & Mitigations

### Failure Mode 1: High Lane Overflow
**Symptom**: `HighLaneFull` errors from timer/supervision systems

**Mitigation**:
```rust
// Timer system overflow buffer
const TIMER_OVERFLOW_CAPACITY: usize = 16;

if let Err(SendError::HighLaneFull { .. }) = ctx.send_high_priority(...) {
    self.overflow_buffer.push(timer_id);
    if self.overflow_buffer.len() > TIMER_OVERFLOW_CAPACITY {
        // Panic or drop oldest
        panic!("Timer system collapse - high lane continuously full");
    }
}
```

### Failure Mode 2: Normal Lane Starvation (Impossible)
**Symptom**: Normal messages never process

**Why Impossible**: MAX_HIGH_PER_TICK cap guarantees normal gets budget

**Validation**: Formal proof via invariants

### Failure Mode 3: Priority Inversion
**Symptom**: High-priority message stuck behind normal message

**Why Impossible**: Separate channels, high processed first

**Edge Case**: Message already being processed when high message arrives
- Acceptable: Current message completes, high processes next
- Not true priority inversion (no blocking)

---

## Open Questions

### Q1: Should high lane be smaller than normal?
**Answer**: No - same capacity for simplicity.
- Rationale: High-priority messages are rare, overflow unlikely
- If overflow occurs, it's a system-wide failure (panic acceptable)

### Q2: What if actor panics while processing high message?
**Answer**: Same panic recovery as normal messages.
- High lane doesn't change error handling
- Supervisor gets notified via existing path

### Q3: Should we track high/normal in metrics separately?
**Answer**: Yes, add to ActorMetrics.
```rust
pub struct ActorMetrics {
    pub messages_processed_high: AtomicU64,
    pub messages_processed_normal: AtomicU64,
    // ...
}
```

---

## Alternatives Considered

### Alternative 1: Single Queue with Priority Field
**Rejected**: Requires sorting/heap, unpredictable latency

### Alternative 2: Three+ Priority Levels
**Rejected**: Complexity, benchmarking nightmare, fairness hard to reason about

### Alternative 3: Configurable MAX_HIGH_PER_TICK
**Rejected**: Configuration surface, unpredictable behavior

### Alternative 4: Dynamic Priority Adjustment
**Rejected**: Non-deterministic, breaks fairness guarantees

---

## Approval Checklist

- [ ] Design reviewed by runtime team
- [ ] Invariants validated (no starvation proof)
- [ ] Benchmark strategy approved
- [ ] Overflow behavior acceptable
- [ ] Migration plan feasible
- [ ] Test coverage sufficient
- [ ] Documentation complete

---

## Sign-off

**Design Status**: Ready for Implementation  
**Estimated Effort**: 8 hours  
**Risk Level**: Low (isolated changes, well-defined invariants)  
**Rollback Plan**: Feature flag + fallback to single queue

