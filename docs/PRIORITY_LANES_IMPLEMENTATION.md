# Priority Lanes Implementation Checklist

**Goal**: Add high-priority lane with minimal, surgical changes.  
**Estimated Effort**: 8 hours  
**Risk**: Low (isolated changes, proven correct)

---

## Phase 1: Mailbox Changes (2 hours)

### File: `src/runtime/mailbox.rs`

**Changes**:
1. Add second channel for high-priority
2. Make `high_priority_sender()` pub(crate)
3. Add `deliver_high_priority()` to MailboxSink

**Diff Preview**:
```rust
pub struct Mailbox {
+   high_priority: Sender<Envelope>,
+   high_receiver: Receiver<Envelope>,
    sender: Sender<Envelope>,
    receiver: Receiver<Envelope>,
    capacity: usize,
}

impl Mailbox {
    pub fn new(capacity: usize) -> Self {
+       let (high_tx, high_rx) = bounded(capacity);
        let (sender, receiver) = bounded(capacity);
        Self {
+           high_priority: high_tx,
+           high_receiver: high_rx,
            sender,
            receiver,
            capacity,
        }
    }

+   pub(crate) fn high_priority_sender(&self) -> Sender<Envelope> {
+       self.high_priority.clone()
+   }

+   pub(crate) fn high_priority_receiver(&self) -> &Receiver<Envelope> {
+       &self.high_receiver
+   }

+   pub fn high_priority_len(&self) -> usize {
+       self.high_receiver.len()
+   }
}

impl MailboxSink for Mailbox {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // Existing code unchanged
    }

+   fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
+       self.high_priority.try_send(envelope).map_err(|e| match e {
+           TrySendError::Full(_) => DeliveryError::HighLaneFull {
+               capacity: self.capacity,
+               current_len: self.high_receiver.len(),
+           },
+           TrySendError::Disconnected(_) => DeliveryError::ActorStopped,
+       })
+   }
}
```

---

## Phase 2: Router Changes (1 hour)

### File: `src/runtime/router.rs`

**Changes**:
1. Add `HighLaneFull` variant to `DeliveryError`
2. Add `route_high_priority()` method (pub(crate))
3. Add `deliver_high_priority()` to MailboxSink trait

**Diff Preview**:
```rust
pub enum DeliveryError {
    MailboxFull { capacity: usize, current_len: usize },
+   HighLaneFull { capacity: usize, current_len: usize },
    ActorStopped,
}

pub trait MailboxSink: Send + Sync {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError>;
+   fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError>;
}

impl Router {
+   pub(crate) fn route_high_priority(&self, envelope: Envelope) -> Result<(), RouteError> {
+       let dest = envelope.destination().clone();
+       let sink = self.registry.get(&dest)
+           .ok_or_else(|| RouteError::RouteNotFound(dest.clone()))?;
+       sink.deliver_high_priority(envelope)
+           .map_err(|e| RouteError::DeliveryFailed(dest, e))
+   }
}
```

---

## Phase 3: Scheduler Loop Changes (3 hours)

### File: `src/runtime/scheduler.rs`

**Changes**:
1. Add constants for MAX_HIGH/MAX_NORMAL
2. Rewrite processing loop with two phases
3. Add high-priority receiver extraction

**Diff Preview**:
```rust
+const MAX_HIGH_PER_TICK: usize = 4;
+const MAX_NORMAL_PER_TICK: usize = 12;
 const MIN_POLL_TIMEOUT_MS: u64 = 10;
 const MAX_POLL_TIMEOUT_MS: u64 = 100;

 pub fn spawn<A>(...) -> ActorRef<A::Message> {
     let mailbox = Mailbox::new(mailbox_capacity);
     // ... existing setup ...

     let receiver = mailbox.receiver().clone();
+    let high_receiver = mailbox.high_priority_receiver().clone();
     let router_clone = self.router.clone();
     let metrics_clone = metrics.clone();

     thread::spawn(move || {
         let mut ctx = Context::with_metrics(address.clone(), router_clone, metrics_clone);
         actor.started(&mut ctx);

         while ctx.is_running() {
             let occupancy = mailbox.len() as f64 / mailbox.capacity() as f64;
             let timeout_ms = if occupancy > 0.5 {
                 MIN_POLL_TIMEOUT_MS
             } else {
                 MAX_POLL_TIMEOUT_MS
             };

+            let mut processed_high = 0;
             let mut processed_normal = 0;

+            // Phase 1: High-priority (capped)
+            while processed_high < MAX_HIGH_PER_TICK {
+                match high_receiver.try_recv() {
+                    Ok(envelope) => {
+                        let start = Instant::now();
+                        if envelope.is_expired() {
+                            ctx.metrics().record_expired();
+                            continue;
+                        }
+                        process_envelope(envelope, &mut actor, &mut ctx, start);
+                        processed_high += 1;
+                    }
+                    Err(TryRecvError::Empty) => break,
+                    Err(TryRecvError::Disconnected) => {
+                        ctx.stop();
+                        break;
+                    }
+                }
+            }

+            // Phase 2: Normal-priority (remaining budget)
+            let normal_budget = if processed_high == 0 {
+                MAX_HIGH_PER_TICK + MAX_NORMAL_PER_TICK
+            } else {
+                MAX_NORMAL_PER_TICK
+            };

-            loop {
-                if processed >= MAX_BATCH_SIZE {
-                    break;
-                }
+            while processed_normal < normal_budget {
+                let envelope = if processed_high == 0 && processed_normal == 0 {
                     match receiver.recv_timeout(Duration::from_millis(timeout_ms)) {
                         Ok(env) => env,
                         Err(RecvTimeoutError::Timeout) => break,
                         Err(RecvTimeoutError::Disconnected) => {
                             ctx.stop();
                             break;
                         }
                     }
+                } else {
+                    match receiver.try_recv() {
+                        Ok(env) => env,
+                        Err(TryRecvError::Empty) => break,
+                        Err(TryRecvError::Disconnected) => {
+                            ctx.stop();
+                            break;
+                        }
+                    }
+                };

                 let start = Instant::now();
                 if envelope.is_expired() {
                     ctx.metrics().record_expired();
+                    processed_normal += 1;
                     continue;
                 }

                 process_envelope(envelope, &mut actor, &mut ctx, start);
+                processed_normal += 1;
-                processed += 1;
             }
         }

         actor.stopped();
     });

     actor_ref
 }

+fn process_envelope<A: Actor>(
+    envelope: Envelope,
+    actor: &mut A,
+    ctx: &mut Context<A>,
+    start: Instant,
+) {
+    let (metadata, msg) = envelope.into_parts::<A::Message>();
+    let msg = match msg {
+        Some(m) => m,
+        None => {
+            eprintln!("Type mismatch: envelope {:?}", metadata.id);
+            return;
+        }
+    };
+
+    ctx.set_current_metadata(metadata);
+
+    if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
+        actor.receive(msg, ctx);
+    })) {
+        ctx.metrics().record_panic();
+        let error = ActorError::Panic(format!("{:?}", e));
+        actor.on_error(error, ctx);
+    } else {
+        let elapsed = start.elapsed().as_micros() as u64;
+        ctx.metrics().record_processed(elapsed);
+    }
+}
```

---

## Phase 4: Context Changes (1 hour)

### File: `src/runtime/actor.rs`

**Changes**:
1. Add `send_high_priority()` method (pub(crate))
2. Add `HighLaneFull` variant to SendError

**Diff Preview**:
```rust
pub enum SendError {
    MailboxFull { target: RouteAddress, occupancy: f64 },
+   HighLaneFull { target: RouteAddress, occupancy: f64 },
    ActorStopped { target: RouteAddress },
    RouteNotFound { target: RouteAddress },
}

impl<A: Actor + ?Sized> Context<A> {
+   pub(crate) fn send_high_priority<M>(&self, dest: RouteAddress, msg: M) -> Result<(), SendError>
+   where
+       M: Send + Sync + 'static,
+   {
+       let mut envelope = Envelope::from_route(self.address.clone(), dest.clone(), msg);
+
+       if let Some(metadata) = &self.current_metadata {
+           envelope = envelope.with_causation(metadata.id);
+           if let Some(deadline) = metadata.deadline {
+               envelope = envelope.with_deadline(deadline);
+           }
+       }
+
+       self.router.route_high_priority(envelope).map_err(|e| match e {
+           RouteError::RouteNotFound(target) => SendError::RouteNotFound { target },
+           RouteError::DeliveryFailed(target, delivery_err) => match delivery_err {
+               DeliveryError::HighLaneFull { capacity, current_len } => {
+                   SendError::HighLaneFull {
+                       target,
+                       occupancy: current_len as f64 / capacity as f64,
+                   }
+               }
+               DeliveryError::ActorStopped => SendError::ActorStopped { target },
+               _ => SendError::RouteNotFound { target },
+           },
+       })
+   }
}
```

---

## Phase 5: Metrics Updates (30 min)

### File: `src/runtime/actor.rs`

**Changes**:
1. Add high/normal counters to ActorMetrics

**Diff Preview**:
```rust
pub struct ActorMetrics {
    pub messages_processed: AtomicU64,
+   pub messages_processed_high: AtomicU64,
+   pub messages_processed_normal: AtomicU64,
    pub messages_expired: AtomicU64,
    pub messages_panicked: AtomicU64,
    pub total_processing_time_us: AtomicU64,
}

impl ActorMetrics {
    pub fn record_processed(&self, processing_time_us: u64) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
        self.total_processing_time_us.fetch_add(processing_time_us, Ordering::Relaxed);
    }

+   pub fn record_processed_high(&self, processing_time_us: u64) {
+       self.record_processed(processing_time_us);
+       self.messages_processed_high.fetch_add(1, Ordering::Relaxed);
+   }

+   pub fn record_processed_normal(&self, processing_time_us: u64) {
+       self.record_processed(processing_time_us);
+       self.messages_processed_normal.fetch_add(1, Ordering::Relaxed);
+   }
}

pub struct ActorMetricsSnapshot {
    pub messages_processed: u64,
+   pub messages_processed_high: u64,
+   pub messages_processed_normal: u64,
    pub messages_expired: u64,
    pub messages_panicked: u64,
    pub avg_processing_time_us: u64,
}
```

---

## Phase 6: Testing (1 hour)

### New Tests

**File**: `src/runtime/scheduler.rs` (test module)

```rust
#[test]
fn should_process_high_priority_first() {
    let scheduler = Scheduler::new(1);
    scheduler.start();
    let actor = TestActor::new();
    let address = test_address(1, "/test/priority");
    let actor_ref = scheduler.spawn(actor, address.clone(), 100);
    
    // Get mailbox to enqueue directly
    let mailbox = /* get mailbox somehow */;
    
    // Enqueue 1 normal, then 1 high
    mailbox.sender().try_send(normal_envelope()).unwrap();
    mailbox.high_priority_sender().try_send(high_envelope()).unwrap();
    
    // Wait and verify high processed first
    thread::sleep(Duration::from_millis(50));
    assert_eq!(actor.processing_order(), vec!["high", "normal"]);
}

#[test]
fn should_cap_high_priority_processing() {
    let scheduler = Scheduler::new(1);
    scheduler.start();
    let actor = TestActor::new();
    let address = test_address(1, "/test/cap");
    let actor_ref = scheduler.spawn(actor, address.clone(), 100);
    
    let mailbox = /* get mailbox */;
    
    // Enqueue 10 high messages
    for i in 0..10 {
        mailbox.high_priority_sender().try_send(high_envelope(i)).unwrap();
    }
    
    // Wait one tick
    thread::sleep(Duration::from_millis(20));
    
    // Verify only 4 processed
    assert_eq!(actor.processed_count(), 4);
}

#[test]
fn should_guarantee_normal_forward_progress() {
    let scheduler = Scheduler::new(1);
    scheduler.start();
    let actor = TestActor::new();
    let address = test_address(1, "/test/progress");
    let actor_ref = scheduler.spawn(actor, address.clone(), 100);
    
    let mailbox = /* get mailbox */;
    
    // Fill high lane
    for i in 0..50 {
        mailbox.high_priority_sender().try_send(high_envelope(i)).unwrap();
    }
    
    // Enqueue 1 normal
    let start = Instant::now();
    mailbox.sender().try_send(normal_envelope()).unwrap();
    
    // Wait for normal to process
    while !actor.has_processed_normal() {
        thread::sleep(Duration::from_millis(10));
        assert!(start.elapsed() < Duration::from_secs(1), "Normal message starved!");
    }
}

#[test]
fn should_return_error_on_high_lane_full() {
    let scheduler = Scheduler::new(1);
    scheduler.start();
    let actor = TestActor::new();
    let address = test_address(1, "/test/overflow");
    let actor_ref = scheduler.spawn(actor, address.clone(), 10);
    
    let mailbox = /* get mailbox */;
    let high_sender = mailbox.high_priority_sender();
    
    // Fill high lane
    for i in 0..10 {
        high_sender.try_send(high_envelope(i)).unwrap();
    }
    
    // Try to overflow
    let result = high_sender.try_send(high_envelope(999));
    assert!(result.is_err());
}
```

---

## Verification Checklist

### Correctness
- [ ] High messages process before normal when both present
- [ ] At most 4 high messages per tick
- [ ] Normal messages always make progress
- [ ] High lane overflow returns immediate error
- [ ] Metrics track high/normal separately

### Performance
- [ ] Normal lane latency unchanged when high idle (baseline)
- [ ] Normal lane latency <2x baseline when high saturated
- [ ] High lane latency <50µs P99
- [ ] No allocations added to hot path

### Safety
- [ ] No panics in scheduler loop
- [ ] No deadlocks
- [ ] No use-after-free
- [ ] No data races (verified by Miri if possible)

### Documentation
- [ ] Invariants documented in ACTOR_RUNTIME_INVARIANTS.md
- [ ] API docs updated for send_high_priority()
- [ ] Migration guide for DeliveryError changes

---

## Rollout Plan

### Stage 1: Deploy with Feature Flag (Week 1)
```rust
const ENABLE_PRIORITY_LANES: bool = cfg!(feature = "priority-lanes");

if ENABLE_PRIORITY_LANES {
    // New dual-channel path
} else {
    // Old single-channel path
}
```

### Stage 2: Enable in Staging (Week 2)
- Run all benchmarks
- Load test at 1M msg/sec for 24 hours
- Monitor metrics for regressions

### Stage 3: Gradual Production Rollout (Week 3)
- 10% of production traffic
- Monitor normal-lane P99 latency
- Monitor high-lane error rates
- Verify timer/supervision improvements

### Stage 4: Full Rollout (Week 4)
- 100% production traffic
- Remove feature flag
- Delete old single-channel code

---

## Success Metrics

**Must Achieve**:
- ✅ Timer latency <50ms P99 under load (down from >1000ms)
- ✅ Supervision commands <100ms P99 (down from >2000ms)
- ✅ Normal lane P99 <20ms (no regression from current 15ms)
- ✅ Zero high-lane overflows in normal operation

**Nice to Have**:
- Normal lane P50 improves by 10% (less jitter from control messages)
- High lane P99 <10ms

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Normal lane regression | Low | High | Extensive benchmarks + feature flag |
| High lane overflow | Medium | Medium | Overflow buffer + panic on critical |
| Deadlock in dual-channel | Very Low | Critical | Formal proof + testing |
| Increased memory usage | Low | Low | Same capacity, 2x channels = 2x capacity |

**Overall Risk**: **Low** - Design is simple, proven correct, rollback plan exists

---

## Estimated Timeline

| Phase | Duration | Dependency |
|-------|----------|------------|
| Mailbox changes | 2 hours | None |
| Router changes | 1 hour | Mailbox |
| Scheduler loop | 3 hours | Mailbox, Router |
| Context API | 1 hour | Router |
| Metrics | 30 min | Context |
| Testing | 1 hour | All |
| **Total** | **8.5 hours** | |

**Buffer**: 1.5 hours for unexpected issues

**Total with Buffer**: **10 hours** (1.25 work days)

