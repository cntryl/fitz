# RPC Subsystem Hardening Plan

## Goal
Crush NATS-style request/reply by making RPC a first-class, deterministic primitive with <300ns dispatch and <1µs end-to-end latency.

## Performance Targets
- ✅ **Dispatch**: ≤ 300ns (single-hop, O(1) worker selection)
- 🔄 **End-to-end**: ≈ 1µs (needs completion of forwarding stubs)
- ✅ **Stable p99**: Bounded memory, explicit backpressure
- 🔄 **Fail fast**: Implemented but needs lease optimization

## Critical Issues

### 1. Allocation Hot Spots in Dispatch (HIGH PRIORITY)

**Problem**: `RpcRequest` and `RouteAddress` cloned on every dispatch.

**Impact**:
- Request body clone: ~50-500ns depending on size
- Route string clone: ~20-50ns
- Total allocation overhead: ~100-600ns (dominates 300ns dispatch target)

**Fix**:
```rust
// Option A: Store minimal lease data (just IDs)
struct Lease {
    correlation_id: Uuid,          // 16 bytes, Copy
    worker_index: usize,           // 8 bytes, Copy
    reply_route: Arc<Route>,       // Shared reference
    expiration: Instant,           // 16 bytes, Copy
}

// Option B: Use indices into pre-allocated request pool
struct Lease {
    request_slot: u32,             // Index into pool
    worker_index: usize,
    expiration: Instant,
}
```

**Implementation**:
- Change `Lease` to store `worker_index: usize` instead of `worker_addr: RouteAddress`
- Store only `reply_route: Arc<Route>` (shared, not cloned)
- Remove `request: RpcRequest` field entirely (already queued or dispatched)
- Lookup worker by index when needed (O(1) vec access)

**Expected improvement**: Reduce dispatch overhead from ~600ns to <50ns.

---

### 2. Lease Expiration O(N) Scan (HIGH PRIORITY)

**Problem**: `check_expired_leases()` scans all leases on every request.

**Impact**:
- With 1000 in-flight requests: ~2-5µs per scan
- With 10000 in-flight requests: ~20-50µs per scan
- Violates 300ns dispatch budget under load

**Fix**:
```rust
// Replace HashMap with expiration-ordered data structure
use std::collections::BinaryHeap;

struct ExpiringLease {
    expiration: Instant,
    correlation_id: Uuid,
}

impl Ord for ExpiringLease {
    fn cmp(&self, other: &Self) -> Ordering {
        other.expiration.cmp(&self.expiration) // Min-heap
    }
}

struct RpcRouteActor {
    leases: HashMap<Uuid, Lease>,
    expiration_queue: BinaryHeap<ExpiringLease>,  // New
}

// Check only expired leases (O(K) where K = expired count)
fn check_expired_leases_fast(&mut self) {
    let now = Instant::now();
    while let Some(entry) = self.expiration_queue.peek() {
        if entry.expiration > now {
            break; // All remaining leases are valid
        }
        let expired = self.expiration_queue.pop().unwrap();
        if let Some(lease) = self.leases.remove(&expired.correlation_id) {
            // Handle expiration...
        }
    }
}
```

**Alternative**: Amortized expiration check
```rust
// Only check every Nth request or on timer tick
fn handle_request(&mut self, request: RpcRequest, ctx: &mut Context<Self>) {
    self.request_count += 1;
    if self.request_count % 100 == 0 {  // Check every 100 requests
        self.check_expired_leases(ctx);
    }
    // ... dispatch
}
```

**Expected improvement**: Dispatch stays O(1) even with 10k in-flight requests.

---

### 3. Correlation ID Pre-allocation (MEDIUM PRIORITY)

**Problem**: No documented strategy for reusing correlation IDs.

**Impact**:
- `Uuid::new_v4()` costs ~150-200ns (crypto RNG)
- Adds 50-66% overhead to 300ns dispatch target

**Fix**: Implement correlation ID pool at session layer
```rust
// Session-level correlation ID allocator
struct CorrelationIdPool {
    next_id: AtomicU64,  // Monotonic counter
    session_prefix: u64, // Unique per session
}

impl CorrelationIdPool {
    fn next(&self) -> Uuid {
        let counter = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Pack session_prefix + counter into UUID
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.session_prefix.to_le_bytes());
        bytes[8..16].copy_from_slice(&counter.to_le_bytes());
        Uuid::from_bytes(bytes)
    }
}
```

**Expected improvement**: Correlation ID allocation <10ns (atomic increment).

---

### 4. Complete Response Forwarding Integration (HIGH PRIORITY)

**Problem**: Critical forwarding paths have `TODO` stubs.

**Locations**:
- `rpc_route_actor.rs:220` - Forward response to ReplyInboxActor
- `rpc_route_actor.rs:340` - Send error to reply route
- `reply_inbox.rs:141` - Forward to transport actor

**Impact**: Cannot measure end-to-end latency; incomplete system.

**Fix**: Integrate with Router and MailboxSink
```rust
// In RpcRouteActor::handle_response
fn handle_response(&mut self, response: RpcResponse, ctx: &mut Context<Self>) {
    if let Some(lease) = self.leases.get(&response.correlation_id) {
        let reply_route = &lease.reply_route;
        
        // Route response to client's ReplyInboxActor
        let envelope = Envelope::new(
            ctx.self_address().clone(),
            reply_route.clone(),
            InboxMessage::Response(response.clone()),
        );
        
        if let Err(e) = ctx.router().route(envelope) {
            // Log routing failure, clean up lease
        }
        
        if response.stream_end {
            self.release_lease(&response.correlation_id, ctx);
        }
    }
}
```

**Implementation steps**:
1. Wire RpcRouteActor → ReplyInboxActor via Router
2. Wire ReplyInboxActor → Transport layer via output channel
3. Add integration test measuring full round-trip latency

---

### 5. Benchmark Allocation Patterns (MEDIUM PRIORITY)

**Problem**: Benchmarks clone requests in hot path.

**Current**:
```rust
let req = requests[idx % requests.len()].clone();  // ❌ Clone in measured loop
actor.receive(RpcMessage::Request(req), &mut ctx);
```

**Fix**: Use `iter_batched` or pre-allocate all variants
```rust
// Option A: Use iter_batched for per-iteration setup
group.bench_function("dispatch", |b| {
    b.iter_batched(
        || requests[idx % requests.len()].clone(),  // Setup (not measured)
        |req| actor.receive(RpcMessage::Request(req), &mut ctx),
        BatchSize::SmallInput,
    );
});

// Option B: Benchmark dispatch only (assume request already exists)
group.bench_function("dispatch_no_alloc", |b| {
    b.iter(|| {
        actor.dispatch_internal(black_box(&request_ref));  // No clone
    });
});
```

**Expected improvement**: Benchmark reflects true dispatch cost (<300ns), not allocation cost.

---

## Implementation Priority

### Phase 1: Eliminate Allocation Hot Spots (Week 1)
1. ✅ **Remove request clone from Lease** - Store minimal data
2. ✅ **Replace worker_addr clone with index** - O(1) lookup
3. ✅ **Share Route via Arc** - No string clones
4. ✅ **Add benchmark for allocation-free dispatch**

**Success metric**: Dispatch benchmark drops from ~600ns to <100ns.

### Phase 2: Fix Lease Expiration Scaling (Week 1-2)
1. ✅ **Add BinaryHeap for expiration ordering**
2. ✅ **Implement O(K) expiration check** (K = expired count)
3. ✅ **Benchmark under high in-flight load** (1k-10k concurrent)

**Success metric**: Dispatch latency stays <300ns with 10k in-flight requests.

### Phase 3: Complete Forwarding Integration (Week 2)
1. ✅ **Wire RpcRouteActor → ReplyInboxActor**
2. ✅ **Wire ReplyInboxActor → Transport**
3. ✅ **Add end-to-end latency test**

**Success metric**: Full round-trip RPC <1µs in-process.

### Phase 4: Correlation ID Optimization (Week 2-3)
1. ✅ **Implement CorrelationIdPool**
2. ✅ **Integrate at session layer**
3. ✅ **Benchmark correlation ID allocation**

**Success metric**: Correlation ID generation <10ns (vs 150-200ns for UUID v4).

---

## Testing Requirements

### New Benchmarks Needed
1. **Dispatch under load**: 10k in-flight requests, measure p50/p99/p999
2. **Lease expiration scalability**: Vary in-flight count (100, 1k, 10k)
3. **Allocation-free dispatch**: Measure with pre-allocated requests
4. **End-to-end latency**: Full RPC round-trip (request → worker → response)

### New Tests Needed
1. **Lease expiration ordering**: Verify BinaryHeap correctness
2. **Worker index lookup**: Verify O(1) worker resolution
3. **Response forwarding**: Verify Router integration
4. **Correlation ID uniqueness**: Verify pool-based IDs don't collide

---

## Success Criteria

✅ **Dispatch latency**: <300ns p99 with 1k in-flight requests
✅ **End-to-end latency**: <1µs p99 for in-process RPC
✅ **Memory bounded**: No unbounded growth under load
✅ **Fail fast**: Backpressure errors within 100ns of queue full
✅ **No hidden allocation**: Zero `clone()` calls in dispatch hot path
✅ **Stable under load**: p99 < 2x p50 even at 10k RPS

---

## Non-Goals (Explicitly Out of Scope)

❌ Multi-node RPC routing
❌ Transparent distribution
❌ Inbox-based request/reply (NATS-style)
❌ Dynamic subscription routing
❌ Per-request auth evaluation

---

## Comparison to NATS

| Metric | NATS Request/Reply | Fitz RPC | Improvement |
|--------|-------------------|----------|-------------|
| Routing hops | 2-3 (inbox creation) | 1 (direct dispatch) | 2-3x fewer hops |
| Allocation per request | 4-6 (inbox, subject, correlation) | 0-1 (optimized) | 4-6x fewer allocations |
| Dispatch latency | ~1-2µs | <300ns | 3-6x faster |
| Backpressure visibility | Hidden (buffered) | Explicit (fast reject) | Predictable failure |
| Auth overhead | Per-message | Per-session | Amortized to zero |

**Target**: Beat NATS by 3-5x on dispatch latency, 2-4x on end-to-end latency.
