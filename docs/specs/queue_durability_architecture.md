# Queue Durability Architecture

## Design Principle: Per-Write Override, Not Global Config

Fitz queue durability policies achieve domain isolation through **per-write options** rather than global Midge configuration changes.

## Architecture

### Single Shared Midge Instance

```rust
// Application initialization (once at startup)
let shared_store = Arc::new(MidgeEngine::open("data")?);

// All domains share the same instance
let queue1 = QueueActor::new(1, "payments", shared_store.clone(), None);
let queue2 = QueueActor::new(1, "analytics", shared_store.clone(), None);
let stream_store = StreamStore::new(shared_store.clone());
let kv_store = KvStore::new(shared_store.clone());
```

### Per-Write Durability Control

Each queue actor translates its durability policy into Midge transaction commit options:

```rust
impl QueueActor {
    fn handle_enqueue_batch(&mut self, messages: Vec<Message>) -> QueueResponse {
        // Begin transaction
        let cf = self.store.default_column_family();
        let mut txn = self.store.begin_transaction(cf)?;
        
        // Add all writes to transaction
        for msg in messages {
            let key = Self::message_key(&self.queue_key, msg.id);
            let value = Self::encode_record(&msg);
            txn.put(&key, &value)?;
        }
        
        // Translate policy to commit options
        let (sync, disable_wal) = self.durability.to_midge_options();
        let mut opts = WriteOptions::default();
        opts.set_sync(sync);
        opts.set_disable_wal(disable_wal);
        
        // Commit with per-transaction durability override
        self.store.commit_transaction_boxed(txn, &opts)?;
        
        Ok(())
    }
}
```

### Policy-to-Options Mapping

| Policy | sync | disable_wal | Behavior | Throughput |
|--------|------|-------------|----------|------------|
| **Strict** | `true` | `false` | fsync on every write | 100-150K msg/s |
| **Grouped{5ms}** | `false` | `false` | async WAL, group commit | 600-800K msg/s |
| **Async** | `false` | `true` | memory-only, no WAL | 1-2M+ msg/s |

### Domain Isolation Guarantees

1. **No Global Config Changes**: Midge instance configuration never modified after initialization
2. **Per-Write Overrides**: Each `put_opt()` / `write_opt()` call specifies durability
3. **Key Prefix Separation**: Different domains use different key prefixes (`queue:`, `kv:`, `stream:`)
4. **Independent Policies**: Queue with `Async` policy won't affect KV with `Strict` writes

## Example: Mixed Durability Workload

```rust
// Payments queue: Never lose data
let payments = QueueActor::with_durability(
    1, "payments", store.clone(), None,
    QueueDurabilityPolicy::Strict
);

// Analytics queue: Best-effort, maximum throughput
let analytics = QueueActor::with_durability(
    1, "analytics", store.clone(), None,
    QueueDurabilityPolicy::Async
);

// KV domain: Always Strict (unaffected by queue policies)
let kv = KvStore::new(store.clone());

// Each transaction commit uses appropriate durability:
payments.enqueue("transaction");   // Commits with sync=true, wal=enabled
analytics.enqueue("pageview");      // Commits with sync=false, wal=disabled
kv.put("user:123", "data");         // Commits with sync=true, wal=enabled (default)
```

## Benefits

### ✅ Efficiency
- Single Midge instance (shared memory, connection pooling)
- No need to spawn multiple Midge processes

### ✅ Flexibility
- Different queues have different durability on the same instance
- Per-queue configuration (no global tradeoffs)

### ✅ Safety
- Domain isolation via per-write options + key prefixes
- KV/streams/leases always Strict (unaffected by queue policies)

### ✅ Simplicity
- No complex Midge namespacing or column family isolation
- Single write API with optional override parameter

## Implementation Status

### ✅ Completed
- `QueueDurabilityPolicy` enum (Strict/Grouped/Async)
- `to_midge_options()` method for policy-to-options translation
- `QueueActor::with_durability()` constructor
- Durability policy stored per-actor
- TODO comments at all write call sites documenting override pattern

### 🔄 Pending Midge API
- `WriteOptions` struct with `set_sync()` and `set_disable_wal()`
- `commit_transaction_boxed(txn, &opts)` method supporting custom WriteOptions
- Transaction `put()` and `delete()` methods
- Grouped commit with interval-based fsync (for Grouped policy)

### 📋 Future Enhancements
- Grouped commit with interval-based fsync (Midge group commit API)
- Per-write latency metrics segmented by durability policy
- Recovery benchmarks for different policies

## Design Rationale

### Why Per-Write Override?

**Rejected Alternative: Multiple Midge Instances**
```rust
// ❌ BAD: Separate instances for different durability
let strict_store = Arc::new(MidgeEngine::open_with_options(strict_opts)?);
let async_store = Arc::new(MidgeEngine::open_with_options(async_opts)?);

// Problems:
// - 2× memory overhead (separate caches)
// - 2× file descriptor usage
// - Complex key routing (which instance for which queue?)
// - No isolation for KV/streams (which instance?)
```

**Chosen Design: Transaction-Based Options**
```rust
// ✅ GOOD: Single instance, per-transaction control
let store = Arc::new(MidgeEngine::open("data")?);
let cf = store.default_column_family();
let mut txn = store.begin_transaction(cf)?;
txn.put(&key, &value)?;

let (sync, disable_wal) = policy.to_midge_options();
let mut opts = WriteOptions::default();
opts.set_sync(sync);
opts.set_disable_wal(disable_wal);
store.commit_transaction_boxed(txn, &opts)?;

// Benefits:
// - Single instance (efficient)
// - Per-transaction control (flexible)
// - No global config changes (safe)
// - Domain isolation via key prefixes + commit options
```

### Why Not Global Midge Config?

Global durability settings would affect **all domains**:

```rust
// ❌ BAD: Global config change affects everything
midge.set_sync(false); // Now ALL commits are async!
queue.enqueue("job");   // Async commit ✓
kv.put("key", "value"); // Async commit ✗ (unintended!)
stream.append("data");  // Async commit ✗ (unintended!)
```

Transaction-based options provide **surgical control**:

```rust
// ✅ GOOD: Per-transaction override
queue.enqueue_with_opts("job", async_opts);   // Async commit ✓
kv.put("key", "value");                       // Strict commit ✓ (default)
stream.append("data");                        // Strict commit ✓ (default)
```

## Testing Strategy

### Unit Tests
- Policy-to-options conversion (`to_midge_options()`)
- Policy properties (`is_durable()`, `may_lose_data()`)
- Throughput range expectations

### Integration Tests
- Mixed durability workload (Strict + Async on same Midge)
- Domain isolation (queue Async doesn't affect KV Strict)
- Recovery behavior for each policy

### Benchmarks
- Throughput by policy (Strict: 100-150K, Grouped: 500K+, Async: 1-2M+)
- Latency distribution by policy
- Recovery time by policy

## References

- [Queue Durability Policies](../../src/domains/queue/durability.rs) - Policy enum and options
- [QueueActor](../../src/domains/queue/queue_actor.rs) - Per-write TODO comments
- [Example: Queue Durability Isolation](../../examples/queue_durability_isolation.rs) - Demo
- [Midge Repository](https://github.com/cntryl/midge) - Storage engine

## FAQ

**Q: Why not use Midge column families for isolation?**

A: Column families provide key namespace separation but don't support per-CF durability settings. We still need per-write options to override sync behavior.

**Q: What happens if I create 100 queues with different policies?**

A: No problem. Each queue actor stores its policy and translates it at write time. All use the same shared Midge instance efficiently.

**Q: Can I change a queue's durability policy at runtime?**

A: Currently no (policy is set at construction). Future enhancement could add `set_durability()` method.

**Q: What if Midge doesn't support per-write options?**

A: We'll implement via Midge wrapper that translates options into appropriate Midge API calls. Worst case: simulate grouped commit via background fsync task.

**Q: Does this work with Midge replication?**

A: Yes. Per-write options control local durability (WAL/fsync). Replication is orthogonal and uses separate configuration.
