# Fitz Schedule: Indexed Windowed Scheduler Design

## Overview

Scale the Schedule domain to millions of schedules by:
1. Maintaining a time-indexed view (schedule_idx) of next-fire times
2. Only scanning schedules due within a windowed time range
3. Using at-least-once semantics with clear crash recovery

## Storage Schema

### Column Families

**schedule_def** (authoritative definition storage)
```
Key:   family:{family_id}:def:{schedule_id:016x}
Value: [route][cron][payload]
```

**schedule_idx** (derived time index, enables windowed scanning)
```
Key:   family:{family_id}:idx:{bucket_ts:016x}/{schedule_id:016x}
Value: (empty or minimal metadata)
```

Where:
- `bucket_ts` = floor(next_fire_time / bucket_size)
- `bucket_size` = 10 seconds (allows granular windowing)
- Key format enables range scan: idx keys are ordered by bucket_ts

## Execution Model

### Tick Loop (Pseudocode)

```rust
fn on_tick(now: DateTime) {
    // 1. Compute window: only look at schedules due soon
    window_start = now - grace_period (e.g., 2s)
    window_end   = now + lookahead (e.g., 5s)
    
    // 2. Scan schedule_idx only in window
    //    No full iteration over all schedules!
    due_schedules = scan_index(window_start..=window_end)
    
    let mut index_updates = Vec::new();
    
    // 3. Dispatch and collect updates
    for schedule_id in due_schedules {
        schedule_def = load(schedule_id)
        
        // Dispatch notice (best-effort, non-blocking)
        emit_notice(schedule_def)
        
        // Compute next fire time
        next_fire = compute_next_fire(schedule_def.cron, now)
        
        // Queue index update (don't persist yet)
        index_updates.push((schedule_id, next_fire))
    }
    
    // 4. Batch persist index updates AFTER all dispatches
    //    Crash here = duplicates (acceptable)
    //    Crash before dispatch = misses (unacceptable)
    batch_update_index(index_updates)
}
```

### Crash Semantics

**Scenario: Crash after dispatch, before persist**
- Index still points to old next_fire_time
- Next tick scans again, finds schedule again
- Emits duplicate notice (acceptable per invariants)
- Fixes index entry on next tick

**Scenario: Crash before dispatch**
- Index already updated to new next_fire_time
- Notice was NOT sent
- Unacceptable? No—next matching time will trigger it
- But: if schedule never matches again, notice is missed
- Solution: Grace period + lookahead ensures re-check

**Scenario: Normal operation**
- Dispatch notice → emit succeeds
- Update index → schedule removed from due set
- Next tick: schedule not due until next_fire_time

## Index Management

### Computing Next Fire Time

```rust
fn compute_next_fire(cron: &CronSchedule, from: DateTime) -> DateTime {
    // Pure function: given cron and a start time,
    // find the next time this cron matches
    
    // Try next minute
    let mut candidate = from + Duration::minutes(1);
    for _ in 0..1440 {  // 24 hours ahead max
        if cron.matches_dt(&candidate) {
            return candidate;
        }
        candidate = candidate + Duration::minutes(1);
    }
    
    // Fallback: 24 hours ahead (never fires)
    from + Duration::days(1)
}
```

### Bucket Calculation

```rust
const BUCKET_SIZE: i64 = 10; // seconds

fn time_to_bucket(dt: DateTime) -> u64 {
    (dt.timestamp() / BUCKET_SIZE) as u64
}

fn bucket_to_key(family_id: u64, bucket: u64) -> String {
    format!("family:{}:idx:{:016x}/", family_id, bucket)
}
```

### Index Lifecycle

**On Create Schedule:**
1. Insert into schedule_def
2. Compute next_fire_time
3. Insert into schedule_idx

**On Tick (due schedule):**
1. Dispatch notice
2. Compute new next_fire_time
3. Queue: delete old index entry, insert new

**On Delete Schedule:**
1. Delete from schedule_def
2. Delete from schedule_idx (all time buckets it might be in)

## Window Parameters

```rust
// Tick window (adjustable based on latency requirements)
const GRACE_PERIOD: Duration = Duration::seconds(2);    // re-check window
const LOOKAHEAD: Duration = Duration::seconds(5);       // early scan window

// Window size = GRACE + LOOKAHEAD = 7 seconds
// Accommodates: skipped ticks, clock drift, reprocessing
```

If tick period is 1 second:
- Window contains ~7 seconds of schedules
- Average: 1 schedule per second × 7 = ~7 active schedules per tick
- Even at 1M schedules total, only scan ~7

## Batching Rules

### Index Update Batch

```rust
// After all notices dispatched:
batch {
    for (id, next_fire) in &updates {
        delete old_idx_key(id)
        insert new_idx_key(id, next_fire)
    }
    commit(write_options)
}
```

Rules:
- ✓ Batch index updates (defer write)
- ✓ Dispatch notices OUTSIDE transaction
- ✓ Use SYNC durability for batch
- ✗ Never wrap dispatch in transaction
- ✗ Never wait for batch to finish before next tick

## Testing Strategy

### 1. No Full Scans Test
```rust
#[test]
fn should_only_scan_windowed_schedules() {
    // Create 10k schedules spread across a month
    // Fire one tick
    // Verify: only ~10 schedules were checked (due in window)
    // NOT all 10k
}
```

### 2. Crash Scenarios
```rust
#[test]
fn should_handle_crash_before_index_update() {
    // Create schedule, emit notice, crash before persist
    // Restart, verify: duplicate notice sent on next tick (OK)
}

#[test]
fn should_handle_crash_after_index_update() {
    // Create schedule, emit notice, persist, crash before next tick
    // Restart, verify: notice not duplicated
}
```

### 3. At-Least-Once Delivery
```rust
#[test]
fn should_emit_at_least_once_across_restart() {
    // Create schedule for specific time
    // Simulate: tick before time, crash, restart at/after time
    // Verify: notice sent exactly once (or more, but ≥1)
}
```

### 4. Batching Correctness
```rust
#[test]
fn should_batch_index_updates_efficiently() {
    // Fire 100 due schedules in one tick
    // Verify: 1 batch write (not 100)
    // Verify: all notices dispatched before batch
}
```

### 5. Window Correctness
```rust
#[test]
fn should_rescan_with_grace_period() {
    // Create schedule that fires "now"
    // Tick 1: within window, dispatch + update index
    // Tick 2 (0.1s later): grace period still active
    // Create new schedule for same time
    // Tick 3: both schedules found (grace catches up)
}
```

## Implementation Checklist

- [ ] Update ScheduleStore to support schedule_idx CF
- [ ] Add `compute_next_fire(cron, from: DateTime) -> DateTime` function
- [ ] Add bucket calculation utilities
- [ ] Refactor `scan_and_fire()` to use windowed index scan
- [ ] Implement batched index update logic
- [ ] Update create_schedule() to populate index
- [ ] Update delete_schedule() to clean index
- [ ] Add Clock trait with configurable windows for testing
- [ ] Write comprehensive scale tests
- [ ] Verify no full scans in production path

## Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Scan per tick | O(due_count) | due_count << total_count |
| Index insert | O(1) | Single Midge put |
| Index delete | O(1) | Single Midge delete |
| Batch update | O(due_count) | Amortized cost |
| Memory usage | O(schedules_in_memory) | Load from def CF |
| Startup scan | O(total_count) | Initial load only |

## Edge Cases

**Multiple schedules with same next_fire_time**
- Bucket key: `bucket_ts/{schedule_id}`
- All are stored in same bucket
- All scanned together (efficient)

**Schedule that never matches**
- `compute_next_fire()` returns far future (24h+)
- Eventually moves into window naturally
- Never "stuck" in system

**Clock going backward**
- Grace period absorbs small drift
- Duplicate notices acceptable
- No correctness violation

**Very frequent cron (every minute)**
- Index updated every minute
- One entry at a time
- No batch explosion

## Non-Goals Reaffirmed

DO NOT implement:
- ❌ Execution state tracking
- ❌ Success/failure history
- ❌ Exactly-once delivery
- ❌ Leases or distributed locking
- ❌ Job execution
- ❌ Workflow state machines

These are consumer's responsibility.

---

## Migration Path from Current

Current code (simple O(n)):
```rust
for (id, def) in self.schedules.iter() {
    if def.cron.matches_dt(&now_dt) {
        emit_notice(def)
    }
}
```

New code (windowed O(due)):
```rust
let due = self.store.scan_window(family.id(), window_start, window_end)?;
for schedule_id in due {
    let def = self.schedules.get(&schedule_id)?;
    emit_notice(def)
    // queue index update
}
self.store.batch_update_index(updates)?;
```

For small deployments (<10k schedules), both are fine.
For large deployments (>100k), second is required.

Current tests continue to pass (no behavior change).
New tests verify scaling properties.
