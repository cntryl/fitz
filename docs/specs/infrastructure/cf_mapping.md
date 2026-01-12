# RouteFamily → ColumnFamily Mapping (LOCKED)

**STATUS:** ✅ ENFORCED across all persisted domains

All persisted Fitz domains MUST map RouteFamily → Midge ColumnFamily explicitly.
The default column family MUST NEVER be used.

## Core Rules (IMMUTABLE)

### 1. RouteFamily Mapping
- Every persisted write MUST resolve to an explicit ColumnFamily
- ColumnFamily is derived from RouteFamily (1:1 mapping by value)
- Mapping is owned by the domain, not the caller

### 2. Default CF Prohibition
- The Midge default column family (CF=0) is FORBIDDEN
- No implicit CF selection
- Any attempt to write without an explicit CF MUST fail fast

### 3. Domain Responsibility
- Domains define and register their ColumnFamily at startup
- ColumnFamily IDs are stable and versioned if schema changes
- RouteFamily → ColumnFamily mapping is immutable for the lifetime of data

### 4. API Invariants
- Writer APIs MUST require a RouteFamily (or resolved handle)
- Persistence layer MUST receive an explicit ColumnFamily argument
- Tests MUST assert that default CF is never touched

### 5. Validation
- On startup, validate all persisted domains have registered CFs
- Panic or hard error if any domain falls back to default CF

## Implementation Status

### ✅ Stream Domain
**Location:** `src/domains/stream/store.rs`

All operations use explicit mapping:
```rust
let txn = self.db
    .begin_tx(
        cntryl_midge::ColumnFamilyId(family as u32),
        cntryl_midge::TransactionMode::ReadWrite
    )
```

**Operations:**
- `begin_append_session()` - ✅ Uses RouteFamily
- `commit_session()` - ✅ Uses RouteFamily
- `read_stream()` - ✅ Uses RouteFamily
- `read_stream_window()` - ✅ Uses RouteFamily
- `read_manifest()` - ✅ Uses RouteFamily
- `write_manifest()` - ✅ Uses RouteFamily
- `read_segments()` - ✅ Uses RouteFamily
- All other operations - ✅ Uses RouteFamily

### ✅ Schedule Domain
**Location:** `src/domains/schedule/store.rs`

All operations use explicit mapping:
```rust
let mut txn = self.db
    .begin_tx(
        cntryl_midge::ColumnFamilyId(family as u32),
        cntryl_midge::TransactionMode::ReadWrite
    )
```

**Operations:**
- `insert()` - ✅ Uses RouteFamily
- `delete()` - ✅ Uses RouteFamily
- `pop()` - ✅ Uses RouteFamily

### ✅ Queue Domain
**Location:** `src/domains/queue/queue_actor.rs`

All operations now use explicit mapping:
```rust
let cf_id = cntryl_midge::ColumnFamilyId(self.family.id() as u32);
let txn = self.store
    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
```

**Operations Fixed:**
- `recover_next_id()` - ✅ Fixed (uses `queue_key.family`)
- `next_message_id()` - ✅ Fixed (uses `self.family`)
- `enqueue_batch()` - ✅ Fixed (uses `self.family`)
- `reserve_batch()` - ✅ Fixed (uses `self.family`)
- `complete()` - ✅ Fixed (uses `self.family`)
- `expire_lease()` - ✅ Fixed (uses `self.family`)
- Test utilities - ✅ Fixed (uses `queue_key.family`)

**Previous violations:** 7 occurrences of `default_column_family()` - ALL REMOVED ✅

## Validation Helpers

**Location:** `src/runtime/cf_validation.rs`

```rust
/// Validate CF is not default (panics if CF=0)
pub fn validate_cf_not_default(cf_id: ColumnFamilyId)

/// Validate RouteFamily doesn't map to default CF (panics if id=0)
pub fn validate_route_family(family: RouteFamily)

/// Convert RouteFamily → ColumnFamilyId with validation
pub fn route_family_to_cf(family: RouteFamily) -> ColumnFamilyId
```

## Testing Requirements

All tests MUST verify:
1. ✅ No default CF usage (CF=0 never written to)
2. ✅ RouteFamily → CF mapping is explicit
3. ✅ Data isolation between route families
4. ✅ Panic on RouteFamily(0) attempts

## Audit Trail

- **2026-01-12:** Queue domain fixed - 7 violations removed
- **2026-01-12:** Validation module created (`cf_validation.rs`)
- **2026-01-12:** Documentation created (this file)
- **Status:** Stream ✅ | Schedule ✅ | Queue ✅ (code fixed, tests blocked by Midge limitation)

## Known Issues

### Midge Column Family Support

**STATUS:** ⚠️ BLOCKING TESTS

**Issue:** Midge's in-memory engine (used in tests) may not properly support multiple column families beyond the default (CF=0). When tests attempt to use explicit CFs (e.g., CF=1 for RouteFamily::new(1)), writes succeed but reads return empty results, suggesting CF isolation isn't working correctly.

**Symptoms:**
- Queue tests fail with "Message X disappeared from storage"
- Enqueue succeeds (writes to CF=1)
- Reserve fails (reads from CF=1 return nothing)
- Stream/schedule domains may have same issue (needs verification)

**Root Cause:**
Midge (based on RocksDB) requires column families to be explicitly registered/created before use. The in-memory engine created with `MidgeOptions::default()` may only support CF=0, or may not properly isolate CFs.

**Temporary Workarounds (NOT RECOMMENDED):**
1. ❌ Revert to default CF - **VIOLATES ARCHITECTURAL RULE**
2. ❌ Use RouteFamily::new(0) in tests - **VIOLATES ARCHITECTURAL RULE**

**Proper Solutions:**
1. ✅ Add CF pre-registration support to MidgeOptions
2. ✅ Update Midge to auto-create CFs on first transaction
3. ✅ Use persistent Midge instance for tests (creates CFs on disk)
4. ✅ Create mock storage layer for tests that properly supports multiple CFs

**Action Required:**
- [ ] Investigate Midge CF creation API
- [ ] Add MidgeOptions builder for CF configuration
- [ ] Update all test helpers to pre-register required CFs
- [ ] Verify stream/schedule domain tests pass with explicit CFs

**References:**
- Test utilities: `src/testkit/midge.rs`
- Validation module: `src/runtime/cf_validation.rs`
- Queue tests: `src/domains/queue/queue_actor.rs` (lines 990+)

## Future Domains

Any new persisted domain MUST:
1. Accept `RouteFamily` parameter in constructor
2. Use `ColumnFamilyId(family.id() as u32)` for all transactions
3. NEVER call `default_column_family()`
4. Add tests verifying explicit CF usage
5. Document CF mapping strategy

## References

- Architecture: `docs/specs/ARCHITECTURE.md`
- Routing: `src/runtime/routing.rs`
- Midge API: `cntryl_midge` crate
