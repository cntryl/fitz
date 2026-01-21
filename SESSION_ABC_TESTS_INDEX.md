# Complete Test Suite Index - Session A/B/C

**Status:** ✅ All three phases complete  
**Date:** January 21, 2026  
**Total Tests:** 92 (failing, by design)  
**Compilation:** 100% success

---

## Quick Navigation

### Failing Test Files (Phase A, B, C)

| Phase | File | Tests | Purpose | Documentation |
|-------|------|-------|---------|-----------------|
| A | [tests/error_handling_recovery.rs](tests/error_handling_recovery.rs) | 28 | Transport errors, timeouts, recovery | [See PHASE_ABC_FAILING_TESTS_SUMMARY.md](PHASE_ABC_FAILING_TESTS_SUMMARY.md#phase-a-error-handling--recovery-28-tests) |
| B | [tests/full_domain_implementations.rs](tests/full_domain_implementations.rs) | 30 | KV & Stream domain implementations | [See PHASE_ABC_FAILING_TESTS_SUMMARY.md](PHASE_ABC_FAILING_TESTS_SUMMARY.md#phase-b-full-domain-implementations-30-tests) |
| C | [tests/edge_cases_recovery.rs](tests/edge_cases_recovery.rs) | 34 | Boundaries, limits, recovery scenarios | [See PHASE_ABC_FAILING_TESTS_SUMMARY.md](PHASE_ABC_FAILING_TESTS_SUMMARY.md#phase-c-edge-cases--boundary-conditions-34-tests) |

### Documentation Files

| File | Purpose | Audience |
|------|---------|----------|
| [PHASE_ABC_FAILING_TESTS_SUMMARY.md](PHASE_ABC_FAILING_TESTS_SUMMARY.md) | Comprehensive test documentation with implementation guidance | Implementers |
| [PHASE_ABC_COMPLETE_SUMMARY.md](PHASE_ABC_COMPLETE_SUMMARY.md) | Summary, roadmap, and statistics | Project leads |
| [SESSION_ABC_PHASES_COMPLETE.md](SESSION_ABC_PHASES_COMPLETE.md) | Session completion summary | Everyone |
| [TODO.md](TODO.md) | Updated with test file references (see MEDIUM section) | Project tracking |

---

## Phase A: Error Handling & Recovery (28 Tests)

### Focus
Transport-layer resilience, timeout handling, and graceful error recovery

### Test Categories
1. **Connection Errors (4):** Retry with exponential backoff
2. **Connection Reset (4):** Graceful reconnection and session preservation
3. **Frame Validation (3):** Size limits and protocol safety
4. **UTF-8 Validation (3):** String integrity enforcement
5. **Timeout Handling (4):** Response and streaming timeouts
6. **Partial Frames (4):** Multi-packet reassembly and buffering
7. **Error Classification (2):** Retryable vs. fatal decisions
8. **Integration (2):** Domain-specific error handling

### Key Implementation Specs
- **Exponential backoff:** base 100ms, max 30s
- **Max retries:** 10 (configurable)
- **Response timeout:** 30s (per-operation configurable)
- **Frame assembly timeout:** 5s idle
- **Retryable errors:** ECONNREFUSED, ECONNRESET, timeout
- **Fatal errors:** Frame too large, invalid UTF-8, unauthorized

### Running Phase A Tests
```bash
cargo test --test error_handling_recovery -- --nocapture
```

---

## Phase B: Full Domain Implementations (30 Tests)

### Focus
Complete KV and Stream domain operation implementations with semantics validation

### Test Categories

#### KV Domain (15 tests)
1. **Core Operations (4):** BEGIN, PUT, GET, DELETE
2. **Advanced Operations (4):** SCAN, ROLLBACK, isolation levels
3. **Semantics (5):** Atomicity, write-skew, realm/area isolation, lifecycle, consistency
4. **Cross-Domain (2):** Concurrent writes, error handling

#### Stream Domain (12 tests)
1. **Core Operations (4):** BEGIN, APPEND, READ, LAST
2. **Advanced Operations (3):** COMMIT, ABORT, multi-frame responses
3. **Semantics (5):** Ordering, concurrency, watermarking, realm/area isolation

#### Cross-Domain & Performance (3 tests)
1. **Concurrency (3):** Consistency under concurrent operations
2. **Scale (2):** Large data, many concurrent operations

### Key Implementation Specs

**KV Model:**
- Transaction ID: UUID or monotonic
- Isolation levels: READ_COMMITTED, SNAPSHOT, SERIALIZABLE
- Atomicity: All-or-nothing COMMIT/ROLLBACK
- Realm isolation: Can't see other realm's data
- Area isolation: Can only see same area

**Stream Model:**
- Watermark: Separates committed (readable) from uncommitted
- Offsets: Strictly monotonic, no reordering
- Concurrency: One append session at a time
- Durability: Offset order persists through crashes

### Running Phase B Tests
```bash
cargo test --test full_domain_implementations -- --nocapture
```

---

## Phase C: Edge Cases & Boundary Conditions (34 Tests)

### Focus
Boundary conditions, resource limits, timeout enforcement, and recovery scenarios

### Test Categories
1. **Size Boundaries (6):** Zero-length and max-size validation
2. **Numeric Limits (4):** ID wraparound, offset overflow, realm/area counts
3. **Timeouts/Expiry (4):** Transaction, session, subscription, lease TTLs
4. **Concurrent Conflicts (5):** Concurrent PUTs, phantom reads, concurrent appends
5. **Resource Limits (4):** Quotas, connection limits, memory safety
6. **Recovery Scenarios (5):** Crash recovery, network partitions, restart handling
7. **Data Integrity (4):** Ordering consistency, checksums, deduplication
8. **Protocol Edge Cases (4):** Unknown operations, malformed TLV, permission changes

### Key Implementation Specs

**Size Limits (Recommended):**
- MAX_KEY_SIZE = 1 MB
- MAX_VALUE_SIZE = 100 MB
- MAX_FRAME_SIZE = 100 MB
- MAX_BUFFER_SIZE = 500 MB
- MAX_EVENT_SIZE = 50 MB

**Timeout Defaults:**
- TRANSACTION_TIMEOUT = 1 hour
- SESSION_TIMEOUT = 1 hour
- RESPONSE_TIMEOUT = 30 seconds
- PARTIAL_FRAME_TIMEOUT = 5 seconds

**Data Integrity:**
- CRC32 for frame checksums
- Blake3 for stored data
- Persistent deduplication tracking

### Running Phase C Tests
```bash
cargo test --test edge_cases_recovery -- --nocapture
```

---

## All Three Phases (92 Tests)

```bash
cargo test --test error_handling_recovery --test full_domain_implementations --test edge_cases_recovery -- --nocapture
```

**Expected Output:**
```
running 92 tests

failures (92 total - all intentional):
...panic!("reason for implementation")...
```

---

## Test Design Pattern

### All 92 Failing Tests Use This Pattern:

```rust
#[test]
fn should_do_specific_thing() {
    // What: Clear test purpose
    //
    // Scenario: Step-by-step setup
    //
    // Expected: Implementation requirements
    //
    // Reference: CLIENT.md lines XXX–YYY
    
    panic!("Implementation guidance");
}
```

### Why Panic Pattern?
- **Clear specs:** Each panic message explains what needs implementation
- **TDD guidance:** Test tells you exactly what to build
- **Compilation:** All tests compile, panic on execution
- **Documentation:** Implementation spec lives in test code

---

## Implementation Order (Recommended)

### Phase A First
- Implement transport error handling
- Add retry with backoff
- Validate frame sizes
- Handle UTF-8 errors
- Implement timeout logic

### Phase B Second
- Build KV transaction model
- Build Stream append model
- Implement isolation enforcement
- Add atomicity in COMMIT/ROLLBACK
- Validate realm/area separation

### Phase C Last
- Add size limit enforcement
- Implement timeout tracking
- Add crash recovery
- Implement data integrity checks
- Handle edge cases

---

## Compilation Status

✅ **All 92 tests compile successfully**

```
tests/error_handling_recovery.rs ......... 28 tests ✅
tests/full_domain_implementations.rs .... 30 tests ✅
tests/edge_cases_recovery.rs ............ 34 tests ✅
──────────────────────────────────────────────────
TOTAL .................................. 92 tests ✅
```

No warnings, no errors, ready for implementation.

---

## Documentation References

Every test includes references to:
- **CLIENT.md:** Protocol specification for all domains
- **SERVER.md:** Server-side requirements and session lifecycle

Examples:
- Transport errors: CLIENT.md lines 811–825
- KV operations: CLIENT.md lines 1205–1365
- Stream operations: CLIENT.md lines 1000–1052
- Permission model: CLIENT.md lines 619–675
- Error codes: CLIENT.md lines 1786–1819

---

## Quick Statistics

| Metric | Value |
|--------|-------|
| Total tests | 92 |
| Files | 3 |
| Intentionally failing | 92 (100%) |
| Compilation success | 100% |
| Lines of test code | ~1,800 |
| Test categories | 25 |
| Protocol references | 100+ |
| Documentation pages | 5 |

---

## Files in This Session

### Test Files Created
```
tests/
├── error_handling_recovery.rs ........... 28 tests (Phase A)
├── full_domain_implementations.rs ...... 30 tests (Phase B)
└── edge_cases_recovery.rs .............. 34 tests (Phase C)
```

### Documentation Files Created
```
├── PHASE_ABC_FAILING_TESTS_SUMMARY.md .. Comprehensive test guide
├── PHASE_ABC_COMPLETE_SUMMARY.md ....... Summary and roadmap
├── SESSION_ABC_PHASES_COMPLETE.md ...... Completion summary
├── SESSION_ABC_TESTS_INDEX.md .......... This file
└── TODO.md ............................ Updated with test references
```

---

## Next Steps

1. ✅ Run all tests to see implementation specs
2. ⏳ Implement Phase A (error handling)
3. ⏳ Implement Phase B (domain implementations)
4. ⏳ Implement Phase C (edge cases)
5. ⏳ Watch 92 tests transition from 🔴 failing → 🟢 passing

---

## For Questions

Each test includes:
- **What:** Test purpose (1-2 lines)
- **Scenario:** Setup steps (3-5 lines)
- **Expected:** Requirements (5-10 lines)
- **Reference:** Protocol spec line numbers

Read the panic message and the documentation comments above it. That's the complete implementation specification.

---

**Session Status:** ✅ Complete  
**Ready for:** Implementation  
**Next:** Run tests and follow the panic messages!
