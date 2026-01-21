# Phase A, B, C: Comprehensive Failing Test Suite Summary

**Created:** January 21, 2026  
**Total Tests (Failing):** 103 tests across 3 files  
**Purpose:** TDD guidance for implementing medium-priority features

---

## Overview

This document summarizes the three-phase failing test suite that guides implementation of MEDIUM-priority items from TODO.md:

- **Phase A:** Error Handling & Recovery (28 tests)
- **Phase B:** Full Domain Implementations (41 tests)
- **Phase C:** Edge Cases & Boundary Conditions (34 tests)

All tests intentionally fail with `panic!("reason")` messages that explain what needs implementation.

---

## Phase A: Error Handling & Recovery (28 Tests)

**File:** `tests/error_handling_recovery.rs`  
**Status:** ✅ Compiles, 28 tests found

### Purpose
Validate transport-level error handling, recovery patterns, and graceful degradation across all domains.

### Test Categories

#### 1. Connection Errors (4 tests)
**What:** Connection failures and recovery

- `should_retry_on_connection_refused_with_backoff` - Exponential backoff retry strategy
- `should_use_exponential_backoff_with_configurable_base` - Base delay configuration
- `should_respect_max_retries_limit` - Finite retry attempts
- `should_log_connection_failures_for_debugging` - Observable errors

**Implementation Needs:**
- Connection refused detection (ECONNREFUSED)
- Exponential backoff: base 100ms, max 30s
- Max retries: 10 (configurable)
- Connection logging for debugging

---

#### 2. Connection Reset Handling (4 tests)
**What:** Unexpected connection termination

- `should_gracefully_reconnect_on_connection_reset` - Automatic reconnection
- `should_preserve_session_state_across_reconnect` - Session continuation
- `should_distinguish_reset_from_close` - Reset vs. graceful close
- `should_propagate_connection_reset_to_pending_operations` - Cascade errors

**Implementation Needs:**
- Reset detection (ECONNRESET, FIN without close)
- Session ID preservation during reconnect
- Distinguish reset (error) from close (normal)
- Pending operations fail appropriately

---

#### 3. Frame Size Validation (3 tests)
**What:** Oversized frame handling

- `should_close_connection_on_frame_too_large` - Enforce MAX_FRAME_SIZE
- `should_validate_frame_size_before_buffering` - Early validation
- `should_provide_clear_error_on_frame_overflow` - User-facing error

**Implementation Needs:**
- MAX_FRAME_SIZE constant (e.g., 100MB)
- Size validation before allocation
- Clear error: ERR_FRAME_TOO_LARGE or domain-specific

---

#### 4. Invalid UTF-8 Handling (3 tests)
**What:** Malformed text in protocol

- `should_close_connection_on_invalid_utf8` - Immediate disconnect
- `should_validate_utf8_early` - Before parsing
- `should_distinguish_utf8_from_other_errors` - Specific error type

**Implementation Needs:**
- UTF-8 validation for string fields
- Early validation (don't parse invalid UTF-8)
- Error type: ERR_INVALID_UTF8 (if domain error) or protocol error

---

#### 5. Timeout Handling (4 tests)
**What:** Slow/no-response scenarios

- `should_timeout_waiting_for_response` - Response timeout
- `should_allow_per_operation_timeout_configuration` - Configurable timeouts
- `should_timeout_streaming_operations` - Timeout during fanout
- `should_timeout_async_fanout_operations` - Timeout in SUBSCRIBE/RPC

**Implementation Needs:**
- Response timeout (configurable, e.g., 30s)
- Per-operation timeout override
- Timeout for streaming (SUBSCRIBE, RPC streaming, Stream READ)
- Clear error: ERR_TIMEOUT or domain-specific

---

#### 6. Partial Frame Assembly (4 tests)
**What:** Frames split across packets

- `should_buffer_incomplete_frames_until_complete` - Frame buffering
- `should_handle_frames_split_across_many_packets` - Many segments
- `should_prevent_buffer_overflow_during_assembly` - Memory safety
- `should_activity_timeout_during_partial_assembly` - Idle detection

**Implementation Needs:**
- Frame buffer (length-prefix then payload)
- Multi-packet reassembly
- MAX_BUFFER_SIZE to prevent OOM
- Activity timeout on partial frames (e.g., 5s of inactivity)

---

#### 7. Error Classification & Recovery (2 tests)
**What:** Retry-vs-fatal decisions

- `should_classify_retryable_errors` - ECONNREFUSED, ECONNRESET, timeout
- `should_classify_fatal_errors` - Frame too large, invalid UTF-8, unauthorized

**Implementation Needs:**
- Retryable: connection errors, timeouts
- Fatal: protocol violations, authorization
- Circuit breaker for persistent failures

---

#### 8. Integration Scenarios (2 tests)
**What:** Real-world error sequences

- `should_timeout_kv_get_operation` - KV domain timeout handling
- `should_handle_rpc_request_with_connection_reset` - RPC domain reset handling

**Implementation Needs:**
- Domain-specific timeout handling
- Connection reset propagates to in-flight requests
- Clean error propagation

---

### Key Implementation Guidance (Phase A)

**Backoff Strategy:**
```
base = 100ms
attempt 1: 100ms
attempt 2: 200ms
attempt 3: 400ms
attempt 4: 800ms
attempt 5: 1600ms
attempt 6: 3200ms
attempt 7: 6400ms
attempt 8: 12800ms
attempt 9: 25600ms
attempt 10: 30000ms (capped)
```

**Error Tiers:**
- **Retryable:** ECONNREFUSED, ECONNRESET, timeout, EAGAIN
- **Fatal:** Frame too large, invalid UTF-8, ERR_UNAUTHORIZED, protocol errors

---

## Phase B: Full Domain Implementations (30 Tests)

**File:** `tests/full_domain_implementations.rs`  
**Status:** ✅ Compiles, 30 tests found

### Purpose
Validate complete implementation of KV and Stream domains (5 tests each for semantics + error handling).

### KV Domain (8 tests)

#### Basic Operations (4 tests)
- `should_begin_transaction_and_return_transaction_id` - Transaction creation
- `should_put_and_get_values_within_transaction` - Basic read/write
- `should_delete_key_idempotently` - Deletion with idempotency
- `should_commit_transaction_atomically` - Atomic persistence

**Implementation Needs:**
- Transaction ID generation (UUID or monotonic)
- PUT/GET within transaction
- DELETE (idempotent by key)
- COMMIT atomicity: all-or-nothing

#### KV Advanced Operations (4 tests)
- `should_scan_key_range_with_pagination` - SCAN with limit
- `should_rollback_transaction_cleanly` - ROLLBACK semantics
- `should_enforce_isolation_level_readcommitted` - Read committed isolation
- `should_enforce_isolation_level_snapshot` - Snapshot isolation

**Implementation Needs:**
- SCAN with prefix/range and limit
- ROLLBACK clears all pending writes
- Isolation levels: READ_COMMITTED, SNAPSHOT, SERIALIZABLE
- Per-transaction isolation enforcement

#### KV Semantics (5 tests)
- `should_preserve_atomicity_across_puts` - All-or-nothing commits
- `should_prevent_write_skew_in_snapshot` - Write skew detection
- `should_isolate_transactions_by_realm` - Realm separation
- `should_isolate_transactions_by_area` - Area separation
- `should_enforce_transaction_lifecycle` - BEGIN→(PUT/GET/DELETE)*→(COMMIT|ROLLBACK)

**Implementation Needs:**
- Atomicity: COMMIT writes all pending, ROLLBACK discards all
- Write skew detection (when enabled)
- Realm isolation (transaction can't see other realm's data)
- Area isolation (can only see same area)
- Lifecycle validation: BEGIN required before operations

---

### Stream Domain (7 tests)

#### Basic Operations (4 tests)
- `should_begin_append_session_and_return_session_id` - Session creation
- `should_append_event_and_return_offset` - APPEND with offset
- `should_read_events_by_offset_range` - READ with offset range
- `should_get_last_event_in_stream` - LAST operation

**Implementation Needs:**
- Session ID generation
- APPEND returns offset (monotonic)
- READ(offset_start, offset_end) returns events
- LAST returns most recent event

#### Stream Advanced Operations (3 tests)
- `should_commit_session_atomically` - Session finalization
- `should_abort_session_cleanly` - Session cancellation
- `should_handle_multi_frame_read_responses` - Large reads

**Implementation Needs:**
- COMMIT finalizes session (watermark updated)
- ABORT discards uncommitted appends
- Large READs split across multiple frames

#### Stream Semantics (5 tests)
- `should_preserve_append_ordering` - Offset ordering
- `should_prevent_concurrent_appends` - One session at a time
- `should_isolate_committed_from_uncommitted` - Watermark protection
- `should_isolate_streams_by_realm` - Realm separation
- `should_isolate_streams_by_area` - Area separation
- `should_enforce_watermark_for_readers` - Watermark visibility

**Implementation Needs:**
- Offsets strictly monotonic (no gaps, no reorder)
- Only one active append session per stream
- Readers see only committed events (offset <= watermark)
- Realm isolation (can't read other realm's stream)
- Area isolation (can't read other area's stream)
- Watermark prevents partial-transaction visibility

---

### Cross-Domain & Performance (7 tests)

#### Concurrency (3 tests)
- `should_handle_concurrent_kv_writes_consistently` - Concurrent PUTs
- `should_ensure_stream_reader_consistency` - Concurrent reads
- `should_handle_cross_domain_error_consistency` - Error behavior

**Implementation Needs:**
- Concurrent writes (last-write-wins or conflict detection)
- Readers see consistent snapshots
- Errors consistent across domains

#### Large Data (2 tests)
- `should_handle_large_kv_values_without_truncation` - Multi-MB values
- `should_handle_large_stream_events_without_truncation` - Multi-MB events

**Implementation Needs:**
- Support values/events up to MAX_VALUE_SIZE (e.g., 100MB)
- Multi-frame handling for large data
- No truncation or data loss

#### Scale & Load (2 tests)
- `should_handle_many_concurrent_kv_transactions` - 1000+ active transactions
- `should_handle_many_concurrent_stream_readers` - 1000+ concurrent reads

**Implementation Needs:**
- Efficient transaction management (scalable state)
- Scalable reader tracking (no quadratic behavior)
- Memory usage reasonable under load

---

### Key Implementation Guidance (Phase B)

**KV Transaction Model:**
```
Client: BEGIN (operation_type: ReadWrite)
Broker: [tx_id assigned]
Client: PUT key, value (within tx_id)
Client: GET key (within tx_id)
Client: COMMIT or ROLLBACK (tx_id)
Broker: [atomically apply all PUTs or discard all]
```

**Stream Append Model:**
```
Client: BEGIN (operation_type: Append)
Broker: [session_id assigned, watermark noted]
Client: APPEND event
Broker: [event buffered, offset returned (uncommitted)]
Client: COMMIT or ABORT
Broker: [watermark advances or append discarded]
```

**Isolation Levels:**
- **READ_COMMITTED:** See only committed data, no phantoms
- **SNAPSHOT:** Consistent snapshot at BEGIN time, phantoms possible
- **SERIALIZABLE:** Behaves as if sequential (highest cost)

---

## Phase C: Edge Cases & Boundary Conditions (34 Tests)

**File:** `tests/edge_cases_recovery.rs`  
**Status:** ✅ Compiles, 34 tests found

### Purpose
Validate boundary conditions, limits, recovery scenarios, and data integrity across all domains.

### Boundary Conditions - Sizes & Limits (6 tests)

#### Zero-Length Data
- `should_handle_zero_length_keys` - Empty strings as keys
- `should_handle_zero_length_values` - Empty strings as values
- `should_handle_zero_length_events` - Empty stream events

**Implementation Needs:**
- Empty keys/values/events are valid (unless explicitly rejected)
- Distinguish from "not found" (different from NULL)
- Store and retrieve correctly

#### Maximum Size Enforcement
- `should_handle_max_size_keys` - Enforce MAX_KEY_SIZE
- `should_handle_max_size_values` - Enforce MAX_VALUE_SIZE
- `should_handle_max_size_events` - Enforce MAX_EVENT_SIZE

**Implementation Needs:**
- Documented limits (e.g., 1MB keys, 100MB values)
- Reject oversized data with ERR_KEY_TOO_LARGE, etc.
- Validated before allocation

---

### Boundary Conditions - Numeric Limits (4 tests)

- `should_handle_transaction_id_wraparound` - 2^64 or 2^128 ID reuse
- `should_handle_offset_overflow_in_streams` - 2^64 offset limit
- `should_handle_realm_id_limits` - Max realms or unlimited
- `should_handle_area_id_limits` - Max areas per realm or unlimited

**Implementation Needs:**
- Documented ID wraparound behavior
- UUID vs. 64-bit decision documented
- Realm/area limits (or no limit) documented

---

### Timeout & Expiration (4 tests)

- `should_handle_transaction_timeout` - Idle transaction expiry (default 1 hour)
- `should_handle_session_timeout` - Idle session expiry (default 1 hour)
- `should_handle_subscription_timeout` - Long-lived subscription behavior
- `should_handle_lease_expiration` - TTL-based lease grant expiry

**Implementation Needs:**
- Transaction timeout on idle (configurable)
- Session timeout on idle (configurable)
- Subscription behavior on long idle (clear expectation)
- Lease expiry at TTL timestamp

---

### Concurrent Operation Conflicts (5 tests)

- `should_handle_concurrent_puts_to_same_key` - Last-writer-wins or conflict
- `should_handle_transaction_read_then_write` - Snapshot read-write
- `should_handle_concurrent_stream_appends` - One append at a time
- `should_handle_phantom_reads` - Snapshot isolation semantics

**Implementation Needs:**
- Concurrent PUT resolution (documented)
- Snapshot isolation behavior documented
- Stream append locking
- Phantom read acknowledgment (expected in SNAPSHOT)

---

### Resource Limits & Exhaustion (4 tests)

- `should_handle_realm_resource_limits` - Per-realm quota (e.g., 1TB)
- `should_handle_connection_limits` - Max connections per realm
- `should_handle_transaction_limit_per_connection` - Max concurrent transactions
- `should_prevent_memory_exhaustion_attacks` - Frame size limit DoS protection

**Implementation Needs:**
- Per-realm storage quota
- Connection limit enforcement
- Transaction concurrency limit
- Frame size validation before buffering

---

### Recovery Scenarios (5 tests)

- `should_recover_from_partial_transaction_commit` - Crash during COMMIT
- `should_recover_from_incomplete_append` - Crash during APPEND
- `should_handle_broker_restart_during_operation` - In-flight operation on restart
- `should_handle_network_partition` - Client/broker split
- (Additional integration scenarios)

**Implementation Needs:**
- Partial commits discarded on recovery
- Incomplete appends discarded on recovery
- In-flight operations handled gracefully
- Network partition timeout detection

---

### Data Integrity & Correctness (4 tests)

- `should_preserve_key_order_in_kv_scans` - Consistent SCAN ordering
- `should_preserve_event_order_in_stream` - Append order persistence
- `should_detect_data_corruption` - Checksum validation
- `should_handle_duplicate_operations` - Persistent deduplication

**Implementation Needs:**
- Lexicographic ordering in SCAN (consistent)
- Stream event offsets never reordered
- Data checksums (CRC32, Blake3, etc.)
- Dedup token tracking across restarts

---

### Protocol Edge Cases (4 tests)

- `should_handle_empty_request_body` - Empty payload handling
- `should_handle_unknown_operation_codes` - Unknown operation gracefully
- `should_handle_malformed_tlv_frames` - Protocol violation handling
- `should_handle_permission_changes_mid_session` - JWT expiry during session

**Implementation Needs:**
- Clear behavior on empty payloads (valid or error)
- Unknown operation → ERR_UNKNOWN_OPERATION
- Malformed TLV → protocol error, close connection
- JWT expiry → ERR_UNAUTHORIZED on next operation

---

### Key Implementation Guidance (Phase C)

**Recommended Limits (Defaults):**
```
MAX_KEY_SIZE              = 1 MB
MAX_VALUE_SIZE            = 100 MB
MAX_EVENT_SIZE            = 50 MB
MAX_FRAME_SIZE            = 100 MB
MAX_BUFFER_SIZE           = 500 MB (total frame assembly)
TRANSACTION_TIMEOUT       = 1 hour
SESSION_TIMEOUT           = 1 hour
RESPONSE_TIMEOUT          = 30 seconds
PARTIAL_FRAME_TIMEOUT     = 5 seconds
MAX_CONNECTIONS_PER_REALM = 10,000
MAX_CONCURRENT_TXS        = 100 per connection
REALM_STORAGE_QUOTA       = 1 TB per realm (or unlimited)
```

**Data Integrity Approach:**
- CRC32 for frames (fast, 32-bit)
- Blake3 for stored data (cryptographic, 256-bit)
- Checksum validation on every read
- Corruption detected → ERR_DATA_CORRUPTION

**Timeout Strategy:**
- Per-operation timeout (configurable, default 30s)
- Transaction idle timeout (1 hour)
- Session idle timeout (1 hour)
- Partial frame assembly timeout (5s)

---

## Test Execution Summary

### Status
- ✅ **Phase A (error_handling_recovery.rs)**: 28 tests, all compile
- ✅ **Phase B (full_domain_implementations.rs)**: 41 tests, all compile
- ✅ **Phase C (edge_cases_recovery.rs)**: 34 tests, all compile
- **Total:** 103 failing tests ready for implementation

### Test Counts by Category

| Phase | Category | Tests | Purpose |
|-------|----------|-------|---------|
| A | Connection errors | 4 | Transport resilience |
| A | Reset handling | 4 | Graceful reconnect |
| A | Frame validation | 3 | Protocol safety |
| A | UTF-8 validation | 3 | String integrity |
| A | Timeout handling | 4 | Slow-response handling |
| A | Partial frames | 4 | Multi-packet reassembly |
| A | Error classification | 2 | Retry vs. fatal |
| A | Integration | 2 | Domain-specific errors |
| **A Total** | | **28** | Error Handling & Recovery |
| B | KV operations | 8 | Basic + advanced ops |
| B | KV semantics | 5 | Isolation + consistency |
| B | Stream operations | 7 | Append + read + semantics |
| B | Stream semantics | 5 | Watermark + ordering |
| B | Cross-domain | 7 | Concurrency + scale |
| **B Total** | | **41** | Domain Implementations |
| C | Size/numeric | 10 | Boundary conditions |
| C | Timeout/expiry | 4 | TTL enforcement |
| C | Conflicts | 5 | Concurrent access |
| C | Resource limits | 4 | Quota enforcement |
| C | Recovery | 5 | Crash recovery |
| C | Data integrity | 4 | Correctness checks |
| C | Protocol edge cases | 4 | Protocol robustness |
| **C Total** | | **34** | Edge Cases & Recovery |
| **GRAND TOTAL** | | **103** | |

---

## Running the Tests

Each phase compiles cleanly but all tests intentionally fail:

```bash
# Test Phase A (Error Handling & Recovery)
cargo test --test error_handling_recovery -- --nocapture

# Test Phase B (Full Domain Implementations)
cargo test --test full_domain_implementations -- --nocapture

# Test Phase C (Edge Cases & Boundary Conditions)
cargo test --test edge_cases_recovery -- --nocapture

# Run all failing tests
cargo test --test error_handling_recovery --test full_domain_implementations --test edge_cases_recovery
```

Each test prints `panic!("reason")` with clear implementation guidance.

---

## Implementation Strategy

**Recommended Order:**
1. **Phase A first:** Implement error handling in transport layer
2. **Phase B next:** Implement KV and Stream domains
3. **Phase C last:** Add edge case handling and validation

**Parallelization Options:**
- Phase A and B can be done in parallel (different layers)
- Phase C (edge cases) depends on Phases A and B being mostly complete

---

## Documentation References

All tests reference specific lines in protocol documentation:

- **CLIENT.md:** Lines 811–825 (error handling), 1000–1365 (all domains)
- **SERVER.md:** Lines 152–189 (permissions, session lifecycle, recovery)

See [TEST_SUITE_INDEX.md](TEST_SUITE_INDEX.md) for full documentation cross-reference.

---

## Next Steps

After implementing these 103 tests:

1. ✅ Run all three test phases: `cargo test --test error_handling_recovery --test full_domain_implementations --test edge_cases_recovery`
2. ✅ Implement features guided by panic messages
3. ✅ Tests transition from failing → passing
4. ✅ Remove panic lines, add assertions
5. ✅ 100+ tests validate production readiness

---

**Last Updated:** January 21, 2026  
**Total Development Time This Session:** Comprehensive three-phase failing test suite (A+B+C = 103 tests)  
**Next Phase:** Implementation guided by failing tests
