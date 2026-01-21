# Idempotency Classification & Deduplication - Failing Tests (v1.0)

**Date Created:** 2026-01-21  
**File:** `tests/idempotency_classification.rs`  
**Test Count:** 33 tests (ALL CURRENTLY FAILING)  
**Purpose:** Drive implementation of idempotency classification and context-dependent deduplication

---

## Overview

Per CLIENT.md lines 892–950, Fitz requires three categories of operation classification:

1. **Idempotent Operations** (safe to retry unconditionally)
2. **Non-Idempotent Operations** (unsafe to retry automatically)
3. **Context-Dependent Operations** (safe to retry with deduplication)

This test suite validates the classification system and deduplication implementation. All tests **intentionally fail** to highlight what needs to be implemented.

---

## Test Categories

### 1. Idempotent Operations (7 tests)
Operations that can be safely retried without side effects:

#### Classification Tests (6)
- `should_classify_kv_get_as_idempotent` - KV GET is read-only
- `should_classify_kv_scan_as_idempotent` - KV SCAN is read-only
- `should_classify_stream_read_as_idempotent` - Stream READ is read-only
- `should_classify_stream_last_as_idempotent` - Stream LAST is read-only
- `should_classify_queue_reserve_as_idempotent` - Queue RESERVE is non-consuming
- `should_classify_notice_query_as_idempotent` - Notice QUERY is read-only

#### Behavior Test (1)
- `should_allow_retry_of_idempotent_operations` - Verify retries don't cause duplicates

---

### 2. Non-Idempotent Operations (8 tests)
Operations that modify state and should NOT be automatically retried:

#### Classification Tests (6)
- `should_classify_kv_put_as_non_idempotent` - KV PUT modifies state
- `should_classify_kv_insert_as_non_idempotent` - KV INSERT modifies state
- `should_classify_stream_append_as_non_idempotent` - Stream APPEND creates events
- `should_classify_queue_enqueue_as_non_idempotent` - Queue ENQUEUE adds messages
- `should_classify_notice_publish_as_non_idempotent` - Notice PUBLISH fans out to subscribers
- `should_classify_kv_begin_as_non_idempotent` - KV BEGIN creates transactions
- `should_classify_kv_commit_as_non_idempotent` - KV COMMIT finalizes transaction

#### Behavior Tests (2)
- `should_prevent_retry_of_non_idempotent_operations` - Verify retries are prevented
- `should_track_idempotent_classification_per_domain` - Per-domain classification documentation

---

### 3. Context-Dependent Operations (6 tests)
Operations that require deduplication to be safe for retry:

#### COMPLETE Pattern (2)
- `should_implement_queue_complete_deduplication_by_message_id` - Dedup by message_id + token
- `should_prevent_queue_complete_replay_with_different_token` - Token prevents unauthorized completion

#### REQUEST Pattern (2)
- `should_implement_rpc_request_deduplication_by_correlation_id` - Dedup by correlation_id (UUID)
- `should_prevent_rpc_request_replay_with_different_correlation_id` - UUID prevents replay

#### Classification Tests (2)
- `should_classify_complete_as_context_dependent` - Requires (message_id, token) pair
- `should_classify_request_as_context_dependent` - Requires correlation_id (UUID)

---

### 4. Deduplication Implementation (7 tests)
Tests that verify deduplication mechanics:

- `should_deduplicate_queue_complete_by_message_id_and_token` - Composite key (msg_id, token)
- `should_deduplicate_rpc_request_by_correlation_id` - UUID-based deduplication
- `should_store_deduplication_state_per_realm` - Realm isolation for dedup state
- `should_expire_deduplication_state_after_ttl` - Dedup entries have TTL
- `should_log_deduplicated_requests_for_debugging` - Operator visibility

---

### 5. Metadata & Framework Support (5 tests)
Tests for client framework integration:

- `should_communicate_idempotency_in_operation_metadata` - Ops expose idempotency flag
- `should_document_deduplication_keys_per_operation` - COMPLETE/REQUEST document dedup key
- `should_allow_client_framework_to_auto_retry_idempotent_ops` - Frameworks can auto-retry
- `should_require_user_confirmation_for_non_idempotent_retry` - Prevents silent retries
- `should_support_custom_retry_policy_per_operation` - User can specify retry behavior

---

## Implementation Requirements

### Core Requirements
1. **Operation Classification System**
   - Metadata per operation indicating: idempotent, non-idempotent, or context-dependent
   - Stored in operation handler or protocol layer
   - Accessible to transport/session layer for retry logic

2. **Deduplication Storage**
   - Per-realm deduplication state (isolated by realm)
   - For COMPLETE: key = (message_id, token)
   - For REQUEST: key = correlation_id (UUID)
   - TTL-based expiration (e.g., 1 hour for normal, longer for RPC)

3. **Deduplication Logic**
   - Check dedup key before executing operation
   - If found: return previous result (for COMPLETE)
   - If found: resume response stream (for REQUEST)
   - If not found: execute and store result

4. **Metadata API**
   - `operation.is_idempotent() -> bool`
   - `operation.is_context_dependent() -> bool`
   - `operation.deduplication_key() -> String` (for context-dependent)

5. **Logging**
   - Log when deduplication is hit (for debugging)
   - Log deduplication entry expiration
   - Include (realm, operation, dedup_key) in logs

### Per-Domain Classification

**Idempotent:**
- KV: GET, SCAN
- Stream: READ, LAST
- Queue: RESERVE
- Notice: QUERY

**Non-Idempotent:**
- KV: PUT, INSERT, DELETE, BEGIN, COMMIT, ROLLBACK
- Stream: APPEND, BEGIN, COMMIT
- Queue: ENQUEUE
- Notice: PUBLISH, SUBSCRIBE, UNSUBSCRIBE
- Lease: ACQUIRE, RENEW, SURRENDER
- RPC: (handled by context-dependent)
- Schedule: CREATE, UPDATE, DELETE

**Context-Dependent (Deduplication Required):**
- Queue: COMPLETE (dedup by message_id + token)
- RPC: REQUEST (dedup by correlation_id)

---

## Test Failures (Expected)

All 33 tests fail with messages like:
```
panicked at 'KV GET idempotency not yet validated: needs implementation...'
panicked at 'Queue COMPLETE message_id + token deduplication not implemented'
```

These failures clearly indicate what feature is missing.

---

## Progression Path

### Phase 1: Classification Infrastructure
1. Add `is_idempotent()` method to operation types
2. Add `is_context_dependent()` and `deduplication_key()` for context-dependent ops
3. Run tests 1-8 (classification tests should start passing)

### Phase 2: Deduplication for COMPLETE
1. Implement Queue COMPLETE deduplication (message_id + token)
2. Verify token prevents replay
3. Run tests 19-20 (COMPLETE tests should pass)

### Phase 3: Deduplication for REQUEST
1. Implement RPC REQUEST deduplication (correlation_id)
2. Resume response streaming on retry
3. Run tests 21-22 (REQUEST tests should pass)

### Phase 4: Metadata & Framework
1. Expose classification in operation metadata
2. Document deduplication keys
3. Provide framework hooks for retry logic
4. Run tests 28-33 (framework tests should pass)

---

## Spec References

- **Idempotency Classification:** CLIENT.md lines 892–950
- **COMPLETE Deduplication:** CLIENT.md lines 930–935
- **REQUEST Deduplication:** CLIENT.md lines 1055–1108
- **Retry Behavior:** CLIENT.md lines 951–980

---

## Quick Test Run

```bash
# List all tests
cargo test --test idempotency_classification -- --list

# Run all (expect 33 failures)
cargo test --test idempotency_classification

# Run one category
cargo test --test idempotency_classification should_classify

# Run with backtrace
RUST_BACKTRACE=1 cargo test --test idempotency_classification
```

---

## Notes

- Tests use `panic!()` with descriptive messages for clarity
- Each test includes comprehensive documentation of what needs implementation
- Tests are organized by feature (classification → deduplication → metadata)
- No dependencies on actual domain implementations (pure specification tests)
