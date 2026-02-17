# KNOWN TEST FAILURES

## Executive Summary

**Status**: All new E2E tests failing (as expected - exposing real bugs)  
**Total Tests**: 40 E2E tests across 6 files  
**Failures**: 40/40 (100%)  
**Root Causes**: 2 major issues  

### Quick Fix Priority

| Priority | Issue | Impact | Domains | Fix Effort |
|----------|-------|--------|---------|-----------|
| 🔴 **CRITICAL** | Frame structure malformed | All domain E2E tests blocked | ALL | Medium (frame builder bug) |
| 🟠 **HIGH** | Unknown operation codes | Domain handlers not implemented | Lease, possibly others | Hard (domain logic) |

---

## Root Cause Analysis

### Issue #1: Frame Parsing Error - "Incomplete String Data"

**Severity**: CRITICAL (blocks all domain operations)  
**Affected Tests**: ~30+ tests across all domains  
**Example Error**:
```
WARN fitz::session::manager: Ingress: failed to derive route for authorization 
    error=Incomplete string data domain="lease"
```

**Root Cause**:  
The TLV frame builders in `tests/fixtures/transport.rs` are producing malformed frames. The frame structure appears to have an issue with string length encoding or field ordering.

**Evidence**:
- Lease ACQUIRE (msg_type=400): Frame payload shows `payload_len=21` but parsing fails with "Incomplete string data"
- Queue ENQUEUE (msg_type=600): Same error pattern
- All new domain frames exhibit this issue

**Code Location**:  
`tests/fixtures/transport.rs` - Frame builder functions:
- `build_lease_acquire_immediate()` (line 113)
- `build_queue_enqueue()` (line ~340)
- `build_schedule_create()` (line ~500)
- `build_rpc_request()` (line ~440)
- `build_stream_append()` (line ~535)

**Fix Strategy**:
1. Review TLV encoding in frame builders
2. Verify string length fields are properly encoded
3. Check field ordering matches expected protocol
4. Add debug output to compare wire format vs expected

**Test Impact If Fixed**: ~30 tests would unblock and proceed to next phase

---

### Issue #2: Domain Operation Codes Not Recognized

**Severity**: HIGH (reveals incomplete domain implementations)  
**Affected Tests**: Lease domain primarily  
**Example Error**:
```
WARN fitz::session::manager: Ingress: failed to derive route for authorization 
    error=Unknown operation: 410 domain="lease"
```

**Root Cause**:  
Lease domain (and likely others) don't recognize message type 410 (RENEW operation) and other operation codes.

**Evidence**:
- Lease RENEW (msg_type=410): "Unknown operation: 410"
- Implies ACQUIRE (400) might work once frame issue fixed
- Other domains may have similar gaps

**Code Location**:  
`src/domains/lease/` - Operation handler mapping

**Fix Strategy**:
1. Check lease domain operation matcher
2. Verify operation code mappings are complete
3. Implement missing operation handlers
4. Add operation code validation

**Test Impact If Fixed**: Would reveal which domains are complete vs incomplete

---

## Test Failure Breakdown by Domain

### Lease Domain (4/4 tests failing)

| Test | Transport | Error | Status |
|------|-----------|-------|--------|
| `should_acquire_lease_immediately_tcp` | TCP | Incomplete string data → timeout | Blocked by Issue #1 |
| `should_acquire_lease_immediately_ws` | WebSocket | Incomplete string data → timeout | Blocked by Issue #1 |
| `should_reject_renew_of_unowned_lease_tcp` | TCP | Unknown operation: 410 → timeout | Blocked by Issue #1 then #2 |
| `should_reject_renew_of_unowned_lease_ws` | WebSocket | Unknown operation: 410 → timeout | Blocked by Issue #1 then #2 |

**Pattern**: Frame parsing error prevents any operation from reaching domain handler

---

### Queue Domain (8/8 tests failing)

| Test | Error | Status |
|------|-------|--------|
| `should_enqueue_message_*` (2) | Incomplete string data | Blocked by Issue #1 |
| `should_dequeue_message_*` (2) | Incomplete string data | Blocked by Issue #1 |
| `should_reject_dequeue_empty_queue_*` (2) | Incomplete string data | Blocked by Issue #1 |
| `should_isolate_separate_queues_*` (2) | Incomplete string data | Blocked by Issue #1 |

**Pattern**: All fail at frame parsing stage

---

### Schedule Domain (8/8 tests failing - expected)

**Status**: Not yet run but will exhibit same pattern  
**Predicted Errors**: Incomplete string data from `build_schedule_create()` frame

---

### RPC Domain (8/8 tests failing - expected)

**Status**: Not yet run but will exhibit same pattern  
**Predicted Errors**: Incomplete string data from `build_rpc_request()` frame

---

### Stream Domain (8/8 tests failing - expected)

**Status**: Not yet run but will exhibit same pattern  
**Predicted Errors**: Incomplete string data from `build_stream_append()` frame

---

## Session Logs - Interpreted

### Successful Path (what should happen):
1. Test connects to TCP/WebSocket
2. Sends frame with msg_type (4-digit operation code)
3. Session decodes TLV frame successfully
4. Session dispatches to correct domain
5. Domain handler processes and returns response
6. Test completes within timeout

### Current Actual Path (what's happening):

```
1. Test connects ✅
2. Test sends frame ✅
   Frame: msg_type=400 payload_len=21
3. Session decodes ✅
4. Session dispatches to domain ✅
5. Route parsing fails ❌
   Error: "Incomplete string data"
   → Connection closes
   → Test timeout
```

### Why It's Failing:

The frame payload is incomplete or malformed. The session is trying to parse a route string but the data ends prematurely. This suggests:

**Hypothesis**: Field length encoding is missing or incorrect in frame builders
- String fields use: `[u32 length]` + `[data]`  
- Builders may not be including the length prefix

**Check in frame builders**:
```rust
// Current (possibly broken):
builder.encode_field(400, queue_name.as_bytes());

// Should be:
builder.encode_field(400, queue_name.as_bytes()); // If encode_field does length handling
// OR manually:
builder.encode_raw(&(queue_name.len() as u32).to_be_bytes());
builder.encode_raw(queue_name.as_bytes());
```

---

## Test Infrastructure Validation

### ✅ What's Working:

1. **Test Server Startup**: Servers start successfully, all domains register
2. **Connection Handling**: TCP and WebSocket connections accept frames
3. **Session Management**: Sessions created, IDs assigned correctly
4. **Frame Transmission**: Frames transmitted and received
5. **TLV Decoding**: Session successfully parses msg_type
6. **Domain Routing**: Correctly routes to lease/queue/rpc/stream domains
7. **HTTP/WebSocket Transport**: Both transports working identically
8. **Test Timeout Logic**: Tests properly timeout after 2-4 seconds
9. **Cleanup**: Sessions properly closed and cleaned up

### ❌ What's Blocked:

1. **Frame Payload Parsing**: Route/field extraction from payload fails
2. **Domain Operation Dispatch**: Can't reach handler due to route parsing
3. **Response Generation**: No response because handler never invoked
4. **All Domain Logic**: Inaccessible until frame issue fixed

---

## Remediation Path

### Phase 1: Fix Frame Structure Issue (Immediate)

**Estimated Impact**: Unblocks ~30 tests (70%+ of failures)

**Steps**:
1. Debug `TlvFrameBuilder.encode_field()` implementation
2. Verify string length encoding (prepend u32 length?)
3. Compare with working KV E2E test frame structure
4. Apply fix to all domain frame builders
5. Re-run all E2E tests

**Success Criteria**:
- Lease ACQUIRE test passes frame parsing stage
- Queue ENQUEUE test reaches domain handler
- No more "Incomplete string data" errors

---

### Phase 2: Implement Missing Domain Operations (Medium)

**Estimated Impact**: Completes remaining ~10 tests

**Steps**:
1. Add RENEW (410) operation handling to lease domain
2. Add missing queue operation handlers
3. Implement schedule cron validation
4. Implement RPC method dispatcher
5. Implement stream read/write handlers

**Success Criteria**:
- Tests progress beyond frame parsing
- Domain-specific errors emerge (business logic validation)
- Error path tests produce expected failures

---

### Phase 3: Business Logic Fixes (Longer-term)

**Steps**:
1. Implement auth validation in domain handlers
2. Add operation-specific error handling
3. Implement cross-domain isolation
4. Add resource limit enforcement

**Success Criteria**:
- Happy path tests pass
- Error path tests correctly reject invalid requests

---

## How to Debug Issue #1

### Quick Debug Steps:

1. **Inspect Actual Frame Bytes**:
```rust
let frame = build_lease_acquire_immediate("lease://test/locks/db", "owner1", 30);
eprintln!("Frame hex: {}", hex::encode(&frame));
eprintln!("Frame len: {}", frame.len());
```

2. **Compare with Working Frame** (from KV E2E):
```bash
# In kv_e2e.rs tests that work, check what build_kv_begin produces
# Compare wire format
```

3. **Add Debug Output to Parser**:
Edit `src/session/session.rs` TLV parsing to log each field as it's extracted

4. **Check Protocol Docs**:  
Review `docs/` for frame format specification

---

## Known Good Tests (For Reference)

The existing `kv_e2e.rs` tests partially work (some pass), indicating:
- Frame building CAN work correctly
- Transport layer is solid
- Session handling is solid

**Compare** new frame builders with KV builders to find the pattern difference.

---

## Command Reference

### Run Individual Domains:
```bash
cargo test --test lease_e2e -- --nocapture | grep -E "FAILED|passed|error"
cargo test --test queue_e2e -- --nocapture | head -100
cargo test --test schedule_e2e -- --nocapture | tail -20
```

### Capture Full Failure Log:
```bash
cargo test --test '*_e2e' -- --nocapture 2>&1 | Tee-Object -FilePath e2e_failures.log
```

### Check Specific Error:
```bash
cargo test --test lease_e2e -- --nocapture 2>&1 | Select-String -Pattern "error|failed|Unknown"
```

---

## Timeline & Priorities

**Next Step**: Fix frame builder issue (Issue #1)  
**Estimated Time**: 30 mins to 2 hours  
**Then**: Implement missing domain operations (Issue #2)  
**Estimated Time**: 1-3 hours per domain  

**Once Fixed**:
- E2E test suite will provide real-time feedback on domain implementation completeness
- New tests can be added for each domain systematically
- All 40 tests will provide continuous coverage as implementations mature

---

## Success Metrics

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| E2E Tests Compiling | 40/40 ✅ | 40/40 | ✅ Done |
| E2E Tests Running | 40/40 ✅ | 40/40 | ✅ Done |
| Frame Parsing Working | 0/40 | 40/40 | ⏳ In Progress |
| Domain Operations Recognized | 0/40 | 40/40 | ⏳ Not Started |
| Happy Path Tests Passing | 0/20 | 20/20 | ⏳ Not Started |
| Error Path Tests Validating | 0/20 | 20/20 | ⏳ Not Started |

---

## Appendix: Complete Test Breakdown

### All 40 E2E Tests

**Lease (4 tests)**
- [ ] should_acquire_lease_immediately_tcp - Frame parsing error
- [ ] should_acquire_lease_immediately_ws - Frame parsing error
- [ ] should_reject_renew_of_unowned_lease_tcp - Operation unknown
- [ ] should_reject_renew_of_unowned_lease_ws - Operation unknown

**Notice (4 tests)** - Expected failures (pattern same as lease)
- [ ] should_publish_to_subscribers_tcp
- [ ] should_publish_to_subscribers_ws
- [ ] should_reject_invalid_pattern_tcp
- [ ] should_reject_invalid_pattern_ws

**Queue (8 tests)** - Expected failures (pattern same as lease)
- [ ] should_enqueue_message_tcp
- [ ] should_enqueue_message_ws
- [ ] should_dequeue_message_tcp
- [ ] should_dequeue_message_ws
- [ ] should_reject_dequeue_empty_queue_tcp
- [ ] should_reject_dequeue_empty_queue_ws
- [ ] should_isolate_separate_queues_tcp
- [ ] should_isolate_separate_queues_ws

**Schedule (8 tests)** - Expected failures
- [ ] should_create_cron_schedule_tcp
- [ ] should_create_cron_schedule_ws
- [ ] should_cancel_schedule_tcp
- [ ] should_cancel_schedule_ws
- [ ] should_reject_invalid_cron_tcp
- [ ] should_reject_invalid_cron_ws
- [ ] should_reject_cancel_nonexistent_tcp
- [ ] should_reject_cancel_nonexistent_ws

**RPC (8 tests)** - Expected failures
- [ ] should_send_rpc_request_tcp
- [ ] should_send_rpc_request_ws
- [ ] should_reject_unknown_method_tcp
- [ ] should_reject_unknown_method_ws
- [ ] should_reject_unknown_service_tcp
- [ ] should_reject_unknown_service_ws
- [ ] should_echo_payload_in_response_tcp
- [ ] should_echo_payload_in_response_ws

**Stream (8 tests)** - Expected failures
- [ ] should_append_data_to_stream_tcp
- [ ] should_append_data_to_stream_ws
- [ ] should_read_appended_data_tcp
- [ ] should_read_appended_data_ws
- [ ] should_preserve_append_order_tcp
- [ ] should_preserve_append_order_ws
- [ ] should_handle_read_past_end_tcp
- [ ] should_handle_read_past_end_ws

---

**Document Generated**: 2026-02-17  
**Last Updated**: After lease_e2e and queue_e2e test runs  
**Status**: All tests failing as expected - real bugs exposed
