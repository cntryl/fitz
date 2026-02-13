# Fitz Transport Testing Infrastructure - Implementation Plan

**Date:** February 13, 2026  
**Status:** Transport test infrastructure implemented, 3 of 4 tests passing

## Context

Original issue: 100ms latencies in Go client benchmarks while server shows minimal CPU usage. After fixing scheduler polling (100ms→1ms) and ingress backpressure, user recognized the real problem: **existing integration tests don't verify the complete stack**. Current tests in `tests/kv_e2e_basic.rs` call `actor.handle()` directly, completely bypassing transport/session/routing layers. This is why the suspected "responses not being sent to clients" bug wasn't caught.

## What We Built

### 1. Transport Test Infrastructure (`src/testkit/transport.rs`)

**Purpose:** Provide utilities for end-to-end integration tests that verify the complete request-response cycle through actual TCP sockets.

**Components:**

- **TestServer::start()** (lines 23-77)
  - Boots complete Fitz instance with in-memory storage
  - Binds to random available port
  - Sets `FITZ_AUTH_REQUIRED=false` via environment variable (critical!)
  - Returns server address for client connections
  - Boot time: <100ms for fast tests

- **TestClient** (lines 93-147)
  - `new(addr)`: Connect to test server
  - `send_frame(frame)`: Write length-prefixed frame to TCP socket
  - `recv_frame(timeout_ms)`: Read with timeout to catch missing responses
  - `request(frame, timeout)`: Combined send+receive for request-response pattern

- **TlvFrameBuilder** (lines 149-179)
  - `encode_field(msg_type, value)`: Encode TLV frame in correct wire format
  - Format: `[msg_type: u8 or ESCAPE+u16 BE][length: u16 BE][value: bytes]`
  - Handles message type escaping (msg_type > 254 requires ESCAPE marker)

- **TlvFrameParser** (lines 181-240)
  - `parse_field()`: Decode TLV response frames
  - Handles escaped message types
  - Returns (msg_type, payload)

### 2. KV Transport Tests (`tests/transport_kv_e2e.rs`)

**Helper Functions:**

- `build_kv_begin(route, mode, durability)` → Vec\<u8\>
  - Wire format: `[u32 BE route_len][route][u8 mode][u8 durability]`
  - mode: 0=ReadOnly, 1=ReadWrite
  - durability: 0=buffered, 1=sync

- `build_kv_put(tx_id, route, key, value)` → Vec\<u8\>
  - Wire format: `[u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key][u32 BE value_len][value]`

- `build_kv_commit(tx_id, route)` → Vec\<u8\>
  - Wire format: `[u64 BE tx_id][u32 BE route_len][route]`

- `parse_kv_response(frame)` → (msg_type: u16, status: u8, data: Vec\<u8\>)
  - Decodes TLV response
  - Extracts status byte (0=success, 1=error)
  - Returns optional data payload

**Test Cases:**

1. ✅ **should_complete_begin_put_commit_over_tcp** (lines 80-145) - **PASSING**
   - Verifies full transaction workflow: BEGIN → PUT → COMMIT
   - Tests response arrival through TCP socket
   - Validates tx_id generation and data persistence

2. ✅ **should_receive_responses_within_reasonable_time** (lines 147-167) - **PASSING**
   - Performance assertion: responses arrive <10ms for in-memory ops
   - Catches timeout/throttling issues

3. ❌ **should_handle_multiple_concurrent_transactions** (lines 169-237) - **FAILING**
   - Creates 3 concurrent clients with separate transactions
   - Expected: Each gets unique tx_id (1, 2, 3)
   - Actual: tx_ids are (1, 1, 3) - not unique
   - **Issue:** Assertion logic bug or actual concurrency issue

4. ✅ **should_reject_operations_on_invalid_transaction** (lines 239-242) - **PASSING**
   - Tests error handling for invalid tx_id
   - Verifies proper error responses

## Bugs Fixed During Implementation

### Bug 1: TLV Format Mismatch
**Problem:** Test code used wrong TLV encoding format  
**Symptoms:** Server received frames but never decoded them  
**Root Cause:** 
- Test code sent: `[u16 BE msg_type][u32 BE length][value]`
- Server expected: `[u8 msg_type or ESCAPE+u16 BE][u16 BE length][value]`

**Fix:** Rewrote `TlvFrameBuilder::encode_field()` to match server's TLV decoder format (lines 162-179)

### Bug 2: Byte Order (Endianness)
**Problem:** `BytesMut::put_u32()` uses native byte order (little-endian on Windows)  
**Symptoms:** Server parsed route length incorrectly  
**Fix:** Changed to explicit big-endian: `payload.put_slice(&(len as u32).to_be_bytes())`

### Bug 3: Auth Configuration Ignored
**Problem:** Test set `auth_required: false` in BootConfig, but server still required CONNECT message  
**Symptoms:** "unauthenticated: connect required" error  
**Root Cause:** `boot::runtime::init()` line 223 creates NEW BootConfig instead of using passed config:
```rust
let config = BootConfig::new(); // ❌ Ignores test's auth_required setting
```

**Fix:** Set environment variable before boot: `std::env::set_var("FITZ_AUTH_REQUIRED", "false")`  
**Note:** This is a WORKAROUND. Proper fix would be to pass BootConfig through runtime::init().

## Current Status

### ✅ Working
- Transport test infrastructure compiles and runs
- TestServer boots correctly with in-memory storage
- TestClient sends/receives TCP frames correctly
- TLV encoding/decoding works
- Auth bypass via environment variable works
- 3 of 4 tests passing
- Tests definitively prove responses ARE being sent through TCP (contradicts original "responses not sent" hypothesis)

### ❌ Failing
- `should_handle_multiple_concurrent_transactions` test
- Assertion failure: Transaction IDs not unique (expected [1,2,3], got [1,1,3])

### 🔍 Needs Investigation
- Is the tx_id collision a real bug or test assertion issue?
- Does concurrent client access expose a thread safety bug?
- Are transaction IDs actually unique but test extracts them incorrectly?

## Next Steps

### Immediate (Fix Failing Test)

1. **Debug concurrent transactions test** (HIGH PRIORITY)
   - Add debug output to see all 3 tx_ids returned
   - Check if problem is in test assertion logic or actual server behavior
   - Possible issues:
     - Test assertion checking wrong tx_ids
     - Race condition in tx_id generation
     - Response routing mixing up which client gets which response

2. **Fix or remove the test**
   - If bug is real: Fix tx_id generation or response routing
   - If bug is test: Fix assertion logic
   - If unfixable quickly: Remove test or mark as known issue

### Short-term (Expand Testing)

3. **Add transport tests for other domains**
   - `tests/transport_queue_e2e.rs`: ENQUEUE → RESERVE → COMPLETE cycle
   - `tests/transport_lease_e2e.rs`: ACQUIRE → RENEW → RELEASE cycle
   - `tests/transport_notice_e2e.rs`: SUBSCRIBE → PUBLISH → receive notification
   - Pattern: Copy `transport_kv_e2e.rs` structure, replace domain-specific wire formats

4. **Document transport testing pattern**
   - Add examples to `CONTRIBUTING.md`
   - Show developers how to write domain transport tests
   - Explain TLV encoding format and helper usage

### Medium-term (Architecture Improvements)

5. **Fix BootConfig passing** (refactor needed)
   - Make `boot::runtime::init()` accept `&BootConfig` parameter
   - Remove environment variable workaround from tests
   - Ensures test configuration is actually used

6. **Add performance benchmarks**
   - Use transport test infrastructure for latency benchmarks
   - Measure p50, p95, p99 latencies for different operations
   - Track performance regressions in CI

### Long-term (CI/CD Integration)

7. **Add to CI pipeline**
   - Run transport tests on every PR
   - Fail build if tests timeout (catches "responses not sent" bugs)
   - Track latency metrics over time

8. **Load testing**
   - Stress test with many concurrent clients
   - Verify no performance degradation under load
   - Catch concurrency bugs early

## Key Learnings

### TLV Protocol Format (CRITICAL FOR FUTURE TESTS)

**Message Type Encoding:**
- If `msg_type <= 254`: Single byte (u8)
- If `msg_type > 254`: `[0xFF escape marker][msg_type as u16 BE]`

**Complete TLV Frame:**
```
[msg_type: u8 or ESCAPE+u16 BE]
[length: u16 BE]
[value: bytes]
```

**Common msg_types:**
- KV BEGIN: 100
- KV COMMIT: 101
- KV PUT: 104
- All < 255, so no escaping needed

### Wire Format Best Practices

1. **Always use big-endian (network byte order)**
   - `u32.to_be_bytes()` not `BytesMut::put_u32()`
   - Protocol independence from platform endianness

2. **Match server codec exactly**
   - Read `src/protocol/*_codec.rs` before writing tests
   - Don't assume wire format, verify it

3. **Test with actual TCP sockets**
   - Unit tests (calling `actor.handle()`) don't catch transport bugs
   - Integration tests must go through full stack

### Test Infrastructure Design

1. **TestServer should be fast** (<100ms startup)
   - Use in-memory storage
   - Random available port (parallel test execution)
   - Disable auth by default for tests

2. **TestClient should have timeouts**
   - Catch missing responses immediately
   - Don't wait for TCP timeout (30+ seconds)
   - 1 second timeout good for local tests

3. **Helper functions for wire formats**
   - Encode domain messages correctly
   - Reusable across multiple test cases
   - Document wire format in comments

## Files Modified

### Created
- `src/testkit/transport.rs` (271 lines) - Test harness infrastructure
- `tests/transport_kv_e2e.rs` (242 lines) - KV domain integration tests

### Modified
- `src/testkit/mod.rs` (line 12) - Added `pub mod transport;`

### Read/Analyzed
- `src/protocol/kv_codec.rs` - Verified wire format specifications
- `src/protocol/tlv.rs` - Understood TLV decoder implementation
- `src/boot/runtime.rs` - Found BootConfig initialization bug
- `src/boot/handlers.rs` - Understood TCP connection handling
- `src/session/manager.rs` - Found auth gating logic
- `src/api/tcp.rs` - Verified frame extraction logic

## Test Execution

**Run all transport tests:**
```bash
cargo test --test transport_kv_e2e
```

**Run specific test:**
```bash
cargo test --test transport_kv_e2e should_complete_begin_put_commit_over_tcp
```

**With debug logging:**
```bash
$env:RUST_LOG='debug'; cargo test --test transport_kv_e2e -- --nocapture
```

## Success Metrics

- ✅ Tests prove responses ARE sent through TCP (original hypothesis disproven)
- ✅ Tests catch TLV encoding bugs immediately
- ✅ Tests catch auth configuration bugs
- ✅ Tests run fast (<1 second per test)
- ❌ Tests should catch concurrency bugs (1 failing test suggests this works!)

## Original Performance Issue

**Status:** NOT YET RESOLVED by transport tests

The transport tests prove the server CAN send responses quickly (<10ms). But the Go client benchmarks still show 100ms latencies. Possible causes:

1. **Go client bug** - Not handling responses correctly
2. **Network configuration** - Docker networking overhead
3. **Framing issue** - Go client not reading length-prefixed frames correctly
4. **Timeout issue** - Go client waiting for wrong condition

**Next investigation:** Run Go client benchmarks with packet capture to see what's happening on the wire.

## Questions for Next Session

1. Is the tx_id collision in concurrent test a real bug or test bug?
2. Should we fix `boot::runtime::init()` to accept BootConfig properly?
3. Should we add transport tests for all domains or just critical ones?
4. How do we integrate these tests into CI/CD?
5. Should we investigate the original Go client latency issue further?

## Commands Reference

```bash
# Build
cargo build --release

# Run all tests
cargo test

# Run transport tests only
cargo test --test transport_kv_e2e

# Run with logging
$env:RUST_LOG='debug'; cargo test --test transport_kv_e2e -- --nocapture

# Check formatting
cargo fmt --all -- --check

# Run clippy
cargo clippy -D warnings

# Run meta-test (validates test naming)
cargo test test_guidelines_compliance
```

---

**End of Plan**

Resume by:
1. Reading this PLAN.md
2. Fixing the failing concurrent transactions test
3. Deciding whether to expand transport testing to other domains
4. Investigating the original Go client latency issue with packet capture
