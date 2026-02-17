# Comprehensive Integration Test Implementation Plan

## Current Status (Session: Feb 17, 2026)

### Completed
✅ **Phase 1: Infrastructure Setup**
- Explored existing test patterns across all 7 domains
- Fixed duplicate function definitions in `tests/fixtures/transport.rs`
- Created domain-specific connector types (TcpLeaseConnector, WsLeaseConnector, etc.)
- Established async transport test infrastructure

✅ **Phase 2: Created lease_e2e.rs Template**
- First working E2E test file with 4 tests covering:
  - Happy path: `should_acquire_lease_immediately` (TCP + WS)
  - Error path: `should_reject_renew_of_unowned_lease` (TCP + WS)
- Tests compile and run, **exposing bugs**:
  - Operation types (400, 410) not recognized in lease domain
  - Frame parsing incomplete (payloads malformed)
  - **RESULT:** Tests failing as designed - revealing incomplete codec implementations

### In Progress
🟡 **Phase 3: Create E2E Tests for All Domains**

Following the proven lease_e2e.rs pattern, need to create:
1. `notice_e2e.rs` - Pub/sub domain (subscribe/publish/fanout patterns)
2. `queue_e2e.rs` - Message queue domain (enqueue/dequeue/competing consumers)
3. `rpc_e2e.rs` - Remote procedure calls (request/response streaming)
4. `schedule_e2e.rs` - Task scheduling (cron-based triggers)
5. `stream_e2e.rs` - Append-only log streams (append/read/ordering)

Each file should include:
- **Happy path tests** (basic operation success)
- **Error path tests** (semantic violations)
- **Stress tests** (rapid operations, state isolation)
- **Both transports** (TCP + WebSocket tested via generic connectors)
- **Auth scenarios** (when time permits)

### Test File Template

All E2E tests follow this structure:
```rust
mod fixtures;
use fixtures::transport::*;
use fitz::testkit::TestServer;

// Generic test implementations
async fn test_name<C>(server: &TestServer) where C: DomainConnector { ... }

// Transport-specific test invocations
#[tokio::test]
async fn test_name_tcp() {
    let server = TestServer::start().await.expect("start");
    test_name::<TcpDomainConnector>(&server).await;
}

#[tokio::test]
async fn test_name_ws() {
    let server = TestServer::start().await.expect("start");
    test_name::<WsDomainConnector>(&server).await;
}
```

### Fixture Enhancements

Added to `tests/fixtures/transport.rs`:
- `TcpLeaseConnector` + `WsLeaseConnector` with `send_and_receive()` method
- `LeaseConnector` trait supporting both TCP and WS
- Frame builders: `build_lease_acquire_immediate()`, `build_lease_renew()`, `build_lease_release()`
- Frame parsers: `parse_lease_response()`

Need to add similar connectors/builders for:
- Notice: `TcpNoticeConnector`, `WsNoticeConnector`, etc.
- Queue: `TcpQueueConnector`, `WsQueueConnector`, etc.
- RPC: `TcpRpcConnector`, `WsRpcConnector`, etc.
- Schedule: `TcpScheduleConnector`, `WsScheduleConnector`, etc.
- Stream: `TcpStreamConnector`, `WsStreamConnector`, etc.

## Known Bugs/Incomplete Implementations (from test failures)

### Lease Domain
- **BUG:** Operation type 410 (RENEW) not recognized by codec/domain handler
- **BUG:** Operation type 400 (ACQUIRE) sends incomplete/malformed data
- **Impact:** All lease E2E tests currently timeout with "Unknown operation" errors

### TBD for Other Domains
Once notice/queue/rpc/schedule/stream E2E tests are created and run, will expose similar gaps.

## Deliverables

### Phase 4: Audit Basics/Advanced (When E2E tests stabilize)
Update existing test files to add:
- Edge-case coverage for each realm/domain combination
- Stress tests with concurrent operations  
- Boundary condition tests (empty payloads, max-size data, etc.)
- Semantic violation tests (race conditions, ordering guarantees, durability)

### Phase 5: Consolidate & Document

Create `KNOWN_TEST_FAILURES.md` with:
- Test name
- Expected failure (incomplete feature? design gap?)
- Associated issue/tracking
- Workarounds if any

## Metrics

**Current Test Count:**
- Basics tier: ~150 tests (KV, Lease, Notice, Queue, RPC, Schedule, Stream)
- Advanced tier: ~125 tests  
- E2E tier: 4 tests (lease_e2e only)
- **Target**: 30+ E2E tests per domain (210+ total when complete)

**Expected Failures (Healthy):**
- 40-50% of new E2E tests will fail initially (exposing codec/domain bugs)
- These failures drive implementation of missing features

## Next Steps (Priority Order)

1. **Create notice_e2e.rs** - Medium complexity, good test case for Pub/Sub semantics
2. **Create queue_e2e.rs** - Medium complexity, tests competing consumer fairness
3. **Create rpc_e2e.rs** - Higher complexity, streaming responses
4. **Create stream_e2e.rs** - Higher complexity, multi-frame semantics
5. **Create schedule_e2e.rs** - Lower complexity, cron validation
6. Triage all failures into `KNOWN_TEST_FAILURES.md`
7. Begin fixing discovered bugs (priority by impact)

## Test Running Commands

```bash
# Run all E2E tests
cargo test --test '*_e2e'

# Run specific domain E2E tests
cargo test --test lease_e2e -- --nocapture

# Run all tests (basics + advanced + e2e)
cargo test --all

# Validate test naming compliance
cargo test test_guidelines_compliance

# Lint
cargo clippy -D warnings
```

## Notes

- All tests are **synchronous at the domain/logic level**, async only at transport
- Tests intentionally expose bugs - failing tests = success
- Reusable generic connector pattern minimizes duplication (one test  func, multiple transports)
- Frame builder/parser utilities in fixtures support adding new domains easily
