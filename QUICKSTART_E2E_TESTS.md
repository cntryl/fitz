# Quick Start: Complete the E2E Test Suite  

## What's Been Done ✅

- **lease_e2e.rs**: Created & running, exposing real codec bugs
- **notice_e2e.rs**: Created & compiling
- **Test fixtures**: Fixed, enhanced, ready to extend
- **Pattern proven**: Replicable across all 7 domains
- **Full documentation**: See `TEST_IMPLEMENTATION_PLAN.md` and `SESSION_SUMMARY.md`

## What You Need To Do Next 🚀

### 1. Add Connectors for Remaining 5 Domains (15 min)

In `tests/fixtures/transport.rs`, add these connector blocks (copy/paste pattern from Lease):

```rust
// QUEUE DOMAIN
pub struct TcpQueueConnector(TestClient);
pub struct WsQueueConnector(TestWebSocketClient);

#[async_trait::async_trait]
pub trait QueueConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl QueueConnector for TcpQueueConnector { ... }

#[async_trait::async_trait]
impl QueueConnector for WsQueueConnector { ... }

// Repeat for: RPC, Schedule, Stream
```

**Template**: Copy the Lease connector section and replace:
- `Lease` → `Queue` (or other domain)
- `TcpLeaseConnector` → `TcpQueueConnector`
- `WsLeaseConnector` → `WsQueueConnector`

### 2. Create E2E Test Files (5 min each)

For each domain, create `tests/{domain}_e2e.rs`:

```rust
//! {Domain} end-to-end tests

mod fixtures;
use fixtures::transport::*;
use fitz::testkit::TestServer;

// Copy pattern from lease_e2e.rs:
// - 2-3 generic test functions
// - Each test has TCP and WS wrapper tests
// - Aim for 4-8 tests per domain initially
```

**Quick checklist per domain**:
- [ ] Happy path test (basic operation succeeds)
- [ ] One error path test (semantic violation)
- [ ] Both TCP and WS variants
- [ ] File compiles: `cargo test --test {domain}_e2e --no-run`

**Domains to create** (in order by complexity):
1. `queue_e2e.rs` - Medium (enqueue/dequeue/competing consumers)
2. `schedule_e2e.rs` - Simpler (cron validation)
3. `rpc_e2e.rs` - Complex (request/response streaming)
4. `stream_e2e.rs` - Complex (append/read/ordering)

### 3. Run All E2E Tests & Triage (30 min)

```bash
# Compile all
cargo test --all --no-run

# Run E2E tests, capture failures
cargo test --test '*_e2e' -- --nocapture 2>&1 | tee e2e_results.txt

# Categorize failures into:
cp e2e_results.txt KNOWN_TEST_FAILURES.md
# Add sections like:
# ## Lease Domain
# - **FAIL**: should_acquire_lease... → Codec unknown operation 410
# - **FAIL**: should_renew... → Frame incomplete string data
```

### 4. Optional: Add Auth Scenarios (if time)

Change some tests to use `TestServer::start_with_auth(true)` instead of `start()`:

```rust
#[tokio::test]
async fn should_require_auth_tcp() {
    let server = TestServer::start_with_auth(true).await.expect("start");
    // Test should fail without connect/JWT
}
```

## Current Bug Examples

What you should expect to find (from lease_e2e.rs failure log):

```
❌ should_acquire_lease_immediately_tcp
Error: Unknown operation: 400
Reason: Lease domain codec doesn't recognize ACQUIRE op type

❌ should_acquire_lease_immediately_ws  
Error: Incomplete string data
Reason: Frame builder may be truncating payload or parser miscounting bytes

✅ These failures are SUCCESS - they expose real bugs!
```

## File Templates

### `queue_e2e.rs` Template Structure
```rust
async fn enqueue_message<C>(...) { ... }
async fn dequeue_message<C>(...) { ... }
async fn reject_dequeue_empty<C>(...) { ... }

#[tokio::test]
async fn enqueue_message_tcp() { ... }

#[tokio::test]
async fn enqueue_message_ws() { ... }
```

(Repeat for schedule, rpc, stream with domain-specific tests)

## Commands Cheat Sheet

```bash
# Compile single E2E test
cargo test --test queue_e2e --no-run

# Run single E2E test with output
cargo test --test queue_e2e -- --nocapture

# Run all E2E tests, quick summary
cargo test --test '*_e2e' -- --nocapture | grep -E "test |failures"

# Run all tests (unit + advanced + e2e)
cargo test --all

# Format and lint
cargo fmt --all && cargo clippy -D warnings

# Validate test naming
cargo test test_guidelines_compliance
```

## Success Criteria

By the end, you should have:

- [ ] 6 E2E test files (one per domain, KV already complete)
- [ ] 30+ E2E tests total (at least 4 per domain)
- [ ] 50%+ of them failing (exposing codec/implementation bugs - this is good!)
- [ ] `KNOWN_TEST_FAILURES.md` documenting each failure
- [ ] All tests following `should_*` naming convention
- [ ] AAA structure (Arrange/Act/Assert) in all tests >5 lines

## Estimated Time

- Connectors: 15 min
- Create 5 test files: 25-40 min (5-8 min each)
- Run & triage: 30 min
- **Total: ~70-85 minutes**

## Key Reminders

1. **Failing tests = victory** - Each failure reveals a real implementation gap
2. **Pattern is proven** - lease_e2e and notice_e2e work, just replicate
3. **Both transports automatic** - Generic connectors handle TCP + WS
4. **Keep it simple initially** - 2-3 tests per domain is fine, can expand later
5. **No auth yet** - Start with `TestServer::start()`, add auth scenarios after

## Questions?

See:
- `TEST_IMPLEMENTATION_PLAN.md` - Full requirements
- `SESSION_SUMMARY.md` - What was accomplished
- `tests/lease_e2e.rs` - Working template to copy
- `tests/fixtures/transport.rs` - Available builders/parsers
