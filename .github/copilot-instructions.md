# GitHub Copilot Instructions for Fitz Project

- did you validate tests? `cargo fitz-tools validate-tests --summary`
- did you fix all clippy warnings? `cargo clippy --workspace --all-targets`

## Test Writing Guidelines - STRICTLY ENFORCE

When generating or suggesting tests, **ALWAYS** follow these rules:

### 1. Naming Convention (MANDATORY)

- ✅ Use `should_*` naming pattern
- ❌ NEVER use `test_*` naming
- Format: `should_{action}_{condition}_given_{context}`

```rust
// ✅ CORRECT
#[test]
fn should_return_value_when_key_exists() { }

// ❌ WRONG - Will fail meta-test!
#[test]
fn test_get_value() { }
```

### 2. AAA Structure (MANDATORY for tests >5 lines)

Every test MUST have exactly these three comments:

```rust
#[test]
fn should_do_something() {
    // Arrange
    let setup = create_test_data();

    // Act
    let result = perform_operation(setup);

    // Assert
    assert_eq!(result, expected);
}
```

**NEVER use:**

- ❌ `// Arrange & Act` (combined)
- ❌ `// Act & Assert` (combined)
- ❌ `// Setup` (use Arrange)
- ❌ `// Arrange: setup data` (no suffixes)
- ❌ Descriptive AAA comments like `// Arrange - create database`

**ALWAYS use:**

- ✅ Exactly `// Arrange` (no suffix, no combination)
- ✅ Exactly `// Act` (no suffix, no combination)
- ✅ Exactly `// Assert` (no suffix, no combination)

### 3. Single Behavior Principle (PEDANTIC RULE)

**CRITICAL: If each assert_eq! describes a different input-output mapping, create separate tests.**

```rust
// ❌ WRONG - Testing 3 different inputs
#[test]
fn should_return_files_at_level() {
    let l0 = manifest.files_at_level(0);
    let l1 = manifest.files_at_level(1);
    let l2 = manifest.files_at_level(2);
    assert_eq!(l0.len(), 1);  // Different input!
    assert_eq!(l1.len(), 2);  // Different input!
    assert_eq!(l2.len(), 0);  // Different input!
}

// ✅ CORRECT - 3 focused tests
#[test]
fn should_return_files_at_level_zero() {
    // Arrange
    let manifest = setup_with_level_0_files();

    // Act
    let result = manifest.files_at_level(0);

    // Assert
    assert_eq!(result.len(), 1);
}

#[test]
fn should_return_files_at_level_one() {
    // Arrange
    let manifest = setup_with_level_1_files();

    // Act
    let result = manifest.files_at_level(1);

    // Assert
    assert_eq!(result.len(), 2);
}
```

**Exception:** Multiple assertions checking facets of ONE property are OK:

```rust
// ✅ CORRECT - All assertions verify one operation
#[test]
fn should_preserve_data_across_save_load() {
    // Arrange
    let original = create_manifest();

    // Act
    let loaded = save_and_load(original);

    // Assert
    assert_eq!(loaded.id, original.id);      // ✅ Same operation
    assert_eq!(loaded.name, original.name);  // ✅ Same operation
    assert_eq!(loaded.size, original.size);  // ✅ Same operation
}
```

### 4. No Multiple Act Sections

**NEVER have multiple `// Act` comments in one test.**

```rust
// ❌ WRONG - Two operations
#[test]
fn should_upload_and_download() {
    // Arrange
    let backend = Backend::new();

    // Act
    backend.upload("data");  // First operation

    // Assert
    assert_eq!(backend.count(), 1);

    // Act  // ❌ SECOND ACT - WRONG!
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");
}

// ✅ CORRECT - Split into 2 tests
#[test]
fn should_upload_data_successfully() {
    // Arrange
    let backend = Backend::new();

    // Act
    backend.upload("data");

    // Assert
    assert_eq!(backend.count(), 1);
}

#[test]
fn should_download_uploaded_data() {
    // Arrange
    let backend = Backend::new();
    backend.upload("data");

    // Act
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");
}
```

### 5. Small Tests Can Omit AAA

Tests with ≤5 lines don't need AAA comments, but still need proper naming:

```rust
// ✅ CORRECT - Small test, no AAA needed
#[test]
fn should_create_default_config() {
    let config = Config::default();
    assert_eq!(config.timeout, 30);
}
```

## Common Patterns

### Testing Serialization/Deserialization

**ALWAYS split serialize and deserialize into separate tests:**

```rust
// ✅ CORRECT
#[test]
fn should_serialize_manifest() {
    // Arrange
    let manifest = create_manifest();

    // Act
    let result = serde_json::to_string(&manifest);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_deserialize_manifest() {
    // Arrange
    let original = create_manifest();
    let json = serde_json::to_string(&original).unwrap();

    // Act
    let deserialized: Manifest = serde_json::from_str(&json).unwrap();

    // Assert
    assert_eq!(deserialized.id, original.id);
}
```

### Testing Multiple Scenarios

Create separate tests for each scenario:

```rust
// ✅ CORRECT - Separate tests per scenario
#[test]
fn should_return_value_when_key_exists() {
    // Arrange
    let db = Database::new();
    db.insert("key", "value");

    // Act
    let result = db.get("key");

    // Assert
    assert_eq!(result, Some("value"));
}

#[test]
fn should_return_none_when_key_does_not_exist() {
    // Arrange
    let db = Database::new();

    // Act
    let result = db.get("nonexistent");

    // Assert
    assert_eq!(result, None);
}
```

### Table-Driven Tests (When Appropriate)

Use for same operation with different inputs:

```rust
#[test]
fn should_validate_range_bounds_correctly() {
    // Arrange
    let test_cases = vec![
        (0, 10, true),   // valid
        (10, 0, false),  // invalid
        (5, 5, false),   // invalid
    ];

    // Act & Assert
    for (start, end, expected) in test_cases {
        let result = is_valid_range(start, end);
        assert_eq!(result, expected, "Failed for ({}, {})", start, end);
    }
}
```

## Meta-Test Enforcement

**All tests are validated by `tests/test_guidelines_compliance.rs`**

The meta-test will FAIL if:

- Any test uses `test_*` naming
- Tests >5 lines are missing AAA comments
- Tests have combined AAA comments (`// Arrange & Act`)

Run it with:

```bash
cargo test test_guidelines_compliance
```

## Quick Checklist for Copilot

Before suggesting a test, verify:

- [ ] Name starts with `should_`
- [ ] If >5 lines, has `// Arrange`, `// Act`, `// Assert` (exact format)
- [ ] Only ONE `// Act` section
- [ ] Each test verifies ONE specific behavior
- [ ] Multiple assertions only if they verify facets of the SAME operation

## Examples from Codebase

See these files for excellent examples:

- `src/manifest.rs` - Clean AAA structure, proper splitting
- `src/index/range_tombstone.rs` - Single-behavior tests
- `src/cloud/mock.rs` - Upload/download properly split

## Why These Rules?

1. **Consistency**: All tests look the same → easier to read
2. **Debuggability**: One test fails → know exactly what broke
3. **Maintainability**: Change behavior → update one focused test
4. **Documentation**: Tests serve as examples of how to use code
5. **CI/CD**: Meta-test enforces rules automatically

---

**REMEMBER: When in doubt, create MORE smaller tests rather than fewer large tests!**

---

## Fitz Domain Layer Architecture - Sync-Only Rules

> **CRITICAL FOR COPILOT:**
> Domain handlers and services are 100% synchronous.
> Never use `async fn`, `.await`, `tokio::spawn`, `oneshot`, or `tokio::sync` types in domain code.

**Fitz Domain Layer — Authoritative Rules for Copilot**

Fitz uses a **strict async-at-transport, sync-in-domain** model.
Copilot must follow these rules when generating domain code:

### 1. Domain handlers are 100% synchronous

Domain functions:

- **MUST NOT be async**
- **MUST NOT return futures**
- **MUST NOT use `.await`**
- **MUST NOT use tokio types** (`tokio::spawn`, `oneshot`, `tokio::sync::Mutex`, `tokio::sync::RwLock`, etc.)
- **MUST NOT perform async I/O**

A domain handler signature must look like:

```rust
fn handle(&self, req: DomainContext) -> DomainResponse
```

All work is synchronous and returns immediately.

### 2. Async boundaries are _before_ domain handlers

WebSocket tasks do:

- framing
- read/write
- correlation
- passing bytes into the engine

The engine does:

- parsing
- route lookup
- **calls the domain handler synchronously**
- returns a `DomainResponse` immediately

The domain itself does **zero async** work.

### 3. Long-running or waiting operations must use synchronous primitives

If a domain needs to:

- block
- wait for a lease
- contest ownership
- coordinate state

…then it must use **synchronous concurrency primitives**:

- `std::sync::Mutex` / `RwLock`
- `parking_lot::Mutex` / `RwLock`
- `DashMap`
- `std::sync::Condvar`
- lock-free structures
- sharded state

**Not tokio waits, not timers, not async sleepers.**

### 4. DomainContext arrives fully parsed and ready

Transport + engine provide:

- parsed route
- borrowed payload slice
- channel ID
- route family
- possibly a sender for publish-style operations

Domains never parse HTTP, never deal with WebSocket framing, never work with async readers/writers.

### 5. DomainResponse is always produced synchronously

Copilot must implement:

```rust
DomainResponse::Frame(PooledFrame)
DomainResponse::Error(String)
DomainResponse::Ok
```

with no async.

Building TLV, validating tags, constructing responses—everything is sync, using pre-allocated buffers where possible.

### 6. Example of correct domain structure

Copilot should follow this pattern:

```rust
impl Domain for LeaseDomain {
    fn handle(&self, req: DomainContext) -> DomainResponse {
        let op = LeaseOperation::from_route(&req.route)?;
        match op {
            LeaseOperation::Acquire => self.handle_acquire(&req),
            LeaseOperation::Renew   => self.handle_renew(&req),
            LeaseOperation::Surrender => self.handle_surrender(&req),
        }
    }
}

impl LeaseDomain {
    fn handle_acquire(&self, req: &DomainContext) -> DomainResponse {
        let ttl = parse_ttl(req.payload)?;
        let grant = self.service.acquire_sync(req.route_family, req.route_str, ttl);
        build_tlv_response(grant)
    }

    // etc.
}
```

Where `acquire_sync` is a synchronous function using only sync primitives.

### 7. Never reintroduce the old async pattern

Copilot must **not** generate:

```rust
async fn process(...)
impl Domain for X {
    fn handle(...) -> Pin<Box<dyn Future>>
}
tokio::spawn(...)
oneshot::channel()
tokio::time::sleep(...)
tokio::sync::RwLock
```

These are **forbidden** in domain code.

### 8. The only async code in Fitz lives in:

- HTTP transport
- WebSocket transport
- TLS upgrade
- async file/network ops for _external_ systems (never internal operations)

Everything else is deterministic synchronous Rust.

---

## Fitz System Architecture - Async/Sync Boundary Rules

> **NOTE FOR COPILOT:**
> Fitz uses async only in WebSocket/HTTP transport.
> All engine and domain code must be synchronous.
> Never introduce `.await`, `tokio::spawn`, `oneshot`, or async locks inside the engine.

**Fitz System Architecture (Authoritative Instruction for Copilot)**

Fitz uses a **strict async-at-the-edges, sync-in-the-core** design.
Copilot must generate code that follows these rules:

### 1. Transport layers are async

- HTTP (Hyper)
- WebSockets (tokio-tungstenite)
- TLS upgrades
- Socket I/O

These layers are responsible only for:

- accepting connections
- reading frames
- writing frames
- performing upgrades
- lightweight routing to the engine boundary

**No domain logic may run inside async tasks.**

### 2. Engine is 100% synchronous

- The core engine is a **single-threaded or sharded deterministic loop**
- No async, no `.await`, no tokio tasks, no oneshot channels
- All domain logic (routing, RPC, queues, leases, streams, KV, WAL interaction) occurs synchronously
- The engine is driven by an event queue (`EngineEvent`)
- Domain handlers are synchronous functions returning results immediately

### 3. Async ↔ Sync boundary

- WebSocket tasks forward inbound frames to the engine via a **crossbeam or SPSC queue**
- The engine pushes outbound frames via a per-connection **Sender<Vec<u8>>**
- Async tasks must not hold or call any engine domain types directly
- The boundary is always:
  ```
  WS frame → engine.on_frame(conn_id, bytes)
  engine → outbound_tx.send(binary_frame)
  ```

### 4. No "spawn per request" patterns

- The previous async spawn-per-dispatch model is removed
- Domain operations must _never_ spawn tokio tasks
- Domain concurrency must only come from engine sharding (realm/tenant partitioning), not async tasks

### 5. Data flow summary

```
[WS/HTTP async]
       ↓
(frame bytes)
       ↓
[Engine.sync.handle_frame()]
       ↓
(domain work sync)
       ↓
(outbound bytes)
       ↓
[WS async writer]
```

### 6. Performance intent

- Minimize jitter and remove async scheduling from hot paths
- Maintain strong ordering, deterministic processing, and predictable latency
- Avoid tokio locks; prefer parking_lot and lock-free structures
- No async IO inside engine code

### 7. What Copilot should produce

Copilot must:

- Keep transport async but extremely thin
- Keep all engine and domain logic synchronous
- Use crossbeam channels or SPSC ring buffers for engine event queues
- Avoid introducing any async constructs inside engine code
- Prefer small, deterministic synchronous functions
- Assume thousands of WS connections but only a handful of engine threads/shards

**Examples of CORRECT patterns:**

```rust
// ✅ CORRECT - Sync domain handler
pub fn handle_kv_put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
    self.kv_store.put(key, value)?;
    Ok(())
}

// ✅ CORRECT - Async transport wrapper
async fn handle_websocket(socket: WebSocket, engine_tx: Sender<EngineEvent>) {
    while let Some(msg) = socket.next().await {
        let frame = msg?;
        engine_tx.send(EngineEvent::Frame(conn_id, frame.into_data()))?;
    }
}
```

**Examples of FORBIDDEN patterns:**

```rust
// ❌ WRONG - Async in domain logic
pub async fn handle_kv_put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
    self.kv_store.put(key, value).await?;  // ❌ .await in engine
    Ok(())
}

// ❌ WRONG - Spawning in domain logic
pub fn handle_request(&mut self, req: Request) -> Result<(), String> {
    tokio::spawn(async move {  // ❌ spawn in engine
        // process request
    });
    Ok(())
}

// ❌ WRONG - Async locks in engine
pub fn get_value(&self, key: &[u8]) -> Result<Vec<u8>, String> {
    let guard = self.data.lock().await?;  // ❌ async lock
    Ok(guard.get(key).cloned())
}
```

---

## Benchmark Guidelines - STRICTLY ENFORCE

**CRITICAL: All benchmarks must follow Pebble-quality microbenching standards.**

### Benchmark Philosophy

All benchmarks MUST:

- Measure **only the hot path** (service logic, no setup)
- Avoid **all allocations** in measured loop
- Avoid RNG inside hot path
- Avoid thread creation inside hot path
- Precompute keys/values/data outside loops
- Use deterministic seeds when randomness needed
- Use `SamplingMode::Flat` for consistent measurements
- Run fast (hotpath <1s, subsystem <3s, system <10s)

### Benchmark Structure (MANDATORY)

Every benchmark file must:

1. Import from criterion: `criterion::{black_box, criterion_group, criterion_main, Criterion}`
2. Use shared `criterion_config()` from `benches/config.rs`
3. Put **all setup outside** the `b.iter()` or `b.iter_batched()` call
4. Terminate with proper criterion group/main

Example structure:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fitz::routing::GlobalInternTable;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

fn bench_operation(c: &mut Criterion) {
    // Setup OUTSIDE the benchmark
    let interner = Arc::new(GlobalInternTable::new());
    let mut service = MyService::new(interner);
    service.setup_test_data();

    let mut group = c.benchmark_group("my_operation");
    group.bench_function("operation_name", |b| {
        b.iter(|| {
            // ONLY hot path here
            let _result = service.do_operation(black_box("input"));
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_operation
}
criterion_main!(benches);
```

### Service Initialization Pattern

For service benchmarks (notice, lease, rpc):

```rust
// ✅ CORRECT - Service created once outside loop
fn bench_publish(c: &mut Criterion) {
    let mut svc = NoticeService::new(Arc::new(GlobalInternTable::new()));
    svc.subscribe(0, "notice://realm/area/events".to_string(), 1);

    let mut group = c.benchmark_group("notice_publish");
    group.bench_function("publish", |b| {
        b.iter(|| {
            let _result = svc.publish(0, black_box("notice://realm/area/events"), None, &[]);
        })
    });
    group.finish();
}

// ❌ WRONG - Service created in every iteration
fn bench_publish_wrong(c: &mut Criterion) {
    let mut group = c.benchmark_group("notice_publish");
    group.bench_function("publish", |b| {
        b.iter(|| {
            let mut svc = NoticeService::new(Arc::new(GlobalInternTable::new())); // ❌
            let _result = svc.publish(0, "notice://realm/area/events", None, &[]);
        })
    });
    group.finish();
}
```

### Precomputation Patterns

#### Fixed Key/Value Buffers

```rust
fn make_fixed_data(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| format!("route://realm/area/res{}", i))
        .collect()
}

fn bench_with_precomputed(c: &mut Criterion) {
    let routes = make_fixed_data(1000); // Outside benchmark
    let mut svc = MyService::new();

    c.bench_function("operation", |b| {
        b.iter(|| {
            for route in &routes {
                svc.process(black_box(route));
            }
        })
    });
}
```

#### Deterministic Shuffle

```rust
fn shuffle_indices(len: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..len).collect();
    let mut seed = 0xDEADBEEFCAFEBABE_u64;
    for i in (1..len).rev() {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let j = (seed as usize) % (i + 1);
        idx.swap(i, j);
    }
    idx
}
```

### Using iter_batched

When setup is needed per iteration but shouldn't be measured:

```rust
use criterion::BatchSize;

group.bench_function("subscribe", |b| {
    b.iter_batched(
        || NoticeService::new(Arc::new(GlobalInternTable::new())), // Setup
        |mut svc| {
            svc.subscribe(0, black_box("route"), 1); // Measured
        },
        BatchSize::SmallInput,
    )
});
```

### Required API Usage

Copilot must always include:

```rust
group.sampling_mode(SamplingMode::Flat);
group.throughput(Throughput::Elements(N as u64));
```

### Benchmark Quality Checklist

Before generating a benchmark, verify:

- [ ] All data precomputed outside hot path
- [ ] No allocations in measured loop
- [ ] No string formatting in measured loop
- [ ] No Vec::push in measured loop
- [ ] No thread spawns in measured loop
- [ ] Uses `black_box()` for inputs
- [ ] Uses real Fitz types
- [ ] Fast execution (<3s for subsystem, <1s for hotpath)
- [ ] Uses `config::criterion_config()`
- [ ] Proper criterion_group/criterion_main structure

### Hot-Path Benchmarks to Generate

For each domain service:

- Subscribe/register operation
- Unsubscribe/cleanup operation
- Core routing/matching logic
- Fanout/delivery path

### Benchmark Tiers

**Tier 1 Hotpath** (benches/tier1_hotpath_*.rs):

- Pure sync service logic (routing, envelope, matcher, TLV, mux, permissions, context, actor_messaging)
- No Arc/RwLock overhead in measured path
- Measures <100ns to <10µs operations
- Target: <1s total runtime
- Use Criterion + `config::criterion_config()`

**Tier 2 Subsystem** (benches/tier2_subsystem_*.rs):

- Scheduler, mailbox, subscriptions, TLV pipeline
- Use Criterion + `config::criterion_config()`
- Target: <3s total runtime

**Tier 3 System** (benches/tier3_system_*.rs):

- One bench per domain (kv, lease, notice, queue, rpc, schedule, stream)
- In-process actor + test engine, no network
- Use cntryl-stress `#[stress_test]` + `ctx.measure(|| { ... })`
- Target: <10s total runtime

**Tier 4 Integration** (benches/tier4_integration_*.rs):

- Full stack (direct → TCP → WebSocket → multiclient)
- Use cntryl-stress; setup (Runtime, TestServer) outside `ctx.measure()`
- Target: identify E2E performance cliffs

### Examples from Codebase

See these files for excellent benchmark patterns:

- `benches/tier1_hotpath_routing.rs` - Hotpath with precomputed data and Throughput
- `benches/tier1_hotpath_envelope.rs` - Envelope and MessageId hot path
- `benches/tier2_subsystem_scheduler.rs` - iter_batched and subsystem setup
- `benches/tier3_system_kv.rs` - Stress tests with set_elements and tags
- `benches/tier4_integration_kv.rs` - Integration layers (direct, TCP, WS, multiclient)

### Why These Rules?

1. **Accuracy**: Measure only what matters (hot path)
2. **Stability**: Deterministic, reproducible results
3. **CI-Friendly**: Fast enough for CI pipelines
4. **Debuggability**: Clear what's being measured
5. **Comparability**: Consistent methodology across benchmarks

---

**REMEMBER: Benchmarks should be allocation-free, deterministic, and fast!**

---

## Build, Test & Lint Commands - QUICK REFERENCE

**Local development:**

```bash
cargo test --workspace              # Run all tests across workspace members
cargo test --lib                    # Unit tests only
cargo test --test '*'               # Integration tests only
cargo test test_guidelines_compliance  # Meta-test for test naming/structure
cargo fmt --all -- --check          # Check formatting
cargo clippy --workspace --all-targets -- -D warnings  # Lint with warnings as errors
cargo build --workspace --release   # Optimized build
```

**Common patterns:**

- Run tests with output: `cargo test -- --nocapture`
- Run specific integration test: `cargo test --test kv_e2e_basic`
- Check single domain: `cargo test kv::` (tests containing "kv::")
- Run with backtrace: `RUST_BACKTRACE=1 cargo test`

**Before committing:**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

---

## Module Layer Architecture - THE SYSTEM DESIGN

Fitz has **4 distinct layers**, each with strict responsibilities and boundaries:

### Layer 1: API (Transport Edge)

**Location:** `src/api/`  
**Responsibility:** Socket I/O only  
**MUST contain:**

- Tokio async loops (TCP, WebSocket)
- Socket accept/read/write
- Protocol framing (TCP length-prefix, WebSocket framing)
- Session creation and lifecycle management

**MUST NOT contain:**

- Routing logic
- Domain business logic
- Message parsing beyond framing

**Example files:**

- `src/api/tcp.rs` - TCP listener, length-prefixed frames
- `src/api/ws.rs` - WebSocket upgrade, frame forwarding
- `src/api/ingress.rs` - Async accept loop

### Layer 2: Session (Middleware/Dispatcher)

**Location:** `src/session/`  
**Responsibility:** Frame parsing and permission checking  
**MUST contain:**

- TLV/codec parsing (calls Protocol layer)
- Session-scoped state (realm, permissions, auth)
- Permission enforcement
- Frame → Message translation

**MUST NOT contain:**

- Actor routing
- Domain logic
- Async work beyond reading frames

**Example files:**

- `src/session/session.rs` - Frame receiving, routing to Runtime
- `src/session/permissions.rs` - Authorization rules
- `src/session/manager.rs` - Session lifecycle

### Layer 3: Runtime (Deterministic Actor Engine)

**Location:** `src/runtime/`  
**Responsibility:** Actor lifecycle, message routing, scheduling  
**MUST contain:**

- `routing/` - Route addressing (RouteFamily, Route, RouteAddress)
- `actor/` - Actor trait, mailboxes, lifecycle
- `router/` - Message delivery and subscription indexing
- `scheduler/` - Actor scheduling (priority lanes)
- `matcher/` - Wildcard route pattern matching
- `subscriptions/` - High-performance subscription lookup

**MUST NOT contain:**

- Async code
- Domain business logic
- Socket I/O

**100% Synchronous. No `.await`, no tokio types.**

**Example files:**

- `src/runtime/routing.rs` - Route parsing, RouteFamily management
- `src/runtime/router.rs` - Fanout, delivery
- `src/runtime/actor.rs` - Actor trait, ActorRef, Context

### Layer 4: Domains (Business Logic)

**Location:** `src/domains/`  
**Domains:** `kv/`, `lease/`, `notice/`, `queue/`, `rpc/`, `stream/`, `schedule/`  
**Responsibility:** Domain-specific actor implementations  
**MUST contain:**

- Domain-specific `Actor` implementations
- Request/response message types (e.g., `KvMessage`, `KvResponse`)
- Domain state management
- Business logic (transactions, leasing, fanout, etc.)

**MUST NOT contain:**

- Async code
- Socket I/O
- Routing or frame dispatch

**100% Synchronous. All message handling is deterministic.**

**Example structure:**

```rust
pub struct KvActor { /* state */ }
impl Actor for KvActor {
    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) -> Self::Response {
        // Synchronous business logic
    }
}
```

### Data Flow Through Layers

```
[CLIENT SOCKET]
       ↓
  API Layer (async)
  - Read frame bytes
  - Detect protocol
       ↓
  Session Layer (sync, validates auth)
  - Parse TLV/codec
  - Check permissions
       ↓
  Runtime Layer (sync, deterministic)
  - Route message to actor
  - Deliver to mailbox
  - Schedule actor execution
       ↓
  Domains Layer (sync, business logic)
  - Handle request
  - Return typed response
       ↓
  [Response → Session → API → Socket]
```

**Critical: No stepping backwards. Data flows down only (except responses).**

---

## Actor Model Patterns - FITZ ACTORS

All domain actors follow the **Fitz Actor Pattern**:

### Actor Trait Implementation

```rust
use fitz::runtime::{Actor, Context, ActorId};

pub struct MyDomainActor {
    state: SomeState,
}

// Define message and response types
pub enum MyMessage {
    RequestA { param: String },
    RequestB { count: usize },
}

pub enum MyResponse {
    Ok { result: String },
    Error { reason: String },
}

impl Actor for MyDomainActor {
    type Message = MyMessage;
    type Response = MyResponse;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) -> Self::Response {
        match msg {
            MyMessage::RequestA { param } => {
                // Pure synchronous logic
                MyResponse::Ok { result: param.to_uppercase() }
            }
            MyMessage::RequestB { count } => {
                // Can use ctx for timers, but NO async
                MyResponse::Ok { result: count.to_string() }
            }
        }
    }
}
```

### Key Invariants

1. **Message enum** - Define all possible requests
2. **Response enum** - Define all possible responses
3. **Synchronous `receive()`** - No `.await`, no tokio types
4. **Single responsibility** - One actor per domain/realm/area
5. **State isolation** - Each actor has isolated state (no Arc<Mutex>)

### Sending Messages to Actors

**From within domain:**

```rust
// Send to another actor by ActorRef
let response = ctx.send_to(&target_ref, MyMessage::RequestA { param: "test".into() })?;
```

**From Session/Runtime layer:**

```rust
// Runtime routes messages to correct actor automatically
// No manual actor addressing needed - Runtime.router handles it
```

### Actor Lifecycle

- **Created:** Session creates first actor for realm/area on demand
- **Receives messages:** Via mailbox (queue in Runtime)
- **Responds:** Return `Response` type directly (not async)
- **Cleanup:** Runtime handles supervision and termination

---

## Protocol & Codec Architecture - WIRE FORMAT

Fitz uses **TLV (Tag-Length-Value) encoding** for all domain operations.

### Layer Location

**Location:** `src/protocol/`

### Codec Files Structure

Each domain has a codec:

- `src/protocol/kv_codec.rs` - KV encoding/decoding
- `src/protocol/lease_codec.rs` - Lease encoding/decoding
- `src/protocol/notice_codec.rs` - Notice encoding/decoding
- `src/protocol/rpc_codec.rs` - RPC encoding/decoding
- `src/protocol/stream_codec.rs` - Stream encoding/decoding
- `src/protocol/queue_codec.rs` - Queue encoding/decoding

### Codec Pattern (MANDATORY)

Every codec must implement `CodecTrait`:

```rust
use fitz::protocol::CodecTrait;

pub struct MyDomainCodec;

impl CodecTrait for MyDomainCodec {
    type Message = MyDomainMessage;
    type Response = MyDomainResponse;

    fn encode_message(msg: &Self::Message) -> Result<Vec<u8>, CodecError> {
        // TLV encode: [tag, length, value]
        // Use src/protocol/tlv.rs helpers
        Ok(encoded_bytes)
    }

    fn decode_message(bytes: &[u8]) -> Result<Self::Message, CodecError> {
        // TLV decode
        Ok(decoded_message)
    }

    fn encode_response(resp: &Self::Response) -> Result<Vec<u8>, CodecError> {
        Ok(encoded_bytes)
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Response, CodecError> {
        Ok(decoded_response)
    }
}
```

### TLV Encoding Helpers

**Location:** `src/protocol/tlv.rs`

```rust
use fitz::protocol::tlv::{encode_u64, encode_string, encode_bytes};

// Encoding example
let mut buffer = Vec::new();
buffer.extend(encode_u64(TAG_TIMEOUT, 30)?);
buffer.extend(encode_string(TAG_NAME, "lease-1")?);
```

### RouteFamily Routing

Every codec must handle `RouteFamily`:

```rust
// In KvMessage or similar:
pub struct BeginRequest {
    pub route_family: RouteFamily,  // Always included
    pub realm: String,
    pub area: String,
    pub resource: String,
    // ... domain-specific fields
}
```

**Why RouteFamily?** Partitions domain state for sharding/performance.

---

## Integration Testing Patterns - TEST ARCHITECTURE

### Test Organization

**Unit tests:** Inside domain modules, test `Actor::receive()` directly

```rust
// In src/domains/kv/mod.rs or tests file
#[test]
fn should_return_value_when_key_exists() {
    // Arrange
    let mut actor = KvActor::new(test_store());

    // Act & Assert
    // Direct actor testing
}
```

**Integration tests:** `tests/*.rs` files, test full pipeline

```
tests/
  kv_e2e_basic.rs           # Happy path
  kv_auth.rs                # Authorization
  kv_realm_isolation.rs     # Multi-realm
  kv_session_permissions.rs # Permissions
  ...
```

### Creating Test Engines

Use `testkit` module for test setup:

```rust
use fitz::testkit::create_test_engine_with_cfs;
use fitz::domains::kv::{KvActor, KvMessage, KvResponse};

#[test]
fn should_complete_transaction_begin_put_get_sequence() {
    // Arrange - Create test actor with storage
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    let mut actor = KvActor::new(store);
    let route_family = RouteFamily::new(1);

    // Act - Send message to actor
    let response = actor.handle(KvMessage::Begin {
        route_family,
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert - Check response type
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Continue with Put, Get, Commit/Rollback
}
```

### Test Coverage Tiers

1. **Happy path** (`e2e_basic.rs`)
   - Normal operation sequences
   - Basic request/response
   - State transitions

2. **Authorization** (`*_auth.rs`)
   - Permission checks
   - Denied access
   - Scope validation

3. **Semantics** (`*_semantics.rs`)
   - Ordering guarantees
   - Isolation levels
   - Consistency properties

4. **Scale** (`*_scale.rs`)
   - Large datasets
   - Many subscribers
   - High throughput

5. **Realm isolation** (`*_realm_isolation.rs`)
   - Cross-realm state separation
   - Performance isolation

### Example: Multi-Step Operation Test

```rust
#[test]
fn should_isolate_transactions_across_resources() {
    // Arrange
    let mut actor = create_kv_actor();

    // Act - Begin transaction on "users"
    let response = actor.handle(KvMessage::Begin { /* users resource */ });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act - Put to users
    actor.handle(KvMessage::Put {
        tx_id,
        resource: "users".to_string(),
        // ...
    });

    // Act & Assert - Try to put to different resource
    let response = actor.handle(KvMessage::Put {
        tx_id,
        resource: "posts".to_string(),  // Should fail or isolate
        // ...
    });

    // Assert - Verify isolation
    assert!(response_indicates_isolation_or_error(&response));
}
```

---

## Routing System - FTZ:// PROTOCOL

Fitz routes are hierarchical and strictly structured.

### Route Format

```
ftz://[route_family]/[domain]/[realm]/[area]/[resource]/[operation]
ftz://1/kv/acme/app/users/get
     ^^  domain  realm  area   resource  operation
```

### RouteFamily

- **Numeric identifier** for route partitioning
- Used for **sharding state** across multiple instances
- Set by client in every request
- **Must be consistent** for same realm/resource pair
- Example: `RouteFamily::new(1)` or `RouteFamily::new(42)`

### Route Components

| Component   | Example                                           | Purpose                |
| ----------- | ------------------------------------------------- | ---------------------- |
| `domain`    | `kv`, `notice`, `rpc`, `lease`, `queue`, `stream` | Service selector       |
| `realm`     | `acme`, `tenant-123`, `prod`                      | Isolation boundary     |
| `area`      | `app`, `system`, `cache`                          | Namespace within realm |
| `resource`  | `users`, `posts`, `events`                        | Entity type            |
| `operation` | `get`, `put`, `subscribe`                         | Action verb            |

### Pattern Matching (Subscriptions)

Routes support **wildcard patterns** for subscriptions:

```rust
// Exact
"ftz://1/notice/acme/app/users/change"

// Wildcards (* = single segment)
"ftz://1/notice/acme/app/*"              // Any resource in app/area
"ftz://1/notice/acme/*/users"            // users across all areas

// Multi-segment wildcard (** = any depth)
"ftz://1/notice/acme/**"                 // Everything in acme realm
```

### Routing in Domain Handlers

Session layer routes based on domain prefix:

```
kv://     → KvActor
notice:// → NoticeActor
rpc://    → RpcActor
```

Actors dispatch within their domain based on remaining route.

**Critical:** Domain actors receive already-routed messages. They don't route themselves.

---

## Quick Reference - File Organization

```
src/
├── api/                # Layer 1: Transport (ASYNC)
│   ├── tcp.rs         # TCP listener
│   ├── ws.rs          # WebSocket upgrade
│   └── ingress.rs     # Connection accept loop
├── session/           # Layer 2: Middleware (MOSTLY SYNC)
│   ├── session.rs     # Frame parsing, routing
│   ├── permissions.rs # Authorization logic
│   └── manager.rs     # Session lifecycle
├── runtime/           # Layer 3: Engine (100% SYNC)
│   ├── routing.rs     # Route parsing, family management
│   ├── router.rs      # Message delivery, fanout
│   ├── actor.rs       # Actor trait, mailbox
│   ├── scheduler.rs   # Actor scheduling
│   └── subscriptions/ # Subscription indexing
├── protocol/          # Layer 4: Codecs (100% SYNC)
│   ├── kv_codec.rs
│   ├── notice_codec.rs
│   ├── *_codec.rs
│   └── tlv.rs         # TLV encoding helpers
├── domains/           # Layer 5: Business Logic (100% SYNC)
│   ├── kv/            # KV actor and messages
│   ├── notice/        # Pub/sub actor
│   ├── rpc/           # RPC actor
│   ├── lease/         # Lease actor
│   ├── queue/         # Queue actor
│   ├── stream/        # Stream actor
│   └── schedule/      # Schedule actor
└── testkit/           # Test utilities
    └── *.rs           # Test helpers per domain
```
