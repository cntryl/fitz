# GitHub Copilot Instructions for Shale Project

## Terminology Rules - STRICTLY ENFORCE

**CRITICAL: Use correct Fitz terminology in ALL code, tests, documentation, and comments.**

### Correct Terms

- ✅ **realm** - The isolation boundary for resources (NOT "tenant")
- ✅ **area** - Namespace within a realm
- ✅ **resource** - Specific entity within an area
- ✅ **operation** - Action performed on a resource

### Forbidden Terms

- ❌ **tenant** - NEVER use this term, always use "realm"
- ❌ **namespace** - Use "area" instead (namespace is too generic)
- ❌ **endpoint** - Use "route" for Fitz routing paths
- ❌ **topic** - Use "route" for pub/sub patterns (in notice domain)
- ❌ **channel** - This has a specific meaning (connection ID), don't use for routes

### Examples

```rust
// ✅ CORRECT
let realm = "realm123";
let route = "ftz://realm123/kv/users/get";
pub struct RealmMap { ... }

// ❌ WRONG
let tenant = "tenant123";
let endpoint = "ftz://tenant123/kv/users/get";
pub struct TenantMap { ... }
```

**This applies to:**
- Variable names
- Function names
- Struct/enum names
- Comments and documentation
- Test names and descriptions
- Error messages
- Log statements

---

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

* **MUST NOT be async**
* **MUST NOT return futures**
* **MUST NOT use `.await`**
* **MUST NOT use tokio types** (`tokio::spawn`, `oneshot`, `tokio::sync::Mutex`, `tokio::sync::RwLock`, etc.)
* **MUST NOT perform async I/O**

A domain handler signature must look like:

```rust
fn handle(&self, req: DomainContext) -> DomainResponse
```

All work is synchronous and returns immediately.

### 2. Async boundaries are *before* domain handlers

WebSocket tasks do:

* framing
* read/write
* correlation
* passing bytes into the engine

The engine does:

* parsing
* route lookup
* **calls the domain handler synchronously**
* returns a `DomainResponse` immediately

The domain itself does **zero async** work.

### 3. Long-running or waiting operations must use synchronous primitives

If a domain needs to:

* block
* wait for a lease
* contest ownership
* coordinate state

…then it must use **synchronous concurrency primitives**:

* `std::sync::Mutex` / `RwLock`
* `parking_lot::Mutex` / `RwLock`
* `DashMap`
* `std::sync::Condvar`
* lock-free structures
* sharded state

**Not tokio waits, not timers, not async sleepers.**

### 4. DomainContext arrives fully parsed and ready

Transport + engine provide:

* parsed route
* borrowed payload slice
* channel ID
* route family
* possibly a sender for publish-style operations

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

* HTTP transport
* WebSocket transport
* TLS upgrade
* async file/network ops for *external* systems (never internal operations)

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

* HTTP (Hyper)
* WebSockets (tokio-tungstenite)
* TLS upgrades
* Socket I/O

These layers are responsible only for:
* accepting connections
* reading frames
* writing frames
* performing upgrades
* lightweight routing to the engine boundary

**No domain logic may run inside async tasks.**

### 2. Engine is 100% synchronous

* The core engine is a **single-threaded or sharded deterministic loop**
* No async, no `.await`, no tokio tasks, no oneshot channels
* All domain logic (routing, RPC, queues, leases, streams, KV, WAL interaction) occurs synchronously
* The engine is driven by an event queue (`EngineEvent`)
* Domain handlers are synchronous functions returning results immediately

### 3. Async ↔ Sync boundary

* WebSocket tasks forward inbound frames to the engine via a **crossbeam or SPSC queue**
* The engine pushes outbound frames via a per-connection **Sender<Vec<u8>>**
* Async tasks must not hold or call any engine domain types directly
* The boundary is always:
  ```
  WS frame → engine.on_frame(conn_id, bytes)
  engine → outbound_tx.send(binary_frame)
  ```

### 4. No "spawn per request" patterns

* The previous async spawn-per-dispatch model is removed
* Domain operations must *never* spawn tokio tasks
* Domain concurrency must only come from engine sharding (realm/tenant partitioning), not async tasks

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

* Minimize jitter and remove async scheduling from hot paths
* Maintain strong ordering, deterministic processing, and predictable latency
* Avoid tokio locks; prefer parking_lot and lock-free structures
* No async IO inside engine code

### 7. What Copilot should produce

Copilot must:

* Keep transport async but extremely thin
* Keep all engine and domain logic synchronous
* Use crossbeam channels or SPSC ring buffers for engine event queues
* Avoid introducing any async constructs inside engine code
* Prefer small, deterministic synchronous functions
* Assume thousands of WS connections but only a handful of engine threads/shards

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
