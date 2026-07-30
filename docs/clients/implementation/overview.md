## Cross-Language Conformance Suite

Use these companion artifacts when implementing or reviewing multiple SDKs:

- `cross-language-conformance-suite.yaml`: canonical scenario definitions and pass/fail policy
- `cross-language-conformance-runner.md`: harness input/output contract and CI matrix guidance

---

## Overview

This guide complements **client-spec.md** (wire protocol) and
**client-acceptance-criteria.md** (test requirements) with language-native API
patterns and implementation examples.

### Document Relationship

```
client-spec.md
└─ Defines: Wire format, TLV encoding, domain operations
   │
    ├─> client-acceptance-criteria.md
   │   └─ Defines: Testable requirements (Given-When-Then)
   │
    └─> client-implementation-guide.md (this doc)
       └─ Shows: Idiomatic patterns, best practices, examples
```

### Who Should Read This

- **Client library authors** building multi-language support
- **Application developers** integrating Fitz directly
- **Architects** designing Fitz-based systems

---

## Design Principles

### 1. Hide Wire Complexity from Users

**Bad API:**
```python
# ❌ User shouldn't see TLV encoding
client.send_frame([0x00, 0x68, 0x00, 0x2F, ...])
```

**Good API:**
```python
# ✅ Clean, type-safe interface
tx.put("user:123", b"alice")
```

### 2. Use Language-Native Types

**Bad API:**
```go
// ❌ Exposing broker internals
func (c *Client) CurrentShard() uint64
```

**Good API:**
```go
// ✅ Broker routing stays internal
func (c *Client) Connect(jwt string) error
```

### 3. Object-Based State Management

**Bad API:**
```typescript
// ❌ User manages tx_id manually
await client.kvPut(txId, route, key, value);
await client.kvCommit(txId);
```

**Good API:**
```typescript
// ✅ Transaction object encapsulates state
const tx = await client.kv.begin("kv://prod/users", {
  durability: "Sync",
});
await tx.put("user:123", "alice");
await tx.commit();
```

### 4. Make Durability Explicit

**Bad API:**
```rust
// ❌ Hidden durability choice
let tx = client.begin("kv://prod/users");
```

**Good API:**
```rust
// ✅ Caller chooses durability
let tx = client.begin("kv://prod/users", Durability::Sync);
```

### 5. Fail Fast, Fail Clear

**Bad API:**
```csharp
// ❌ Silent failure
var value = await tx.GetAsync("key"); // Returns null for both "missing" and "error"
```

**Good API:**
```csharp
// ✅ Explicit error types
var result = await tx.GetAsync("key");
if (result is GetResult.Found(var value)) { ... }
else if (result is GetResult.NotFound) { ... }
```

### 6. Keep Stream Metadata Explicit

Stream clients SHOULD expose an optional discriminator on append and an optional filter object on read. Treat both as ordinary request fields, not client-global state, and default them to omitted/null so older call sites stay compatible.

If your language offers builders, wrap the shared `StreamFilterSet` and `StreamFilterClause` shapes directly instead of inventing a separate filter DSL per SDK.

The wire encoding for stream reads is fixed: `route`, `from_offset`, `limit`, optional `max_bytes`, then an optional raw `StreamFilterSet` blob. Clients SHOULD encode the filter blob with the server's versioned marker and big-endian length fields, and MUST surface `ERR_STREAM_FILTER_UNSUPPORTED_VERSION` and `ERR_STREAM_FILTER_INVALID_PAYLOAD` as typed request errors rather than transport failures.

---

## Architecture Patterns

### Recommended Client Structure

```
fitz-client/
├── connection.{ext}        # WebSocket management, reconnect logic
├── protocol/
│   ├── tlv.{ext}          # TLV encoder/decoder
│   └── messages.{ext}      # Message type constants, wire structs
├── domains/
│   ├── kv.{ext}           # KV domain client
│   ├── notice.{ext}       # Notice domain client
│   ├── rpc.{ext}          # RPC domain client
│   ├── queue.{ext}        # Queue domain client
│   ├── lease.{ext}        # Lease domain client
│   ├── stream.{ext}       # Stream domain client
│   └── schedule.{ext}     # Schedule domain client
├── errors.{ext}           # Domain-specific error types
└── client.{ext}           # Main client facade
```

### Layered Design

```
┌─────────────────────────────────────┐
│   User Application Code             │
├─────────────────────────────────────┤
│   Domain Facades (KV, RPC, Notice)  │  ← High-level, idiomatic APIs
├─────────────────────────────────────┤
│   Connection Manager                 │  ← WebSocket, auth, reconnect
├─────────────────────────────────────┤
│   Protocol Layer (TLV codec)        │  ← Wire format encoding/decoding
├─────────────────────────────────────┤
│   Transport (WebSocket/TCP)         │  ← Low-level I/O
└─────────────────────────────────────┘
```

---

## Language-Specific Guidance
