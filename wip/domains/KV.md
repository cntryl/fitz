# KV Domain Specification (v2 — Actor Model MVP)

**Version:** 2.0  
**Status:** MVP Specification  
**Durability:** Fully Durable (backed by Midge)  
**Last Updated:** December 11, 2025

---

# 1. Overview

Fitz KV provides **simple, durable key-value storage** built on top of **Midge**, exposed through Fitz’s messaging protocol.

The KV domain is:

* **Durable** (backed by Midge)
* **Single-node authoritative** (no distributed consensus)
* **Actor-driven** (no shared state, no locks)
* **Non-transactional (MVP)**
* **Fast** (one actor hop + Midge write)
* **Hierarchically namespaced** (realm/area/resource/user_key)

This is not a distributed KV. It is **a durable KV API façade around a local Midge instance**.

---

# 2. Route Format

```
kv://{realm}/{area}/{resource}/{operation}
```

Examples:

```
kv://acme/config/db/put
kv://acme/sessions/user123/get
kv://acme/cache/items/delete
kv://acme/users/*/scan
```

---

# 3. Actor Model Architecture

There is exactly **one KVActor per route family**. It owns no data directly — it **delegates to Midge**.

```rust
struct KvActor {
    store: MidgeHandle, // injected at startup
}
```

Midge handles durability, ordering, and atomicity of single-key writes.

KVActor:

* Parses keys
* Applies namespacing
* Calls into Midge
* Packages TLV replies
* Never locks
* Never blocks

---

# 4. Core Operations (MVP)

## 4.1 Put

**Route:** `kv://{realm}/{area}/{resource}/put`

**Request Tags:**

* key (derived from route)
* TAG_BODY → value bytes

**Behavior:**
Writes value to Midge using derived composite key.

**Response:**
`status = ok`

---

## 4.2 Get

**Route:** `kv://{realm}/{area}/{resource}/get`

**Behavior:**
Fetches value from Midge.

**Response:**
`TAG_BODY` containing value or empty if missing.

---

## 4.3 Delete

**Route:** `kv://{realm}/{area}/{resource}/delete`

**Behavior:**
Deletes one key.

**Response:**
`status = ok`

---

## 4.4 Scan

**Route:** `kv://{realm}/{area}/*/scan`

**Request Tags:**

* TAG_START_KEY
* TAG_END_KEY
* TAG_LIMIT (optional)

**Behavior:**
Delegates to Midge range scan. Returns ordered key/value pairs.

**Response:**
Sequence of (TAG_ID, TAG_BODY) pairs.

---

## 4.5 Delete Range

**Route:** `kv://{realm}/{area}/*/delete_range`

**Behavior:**
Delegates to Midge range deletion.

**Response:**
`TAG_COUNT` = number of deleted keys

---

# 5. Removed From MVP (Important)

The following features from earlier specs are **NOT in Fitz v2 MVP**:

* ❌ Multi-key transactions
* ❌ Optimistic concurrency
* ❌ Batch operations
* ❌ Multi-get
* ❌ Transaction begin/commit/rollback
* ❌ Lease-based locking of keys
* ❌ Cross-area or cross-realm operations
* ❌ TTL on values (can be added later)

These all move to **future roadmap**.

MVP is:

> **Durable single-key operations + range ops** via Midge, wrapped in an actor façade.

Which is exactly the correct model for Fitz v2.

---

# 6. Namespacing Model

Full storage key = route family + realm + area + resource + user_key.

Example:

```
kv://acme/config/db/put
```

Maps internally to something like:

```
<route_family> / acme / config / db
```

Midge handles the final user_key directly from the route’s resource component.

---

# 7. Data Model (Simplified)

```rust
struct KvEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}
```

No version numbers.  
No timestamps.  
No metadata.

Midge persists and indexes the data.

---

# 8. Error Codes (Simplified)

| Code             | Meaning                                               |
| ---------------- | ----------------------------------------------------- |
| KV_KEY_NOT_FOUND | Returned only on explicit "get-strict" ops (optional) |
| KV_BAD_RANGE     | Start > end                                           |
| KV_TOO_LARGE     | Key or value exceeds limits                           |
| KV_BACKEND_ERROR | Midge failure                                         |

Transactions errors removed.

---

# 9. Observability

Metrics:

* `kv_put_total`
* `kv_get_total`
* `kv_delete_total`
* `kv_scan_total`
* `kv_op_duration_seconds{op}`

Logs:

* `kv_put`
* `kv_delete_range`
* `kv_scan`

---

# 10. Testing Requirements

### Unit

* Put/Get/Delete correctness
* ASCII + binary keys
* Range semantics
* TLV validation
* Bad routes

### Integration

* Midge durability
* Restart behavior
* Range scans on large datasets

### Performance

* Put latency
* Scan throughput
* Delete_range cost

---

# 11. Usage Patterns

### Config Storage

```
kv://acme/config/db/put
```

### Session Storage

```
kv://acme/sessions/user123/get
```

### Metadata & Flags

```
kv://acme/feature-flags/*/scan
```

---

# ⭐ **Final Summary (What Fitz KV v2 Really Is)**

> **Fitz KV is a simple, actor-based, durable facade over Midge, supporting only single-key CRUD + range ops.**

This is perfect because:

* It is blazing fast
* It uses zero locks
* It is consistent within one node
* It aligns perfectly with Midge’s LSM architecture
* It keeps Fitz predictable and lightweight
* It allows a clean path to future expansion

---

# If you'd like, I can now produce:

✅ **The corrected spec for Queue (v2)**
✅ **The corrected spec for Stream (v2)**
✅ **The corrected spec for Notice (v2)**
✅ **The corrected spec for RPC (v2)**

Just say:

**“next domain: queue”**# KV Domain Specification

**Version:** 1.0  
**Status:** Implementation Complete  
**Last Updated:** November 15, 2025  

---

## Overview

Fitz KV provides distributed key-value storage with transactional semantics and range operations. Keys are namespaced by realm and area, enabling multi-tenant isolation while supporting cross-area operations within a realm.

### Key Features

- **Hierarchical namespacing**: `realm/area/resource` key organization
- **Transactional operations**: Atomic multi-key transactions
- **Range operations**: Scan and delete ranges of keys
- **Batch operations**: Atomic multi-operation transactions
- **Lease integration**: Optional lease-based key locking
- **Multi-get operations**: Efficient bulk key retrieval

### Use Cases

- Configuration storage and retrieval
- Session state management
- Distributed caching
- Metadata storage
- Feature flags and settings

---

## Route Format

KV routes follow the standard Fitz format:

```
kv://{realm}/{area}/{resource}[/{operation}]
```

### Examples
- `kv://acme/config/database/url` - Single key operations
- `kv://acme/sessions/*/get` - Multi-key operations (wildcard)
- `kv://acme/cache/*/scan` - Range scan operations
- `kv://acme/config/*/batch` - Batch operations

---

## Core Operations

### 1. Put (Store Key-Value)

**Route Operation:** `kv://{realm}/{area}/{resource}/put`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (key), `TAG_BODY` (value)

**Behavior:**
- Stores a key-value pair
- Overwrites existing values
- Keys are scoped to realm/area/resource

**Response TLV:** Success acknowledgment

### 2. Get (Retrieve Value)

**Route Operation:** `kv://{realm}/{area}/{resource}/get`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (key)

**Behavior:**
- Retrieves value for a specific key
- Returns empty body if key doesn't exist

**Response TLV:** `TAG_BODY` (value, empty if not found)

### 3. Delete (Remove Key)

**Route Operation:** `kv://{realm}/{area}/{resource}/delete`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (key)

**Behavior:**
- Removes a specific key-value pair
- No-op if key doesn't exist

**Response TLV:** Success acknowledgment

### 4. Delete Range

**Route Operation:** `kv://{realm}/{area}/*/delete`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY` ("start_key\nend_key")

**Behavior:**
- Removes all keys in the specified range
- Inclusive start, exclusive end
- Returns count of deleted keys

**Response TLV:** `TAG_COUNT` (number deleted)

### 5. Scan (List Keys/Values)

**Route Operation:** `kv://{realm}/{area}/*/scan`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY` ("start_key\nend_key"), `TAG_LIMIT` (optional)

**Behavior:**
- Returns key-value pairs in lexicographic order
- Supports pagination with limit
- Empty end_key means scan to end

**Response TLV:** Multiple `TAG_ID`, `TAG_BODY` pairs

### 6. Batch Operations

**Route Operation:** `kv://{realm}/{area}/*/batch`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY` (batch operations)

**Behavior:**
- Atomic execution of multiple operations
- All operations succeed or all fail
- Supports put, get, delete in single transaction

**Response TLV:** Results of each operation

### 7. Get Many

**Route Operation:** `kv://{realm}/{area}/*/get-many`  
**TLV Tags:** `TAG_ROUTE`, `TAG_BODY` (newline-separated keys)

**Behavior:**
- Retrieves multiple keys in single operation
- Returns results in request order
- Missing keys return empty values

**Response TLV:** Multiple `TAG_BODY` values in request order

---

## Transaction Operations

### 8. Begin Transaction

**Route Operation:** `kv://{realm}/{area}/*/begin_transaction`  
**TLV Tags:** `TAG_ROUTE`

**Behavior:**
- Starts a new transaction
- Returns transaction ID for subsequent operations

**Response TLV:** `TAG_ID` (transaction ID)

### 9. Commit Transaction

**Route Operation:** `kv://{realm}/{area}/*/commit_transaction`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (transaction ID)

**Behavior:**
- Atomically commits all transaction operations
- Makes all changes visible
- Releases transaction resources

**Response TLV:** Success acknowledgment

### 10. Rollback Transaction

**Route Operation:** `kv://{realm}/{area}/*/rollback_transaction`  
**TLV Tags:** `TAG_ROUTE`, `TAG_ID` (transaction ID)

**Behavior:**
- Discards all transaction operations
- No changes become visible
- Releases transaction resources

**Response TLV:** Success acknowledgment

---

## Data Model

### Key Structure

Keys follow hierarchical naming with realm/area/resource scoping:

```
Full Key: {realm}/{area}/{resource}/{user_key}
Storage Key: 0x03 {rf} {realm} {area} {resource} {user_key}
```

### Value Storage

```rust
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,        // For optimistic concurrency
    pub created_at: u64,
    pub updated_at: u64,
}
```

### Transaction Context

```rust
#[derive(Debug)]
pub struct Transaction {
    pub id: String,
    pub realm: String,
    pub area: String,
    pub operations: Vec<TransactionOperation>,
    pub created_at: u64,
}

#[derive(Debug)]
pub enum TransactionOperation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Get { key: String },
}
```

---

## Range Operations

### Key Ranges

Range operations use start and end key bounds:

```rust
// Inclusive start, exclusive end
let range = KeyRange {
    start: "user:0000".to_string(),
    end: "user:1000".to_string(), // exclusive
};
```

### Scan Implementation

```rust
async fn scan_range(&self, range: KeyRange, limit: Option<usize>) -> Result<Vec<KeyValue>, KvError> {
    let mut results = Vec::new();
    let mut count = 0;

    // Iterate through keys in range
    for kv in self.store.scan_range(&range.start, &range.end)? {
        results.push(kv);
        count += 1;

        if let Some(limit) = limit {
            if count >= limit {
                break;
            }
        }
    }

    Ok(results)
}
```

### Delete Range Implementation

```rust
async fn delete_range(&self, range: KeyRange) -> Result<u64, KvError> {
    let mut deleted_count = 0;

    // Collect keys to delete (to avoid modifying while iterating)
    let keys_to_delete: Vec<String> = self.store
        .scan_range(&range.start, &range.end)?
        .map(|kv| kv.key)
        .collect();

    // Delete each key
    for key in keys_to_delete {
        if self.store.delete(&key)? {
            deleted_count += 1;
        }
    }

    Ok(deleted_count)
}
```

---

## Batch Operations

### Batch Request Format

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BatchOperation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Get { key: String },
}
```

### Atomic Execution

```rust
async fn execute_batch(&self, batch: BatchRequest) -> Result<BatchResponse, KvError> {
    // Start transaction
    let txn_id = self.begin_transaction().await?;

    let mut results = Vec::new();

    // Execute all operations
    for op in batch.operations {
        let result = match op {
            BatchOperation::Put { key, value } => {
                self.put_in_transaction(txn_id, &key, &value).await?;
                BatchResult::Put
            }
            BatchOperation::Delete { key } => {
                let existed = self.delete_in_transaction(txn_id, &key).await?;
                BatchResult::Delete { existed }
            }
            BatchOperation::Get { key } => {
                let value = self.get_in_transaction(txn_id, &key).await?;
                BatchResult::Get { value }
            }
        };
        results.push(result);
    }

    // Commit transaction
    self.commit_transaction(txn_id).await?;

    Ok(BatchResponse { results })
}
```

---

## TLV Framing Details

### Single Key Operations
```
DAT Frame:
- TAG_ROUTE (0x20): "kv://acme/config/database/url"
- TAG_ID (0x??): "connection_string"
- TAG_BODY (0x22): <value data>
```

### Range Operations
```
DAT Frame:
- TAG_ROUTE (0x20): "kv://acme/config/*/scan"
- TAG_BODY (0x22): "database/\nuser/"
- TAG_LIMIT (0x??): 100
```

### Batch Operations
```
DAT Frame:
- TAG_ROUTE (0x20): "kv://acme/config/*/batch"
- TAG_BODY (0x22): <JSON/CBOR encoded batch request>
```

### Transaction Operations
```
DAT Frame:
- TAG_ROUTE (0x20): "kv://acme/config/*/begin_transaction"
// Response:
- TAG_ID (0x??): "txn_12345"
```

---

## Error Handling

### Error Codes

| Code | Name | Description | Client Action |
|---|---|---|---|
| 6001 | ERR_KEY_NOT_FOUND | Key does not exist | Check key spelling |
| 6002 | ERR_TRANSACTION_NOT_FOUND | Invalid transaction ID | Begin new transaction |
| 6003 | ERR_TRANSACTION_CONFLICT | Concurrent modification | Retry transaction |
| 6004 | ERR_RANGE_INVALID | Invalid key range | Check start/end keys |
| 6005 | ERR_BATCH_TOO_LARGE | Batch exceeds size limit | Split into smaller batches |
| 6006 | ERR_KEY_TOO_LONG | Key exceeds length limit | Use shorter key |
| 6007 | ERR_VALUE_TOO_LARGE | Value exceeds size limit | Compress or split value |

### Transaction Error Handling

```rust
async fn execute_with_retry<F, Fut, T>(
    operation: F,
    max_retries: usize,
) -> Result<T, KvError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, KvError>>,
{
    let mut attempt = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(KvError::TransactionConflict) if attempt < max_retries => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}
```

---

## Configuration

### KV Settings

```yaml
kv:
  # Storage limits
  max_key_length: 1024
  max_value_length: 1048576        # 1MB
  max_batch_size: 100              # Operations per batch
  max_transaction_operations: 50

  # Performance tuning
  scan_page_size: 100
  transaction_timeout_seconds: 30

  # Per-area settings
  areas:
    "acme/cache":
      ttl_seconds: 3600            # Auto-expire cache entries
      max_keys: 10000

    "acme/sessions":
      ttl_seconds: 86400           # 24 hour sessions
      compression: true
```

### Storage Backend

```yaml
storage:
  kv_backend: "midge"              # midge, memory, azure, aws
  midge:
    path: "/data/fitz/kv"
    sync_writes: true
    compression: "snappy"
```

---

## Observability

### Metrics

- `kv_operations_total{operation,type}`
- `kv_keys_stored{area}`
- `kv_transactions_active`
- `kv_scan_operations_total{area}`
- `kv_batch_operations_total{area}`
- `kv_operation_duration_seconds{operation}`

### Logging

```json
{
  "timestamp": "2025-11-15T10:30:00Z",
  "level": "info",
  "message": "kv_batch_executed",
  "area": "acme/config",
  "operation_count": 5,
  "duration_ms": 45
}
```

```json
{
  "timestamp": "2025-11-15T10:30:05Z",
  "level": "warn",
  "message": "kv_transaction_conflict",
  "area": "acme/inventory",
  "transaction_id": "txn_12345",
  "retries": 2
}
```

---

## Implementation Status

### ✅ Completed
- Single key operations (put/get/delete)
- Range operations (scan/delete_range)
- Batch operations with atomicity
- Transaction support (begin/commit/rollback)
- TLV framing and parsing
- Hierarchical key namespacing
- Storage backend abstraction

### 🚧 In Progress
- Optimistic concurrency control
- TTL-based key expiration
- Cross-area transactions
- Secondary indexes
- Query language support

### 📋 TODO
- Compression for large values
- Backup and restore operations
- Cross-realm key references
- Eventual consistency mode
- Key versioning and history

---

## Testing Requirements

### Unit Tests
- Single key CRUD operations
- Range scan and delete operations
- Batch operation atomicity
- Transaction lifecycle management
- TLV parsing and validation
- Error condition handling

### Integration Tests
- End-to-end key operations
- Transaction isolation and rollback
- Concurrent access patterns
- Storage backend durability
- Large value handling

### Performance Benchmarks
- Single key operation latency
- Range scan throughput
- Batch operation performance
- Transaction commit latency
- Memory usage scaling

---

## Usage Patterns

### Configuration Management

```rust
// Store configuration
let config_key = "kv://acme/config/database/url";
let config_value = b"postgresql://localhost:5432/myapp";
kv_client.put(config_key, config_value).await?;

// Retrieve configuration
let url = kv_client.get("kv://acme/config/database/url").await?;
let connection_string = String::from_utf8(url)?;
```

### Session Storage

```rust
// Store session data
let session_id = generate_session_id();
let session_key = format!("kv://acme/sessions/{}/data", session_id);
let session_data = serde_json::to_vec(&session)?;
kv_client.put(&session_key, &session_data).await?;

// Batch retrieve multiple sessions
let keys = vec![
    format!("kv://acme/sessions/{}/data", session_id1),
    format!("kv://acme/sessions/{}/data", session_id2),
];
let sessions = kv_client.get_many(keys).await?;
```

### Cache Operations

```rust
// Cache with TTL
let cache_key = format!("kv://acme/cache/user/{}", user_id);
if let Some(cached) = kv_client.get(&cache_key).await? {
    return Ok(deserialize_user(&cached));
}

// Cache miss - fetch from database
let user = fetch_user_from_db(user_id).await?;
kv_client.put(&cache_key, &serialize_user(&user)).await?;
Ok(user)
```

### Transactional Updates

```rust
// Atomic balance transfer
let txn_id = kv_client.begin_transaction("acme/banking").await?;

kv_client.put_in_transaction(txn_id, "account:123/balance", b"900").await?;
kv_client.put_in_transaction(txn_id, "account:456/balance", b"1100").await?;

kv_client.commit_transaction(txn_id).await?;
```

---

*See ARCHITECTURE.md for system-level context and other domain specifications.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\KV_SPEC.md