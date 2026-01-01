# **KV Domain Specification (v2 — Actor Model MVP)**

**Version:** 2.0
**Status:** MVP
**Durability:** Fully durable via Midge
**Last Updated:** Dec 11, 2025

---

# **1. Overview**

Fitz KV is a **simple, durable key-value interface** built entirely on top of **Midge**, wrapped in Fitz messaging semantics and the actor model.

It is:

* **Durable** (delegates all persistence to Midge)
* **Single-node authoritative** (no distributed consensus)
* **Actor-driven** (zero locks, zero shared state)
* **Non-transactional for MVP**
* **Fast** (single actor hop + Midge write)
* **Hierarchically namespaced** (`realm/area/resource/user_key`)

Think of Fitz KV v2 as:

> **A minimal KV façade over Midge with PUT, GET, DELETE, SCAN, and DELETE RANGE.**

Nothing more.
Nothing fancy.
Perfect for system metadata, config, flags, and session-like storage.

---

# **2. Route Format**

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

# **3. Architecture (Actor Model)**

**One KvActor per route family.**
It owns no data and only delegates to Midge.

```rust
struct KvActor {
    store: MidgeHandle,
}
```

KvActor responsibilities:

* Parse the TLV frame
* Build the fully-qualified storage key
* Delegate to Midge (`put / get / range / delete`)
* Emit notifications (debounced)
* Return TLV responses

It never blocks, waits, or locks.

---

# **4. Core Operations (MVP)**

## **4.1 PUT**

```
kv://{realm}/{area}/{resource}/put
```

**Tags:**

* TAG_BODY = value
* Key is derived from `{resource}` or user key segment

**Behavior:**
Writes a single durable key into Midge.

**Response:** `ok`

---

## **4.2 GET**

```
kv://{realm}/{area}/{resource}/get
```

**Behavior:**
Returns stored value or empty if missing.

**Response:**

* TAG_BODY = value or empty

---

## **4.3 DELETE**

```
kv://{realm}/{area}/{resource}/delete
```

Deletes one key.
Always returns `ok`.

---

## **4.4 SCAN**

```
kv://{realm}/{area}/*/scan
```

**Tags:**

* TAG_START_KEY
* TAG_END_KEY
* TAG_LIMIT (optional)

**Behavior:**
Delegates to Midge range scan and returns sorted key/value pairs.

**Response:**
List of `(TAG_ID, TAG_BODY)` pairs.

---

## **4.5 DELETE RANGE**

```
kv://{realm}/{area}/*/delete_range
```

Deletes all keys in `[start, end)`.

**Response:**
`TAG_COUNT = number_deleted`

---

# **5. Explicitly Not in MVP**

* Transactions
* Multi-key atomic ops
* Batch writes
* Multi-get
* Optimistic concurrency
* TTL
* Leases on keys
* Cross-realm operations

MVP = **simple CRUD + range ops**.

---

# **6. Namespacing Model**

Full storage key:

```
<route_family>/<realm>/<area>/<resource>/<user_key>
```

Fitz KV only builds this prefix.
Midge does all real work (durability, ordering, range iteration).

---

# **7. Data Model**

```rust
struct KvEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}
```

No versions, timestamps, or metadata.
Pure durable bytes.

---

# **8. Error Codes (MVP)**

| Code             | Meaning                   |
| ---------------- | ------------------------- |
| KV_KEY_NOT_FOUND | Only for strict-get modes |
| KV_BAD_RANGE     | Invalid scan/delete range |
| KV_TOO_LARGE     | Exceeds key/value limits  |
| KV_BACKEND_ERROR | Midge failure             |

---

# **9. Observability**

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

All via the actor context.

---

# **10. Testing Requirements**

### Unit

* PUT/GET/DELETE correctness
* Range scan behavior
* TLV validation
* Area correctness
* Error handling

### Integration

* Midge durability
* Restart behavior
* Scan correctness with large datasets

### Performance

* PUT latency
* Scan throughput
* Range delete cost

---

# **11. Usage Patterns**

### Config Storage

```
kv://acme/config/db/put
```

### Sessions

```
kv://acme/sessions/user123/get
```

### Metadata / flags / bootstrap state

```
kv://acme/feature-flags/*/scan
```

---

# **12. Change Notifications (Durable Domains Integration)**

KV modifications emit **debounced ephemeral notifications**:

```
notice://{realm}/{area}/{resource}/changed
```

Payload includes metadata like:

```json
{ "key": "...", "ts": 1734057430 }
```

Rules:

* Best-effort
* Non-durable
* Debounced (10–50 ms)
* Consumers must re-fetch via durable APIs

This keeps subscribers reactive without creating fanout storms.

---

# ⭐ Final Summary

Fitz KV v2 is an actor-driven, minimal, durable key-value interface wrapped around Midge, supporting only single-key CRUD and range operations.
<parameter name="filePath">d:\repos\cntryl\fitz\docs\KV_SPEC.md
