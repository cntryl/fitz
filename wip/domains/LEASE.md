# Lease Domain Specification (v2 — Actor Model MVP Corrected Version)

**Version:** 2.0  
**Status:** MVP Complete  
**Durability:** Ephemeral (lost on restart)  
**Applies To:** Fitz v2 (Actor Runtime)  
**Last Updated:** December 11, 2025

---

# 1. Overview

Fitz Leases provide **ephemeral coordination primitives** for single-node exclusive resource ownership inside the Fitz runtime.

This is a **lightweight, in-memory lease system** with:

* Exclusive access
* TTL-based expiration
* Token-based ownership
* Actor-supervised state
* Fast-fail acquire semantics

It is NOT:

* durable
* distributed
* consensus-based
* queued

This keeps the system **simple, predictable, and extremely fast**, aligning perfectly with the actor model.

### Key MVP Features

* **Exactly-one** ownership per resource
* **Immediate failure** if held (no wait queues)
* **Automatic expiry**
* **Secure tokens (32-byte)**
* **Per-realm/area/resource hierarchy**
* **Actor-local state** (no locks, no shared memory)
* **No persistence** (ephemeral)

---

# 2. Route Format

Leases use the universal Fitz route format:

```
lease://{realm}/{area}/{resource}/{operation}
```

Examples:

```
lease://acme/locks/db/migration/acquire
lease://acme/election/coord/primary/renew
lease://acme/jobs/worker/run/release
```

---

# 3. Actor Model Architecture

## LeaseActor (per route family or global — implementation choice)

State is stored entirely inside this actor:

```rust
struct LeaseState {
	map: HashMap<ResourceKey, LeaseEntry>,
}
```

Where:

```rust
type ResourceKey = String; // "realm/area/resource"

struct LeaseEntry {
	token: [u8; 32],
	expiry: Instant,
}
```

No locks.  
No shared structures.  
Mailbox guarantees exclusive access.

---

# 4. Core Operations

## 4.1 Acquire Lease

**Route:**

```
lease://{realm}/{area}/{resource}/acquire
```

**Request TLV:**

| Tag  | Meaning  |
| ---- | -------- |
| 0x01 | realm    |
| 0x02 | area     |
| 0x03 | resource |
| 0x10 | ttl_secs |

**Success Response:**

```
status = ok
token = [u8; 32]
expires_at = unix timestamp
```

**Failure Response:**

```
status = error
error_code = LEASE_HELD
expires_at = current_expiry_timestamp
```

### Semantics

* If resource **free**, grant and create entry.
* If resource **held and unexpired**, return `LEASE_HELD`.
* TTL determines new expiry.
* Token is random 32 bytes.

---

## 4.2 Renew Lease

**Route:**

```
lease://{realm}/{area}/{resource}/renew
```

**Request:**

* token (required)
* new ttl

**Success Response:**

```
status = ok
expires_at
```

**Errors:**

* `INVALID_TOKEN`
- `LEASE_EXPIRED`

If expired, client must acquire again.

---

## 4.3 Release Lease

**Route:**

```
lease://{realm}/{area}/{resource}/release
```

**Request:**

* token

**Success Response:**

```
status = ok
```

**Errors:**

* `INVALID_TOKEN`
* `LEASE_NOT_HELD`

---

# 5. Lease Semantics

## Exclusive Access

One holder at a time.

## Fast-Fail Acquire (no queues)

If lease is held, caller receives immediate error.

## Auto-Expiry

Actor schedules a timer message to revoke expired leases.

## Token-Based Ownership

Possession of the 32-byte token = authority to renew/release.

## No Persistence

All leases disappear on restart.

---

# 6. Data Model (Corrected for Actor MVP)

```rust
struct LeaseEntry {
	token: [u8; 32],
	expiry: Instant,
}
```

All higher-level constructs removed.  
No body.  
No waiters.  
No RwLocks.  
No DashMap.

This is pure actor memory.

---

# 7. Error Codes (Actor-Model Corrected)

| Code           | Meaning                                  |
| -------------- | ---------------------------------------- |
| LEASE_HELD     | Resource already held                     |
| INVALID_TOKEN  | Token mismatch                            |
| LEASE_EXPIRED  | Expired before operation                  |
| LEASE_NOT_HELD | Tried to renew/release nonexistent lease  |
| BAD_ROUTE      | Invalid realm/area/resource               |

No `TIMEOUT` in the actor model because acquire is synchronous and never queues.

---

# 8. Configuration (MVP)

Only 3 relevant configs:

```yaml
lease:
  default_ttl_seconds: 30
  max_ttl_seconds: 3600
  token_length: 32
```

Everything else removed.

---

# 9. Observability

Metrics:

* `lease_acquire_total{status}`
* `lease_renew_total{status}`
* `lease_release_total{status}`
* `lease_active_gauge`

Logs:

* `lease_acquired`
* `lease_expired`
* `lease_renewed`
* `lease_released`

---

# 10. Testing Requirements

## Unit Tests

* Acquire → success
* Acquire twice → second fails
* Renew before expiry
* Renew after expiry → error
* Release with correct token
* Release with wrong token

## Integration Tests

* Multiple concurrent attempts
* Timer-driven expiry
* TTL boundary cases

No queue fairness tests (removed).  
No persistence tests (removed).

---

# 11. Usage Patterns

### Leader Election

Use acquire → renew loop.

### Distributed Lock

Acquire → do work → release.

### Single Worker Scheduling

Acquire → process tasks → renew as needed.

---

# 🎯 This is the corrected, actor-model-accurate Lease Specification.

If you'd like, I can now generate specs for the other domains in the same format:

* **Queue Domain**
* **Stream Domain**
* **KV Domain**
* **Notice Domain**
* **RPC Domain**

Just say “next domain: queue” and I’ll produce the full spec.