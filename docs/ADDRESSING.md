# Fitz Addressing Model

**Version:** 1.0  
**Last Updated:** January 1, 2026

---

## Overview

Fitz uses **RouteFamily + Route** as the universal addressing and isolation model across the entire runtime. All domains (RPC, Notifications, Queue, Stream, KV, Lease) use this same addressing pattern.

---

## Core Concepts

### RouteFamily

A **RouteFamily** is a hard isolation boundary represented as an integer (u64).

**Properties:**
- Opaque identifier with no semantic meaning to the runtime
- No hierarchy, prefix semantics, or inheritance between families
- Isolation applies to routing, leasing, coordination, and state
- **Alignment:** RouteFamilyId aligns 1:1 with Midge ColumnFamilyId (same value, same type)

**Guarantees:**
- No routing across RouteFamilies
- No lease conflicts across RouteFamilies
- No message delivery across RouteFamilies
- No shared state across RouteFamilies

### Route

A **Route** is a structured identifier following this pattern:

```text
{scheme}://{realm}/{area}/{resource}/{operation}
```

**Components:**

| Component  | Description | Example |
|-----------|-------------|---------|
| **scheme** | Addressing intent or interaction pattern | `rpc`, `inbox`, `notify`, `queue`, `stream`, `lease` |
| **realm** | Top-level logical namespace | `acme`, `tenant-123`, `prod`, `staging` |
| **area** | Logical subsystem or bounded context | `auth`, `orders`, `events`, `locks` |
| **resource** | Entity, stream, queue, or keyspace identifier | `users`, `db/migration`, `worker/run` |
| **operation** | Action or verb (optional) | `authenticate`, `acquire`, `process`, `append` |

**Important Distinctions:**

- **scheme is NOT a domain**: Multiple schemes may map to the same domain. Domain dispatch is resolved from the full Route, not assumed from the scheme alone.

- **realm is NOT RouteFamily**: 
  - RouteFamily = opaque integer providing hard isolation
  - realm = string in the route path providing logical organization
  - Same realm value can appear in different RouteFamilies

**Examples:**
```text
rpc://acme/auth/users/authenticate
notify://acme/events/orders/created
queue://acme/jobs/worker/process
stream://acme/analytics/events/append
lease://acme/locks/db/migration/acquire
```

### Full Address

A full address is **always** the pair: `(RouteFamilyId, Route)`

**Rules:**
- Every message send must specify a RouteFamilyId
- RouteFamilyId is never optional and has no default
- Replies must preserve the original RouteFamilyId
- Domains must never observe or influence state outside their RouteFamily

---

## Usage Patterns

### Multi-Tenancy

**Option 1: RouteFamily-per-tenant (strongest isolation)**

```rust
let tenant_a_family = RouteFamily::new(100);
let tenant_b_family = RouteFamily::new(200);

let tenant_a_addr = RouteAddress::new(
    tenant_a_family,
    Route::new("rpc://app/orders/create".to_string())
);

let tenant_b_addr = RouteAddress::new(
    tenant_b_family,
    Route::new("rpc://app/orders/create".to_string())
);

// Complete isolation: routing, leases, state all independent
```

**Option 2: Shared RouteFamily, realm-per-tenant (logical isolation)**

```rust
let shared_family = RouteFamily::new(1);

let tenant_a_logical = RouteAddress::new(
    shared_family,
    Route::new("rpc://tenant-a/orders/create".to_string())
);

let tenant_b_logical = RouteAddress::new(
    shared_family,
    Route::new("rpc://tenant-b/orders/create".to_string())
);

// Logical isolation via realm, but shares RouteFamily resources
```

### Environment Separation

```rust
// Production and staging as separate RouteFamilies
let prod_family = RouteFamily::new(1);
let staging_family = RouteFamily::new(2);

let prod_addr = RouteAddress::new(
    prod_family,
    Route::new("rpc://acme/auth/users/authenticate".to_string())
);

let staging_addr = RouteAddress::new(
    staging_family,
    Route::new("rpc://acme/auth/users/authenticate".to_string())
);

// Same route pattern, complete isolation between environments
```

---

## Domain Integration

All Fitz domains use this addressing model:

### Lease Domain

```text
lease://acme/locks/db/migration/acquire
```

- Lease identity: `(RouteFamily, realm, area, resource)`
- Same resource in different families = independent leases

### RPC Domain

```text
rpc://acme/auth/users/authenticate
```

- Service identity: `(RouteFamily, realm, area, resource)`
- Operation: verb at the end

### Queue Domain

```text
queue://acme/jobs/worker/process
```

- Queue identity: `(RouteFamily, realm, area, resource)`
- Operation: enqueue/dequeue

### Stream Domain

```text
stream://acme/analytics/events/append
```

- Stream identity: `(RouteFamily, realm, area, resource)`
- Operation: append/read

### Notification Domain

```text
notify://acme/events/orders/created
```

- Topic identity: `(RouteFamily, realm, area, resource)`
- Operation: publish/subscribe

---

## Invariants

**CRITICAL: These invariants must be maintained:**

1. **No cross-family routing**: Route lookup in family A never returns results from family B, even with matching route strings.

2. **No cross-family leases**: Leases acquired in family A have no effect on family B.

3. **No cross-family messages**: Messages sent to (family A, route X) never reach (family B, route X).

4. **No cross-family state**: Domain state changes in family A never affect family B.

5. **Schemes don't imply domains**: Multiple schemes can map to the same domain; routing is resolved from the full Route.

6. **Realm semantics are opaque**: The runtime doesn't interpret realm values; users define their own organizational semantics.

7. **RouteFamilyId is never optional**: Every address, lease, message, and state operation must specify a RouteFamily.

---

## Alignment with Storage

**RouteFamilyId ↔ Midge ColumnFamilyId**

- Same underlying type: `u64`
- Same value represents the same isolation boundary
- Alignment is contractual, not enforced by storage code
- Allows storage layer to maintain isolation guarantees

---

## Testing Requirements

All domains must test these isolation properties:

### Router Tests
- ✅ Same route in different families resolves independently
- ✅ Registration in family A doesn't affect family B
- ✅ Unregistration in family A doesn't affect family B

### Lease Tests
- ✅ Same resource in different families can be leased independently
- ✅ Lease conflicts only occur within the same family
- ✅ Release in family A doesn't affect family B

### Message Tests
- ✅ Messages never cross RouteFamily boundaries
- ✅ Replies preserve original RouteFamily

---

## Implementation Notes

### Current State (v1.0)

- ✅ RouteFamily and Route types implemented
- ✅ Router enforces family isolation
- ✅ Lease domain uses (RouteFamily, Route) addressing
- ✅ All routing tests validate isolation
- ⏳ Lease tests need updates for new LeaseKey format
- ⏳ Other domains (RPC, Queue, Stream, Notification) to be implemented

### Future Work

- Add route parsing utilities for scheme/realm/area/resource extraction
- Domain dispatching based on scheme
- Wildcards and prefix matching (optional, per-domain)
- Persistence integration with Midge ColumnFamily alignment

---

## References

- Implementation: `src/transport/routing.rs`
- Lease domain: `src/domains/lease/`
- Router: `src/transport/router.rs`
- Tests: `src/transport/routing.rs` (tests module)
