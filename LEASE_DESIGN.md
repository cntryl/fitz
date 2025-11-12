# Lease Domain Design

## Route Structure

The lease domain is designed around the following route pattern:

```
lease://{realm}/{area}/{resource}/{operation}
```

Where:
- **realm**: Logical grouping for lease collections (e.g., tenant, organization)
- **area**: Sub-grouping within a realm (e.g., service, environment)
- **resource**: The specific resource being leased
- **operation**: The lease operation to perform

## Operations

The lease domain supports three core operations:

### 1. Acquire (`/acquire`)
Request a lease on a resource.

**Route:** `lease://{realm}/{area}/{resource}/acquire` (or no operation defaults to acquire)

**Required TLV Tags:**
- `TAG_LEASE` (u32): TTL in seconds

**Response:** `LeaseGrant` containing:
- `id`: Unique lease identifier (UUID)
- `token`: HMAC-SHA256 token for authentication
- `ttl_secs`: Granted TTL
- `body`: Optional metadata

**Behavior:**
- If resource is free, immediately grant the lease
- If resource is busy, enqueue requester (FIFO) and wait for release
- Supports timeout via `FITZ_LEASE_ACQUIRE_TIMEOUT` (default 10s, max 20s)

### 2. Renew (`/renew`)
Extend an existing lease by adding time.

**Route:** `lease://{realm}/{area}/{resource}/renew`

**Required TLV Tags:**
- `TAG_ID` (string): Lease ID from grant
- `TAG_DELIVERY_TOKEN` (string): Token from grant
- `TAG_LEASE` (u32): Additional seconds to add

**Response:**
- `TAG_LEASE` (u32): Remaining seconds after renewal

**Behavior:**
- Validates lease ID and token match
- Rejects if lease has expired
- Extends expiry time by requested seconds

### 3. Release (`/release` or `/surrender`)
Voluntarily release a lease and potentially hand off to waiting requesters.

**Route:** `lease://{realm}/{area}/{resource}/release`

**Required TLV Tags:**
- `TAG_ID` (string): Lease ID from grant
- `TAG_DELIVERY_TOKEN` (string): Token from grant

**Response:**
- OK on success
- Error if ID/token mismatch or lease not found

**Behavior:**
- Validates lease ID and token match
- If waiters exist: hands off lease to first waiter (FIFO)
- If no waiters: clears lease and prunes empty maps

## Multi-Tenant Isolation

Leases are namespaced by **route_family** (tenant ID) to prevent cross-tenant access:

```
route_family → realm → area → resource → LeaseEntry
```

- A tenant with `route_family=1` cannot access leases of `route_family=2`
- Sharding uses hash of both route_family and realm for distribution
- Each shard maintains a route_family map for consistent isolation

## Key Features

### FIFO Waiter Queue
When a resource is busy, requesters are enqueued in order and granted in FIFO order upon release:

```
Requester 1: acquire(res, 10s) → waits
Requester 2: acquire(res, 5s)  → waits (enqueued after Requester 1)
Holder:      release(res)       → grants to Requester 1
Requester 1: ← receives lease
Requester 2: ← still waiting
Holder 2:    release(res)       → grants to Requester 2
Requester 2: ← receives lease
```

### Automatic Expiration
- Background expirer task runs per shard every 100ms
- Expired leases trigger handoff to waiters or cleanup
- Skips locked entries to avoid blocking acquire/renew/release

### Memory Efficiency
- Maps are pruned when empty (resource → area → realm → route_family)
- Buffer pooling for response building
- SmallVec for token building to avoid heap allocations

## Implementation Files

- **`src/core/lease/types.rs`**: `LeaseOperation` enum and data structures
- **`src/core/lease/handler.rs`**: Domain handler routing to operations
- **`src/core/lease/service.rs`**: Service implementation with FIFO queue and expiration
- **Tests**: 35+ test cases covering all operations, multi-tenancy, and edge cases

## Example Usage

### Acquire a lease
```
route: lease://myapp/production/database/acquire
payload:
  TAG_LEASE: 30  # 30-second lease
```

### Renew an existing lease
```
route: lease://myapp/production/database/renew
payload:
  TAG_ID: "uuid-string"
  TAG_DELIVERY_TOKEN: "hmac-token"
  TAG_LEASE: 10  # extend by 10 more seconds
```

### Release a lease
```
route: lease://myapp/production/database/release
payload:
  TAG_ID: "uuid-string"
  TAG_DELIVERY_TOKEN: "hmac-token"
```

## Validation

All 35 lease tests pass, including:
- Operation parsing from routes
- TLV tag validation
- FIFO waiter ordering
- Multi-key independence
- Tenant isolation
- Expiration handling
