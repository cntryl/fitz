# Fitz Route Design

**Status**: Authoritative  
**Last Updated**: 2026-02-14  
**Purpose**: Define routing patterns and requirements for all Fitz domains

---

## Overview

Fitz uses hierarchical route-based addressing for all operations. Every message is addressed using a `(RouteFamily, Route)` pair:

- **RouteFamily** (u32): Hard isolation boundary for multi-tenancy/environments
- **Route** (String): Hierarchical path for logical organization

### Route Anatomy

```
{scheme}://{realm}/{area}/{resource}/{operation}
```

| Component | Type | Purpose | Example |
|-----------|------|---------|---------|
| `scheme` | String | Domain selector (advisory) | `kv`, `notice`, `rpc` |
| `realm` | String | Top-level namespace | `acme`, `prod`, `tenant-123` |
| `area` | String | Logical subsystem | `app`, `system`, `cache` |
| `resource` | String | Entity/stream/queue name | `users`, `orders`, `events` |
| `operation` | String | Action verb (optional) | `get`, `put`, `subscribe` |

**Critical Distinction**: 
- **Realm** = string segment in route path (user-visible namespace)
- **RouteFamily** = numeric isolation boundary (infrastructure-level partitioning)

Same realm string can appear across different RouteFamilies.

### Domain Dispatch

Domains are selected by **message type ranges**, not by scheme:

| Message Type | Domain | State |
|--------------|--------|-------|
| 100-199 | KV | Persistent |
| 200-299 | Queue | Persistent |
| 300-399 | RPC | Ephemeral |
| 400-499 | Lease | Ephemeral |
| 500-504 | Notice | Ephemeral |
| 600-699 | Stream | Persistent |
| 700-799 | Schedule | Persistent |

**Implication**: The scheme in a route (e.g., `kv://`) is advisory only. Actual domain dispatch uses the message type number from the TLV frame.

---

## Domain Route Requirements

### 1. Key-Value (KV) Domain

**Prefix**: `kv://`  
**Message Types**: 100-199  
**State**: Persistent (Midge LSM)

#### Route Format

```
kv://{realm}/{area}/{resource}
```

**Segments**: Exactly 3 (realm, area, resource)  
**Operation**: Not in route path (encoded in message type)

#### Route Identity

```rust
(RouteFamily, realm, area, resource)
```

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `begin` | 100 | Start transaction | Write |
| `commit` | 101 | Commit transaction | Write |
| `rollback` | 102 | Abort transaction | Write |
| `get` | 103 | Retrieve value | Read |
| `put` | 104 | Upsert key-value | Write |
| `insert` | 105 | Insert (fail if exists) | Write |
| `delete` | 106 | Delete key | Write |
| `delete_range` | 107 | Delete key range | Write |
| `scan` | 108 | Scan key range | Read |

#### Route Validation Rules

1. **Segment count**: Must be exactly 3 segments after scheme
2. **RouteFamily**: Must be non-zero (no default CF allowed)
3. **Transaction scope**: All operations within transaction must use same route
4. **Resource binding**: Transaction bound to single resource at BEGIN
5. **Column family mapping**: `RouteFamily.id()` → `ColumnFamilyId` (1:1)

#### Examples

**Valid**:
```
kv://acme/app/users
kv://prod/cache/sessions
kv://tenant-123/orders/items
```

**Invalid**:
```
kv://acme/app              # Too few segments
kv://acme/app/users/get    # Operation in path (not supported)
kv://                      # Missing realm/area/resource
```

#### Parsing Logic

Location: [src/protocol/kv_codec.rs](../../src/protocol/kv_codec.rs)

```rust
// Route parsed into (realm, area, resource) triplet
let parts: Vec<&str> = route.split('/').collect();
if parts.len() != 3 {
    return Err("KV routes require exactly 3 segments: realm/area/resource");
}
let (realm, area, resource) = (parts[0], parts[1], parts[2]);
```

#### Special Rules

- **No wildcards**: Routes must be exact (no `*` or `**` patterns)
- **Transaction isolation**: Each transaction operates on single resource
- **Read-write modes**: Transactions can be read-only or read-write
- **No cross-resource operations**: Cannot access multiple resources in one transaction

---

### 2. Queue Domain

**Prefix**: `queue://`  
**Message Types**: 200-299  
**State**: Persistent (Midge LSM)

#### Route Format

```
queue://{realm}/{area}/{resource}
```

**Segments**: 3+ (realm, area, resource, trailing ignored)  
**Operation**: Not in route path (encoded in message type)

#### Route Identity

```rust
(RouteFamily, realm, area, resource)
```

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `enqueue` | 200 | Add message to queue | Write |
| `reserve` | 202 | Lease messages | Write |
| `extend` | 203 | Extend lease | Write |
| `complete` | 204 | Mark completed | Write |

*Note*: Message type 201 is reserved/unused.

#### Route Validation Rules

1. **Minimum segments**: At least 3 segments required
2. **Trailing segments**: Ignored (allows versioning in route)
3. **RouteFamily**: Must be non-zero
4. **Queue identity**: Defined by (family, realm, area, resource) only

#### Examples

**Valid**:
```
queue://acme/tasks/work
queue://prod/jobs/worker
queue://tenant-a/processing/high-priority
queue://acme/tasks/work/v2  # Trailing segment ignored
```

**Invalid**:
```
queue://acme/tasks          # Too few segments
queue://                    # Missing realm/area/resource
```

#### Parsing Logic

Location: [src/domains/queue/protocol.rs](../../src/domains/queue/protocol.rs)

```rust
pub struct QueueKey {
    pub family: RouteFamily,
    pub realm: String,
    pub area: String,
    pub resource: String,
}

impl QueueKey {
    pub fn from_route(family: RouteFamily, route: &str) -> Result<Self, String> {
        let parts: Vec<&str> = route.split('/').collect();
        if parts.len() < 3 {
            return Err("Queue routes require at least 3 segments".into());
        }
        Ok(Self {
            family,
            realm: parts[0].to_string(),
            area: parts[1].to_string(),
            resource: parts[2].to_string(),
        })
    }
}
```

#### Special Rules

- **Competing consumers**: Multiple workers can reserve from same queue
- **Lease-based visibility**: Reserved messages invisible until lease expires
- **At-least-once delivery**: Messages may be redelivered on lease expiration
- **Token protocol**: Random u64 tokens prevent duplicate operations
- **Optional DLQ**: Dead-letter queue after max_attempts threshold
- **FIFO ordering**: Messages delivered in insertion order
- **No wildcards**: Routes must be exact

---

### 3. RPC Domain

**Prefix**: `rpc://`  
**Message Types**: 300-399  
**State**: Ephemeral (in-memory)

#### Route Format

```
rpc://{realm}/{area}/{resource}/{operation}
```

**Segments**: Flexible (no enforced structure)  
**Operation**: Part of business logic (embedded in route path)

#### Route Identity

```rust
(RouteFamily, route_string)
```

Routes stored as opaque strings. No parsing into components.

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `subscribe` | 300 | Worker registration | Write |
| `unsubscribe` | 301 | Worker deregistration | Write |
| `request` | 302 | Client request | Write |
| `response` | 303 | Worker response | Write |
| `ack` | 304 | Worker completion | Write |

#### Route Validation Rules

1. **No structure enforced**: Routes can have any number of segments
2. **Opaque storage**: Route stored as-is without parsing
3. **Exact matching**: No wildcard support
4. **Worker pool per route**: Each unique route has independent worker pool

#### Examples

**Valid**:
```
rpc://acme/auth/user/create
rpc://prod/compute/job/run
rpc://tenant-a/inventory/item/update
rpc://analytics/reports/monthly/generate
rpc://api/v1/users/profile/get
```

All routes are valid as long as they're non-empty strings.

#### Parsing Logic

Location: [src/domains/rpc/protocol.rs](../../src/domains/rpc/protocol.rs)

```rust
// Routes stored as opaque strings - no parsing
pub struct RpcRoute {
    pub family: RouteFamily,
    pub route: String,  // Stored as-is
}
```

#### Special Rules

- **Worker pools**: Each route maintains independent worker pool
- **Round-robin dispatch**: Requests distributed to available workers
- **Correlation protocol**: UUID correlation_id links request/response
- **Streaming support**: Multi-chunk responses with sequence numbers
- **FIFO ordering**: Requests dispatched in arrival order
- **Bounded queue**: Default 1000 request capacity per route
- **No wildcards**: Workers register for exact routes
- **Session cleanup**: Worker registrations auto-removed on disconnect

---

### 4. Lease Domain

**Prefix**: `lease://`  
**Message Types**: 400-499  
**State**: Ephemeral (in-memory)

#### Route Format

```
lease://{realm}/{area}/{resource}
```

**Segments**: 3+ (realm, area, resource, trailing ignored)  
**Operation**: Can optionally be in route path (but not required)

#### Route Identity

```rust
(RouteFamily, realm, area, resource)
```

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `acquire` | 400 | Request ownership | Write |
| `renew` | 401 | Extend lease | Write |
| `release` | 402 | Relinquish ownership | Write |
| `query` | 403 | Inspect status | Read |

#### Route Validation Rules

1. **Minimum segments**: At least 3 segments required
2. **Scheme stripping**: `lease://` prefix removed if present
3. **Trailing segments**: Ignored
4. **Lease identity**: (family, realm, area, resource) tuple

#### Examples

**Valid**:
```
lease://prod/locks/db-migration
lease://acme/locks/config
lease://tenant-a/critical/database
lease://realm/area/resource/acquire  # Optional operation suffix
```

**Invalid**:
```
lease://prod/locks              # Too few segments
lease://                        # Missing realm/area/resource
```

#### Parsing Logic

Location: [src/domains/lease/protocol.rs](../../src/domains/lease/protocol.rs)

```rust
pub struct LeaseKey {
    pub family: RouteFamily,
    pub realm: String,
    pub area: String,
    pub resource: String,
}

impl LeaseKey {
    pub fn from_route(family: RouteFamily, route: &str) -> Result<Self, String> {
        let route = route.strip_prefix("lease://").unwrap_or(route);
        let parts: Vec<&str> = route.split('/').collect();
        if parts.len() < 3 {
            return Err("Lease routes require at least 3 segments".into());
        }
        Ok(Self {
            family,
            realm: parts[0].to_string(),
            area: parts[1].to_string(),
            resource: parts[2].to_string(),
        })
    }
}
```

#### Special Rules

- **Exclusive ownership**: Only one owner per lease at a time
- **Fencing tokens**: Monotonically increasing u64 for split-brain prevention
- **TTL-based expiration**: Leases auto-expire after configured TTL
- **Idempotent operations**: Safe to retry acquire/renew/release
- **Non-durable**: State lost on server restart
- **No wildcards**: Exact route matching only

---

### 5. Notice Domain (Pub/Sub)

**Prefix**: `notice://`  
**Message Types**: 500-504  
**State**: Ephemeral (in-memory)

#### Route Format

```
notice://{realm}/{area}/{resource}/{event}
```

**Segments**: Flexible (no enforced structure)  
**Wildcard Support**: ✅ Yes (`*` and `**`)

#### Route Identity

```rust
(RouteFamily, route_string)
```

Routes stored as opaque strings with wildcard pattern matching.

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `publish` | 500 | Send to subscribers | Write |
| `subscribe` | 501 | Register subscription | Write |
| `unsubscribe` | 502 | Remove subscription | Write |
| `unsubscribe_all` | 503 | Remove all subscriptions | Write |
| `notify` | 504 | Delivery to subscriber | (Internal) |

#### Route Validation Rules

1. **No structure enforced**: Flexible segment count
2. **Wildcard patterns**:
   - `*` matches single path segment
   - `**` matches zero or more segments
3. **Pattern matching**: Subscriptions can use wildcards, publishes must be exact

#### Wildcard Patterns

| Pattern | Matches | Example |
|---------|---------|---------|
| `notice://acme/events/orders/created` | Exact route only | Exact |
| `notice://acme/events/*` | Any single segment after `events/` | `orders`, `users` |
| `notice://acme/events/orders/*` | Any event type for orders | `created`, `updated` |
| `notice://acme/**` | All routes under `acme` | Any depth |
| `notice://*/events/created` | `created` event in any realm | Cross-realm |

#### Examples

**Valid Publish Routes** (must be exact):
```
notice://acme/events/orders/created
notice://prod/notifications/user/login
notice://tenant-a/analytics/page/view
```

**Valid Subscribe Patterns**:
```
notice://acme/events/*              # Single level
notice://acme/events/**             # Multi-level
notice://acme/events/orders/*       # Orders only
notice://*/events/created           # Cross-realm
```

**Invalid**:
```
notice://acme/events/*/created      # Cannot publish with wildcards
```

#### Parsing Logic

Location: [src/domains/notice/protocol.rs](../../src/domains/notice/protocol.rs)

```rust
// Routes stored as opaque strings
// Pattern matching via runtime/matcher.rs

pub struct NoticeRoute {
    pub family: RouteFamily,
    pub route: String,  // Can contain * or **
}
```

#### Special Rules

- **Fire-and-forget**: No acknowledgements or retries
- **Best-effort delivery**: Only to subscribers alive at publish time
- **Session-scoped**: Subscriptions auto-cleanup on disconnect
- **Zero-copy fanout**: Arc pointer sharing across subscribers
- **No durability**: Messages not persisted
- **Pattern matching**: High-performance trie-based matcher

---

### 6. Stream Domain

**Prefix**: `stream://`  
**Message Types**: 600-699  
**State**: Persistent (Midge LSM)

#### Route Format

```
stream://{realm}/{area}/{resource}
```

**Segments**: 3+ (realm, area, resource)  
**Operation**: Not in route path (encoded in message type)  
**Wildcard Support**: ⚠️ Limited (read operations only)

#### Route Identity

```rust
(RouteFamily, realm, area, resource)
```

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `begin` | 600 | Start append session | Write |
| `append` | 601 | Add event to session | Write |
| `commit` | 602 | Atomic flush | Write |
| `rollback` | 603 | Discard session | Write |
| `read` | 604 | Read events | Read |
| `last` | 605 | Get last offset | Read |
| `get_metadata` | 606 | Get stream metadata | Read |
| `subscribe` | 607 | Subscribe to events | Read |
| `unsubscribe` | 608 | Unsubscribe | Read |

#### Route Validation Rules

1. **Segment count**: At least 3 segments required
2. **Ordered hierarchy**: realm → area → resource
3. **Wildcard reads**: Supported only for read operations:
   - `stream://realm/area/*/read` - Area-level read
   - `stream://realm/*/*/read` - Realm-level read
4. **No wildcard writes**: Append operations require exact route

#### Three-Level Ordering

Stream domain provides ordering guarantees at three levels:

1. **Resource-level ordering**: Sequential offsets within a resource
2. **Area-level ordering**: Global ordering across all resources in an area
3. **Realm-level ordering**: Global ordering across all areas in a realm

#### Examples

**Valid Write Routes**:
```
stream://acme/orders/checkout
stream://prod/logs/application
stream://tenant-a/events/user-activity
```

**Valid Read Routes**:
```
stream://acme/orders/checkout          # Resource-level
stream://acme/orders/*                 # Area-level (all resources)
stream://acme/*/*                      # Realm-level (all areas)
```

**Invalid**:
```
stream://acme/orders                   # Too few segments
stream://acme/*/checkout               # Wildcard in append (not supported)
```

#### Parsing Logic

Location: [src/domains/stream/protocol.rs](../../src/domains/stream/protocol.rs)

```rust
// Parsed into hierarchy
let parts: Vec<&str> = route.split('/').collect();
if parts.len() < 3 {
    return Err("Stream routes require at least 3 segments");
}
let (realm, area, resource) = (parts[0], parts[1], parts[2]);
```

#### Special Rules

- **Server-assigned offsets**: All offsets assigned by server, not client
- **Optimistic concurrency**: `expected_offset` for conflict detection
- **Watermark-gated reads**: Area/realm reads blocked beyond watermarks
- **Gap-free sequences**: Enforced by watermark coordination
- **Offset leases**: AreaActor and RealmActor coordinate via lease protocol
- **Durable state**: All events persisted to LSM
- **Actor hierarchy**: StreamActor → AreaActor → RealmActor

---

### 7. Schedule Domain

**Prefix**: `schedule://`  
**Message Types**: 700-799  
**State**: Persistent (Midge LSM)

#### Route Format

```
schedule://{realm}/{area}/{resource}/{operation}
```

**Segments**: Exactly 4 (realm, area, resource, operation)  
**Operation**: Required in route path  
**Wildcard Support**: ⚠️ Limited (subscribe only)

#### Route Identity

```rust
(RouteFamily, realm, area, resource)
```

#### Supported Operations

| Operation | Message Type | Description | Access Level |
|-----------|--------------|-------------|--------------|
| `create` | 700 | Create scheduled task | Write |
| `cancel` | 701 | Cancel task | Write |
| `list` | 702 | List schedules | Read |
| `subscribe` | 703 | Subscribe to fires | Read |
| `unsubscribe` | 704 | Unsubscribe | Read |
| `notify` | 705 | Schedule fire event | (Internal) |

#### Route Validation Rules

1. **Segment count**: Exactly 4 segments required
2. **Wildcard subscriptions**: Can use `*` for route pattern matching
3. **Exact creation**: Create operations require exact route and operation
4. **Target routes**: Embedded in schedule payload (cross-domain)

#### Examples

**Valid Routes**:
```
schedule://prod/jobs/cleanup/create
schedule://acme/jobs/backup/create
schedule://analytics/reports/monthly/create
schedule://prod/jobs/cleanup/cancel
```

**Valid Subscribe Patterns**:
```
schedule://prod/jobs/*              # All jobs in prod/jobs
schedule://prod/**                  # All schedules in prod
```

**Invalid**:
```
schedule://prod/jobs                # Too few segments
schedule://prod/jobs/cleanup        # Missing operation
```

#### Parsing Logic

Location: [src/domains/schedule/protocol.rs](../../src/domains/schedule/protocol.rs)

```rust
// Parsed into (realm, area, resource, operation)
let parts: Vec<&str> = route.split('/').collect();
if parts.len() != 4 {
    return Err("Schedule routes require exactly 4 segments".into());
}
```

#### Special Rules

- **Cron expressions**: Standard 5-field cron syntax
- **Dual emission**: Fires emit to both:
  1. `schedule://` subscribers (SCHEDULE_NOTIFY message)
  2. Target resource route (cross-domain execution)
- **Coalescing semantics**: Missed ticks fire at most once
- **Time-based**: Wall-clock scheduling (not logical time)
- **Durable**: Schedules persisted to Midge
- **TLV payload**: Schedule definitions encoded as TLV

---

## Cross-Cutting Concerns

### RouteFamily Rules

1. **Hard isolation**: Complete state separation between families
2. **1:1 ColumnFamily mapping**: `RouteFamily.id()` → `ColumnFamilyId`
3. **No default CF**: `RouteFamily=0` forbidden for persistent domains
4. **Wire format**: u64 on wire, clamped to u32 on parse
5. **Opaque identifier**: No semantic meaning to runtime
6. **No hierarchy**: Families are flat, no parent/child relationships

### Wildcard Support Matrix

| Domain | Wildcard Support | Pattern Types | Use Case |
|--------|------------------|---------------|----------|
| KV | ❌ No | N/A | Exact key access |
| Queue | ❌ No | N/A | Specific queue targeting |
| RPC | ❌ No | N/A | Exact service routing |
| Lease | ❌ No | N/A | Specific lock identity |
| Notice | ✅ Yes | `*`, `**` | Event pattern matching |
| Stream | ⚠️ Limited | `*` (reads only) | Area/realm aggregation |
| Schedule | ⚠️ Limited | `*` (subscribe only) | Job monitoring |

### Permission Model

All domains use prefix-based authorization:

```
{domain}://{realm}/**#{access}
```

**Access Levels**:
- `read` - Read operations only
- `write` - Write operations (may imply read)
- `*` - All operations

**Examples**:
```
kv://acme/**#read         # Read-only KV access
queue://prod/**#write     # Queue write access
notice://realm1/**#read   # Subscribe to notices
rpc://tenant-a/**#write   # RPC request access
```

**Authorization Granularity**:

| Domain | Granularity | Notes |
|--------|-------------|-------|
| KV | Operation-level | Separate read/write checks |
| Queue | Operation-level | All operations require write |
| RPC | Route-level | All operations are write |
| Lease | Route-level | All operations are write |
| Notice | Pattern-level | Publish=write, Subscribe=read |
| Stream | Operation-level | Separate read/write checks |
| Schedule | Operation-level | Create=write, Subscribe=read |

### State Durability

| Domain | Persistence | Cleanup | Recovery |
|--------|-------------|---------|----------|
| KV | Midge LSM | Manual | Full recovery |
| Queue | Midge LSM | Manual/DLQ | Full recovery |
| RPC | In-memory | Auto on disconnect | None |
| Lease | In-memory | TTL/Auto | None |
| Notice | In-memory | Auto on disconnect | None |
| Stream | Midge LSM | Manual | Full recovery |
| Schedule | Midge LSM | Manual | Full recovery |

---

## Design Decisions & Rationale

### Why Message Type Dispatch (Not Scheme)?

**Decision**: Domain selection uses message type ranges (100-199 for KV, etc.), not URI scheme.

**Rationale**:
1. **Performance**: Numeric dispatch faster than string matching
2. **Extensibility**: Can add schemes without changing dispatch logic
3. **Wire efficiency**: Message type already in TLV header
4. **Type safety**: Message type enforces valid operation set per domain

**Implication**: Scheme is advisory for clients. Server uses message type only.

### Why Structured vs Flexible Routes?

**Structured (KV, Queue, Stream, Lease)**:
- Need realm/area/resource parsing for authorization
- Require component-level state management
- Support hierarchical operations (e.g., area-level reads)

**Flexible (RPC, Notice)**:
- Opaque business logic in routes
- No need for component extraction
- Better support for API-style paths

### Why No Wildcards in Most Domains?

**Decision**: Only Notice (and limited Stream/Schedule) support wildcards.

**Rationale**:
1. **Performance**: Exact matching orders of magnitude faster
2. **Predictability**: No ambiguous routing
3. **Authorization**: Simpler permission model
4. **State management**: Exact keys enable better sharding

**Exception**: Notice domain designed for pattern-based pub/sub, requires wildcards.

### Operation Placement Variations

**In Message Type** (KV, Queue, Stream):
- Fixed operation set
- Type-safe dispatch
- Better for transactional domains

**In Route Path** (RPC, Lease optional):
- Flexible operation semantics
- API-style routing
- Business logic encapsulation

---

## Reference Tables

### Quick Lookup: Domain → Route Format

| Domain | Format | Segments | Wildcards | State |
|--------|--------|----------|-----------|-------|
| KV | `realm/area/resource` | Exactly 3 | No | Persistent |
| Queue | `realm/area/resource` | 3+ | No | Persistent |
| RPC | Flexible | Any | No | Ephemeral |
| Lease | `realm/area/resource` | 3+ | No | Ephemeral |
| Notice | Flexible | Any | Yes | Ephemeral |
| Stream | `realm/area/resource` | 3+ | Limited | Persistent |
| Schedule | `realm/area/resource/{operation}` | Exactly 4 | Limited | Persistent |

### Message Type Ranges

| Range | Domain | First Operation | Last Operation |
|-------|--------|-----------------|----------------|
| 100-199 | KV | BEGIN (100) | SCAN (108) |
| 200-299 | Queue | ENQUEUE (200) | COMPLETE (204) |
| 300-399 | RPC | SUBSCRIBE (300) | ACK (304) |
| 400-499 | Lease | ACQUIRE (400) | QUERY (403) |
| 500-504 | Notice | PUBLISH (500) | NOTIFY (504) |
| 600-699 | Stream | BEGIN (600) | UNSUBSCRIBE (608) |
| 700-799 | Schedule | CREATE (700) | NOTIFY (705) |

### Implementation File Locations

| Domain | Protocol | Actor | Codec | Session |
|--------|----------|-------|-------|---------|
| KV | [domains/kv/protocol.rs](../../src/domains/kv/protocol.rs) | [domains/kv/actor.rs](../../src/domains/kv/actor.rs) | [protocol/kv_codec.rs](../../src/protocol/kv_codec.rs) | [domains/kv/session.rs](../../src/domains/kv/session.rs) |
| Queue | [domains/queue/protocol.rs](../../src/domains/queue/protocol.rs) | [domains/queue/actor.rs](../../src/domains/queue/actor.rs) | [protocol/queue_codec.rs](../../src/protocol/queue_codec.rs) | [domains/queue/session.rs](../../src/domains/queue/session.rs) |
| RPC | [domains/rpc/protocol.rs](../../src/domains/rpc/protocol.rs) | [domains/rpc/actor.rs](../../src/domains/rpc/actor.rs) | [protocol/rpc_codec.rs](../../src/protocol/rpc_codec.rs) | [domains/rpc/session.rs](../../src/domains/rpc/session.rs) |
| Lease | [domains/lease/protocol.rs](../../src/domains/lease/protocol.rs) | [domains/lease/actor.rs](../../src/domains/lease/actor.rs) | [protocol/lease_codec.rs](../../src/protocol/lease_codec.rs) | [domains/lease/session.rs](../../src/domains/lease/session.rs) |
| Notice | [domains/notice/protocol.rs](../../src/domains/notice/protocol.rs) | [domains/notice/actor.rs](../../src/domains/notice/actor.rs) | [protocol/notice_codec.rs](../../src/protocol/notice_codec.rs) | [domains/notice/session.rs](../../src/domains/notice/session.rs) |
| Stream | [domains/stream/protocol.rs](../../src/domains/stream/protocol.rs) | [domains/stream/actor.rs](../../src/domains/stream/actor.rs) | [protocol/stream_codec.rs](../../src/protocol/stream_codec.rs) | [domains/stream/session.rs](../../src/domains/stream/session.rs) |
| Schedule | [domains/schedule/protocol.rs](../../src/domains/schedule/protocol.rs) | [domains/schedule/actor.rs](../../src/domains/schedule/actor.rs) | [protocol/schedule_codec.rs](../../src/protocol/schedule_codec.rs) | [domains/schedule/session.rs](../../src/domains/schedule/session.rs) |

---

## Validation Checklist

When implementing or modifying route handling:

- [ ] Verify segment count matches domain requirements
- [ ] Validate RouteFamily is non-zero for persistent domains
- [ ] Check wildcard support matches domain capabilities
- [ ] Ensure operation placement follows domain pattern
- [ ] Validate authorization scopes are prefix-based
- [ ] Confirm message type falls in correct range
- [ ] Test route parsing with edge cases (empty segments, special chars)
- [ ] Verify session cleanup for ephemeral subscriptions
- [ ] Check ColumnFamily mapping for persistent domains
- [ ] Document any domain-specific routing rules

---

**See Also**:
- [Fitz Architecture](./architecture.md)
- [Connection Flow](../clients/connection-flow.md)
- [Copilot Instructions](../../.github/copilot-instructions.md)
