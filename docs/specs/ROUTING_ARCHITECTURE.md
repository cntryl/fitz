# Fitz v2 Routing Architecture (Canonical)

**Version:** 1.0  
**Status:** Locked In  
**Last Updated:** December 11, 2025  

---

## The Two-Level Hierarchy

### 1. Route Family = Physical Boundary

**Route Family is the ONLY physical boundary in Fitz.**

#### What Route Family Defines

- ✅ **Storage partition** - Maps to Midge column families
- ✅ **Isolation/environment** - prod, dev, customer-42, region
- ✅ **Resource isolation** - Hard boundary between families
- ✅ **Actor instantiation** - Each family has its own StreamActor, QueueActor, etc.
- ✅ **Control plane scope** - Quotas, limits, policies per family
- ✅ **Authorization scope** - Family-level permissions
- ✅ **Capacity limits** - Max connections, storage per family
- ✅ **Sharding unit** - Future cluster distribution

#### Examples

```
acme-prod          ← Production environment
acme-dev           ← Development environment
customer-42        ← SaaS customer
billing-us-east    ← Regional service
core-internal      ← Internal systems
```

#### Midge Storage Mapping

Each family maps to distinct Midge column families:

```
acme-prod.streams   ← All stream data for acme-prod
acme-prod.queues    ← All queue data for acme-prod
acme-prod.kv        ← All KV data for acme-prod
acme-prod.metrics   ← All metrics for acme-prod

acme-dev.streams    ← Completely separate storage
acme-dev.queues     ← Completely separate storage
acme-dev.kv         ← Completely separate storage
```

**Isolation guarantee:** Data in `acme-prod` can NEVER touch data in `acme-dev`.

---

### 2. Realm = Purely Logical Grouping

**Realm is NOT an isolation boundary and NOT a physical boundary.**

Realm is a **logical grouping inside a family** for organizational clarity.

#### What Realm Is

- ✅ **Subsystem grouping** - orders, auth, billing, analytics
- ✅ **Microservice grouping** - chat, search, recommendations
- ✅ **Conceptual grouping** - Organizational convenience
- ✅ **Routing clarity** - Makes routes self-documenting

#### What Realm Is NOT

- ❌ **NOT an isolation boundary** - Family is the isolation boundary
- ❌ **NOT a storage partition** - No physical storage impact
- ❌ **NOT an isolation boundary** - Data can cross realms
- ❌ **NOT an actor boundary** - No separate actors per realm
- ❌ **NOT a quota/capacity boundary** - Enforced at family level
- ❌ **NOT involved in durability** - Midge doesn't see realms

#### Examples

Within a single family `acme-prod`:

```
realm: orders       ← Logical grouping for order domain
realm: inventory    ← Logical grouping for inventory domain
realm: auth         ← Logical grouping for auth domain
realm: analytics    ← Logical grouping for analytics domain
realm: chat         ← Logical grouping for chat domain
```

All these realms share the same:
- Midge column families
- Actor instances
- Storage partition
- Capacity limits

---

## Complete Route Format

Every Fitz route exists within a **Route Family** context:

```
Route Family: acme-prod
    ↓
Route: {scheme}://{realm}/{area}/{resource}/{operation}
```

### Route Components

- **scheme**: Domain type (`stream`, `queue`, `kv`, `lease`, `rpc`, `notice`, `metrics`)
- **realm**: Logical grouping (orders, auth, billing) - NOT an isolation boundary!
- **area**: Sub-grouping within realm
- **resource**: Specific entity
- **operation**: Verb (append, get, acquire, etc.)

### Complete Examples

```
Family: acme-prod
├─ stream://orders/events/created/append       ← orders realm
├─ queue://orders/jobs/email/enqueue           ← orders realm
├─ kv://auth/tokens/session:123/get            ← auth realm
├─ lease://orders/locks/reconcile/acquire      ← orders realm
├─ rpc://billing/payments/refund/invoke        ← billing realm
└─ notice://chat/rooms/42/publish              ← chat realm

Family: acme-dev (separate storage!)
├─ stream://orders/events/created/append       ← same route, different family = different data!
└─ kv://auth/tokens/session:123/get            ← same route, different family = different data!
```

**Key insight:** Same route string in different families → completely separate data.

---

## Physical vs Logical Boundaries

### Physical Boundary (Route Family)

```
┌──────────────────────────────────────┐
│         Route Family: acme-prod      │  ← Physical isolation
│  ┌────────────────────────────────┐  │
│  │  Midge Column Families         │  │
│  │  - acme-prod.streams           │  │
│  │  - acme-prod.queues            │  │
│  │  - acme-prod.kv                │  │
│  │  - acme-prod.metrics           │  │
│  └────────────────────────────────┘  │
│                                      │
│  Actor Instances:                   │
│  - StreamActor (acme-prod)          │
│  - QueueActor (acme-prod)           │
│  - MidgeActor (acme-prod)           │
│                                      │
│  Quotas: max_conn=1000, max_storage │
│  Auth: family-level permissions     │
└──────────────────────────────────────┘
```

### Logical Grouping (Realm)

```
Within acme-prod:
├─ realm: orders      ← Logical only
│   ├─ stream://orders/events/...
│   ├─ queue://orders/jobs/...
│   └─ kv://orders/state/...
│
├─ realm: auth        ← Logical only
│   ├─ kv://auth/tokens/...
│   └─ kv://auth/sessions/...
│
└─ realm: billing     ← Logical only
    ├─ stream://billing/invoices/...
    └─ rpc://billing/payments/...

All share the same Midge storage!
All use the same actor instances!
```

---

## Why This Separation Is Perfect

### ✅ Clean Scalability

- **Route Families** scale horizontally (different nodes, different processes)
- **Realms** do not scale independently (they're just names)

### ✅ Clean Durability

- **Route Family** → direct Midge mapping
- **Realm** → no storage impact whatsoever

### ✅ Clean Routing

```rust
// RouterActor dispatch logic
1. Resolve Route Family (from connection context or routing table)
2. Parse route within family: scheme://realm/area/resource/op
3. Dispatch to domain actor based on scheme
4. Domain actor uses family for Midge operations
```

### ✅ Clean Authorization

Two-level permission model:

```yaml
# Family-level
family: acme-prod
  permissions:
    - admin: ["*"]              # Can do anything in this family
    - developer: ["read"]       # Read-only access to family

# Route-level (within family)
realm: orders
  path: "stream://orders/events/*"
  permissions:
    - writer: ["append"]
    - reader: ["read"]
```

### ✅ Clean Actor Model

Each family gets its own actor instances:

```rust
struct FamilyActors {
    family: RouteFamily,                      // "acme-prod"
    
    // One set of actors per family
    stream_actor: ActorRef<StreamMsg>,
    queue_actor: ActorRef<QueueMsg>,
    lease_actor: ActorRef<LeaseMsg>,
    rpc_actor: ActorRef<RpcMsg>,
    notice_actor: ActorRef<NoticeMsg>,
    midge_actor: ActorRef<MidgeMsg>,
    
    // Realm is just metadata, no separate actors
    realms: DashMap<String, RealmState>,      // Logical tracking only
}
```

### ✅ Clean Multi-Tenancy

```
SaaS Platform:
├─ customer-1 (Route Family)
│   ├─ realm: app
│   └─ realm: analytics
│
├─ customer-2 (Route Family)
│   ├─ realm: app
│   └─ realm: analytics
│
└─ customer-3 (Route Family)
    ├─ realm: app
    └─ realm: analytics

Each customer = separate Route Family = complete isolation.
```

---

## Visual Summary

```
                ┌─────────────────────────────────┐
                │      Route Family               │  ← PHYSICAL
                │  (isolation / environment / CF) │
                └─────────────────────────────────┘
                       ↓           ↓         ↓
             ┌─────────┼───────────┼─────────┼─────────┐
             │         │           │         │         │
         stream://  queue://    kv://    lease://  rpc://  ← SCHEME
             │         │           │         │         │
           Realm     Realm       Realm     Realm     Realm  ← LOGICAL
             │         │           │         │         │
         area/res   area/res    area/res  area/res  area/res
             ↓         ↓           ↓         ↓         ↓
        operation     ...         ...       ...       ...
```

---

## Implementation Rules

### Rule 1: Family Resolution First

Every incoming connection or request MUST resolve its Route Family before any routing:

```rust
// WebSocket connection handshake
let route_family = resolve_family_from_connection(&socket)?;

// Create or get family actor set
let family_actors = get_or_create_family_actors(&route_family);

// Now route within family
family_actors.router.send(RouteMsg { ... });
```

### Rule 2: Midge Operations Always Include Family

```rust
// CORRECT
midge.kv_put(
    route_family: "acme-prod",
    key: "kv://auth/tokens/session:123",
    value: ...
)

// WRONG - no family context
midge.kv_put(
    key: "kv://auth/tokens/session:123",  // Which family??
    value: ...
)
```

### Rule 3: Realm Is Metadata Only

```rust
// Realm appears in route but doesn't affect storage
let route = "stream://orders/events/created/append";

// These two routes go to the SAME Midge CF:
route_family: "acme-prod", route: "stream://orders/events/..."
route_family: "acme-prod", route: "stream://billing/invoices/..."

// Same family → same storage partition
// Different realms → just different route strings
```

---

## Configuration Example

```yaml
fitz:
  route_families:
    - name: acme-prod
      storage:
        midge_path: /data/acme-prod
      limits:
        max_connections: 1000
        max_storage_gb: 100
      auth:
        jwks_url: https://auth.acme.com/jwks
        
    - name: acme-dev
      storage:
        midge_path: /data/acme-dev
      limits:
        max_connections: 100
        max_storage_gb: 10
      auth:
        jwks_url: https://auth-dev.acme.com/jwks
```

---

## Key Takeaways

1. **Route Family = isolation boundary** (physical, durable, enforced)
2. **Realm = organizational grouping** (logical, ephemeral, convenience)
3. **Scheme = domain type** (determines which actor handles request)
4. **Midge sees families, not realms** (storage partition = family)
5. **Actors are per-family, not per-realm** (one StreamActor per family)
6. **Authorization has two levels** (family-level + route-level)

---

## Next Steps

Implementation priority (Phase 4 from roadmap):

1. Define `RouteFamily` type in `src/routing/family.rs`
2. Update `ParsedRoute` to include `route_family: RouteFamily`
3. Implement family resolution in transport layer
4. Update `RouterActor` to dispatch within family context
5. Update all domain actors to use family for Midge operations

---

**This is the canonical Fitz v2 routing architecture.**  
**All implementation must follow these rules.**

---

*Last Updated: December 11, 2025*
