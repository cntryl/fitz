# Realms Domain Specification

**Version:** 1.0  
**Status:** Specification  
**Durability:** Ephemeral (runtime state only)  
**Last Updated:** December 11, 2025  

---

## Overview

The Realms domain provides **logical grouping within a Route Family**, NOT multi-tenancy. A realm is a semantic namespace for organizing routes (like "auth", "orders", "billing"), but the actual tenant boundary is the **Route Family** (like `acme-prod`, `customer-42`).

### Critical Distinction

❌ **Realm is NOT:**
- A tenant boundary
- A storage partition
- An isolation boundary
- A security boundary

✅ **Realm IS:**
- A logical grouping of routes
- A semantic namespace
- An organizational convenience
- Optional (can use single realm per family)

### Example Hierarchy

```
Route Family: acme-prod        ← TENANT BOUNDARY (storage partition)
    ├─ realm: auth             ← Logical grouping
    │   ├─ area: tokens
    │   └─ area: users
    ├─ realm: orders           ← Logical grouping
    │   ├─ area: processing
    │   └─ area: fulfillment
    └─ realm: billing          ← Logical grouping
        ├─ area: invoices
        └─ area: payments
```

### Key Features

- **Runtime tracking**: Track active realms within a family
- **Resource counting**: Monitor streams/queues/keys per realm
- **Logical organization**: Group related routes
- **No enforcement**: Realm boundaries are semantic, not enforced

### Use Cases

- Organizing routes by business domain
- Grouping related resources
- Metrics and monitoring aggregation
- Organizational clarity in route structure

---

## Route Format

Realms are the first path component in routes:

```
{scheme}://{realm}/{area}/{resource}/{operation}
              ↑
           logical grouping
```

### Examples

```
Route Family: acme-prod
  ├─ stream://orders/events/created/append       realm=orders
  ├─ queue://orders/jobs/email/enqueue           realm=orders
  ├─ kv://auth/tokens/session:123/get            realm=auth
  └─ lease://billing/locks/reconcile/acquire     realm=billing
```

---

## Core Operations (Internal)

### 1. Register Realm

Track that a realm exists within a family.

**Internal Message:**
```rust
RealmMsg::Register {
    route_family: String,
    realm: String,
    reply_to: ActorRef<RealmReply>,
}
```

**State Created:**
```rust
struct RealmState {
    route_family: String,
    realm: String,
    created_at: Instant,
    active_streams: usize,
    active_queues: usize,
    active_leases: usize,
    kv_key_count: usize,
}
```

---

### 2. Increment Resource Count

Track resource creation within a realm.

**Internal Message:**
```rust
RealmMsg::IncrementResource {
    route_family: String,
    realm: String,
    resource_type: ResourceType,
}

enum ResourceType {
    Stream,
    Queue,
    Lease,
    KvKey,
}
```

---

### 3. Query Realm Stats

Get current resource counts for a realm.

**Internal Message:**
```rust
RealmMsg::GetStats {
    route_family: String,
    realm: String,
    reply_to: ActorRef<RealmReply>,
}
```

**Response:**
```rust
RealmReply::Stats {
    realm: String,
    active_streams: usize,
    active_queues: usize,
    active_leases: usize,
    kv_key_count: usize,
}
```

---

## Actor Implementation

### RealmActor State

```rust
pub struct RealmActor {
    /// Realms keyed by (route_family, realm)
    realms: DashMap<(String, String), RealmState>,
    
    /// Configuration (optional quotas per realm)
    realm_config: Arc<DashMap<String, RealmConfig>>,
}

struct RealmState {
    route_family: String,
    realm: String,
    created_at: Instant,
    active_streams: AtomicUsize,
    active_queues: AtomicUsize,
    active_leases: AtomicUsize,
    kv_key_count: AtomicUsize,
}

struct RealmConfig {
    max_streams: Option<usize>,
    max_queues: Option<usize>,
    max_kv_keys: Option<usize>,
}
```

---

### Message Handler

```rust
impl Actor for RealmActor {
    type Message = RealmMsg;
    
    fn on_message(&mut self, msg: Self::Message, ctx: &ActorContext<Self>) {
        match msg {
            RealmMsg::Register { route_family, realm, reply_to } => {
                let key = (route_family.clone(), realm.clone());
                
                self.realms.entry(key).or_insert_with(|| RealmState {
                    route_family,
                    realm: realm.clone(),
                    created_at: Instant::now(),
                    active_streams: AtomicUsize::new(0),
                    active_queues: AtomicUsize::new(0),
                    active_leases: AtomicUsize::new(0),
                    kv_key_count: AtomicUsize::new(0),
                });
                
                reply_to.send(RealmReply::Registered { realm });
            }
            
            RealmMsg::IncrementResource { route_family, realm, resource_type } => {
                let key = (route_family, realm);
                
                if let Some(state) = self.realms.get(&key) {
                    match resource_type {
                        ResourceType::Stream => {
                            state.active_streams.fetch_add(1, Ordering::Relaxed);
                        }
                        ResourceType::Queue => {
                            state.active_queues.fetch_add(1, Ordering::Relaxed);
                        }
                        ResourceType::Lease => {
                            state.active_leases.fetch_add(1, Ordering::Relaxed);
                        }
                        ResourceType::KvKey => {
                            state.kv_key_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            
            RealmMsg::GetStats { route_family, realm, reply_to } => {
                let key = (route_family, realm.clone());
                
                if let Some(state) = self.realms.get(&key) {
                    reply_to.send(RealmReply::Stats {
                        realm,
                        active_streams: state.active_streams.load(Ordering::Relaxed),
                        active_queues: state.active_queues.load(Ordering::Relaxed),
                        active_leases: state.active_leases.load(Ordering::Relaxed),
                        kv_key_count: state.kv_key_count.load(Ordering::Relaxed),
                    });
                } else {
                    reply_to.send(RealmReply::Error("realm_not_found".to_string()));
                }
            }
        }
    }
}
```

---

## Realm vs Route Family

### Route Family (Tenant Boundary)

```rust
// Different families = different tenants = different storage
Family: acme-prod
  - stream://orders/events/created     → midge: acme-prod.streams
  - kv://auth/config/flags              → midge: acme-prod.kv

Family: acme-dev
  - stream://orders/events/created     → midge: acme-dev.streams
  - kv://auth/config/flags              → midge: acme-dev.kv
```

### Realm (Logical Grouping)

```rust
// Different realms = same tenant = same storage family, just organized
Family: acme-prod
  - stream://orders/events/created     realm=orders (logical)
  - stream://billing/invoices/sent     realm=billing (logical)
  - stream://auth/logins/succeeded     realm=auth (logical)
```

All three go to the same `acme-prod.streams` column family in Midge.

---

## Common Realm Patterns

### Domain-Based Organization

```
Family: acme-prod
  ├─ realm: auth        (authentication/authorization)
  ├─ realm: orders      (order management)
  ├─ realm: billing     (invoicing and payments)
  ├─ realm: analytics   (metrics and reporting)
  └─ realm: chat        (messaging)
```

### Service-Based Organization

```
Family: company-prod
  ├─ realm: api         (public API)
  ├─ realm: admin       (admin console)
  ├─ realm: worker      (background jobs)
  └─ realm: internal    (inter-service communication)
```

### Single Realm (Simplest)

```
Family: startup-prod
  └─ realm: app         (everything in one realm)
```

---

## Performance Characteristics

### Latency

- **Realm lookup**: <100ns (DashMap)
- **Counter increment**: <50ns (atomic)
- **Stats query**: <500ns (read atomics)

### Memory

- **RealmState**: ~200 bytes per realm
- **Typical count**: 5-20 realms per family

### Scalability

- Lock-free atomic counters
- DashMap for concurrent access
- No blocking operations

---

## Testing Strategy

### Unit Tests

- Realm registration
- Resource counting
- Stats aggregation
- Concurrent updates

### Integration Tests

- Multi-realm routing
- Resource lifecycle tracking
- Stats accuracy

---

## References

- [Routing Domain](ROUTING.md)
- [Route Family Architecture](../README.md#hierarchy)
- [Control Configuration](../durable/CONTROL_CONFIG.md)
