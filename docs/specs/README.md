# Fitz v2 - Work in Progress Specifications

**Status:** Ready for Self-Driving Implementation  
**Last Updated:** December 11, 2025

This directory contains the complete architectural specifications for Fitz v2, a unified messaging runtime built on an actor model with clean separation between domains and infrastructure.

## 📂 Directory Structure

```
wip/
├── README.md                      ← You are here
├── ARCHITECTURE.md                ← ⭐ START HERE: Canonical architecture
├── ROUTING_ARCHITECTURE.md        ← Route Family vs Realm (detailed)
├── ROADMAP.md                     ← Implementation plan and status
├── DOMAIN_MODEL.md                ← Domain model summary
│
├── domains/                       ← 6 Messaging Primitives
│   ├── STREAM.md                  ← Durable: append-only logs
│   ├── QUEUE.md                   ← Durable: work queues
│   ├── KV.md                      ← Durable: key-value storage
│   ├── NOTICE.md                  ← Ephemeral: pub/sub
│   ├── RPC.md                     ← Ephemeral: request-reply
│   └── LEASE.md                   ← Ephemeral: distributed locks
│
└── infrastructure/                ← 7 System Components (Not Domains)
    ├── AUTH.md                    ← Authentication config (durable)
    ├── AUTH_RUNTIME.md            ← Runtime permission checking
    ├── CONTROL_PLANE.md           ← System config (durable)
    ├── CONTROL_RUNTIME.md         ← Live system state
    ├── ROUTING.md                 ← Route parsing and dispatch
    ├── REALMS.md                  ← Logical grouping
    ├── SESSIONS.md                ← Connection lifecycle
    ├── METRICS.md                 ← Observability
    └── DISCOVERY.md               ← System introspection
```

## 🎯 Start Here

1. **[ARCHITECTURE.md](ARCHITECTURE.md)** - **Read this first!** Complete architectural summary
2. **[ROUTING_ARCHITECTURE.md](ROUTING_ARCHITECTURE.md)** - Route Family vs Realm explained
3. **[ROADMAP.md](ROADMAP.md)** - Implementation phases and current status

## 🏗️ Key Concepts

### Domains = Messaging Primitives (6 total)

**Durable (Midge-backed):**
- stream, queue, kv

**Ephemeral:**
- notice, rpc, lease

### Infrastructure = System Components (7 total)

Not messaging primitives, but required for the system:
- auth, control-plane, routing, realms, sessions, metrics, transport

### Route Family = Physical Boundary

- Isolation/environment boundary
- Maps to Midge column families
- Examples: `acme-prod`, `customer-42`

### Realm = Logical Grouping

- NOT an isolation boundary (Route Family is the isolation boundary)
- Just organizational grouping
- Examples: `orders`, `auth`, `billing`

## 📋 Implementation Status

| Phase | Component | Status |
|-------|-----------|--------|
| ✅ 1 | Core Actor Runtime | Complete |
| ✅ 2 | TLV Protocol & Messages | Complete |
| 🚧 3 | Midge Bridge | Stubs Only |
| 📋 4 | Route Families & Routing | Next Up |
| 📋 5 | Sessions & Transport | Planned |
| 📋 6 | Domains | Planned |
| 📋 7 | Infrastructure | Planned |

See [ROADMAP.md](ROADMAP.md) for detailed breakdown.

## 🧭 Navigation Guide

### For Understanding Architecture
- Start with [ARCHITECTURE.md](ARCHITECTURE.md)
- Read [ROUTING_ARCHITECTURE.md](ROUTING_ARCHITECTURE.md) for routing details
- Check [ROADMAP.md](ROADMAP.md) for implementation order

### For Implementing Domains
- See `domains/` directory
- Each domain has complete specification
- Durable domains interact with MidgeActor
- Ephemeral domains are pure in-memory

### For Implementing Infrastructure
- See `infrastructure/` directory
- These are system components, not user-facing primitives
- Support domains but aren't domains themselves

### For Implementation Planning
- [ROADMAP.md](ROADMAP.md) - Phased plan
 

## 🎓 Terminology

| Term | Meaning |
|------|---------|
| **Domain** | Messaging primitive (stream, queue, kv, notice, rpc, lease) |
| **Persona** | Actor implementation (StreamActor, RouterActor, etc.) |
| **Route Family** | Physical isolation boundary (acme-prod, customer-42) |
| **Realm** | Logical grouping within a family (orders, auth) |
| **Scheme** | Domain type in route (stream://, queue://, etc.) |
| **Infrastructure** | System components that aren't domains |

## ✅ Architecture Validation

This structure ensures:
- ✅ Clear separation: domains vs infrastructure
- ✅ Domains = messaging primitives only
- ✅ Route Family = physical boundary
- ✅ Realm = logical grouping only
- ✅ Actor model throughout
- ✅ Clean durability boundary
- ✅ Self-driving implementation ready

---

**Legend:**
- ✅ Complete
- 🚧 In Progress  
- 📋 Planned

*Last Updated: December 11, 2025*

## Architecture Overview

### Hierarchy

Fitz v2 has a clear top-down hierarchy:

1. **Route Family** - The top-level isolation boundary and storage partition
   - Maps to Midge column families for durable domains
   - Examples: `acme-prod`, `acme-dev`, `saas-customer-42`
  - This is the **real isolation boundary**

2. **Route** - Universal addressing within a family
   - Format: `{scheme}://{realm}/{area}/{resource}/{operation}`
   - `scheme` = domain (stream, queue, kv, lease, rpc, notice, etc.)
  - `realm` = logical grouping (NOT an isolation boundary)
   - `area` = subsystem or category
   - `resource` = specific entity
   - `operation` = verb (optional)

### Domain Organization

Fitz v2 organizes all functionality into **six messaging domains** (stream, queue, kv, notice, rpc, lease) and a set of **infrastructure components** (auth, control-plane, routing, realms, sessions, metrics, discovery). See [DOMAIN_MODEL.md](DOMAIN_MODEL.md) and [ARCHITECTURE.md](ARCHITECTURE.md) for detailed behavior.

## Domain Interaction Patterns

### Message Flow
```
Route Family: acme-prod
    ↓
WebSocket → SessionActor → RouterActor → DomainActor(s) → MidgeActor (if durable)
                                              ↓
                                         Reply Path
```

### Storage Mapping
```
Route Family → Midge Column Family
  acme-prod.streams   ← All stream routes in acme-prod family
  acme-prod.queues    ← All queue routes in acme-prod family
  acme-prod.kv        ← All KV routes in acme-prod family
  acme-prod.metrics   ← All metrics in acme-prod family
```

### Authorization Flow
```
Request → RealmActor (extract identity) → AuthEvalActor (check permissions) → DomainActor
```

### Durability Boundary
```
All Actors (in-memory) → MidgeActor → Midge Storage Engine (disk)
```

## Implementation Status

- ✅ **Core Actor Runtime** - Trait, mailbox, scheduler, system, references
- ✅ **Message Types** - All 9 domain message definitions
- ✅ **TLV Protocol** - Frame encoding/decoding with streaming codec
- ✅ **Storage Bridge** - MidgeActor with all operation stubs
- ✅ **Bootstrap System** - FitzSystemBuilder for initialization
- 🚧 **Domain Actors** - Stubs exist in src/personas/*
- 🚧 **Transport Integration** - TCP/WebSocket connections
- 🚧 **Auth System** - JWT validation and permission evaluation
- 🚧 **Control Plane** - Configuration and runtime state management

## Specification Structure

Each domain specification follows this structure:

1. **Purpose** - What the domain does
2. **Route Format** - ftz://realm/area/resource/operation patterns
3. **Operations** - Complete list with request/response formats
4. **State Model** - What data the domain maintains
5. **Durability** - How state is persisted (if at all)
6. **Error Handling** - Error codes and recovery strategies
7. **Actor Implementation** - Message handling and state transitions
8. **Testing Strategy** - Unit tests, integration tests, benchmarks

## Implementation Roadmap

See [ROADMAP.md](ROADMAP.md) for the phased implementation plan.

## Design Principles

1. **Actor Isolation** - Pure message passing, no shared state
2. **Sync Domain Logic** - All domain handlers are synchronous
3. **Async Only at Edges** - WebSocket/TCP/Midge use async, core is sync
4. **Explicit Durability** - Clear boundary between persistent and ephemeral
5. **Type-Safe Messages** - Each domain has typed message enums
6. **Zero-Copy Where Possible** - Borrowed slices, pooled buffers
7. **Testable** - Each actor can be tested independently

## References

- [ARCHITECTURE.md](ARCHITECTURE.md) - Canonical system architecture
- [DOMAIN_MODEL.md](DOMAIN_MODEL.md) - Domain and persona model
- [ROUTING_ARCHITECTURE.md](ROUTING_ARCHITECTURE.md) - Route Family vs Realm
- [ROADMAP.md](ROADMAP.md) - Implementation status and plan

---

**Status**: Active development - self-driving implementation in progress
**Last Updated**: December 11, 2025
