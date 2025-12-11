# Fitz v2 Architectural Summary

**Version:** 2.0 (Canonical)  
**Status:** Authoritative  
**Last Updated:** December 11, 2025

*Domains, Personas, System Components, Routing, and Boundaries*

---

## 1️⃣ Domains (Messaging Concepts)

A **domain** in Fitz is a *messaging primitive* the system exposes to users — a conceptual communication or storage model.

These are the **six Fitz v2 messaging domains**:

### Durable Domains (backed by Midge)

1. **stream**
   - Append-only logs
   - Replay, subscribe, cursors

2. **queue**
   - Work queues
   - Ack, redelivery, visibility timeout

3. **kv**
   - Durable key-value pairs
   - Metadata storage, consumer offsets, config

### Ephemeral Domains

4. **notice**
   - Fire-and-forget pub/sub
   - Fast broadcast, no replay

5. **rpc**
   - Request/response
   - Synchronous messaging over Fitz

6. **lease**
   - Ephemeral distributed locks
   - Coordination primitives (not correctness guarantees)

**These are the only entities developers think of as "domains."**

Everything else is system infrastructure.

---

## 2️⃣ Personas (Actor Roles)

Each domain + subsystem has a corresponding **actor persona** — the runtime module that owns state and behavior.

### Domain Personas

| Domain | Persona | Durable? | Notes |
|--------|---------|----------|-------|
| stream | `StreamActor` | yes | Manages subscribers, fanout, cursor state; durability via MidgeActor |
| queue | `QueueActor` | yes | Scheduling, inflight state, retry logic |
| kv | *No direct actor* | yes | Calls go through `MidgeActor` or thin `KvActor` facade |
| notice | `NoticeActor` or handled inside `RealmActor` | no | Stateless fanout |
| rpc | `RpcActor` | no | Correlation IDs, timeouts, reply routing |
| lease | `LeaseActor` | no | TTL timers, exclusive ownership |

### Infrastructure Personas

| Subsystem | Persona | Purpose |
|-----------|---------|---------|
| control plane | `ControlPlaneActor` | Create/delete streams, queues; system introspection |
| auth | `AuthActor` | Token validation, RBAC, permissions |
| routing | `RouterActor` | Parse routes, map scheme → persona |
| realms | `RealmActor` | Subscriber membership, notice fanout, grouping |
| durability | `MidgeActor` | Bridge to Midge for stream/queue/kv |
| metrics | `MetricsActor` | Counters, histograms, system metrics |
| sessions | `SessionActor` | Per-connection state, backpressure, TLV decode |
| scheduler | `SystemActor` | Actor lifecycle, supervision |

---

## 3️⃣ System Components (Not Domains)

These are **required** but *not messaging primitives.*
They support the domains but aren't domains themselves.

### Authentication
- Token validation
- RBAC for routes
- Session principal context
- Backed by KV config

### Control Plane
- Create/delete/describe streams and queues
- Admin API
- Discovery (list families, realms, queues, consumers…)
- Backed by KV

### Routing Infrastructure
- Route parsing
- Scheme → persona dispatch
- Realm/area/resource extraction
- Route permissions

### Session Engine
- TCP/WS TLV decode
- Connection lifetime management
- Inbound and outbound flow control

### Metrics
- Internal counters (per actor/domain)
- Exporter → OTEL + optional durable Midge metrics

### Actor Runtime
- Mailboxes
- Scheduler
- Message passing
- Timers

### Transport Layer
- TLV framing (type/length/value)
- TCP
- WebSocket
- Multiplexer

**These are the "bones" of Fitz, not domains.**

---

## 4️⃣ Route Families (Physical Boundaries)

A **Route Family** is the top-level boundary in Fitz.

### Properties

- ✅ Maps to **Midge column families**
- ✅ Defines a **physical namespace**
- ✅ Is the **tenant / environment / partition boundary**
- ✅ Determines which storage partition (streams, queues, kv) an operation uses
- ✅ Separate actor sets per family (e.g., StreamActor per family)

### Examples

```
acme-prod
acme-dev
customer-42
internal-core
```

**Important:** All routes include a family, but the family is *not* part of the URI.
It's part of the envelope that wraps the route.

---

## 5️⃣ Route Structure

Every routed message follows:

```
{scheme}://{realm}/{area}/{resource}/{operation}
```

### Components

| Segment | Meaning |
|---------|---------|
| scheme | Messaging domain (`stream`, `queue`, `notice`, etc.) |
| realm | Purely logical namespace (NOT tenant) |
| area | Subsystem grouping |
| resource | The actual stream, queue, keyset, etc. |
| operation | append, enqueue, ack, acquire, invoke, publish, etc. |

**This is universal across all domains.**

---

## 6️⃣ Putting It All Together (Example)

Say a user publishes to a stream:

```
Route Family: acme-prod
Route: stream://billing/payments/events/append
```

The steps:

1. **SessionActor** receives a TLV frame
2. **RouterActor** parses URI → scheme = `stream`
3. **Control Plane** may authorize/validate this resource
4. **AuthActor** checks permissions
5. **StreamActor** in the correct Route Family handles fanout + cursor updates
6. **MidgeActor** persists the append
7. **MetricsActor** records stats
8. Stream subscribers receive updates via **RealmActor** + fanout

**Everything is cleanly layered and domain-driven.**

---

## 7️⃣ Fitz MVP Domain + Persona Set

### Domains (6 messaging primitives)

```
stream, queue, kv, notice, rpc, lease
```

### Personas (14 actor implementations)

```
Domain Actors:
  StreamActor, QueueActor, KvActor (or MidgeActor facade),
  NoticeActor, RpcActor, LeaseActor

Infrastructure Actors:
  RouterActor, RealmActor, AuthActor, ControlPlaneActor,
  MetricsActor, SessionActor, MidgeActor, SystemActor
```

### System Components (7 infrastructure subsystems)

```
auth, control-plane, routing, realms, metrics, transport, actor-runtime
```

### Physical Boundary

```
Route Family (maps to Midge column families)
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────┐
│                   Route Family (acme-prod)              │
│                    PHYSICAL BOUNDARY                     │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Domains (6):                                           │
│  ┌──────────┬──────────┬──────────┐                    │
│  │ stream   │ queue    │ kv       │  (durable)         │
│  └──────────┴──────────┴──────────┘                    │
│  ┌──────────┬──────────┬──────────┐                    │
│  │ notice   │ rpc      │ lease    │  (ephemeral)       │
│  └──────────┴──────────┴──────────┘                    │
│                                                          │
│  Infrastructure (7):                                    │
│  ┌──────────┬──────────┬──────────┬──────────┐        │
│  │ auth     │ control  │ routing  │ realms   │        │
│  ├──────────┼──────────┼──────────┼──────────┤        │
│  │ metrics  │sessions  │ actor-rt │          │        │
│  └──────────┴──────────┴──────────┴──────────┘        │
│                                                          │
│  Storage:                                               │
│  ┌─────────────────────────────────────────┐           │
│  │ Midge (acme-prod.streams/queues/kv)    │           │
│  └─────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

---

## Specification Organization

### Domain Specifications

Located in `wip/domains/`:

- `STREAM.md` - Stream domain specification
- `QUEUE.md` - Queue domain specification
- `KV.md` - KV domain specification
- `NOTICE.md` - Notice (pub/sub) domain specification
- `RPC.md` - RPC domain specification
- `LEASE.md` - Lease domain specification

### Infrastructure Specifications

Located in `wip/infrastructure/`:

- `AUTH.md` - Authentication and authorization
- `CONTROL_PLANE.md` - System management and discovery
- `ROUTING.md` - Route parsing and dispatch
- `REALMS.md` - Logical grouping and membership
- `METRICS.md` - System metrics and observability
- `SESSIONS.md` - Connection lifecycle
- `TRANSPORT.md` - TLV protocol and network

### Persona Implementations

Located in `src/personas/`:

- Domain actors: `stream_actor.rs`, `queue_actor.rs`, `lease_actor.rs`, etc.
- Infrastructure actors: `router_actor.rs`, `realm_actor.rs`, `auth_actor.rs`, etc.

---

## Key Architectural Principles

1. **Domains are messaging primitives only** - stream, queue, kv, notice, rpc, lease
2. **Personas are actor implementations** - One actor type per domain + infrastructure
3. **Route Family is the physical boundary** - Maps to Midge, defines tenant isolation
4. **Realm is logical grouping only** - No physical storage impact
5. **System components support domains** - Auth, control plane, routing, etc. are not domains
6. **Clean durability boundary** - Only stream/queue/kv touch Midge
7. **Actor model everywhere** - Pure message passing, no shared state
8. **Sync domain logic** - Async only at transport/storage edges

---

## Implementation Status

| Component | Status | Progress |
|-----------|--------|----------|
| Actor Runtime | ✅ Complete | 100% |
| TLV Protocol | ✅ Complete | 100% |
| Midge Bridge | 🚧 Stubs | 50% |
| Route Families | 📋 Planned | 0% |
| Routing | 📋 Planned | 0% |
| Sessions | 📋 Planned | 0% |
| Stream Domain | 📋 Planned | 0% |
| Queue Domain | 📋 Planned | 0% |
| KV Domain | 📋 Planned | 0% |
| Notice Domain | 📋 Planned | 0% |
| RPC Domain | 📋 Planned | 0% |
| Lease Domain | 📋 Planned | 0% |
| Auth | 📋 Planned | 0% |
| Control Plane | 📋 Planned | 0% |

See [ROADMAP.md](ROADMAP.md) for detailed implementation plan.

---

## References

- [ROUTING_ARCHITECTURE.md](ROUTING_ARCHITECTURE.md) - Route Family vs Realm (canonical)
- [ROADMAP.md](ROADMAP.md) - Implementation phases
- [Domain Specifications](domains/) - Per-domain specs
- [Infrastructure Specifications](infrastructure/) - System component specs

---

**This is the authoritative Fitz v2 architecture map.**

*Last Updated: December 11, 2025*
