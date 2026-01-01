# Fitz v2 Architecture

**Version:** 2.0 (Canonical)  
**Status:** Authoritative  
**Last Updated:** January 1, 2026

*Actor Model | Clean Boundaries | Message-Driven*

---

## Executive Summary

Fitz v2 is a **pure actor model** messaging platform built on three core principles:

1. **Actor Model Everywhere** - Every subsystem is an actor with its own mailbox, no shared state
2. **Clean Durability Boundary** - Only 3 things persist (streams, queues, kv via Midge)
3. **Message Passing Only** - All coordination via synchronous message passing, async only at edges

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Transport Layer                       │
│         (TCP, WebSocket, TLV Framing - ASYNC)               │
└──────────────┬─────────────────────────────────────────────┘
               │ TLV Frames
               ↓
┌─────────────────────────────────────────────────────────────┐
│                     API Layer (ASYNC)                        │
│         src/api/ - HTTP, WebSocket, CLI                     │
└──────────────┬─────────────────────────────────────────────┘
               │ Parsed Messages
               ↓
┌─────────────────────────────────────────────────────────────┐
│                  Actor Runtime (SYNC)                        │
│   src/runtime/ - Mailbox, Scheduler, Supervision, Context   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Domain Actors (SYNC):                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ src/domains/notification/ - Pub/Sub (ephemeral)      │  │
│  │ src/domains/stream/ - Append logs (durable)          │  │
│  │ src/domains/queue/ - Work queues (NOT IMPLEMENTED)   │  │
│  │ src/domains/rpc/ - Request/response (ephemeral)      │  │
│  │ src/domains/lease/ - Distributed locks (ephemeral)   │  │
│  │ src/domains/kv/ - Key-value (durable)                │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  Control Actors (SYNC):                                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ src/control/node/ - Node management                  │  │
│  │ src/control/cluster/ - Cluster coordination          │  │
│  │ src/control/health/ - Health monitoring              │  │
│  │ src/control/metrics/ - Metrics collection            │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  Infrastructure (SYNC):                                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ src/transport/router/ - Route dispatch               │  │
│  │ src/security/identity/ - Authentication              │  │
│  │ src/security/policy/ - Authorization                 │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────┬─────────────────────────────────────────────┘
               │ Storage Ops
               ↓
┌─────────────────────────────────────────────────────────────┐
│                  Storage Layer (ASYNC)                       │
│        src/storage/engine/ - Midge adapter ONLY             │
└──────────────┬─────────────────────────────────────────────┘
               │
               ↓
         [ Midge Storage ]
         - Streams (append-only logs)
         - Queues (NOT IMPLEMENTED)
         - KV (key-value pairs)
```

---

## 1. Core Concepts

### 1.1 Domains (Messaging Primitives)

A **domain** is a messaging primitive exposed to users. Fitz provides **6 domains** (5 implemented):

#### Durable Domains (backed by Midge)

| Domain | Status | Module | Description |
|--------|--------|--------|-------------|
| **stream** | ✅ Implemented | `src/domains/stream/` | Append-only logs with replay, subscribe, cursors |
| **queue** | ❌ Not Implemented | - | Work queues with ack, redelivery, visibility timeout |
| **kv** | ✅ Implemented | `src/domains/kv/` | Durable key-value pairs for metadata, offsets, config |

#### Ephemeral Domains

| Domain | Status | Module | Description |
|--------|--------|--------|-------------|
| **notification** | ✅ Implemented | `src/domains/notification/` | Fire-and-forget pub/sub (was "notice") |
| **rpc** | ✅ Implemented | `src/domains/rpc/` | Request/response with correlation IDs |
| **lease** | ✅ Implemented | `src/domains/lease/` | Distributed locks for coordination |

**Note:** Queue domain is planned but not implemented. All others are functional.

### 1.2 Route Families (Physical Boundaries)

A **Route Family** is the top-level isolation boundary in Fitz.

**Properties:**
- Maps to Midge column families (physical storage partition)
- Defines isolation/environment/partition boundary
- Determines which storage partition (streams, kv) an operation uses
- Separate actor instances per family

**Examples:** `acme-prod`, `acme-dev`, `customer-42`, `internal-core`

**Important:** Route families are part of the message envelope, NOT the URI.

### 1.3 Route Structure

Every routed message follows:

```
{scheme}://{realm}/{area}/{resource}/{operation}
```

| Segment | Meaning | Example |
|---------|---------|---------|
| scheme | Messaging domain | `stream`, `notification`, `rpc`, `lease`, `kv` |
| realm | Logical grouping (NOT isolation boundary) | `billing` |
| area | Subsystem grouping | `payments` |
| resource | Actual stream/topic/key | `events` |
| operation | Action to perform | `append`, `publish`, `invoke`, `get` |

**Full Example:**
```
Route Family: acme-prod
Route: stream://billing/payments/events/append
```

**Critical:** Realm is purely logical. Route Family is the physical boundary.

---

## 2. Source Code Organization

```
src/
├── runtime/              # Actor runtime + execution model (SYNC)
│   ├── actor/           # Actor trait, lifecycle, message handling
│   ├── mailbox/         # Bounded message queues, backpressure
│   ├── scheduler/       # Cooperative actor scheduling
│   ├── supervision/     # Fault tolerance, restart strategies
│   └── context/         # Actor execution context, timers
│
├── transport/           # Message transport layer (MIXED)
│   ├── envelope/        # Message envelope structure, metadata
│   ├── router/          # Route parsing, wildcard matching, dispatch
│   ├── codecs/          # TLV encoding/decoding
│   └── backpressure/    # Flow control mechanisms
│
├── storage/             # Thin abstraction over Midge (ASYNC)
│   ├── engine/          # Midge adapter - ONLY place that touches storage
│   ├── state/           # Actor state persistence helpers
│   └── checkpoints/     # State checkpointing and recovery
│
├── security/            # Authentication and authorization (SYNC)
│   ├── identity/        # Token validation, principal extraction
│   ├── claims/          # Claims-based authorization
│   └── policy/          # Policy evaluation and enforcement
│
├── domains/             # Domain-specific actors (SYNC)
│   ├── notification/    # Pub/sub domain (ephemeral)
│   │   ├── actor/       # NotificationActor implementation
│   │   ├── api/         # Public API surface
│   │   └── protocol/    # Wire protocol definitions
│   ├── rpc/             # RPC domain (ephemeral)
│   │   ├── actor/       # RpcActor with correlation tracking
│   │   ├── api/         # RPC API
│   │   └── protocol/    # RPC protocol
│   ├── lease/           # Lease domain (ephemeral)
│   │   ├── actor/       # LeaseActor with TTL timers
│   │   ├── api/         # Lease API
│   │   └── protocol/    # Lease protocol
│   ├── kv/              # Key-value domain (durable)
│   │   ├── actor/       # KvActor (thin facade over storage)
│   │   ├── api/         # KV API
│   │   └── protocol/    # KV protocol
│   └── stream/          # Stream domain (durable)
│       ├── actor/       # StreamActor with fanout logic
│       ├── api/         # Stream API
│       └── protocol/    # Stream protocol
│
├── control/             # System-owned actors (SYNC)
│   ├── node/            # Node lifecycle, local resource management
│   ├── cluster/         # Cluster membership, gossip, coordination
│   ├── health/          # Health checks, probes, status reporting
│   └── metrics/         # Metrics aggregation and export
│
├── api/                 # Edge API surfaces (ASYNC)
│   ├── http/            # HTTP REST endpoints
│   ├── ws/              # WebSocket connection handling
│   └── cli/             # CLI commands and interface
│
├── config/              # Configuration management
│   ├── schema/          # Configuration schema definitions
│   └── loader/          # Config loading and validation
│
├── errors/              # Error types and handling
├── utils/               # Utility functions and helpers
└── prelude/             # Common imports and convenience traits
```

---

## 3. Actor Model Fundamentals

### 3.1 Core Principles

1. **Every subsystem is an actor** - Has its own mailbox, owns its state
2. **No shared mutable state** - All coordination via message passing
3. **Synchronous actors** - Actors process messages synchronously
4. **Async at edges only** - Only transport (TCP/WS) and storage (Midge) are async
5. **Single-threaded per actor** - No locks on hot paths
6. **Cooperative scheduling** - Actors yield after processing messages

### 3.2 Message Flow Example: Stream Append

```
1. [Client] → TLV frame over WebSocket
             ↓
2. [api/ws] → Parse frame (ASYNC)
             ↓
3. [Actor Runtime] → Dispatch to StreamActor mailbox (SYNC)
             ↓
4. [domains/stream/actor] → Process append, update subscribers (SYNC)
             ↓
5. [storage/engine] → Persist to Midge (ASYNC)
             ↓
6. [domains/stream/actor] → Fanout to subscribers (SYNC)
             ↓
7. [transport/router] → Route to subscriber mailboxes (SYNC)
             ↓
8. [api/ws] → Encode TLV frames (ASYNC)
             ↓
9. [Client] ← Receive stream data
```

**Key Observation:** Only steps 2, 5, and 8 are async. Everything else is synchronous message passing.

### 3.3 Durability Boundary

**ONLY these persist data:**

- ✅ `src/domains/stream/` → via `src/storage/engine/` → Midge (append-only logs)
- ✅ `src/domains/kv/` → via `src/storage/engine/` → Midge (key-value)
- ❌ Queue domain (not implemented)

**Everything else is ephemeral:**

- ❌ Routing tables (`transport/router/`)
- ❌ RPC state (`domains/rpc/`)
- ❌ Leases (`domains/lease/`)
- ❌ Subscriptions (`domains/notification/`, `domains/stream/`)
- ❌ Metrics (`control/metrics/`)

**Critical Rule:** Only `storage/engine/` touches Midge. No other module may perform direct storage I/O.

---

## 4. Transport Layer: TLV Protocol

All network communication uses **TLV (Type-Length-Value)** framing:

```
┌─────────────┬──────────────┬───────────────────┐
│ Type (u16)  │ Len (u32)    │ Value (bytes)     │
│  2 bytes    │  4 bytes     │  variable         │
└─────────────┴──────────────┴───────────────────┘
```

Implemented in `src/transport/codecs/`.

**Example Frame Types:**
- `0x0100` - Stream append
- `0x0200` - Queue enqueue (not implemented)
- `0x0300` - RPC invoke
- `0x0400` - Lease acquire
- `0x0500` - KV put
- `0x0600` - Notification publish

**Transports:**
- `src/api/http/` - HTTP REST (JSON over HTTP)
- `src/api/ws/` - WebSocket (TLV binary)
- `src/api/cli/` - CLI (local REPL)

---

## 5. Security Model

Located in `src/security/`:

### 5.1 Identity (`security/identity/`)
- Token validation (JWT, API keys)
- Principal extraction
- Session establishment

### 5.2 Claims (`security/claims/`)
- Claims-based authorization
- Role-based access control (RBAC)
- Attribute-based access control (ABAC)

### 5.3 Policy (`security/policy/`)
- Policy evaluation engine
- Route-level permissions
- Domain-specific authorization rules

**Example Authorization Flow:**
```
1. Client sends request with token
2. identity/ validates token → Principal
3. claims/ extracts claims from Principal
4. policy/ evaluates: can Principal perform operation on route?
5. If authorized → dispatch to domain actor
6. If denied → return error
```

---

## 6. Control Plane

Located in `src/control/`:

### 6.1 Node (`control/node/`)
- Local node lifecycle management
- Resource monitoring (CPU, memory, connections)
- Graceful shutdown coordination

### 6.2 Cluster (`control/cluster/`)
- Cluster membership (gossip, heartbeats)
- Leader election (for control operations)
- Node discovery and health propagation

### 6.3 Health (`control/health/`)
- Health check endpoints
- Readiness and liveness probes
- Dependency health tracking

### 6.4 Metrics (`control/metrics/`)
- Metrics collection (counters, histograms, gauges)
- Per-domain and per-actor metrics
- Export to OTEL/Prometheus

**Control actors are system-owned and not exposed to users.**

---

## 7. Domain Actor Details

### 7.1 Notification (`domains/notification/`)

**Purpose:** Fire-and-forget pub/sub (ephemeral)

**Key Operations:**
- `subscribe(realm, topic)` - Subscribe to topic within realm
- `unsubscribe(realm, topic)` - Unsubscribe
- `publish(realm, topic, payload)` - Broadcast to all subscribers

**State:**
- Subscriber registry (in-memory)
- Topic → [subscriber IDs]

**Durability:** None (ephemeral)

### 7.2 Stream (`domains/stream/`)

**Purpose:** Append-only logs with replay and cursors (durable)

**Key Operations:**
- `append(stream, payload)` - Append entry to log
- `subscribe(stream, cursor)` - Subscribe from cursor position
- `read(stream, start, end)` - Read range of entries

**State:**
- Subscriber registry (in-memory)
- Stream → [subscriber IDs + cursors]

**Durability:** Midge (via `storage/engine/`)

### 7.3 RPC (`domains/rpc/`)

**Purpose:** Request/response with correlation (ephemeral)

**Key Operations:**
- `invoke(route, payload, timeout)` - Send request, wait for reply
- `register(route)` - Register as RPC handler
- `reply(correlation_id, payload)` - Send reply

**State:**
- Correlation ID → reply channel (in-memory)
- Route → handler actor

**Durability:** None (ephemeral)

### 7.4 Lease (`domains/lease/`)

**Purpose:** Distributed locks with TTL (ephemeral)

**Key Operations:**
- `acquire(lease_id, ttl)` - Acquire exclusive lock
- `renew(lease_id, ttl)` - Extend lease
- `release(lease_id)` - Release lock

**State:**
- Lease ID → (owner, expiration timestamp)
- TTL timers (via `runtime/context/`)

**Durability:** None (ephemeral)

### 7.5 KV (`domains/kv/`)

**Purpose:** Durable key-value storage (durable)

**Key Operations:**
- `put(key, value)` - Store key-value pair
- `get(key)` - Retrieve value
- `delete(key)` - Remove key

**State:**
- Minimal (thin facade)

**Durability:** Midge (via `storage/engine/`)

### 7.6 Queue (NOT IMPLEMENTED)

**Purpose:** Work queues with ack/redelivery (durable)

**Status:** Planned but not implemented in current architecture.

---

## 8. Key Architectural Principles

1. **Domains are messaging primitives only** - stream, kv, notification, rpc, lease (+ queue planned)
2. **Actor model everywhere** - Every subsystem is an actor, no shared state
3. **Sync actors, async edges** - Actors are synchronous, only transport/storage are async
4. **Route Family is physical boundary** - Maps to Midge, defines isolation
5. **Realm is logical grouping only** - No physical storage impact
6. **Clean durability boundary** - Only stream/kv touch Midge (via storage/engine)
7. **Single storage adapter** - Only `storage/engine/` touches Midge
8. **Message passing only** - No locks, no shared state, no channels

---

## 9. Implementation Status

| Component | Location | Status |
|-----------|----------|--------|
| Actor Runtime | `src/runtime/` | 🚧 Stubbed |
| Transport (TLV) | `src/transport/codecs/` | 🚧 Stubbed |
| Storage (Midge) | `src/storage/engine/` | 🚧 Stubbed |
| Security | `src/security/` | 🚧 Stubbed |
| Notification | `src/domains/notification/` | 🚧 Stubbed |
| Stream | `src/domains/stream/` | 🚧 Stubbed |
| RPC | `src/domains/rpc/` | 🚧 Stubbed |
| Lease | `src/domains/lease/` | 🚧 Stubbed |
| KV | `src/domains/kv/` | 🚧 Stubbed |
| Queue | - | ❌ Not Implemented |
| Control Plane | `src/control/` | 🚧 Stubbed |
| API Layer | `src/api/` | 🚧 Stubbed |

**All modules are stubbed and ready for implementation.**

---

## 10. Migration from v1

| Aspect | v1 (Old) | v2 (New) |
|--------|----------|----------|
| Architecture | Async handlers + locks | Pure actor model |
| Concurrency | Tokio tasks + Arc\<RwLock\> | Message passing only |
| State | Shared via locks | Each actor owns state |
| Durability | Mixed (unclear) | Clean (3 durable domains) |
| Transport | Multiple protocols | Unified TLV |
| Storage | Multiple callers | Only storage/engine |
| Scheduling | Tokio async | Actor scheduler |
| Domains | 6 domains | 5 implemented (queue missing) |

---

## 11. Next Steps

1. **Implement actor runtime** (`src/runtime/`)
   - Mailbox with backpressure
   - Cooperative scheduler
   - Supervision trees

2. **Implement TLV codec** (`src/transport/codecs/`)
   - Frame encoding/decoding
   - Error handling
   - Streaming support

3. **Implement Midge adapter** (`src/storage/engine/`)
   - Async bridge to Midge
   - Batch operations
   - Error propagation

4. **Implement domain actors** (one at a time)
   - Start with notification (simplest)
   - Then stream, rpc, lease, kv
   - Queue last (most complex)

5. **Implement API layer** (`src/api/`)
   - WebSocket transport
   - HTTP REST (optional)
   - CLI interface

6. **Implement control plane** (`src/control/`)
   - Node management
   - Cluster coordination
   - Metrics collection

---

## 12. References

- [ROADMAP.md](ROADMAP.md) - Implementation phases and timeline
- [ROUTING_ARCHITECTURE.md](ROUTING_ARCHITECTURE.md) - Route Family vs Realm details
- [Domain Specifications](domains/) - Per-domain specs (stream, queue, kv, notice, rpc, lease)
- [Infrastructure Specifications](infrastructure/) - Auth, control plane, routing, etc.
- [Test Guidelines](../docs/dev/test_guidelines.md) - Testing standards
- [Benchmark Guidelines](../docs/dev/bench_guidelines.md) - Performance benchmarking

---

**This is the authoritative Fitz v2 architecture specification.**

*Last Updated: January 1, 2026*
