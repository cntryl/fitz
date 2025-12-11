# Fitz Broker - System Overview

**Version:** 0.5  
**Status:** Implementation in Progress  
**Last Updated:** November 15, 2025  

---

## Purpose and Scope

Fitz is a transport-agnostic, multi-tenant message broker providing unified **notices**, **streams**, **queues**, **RPC**, **inboxes**, and **control-plane coordination** via typed routes over WebSocket with binary TLV framing.

### Key Features

- **Transport Agnostic**: WebSocket today, extensible to QUIC/gRPC/NATS
- **Multi-Tenant**: JWT-based realm isolation with route permissions
- **Unified Routing**: Single route space across all message patterns
- **Durable Storage**: Pluggable backends (Local, Azure, AWS)
- **Crash Safe**: WAL + deterministic recovery
- **Observable**: Rich metrics and control-plane integration

---

## System Architecture

```
          +-------------+
          |   Clients   |
          +------+------+            (WebSocket binary frames)
                 |
                 v
        +--------+--------+
        |   Transport     |   ← abstracts framing
        +--------+--------+
                 |
                 v
        +--------+--------+
        |   Router Core   |   ← route lookup, acks, fanout, backpressure
        +--------+--------+
          |    |      |
          |    |      +-------------------+
          |    |                          |
          v    v                          v
   +--------+  +---------+        +---------------+
   | Streams |  | Queues |        | Notice Router |
   +--------+  +---------+        +---------------+
          |
          v
     +------------------+
     | Storage Provider |  ← pluggable (local/cloud)
     +------------------+

            |
            v
     +---------------+
     | Control Plane |
     +---------------+
```

### Core Components

| Component | Responsibility |
|-----------|----------------|
| **Transport** | WebSocket binding, frame I/O, multiplexing |
| **Router Core** | Route parsing, ACLs, delivery, backpressure |
| **Domain Handlers** | Per-scheme business logic (queue, stream, etc.) |
| **Storage Provider** | WAL + persistence backends |
| **Control Plane** | Heartbeats, metrics, configuration |

---

## Async/Sync Architecture

Fitz treats **async as a boundary concern**, not an internal design constraint. The system performs best when **external I/O is async**, but **core domain logic stays fully synchronous**.

### Architectural Split

#### **Async at the Edges**

Async is used only where it buys real wins:

* Network I/O (WS, TCP, HTTP)
* Storage I/O (flush futures, WAL fsync, cloud ops)
* Timers, heartbeats
* Background tasks that await OS operations

This keeps the broker responsive and cheap under load.

#### **Sync in the Hotpath**

All routing, parsing, domain dispatch, lease/queue logic, and KV interactions run as **pure synchronous code**:

* No `.await` inside handlers
* No async state machines in hot loops
* No locking-based reactors

This guarantees predictable scheduling, lower latency, and stable tail performance.

### Why Not Async All the Way Down?

In .NET, "async all the way down" removes hidden threadpool blocking. In Fitz, the model is inverted:

* Rust async **adds overhead** inside tight paths (state machines, pollers, wakeups).
* Fitz hotpaths are CPU-bound and memory-bound, not I/O-bound.
* Sync code runs immediately on the executor without context switching.

Async is still used for I/O, but once a message reaches the domain layer, it should already be in memory. The domain logic is pure compute; async would only slow it down.

### Domain Handler Rules

**Before the handler:**

* Perform all async work (fetch remote state, load KV rows, read WAL, etc.)
* Prepare a full `DomainContext`
* Parse route, TLV, and metadata

**Inside the handler:**

* Pure synchronous logic
* Deterministic state transitions
* No `.await`
* No external calls

**After the handler:**

* Publish followup events (async)
* Persist results (async)
* Trigger notifications (async)

By the time the handler runs, everything it needs should already be in memory and ready.

### Benefits

* **Lower latency** (no async state-machine overhead)
* **Stronger determinism** in core logic
* **Cleaner testing** (everything is synchronous and pure)
* **More modular** separation between domain logic and I/O orchestration
* **Higher throughput** because executor threads aren't constantly parked/unparked

### Mental Model

Think of Fitz like this:

* **Async → transport & plumbing**
* **Sync → domain & semantics**

You only "await" when talking to the outside world.
Inside Fitz, logic is synchronous and fast, like Go or .NET ValueTasks but without allocs or hidden blocking.

---

## Route Semantics

Fitz uses a unified route space with scheme-based dispatch:

| Scheme | Example | Persistence | Delivery | Use Case |
|---|---|---|---|---|
| `notice://` | `notice://acme/alerts/system` | None | Best-effort broadcast | Real-time notifications |
| `stream://` | `stream://acme/orders/events` | Durable | Ordered, replayable | Event sourcing, audit |
| `queue://` | `queue://acme/jobs/thumbnail` | Durable | At-least-once via leases | Task distribution |
| `rpc://` | `rpc://acme/auth/verify` | Ephemeral | Req/Rep | Service calls |
| `inbox://` | `inbox://client/abcd1234` | Ephemeral | Direct | Reply channels |
| `control://` | `control://broker/heartbeat` | Ephemeral | System | Management |

### Route Format
```
{scheme}://{realm}/{area}/{resource}[/{operation}]
```

- **realm**: Tenant identifier (JWT claim)
- **area**: Functional subsystem
- **resource**: Entity or queue name
- **operation**: Action (optional)

---

## Wire Protocol (TLV Framing)

Fitz uses binary TLV (Type-Length-Value) frames over WebSocket:

```
+----------------+------------+------------+----------------+
| Length (u32)   | Type (u8)  | Flags (u8) | Channel (u32)  |
+----------------+------------+------------+----------------+
|     TLV Payload (repeating [Tag u8][Len u16][Value...])    |
+------------------------------------------------------------+
```

### Frame Types
- `CONN_OPEN/CLOSE`: Connection lifecycle
- `ACK`: Acknowledgments
- `REG`: Subscribe/Unsubscribe
- `PUB`: Publish messages
- `DAT`: Deliveries
- `REQ`: Queue/stream operations
- `ERR`: Error responses

### Common TLV Tags
- `TAG_ROUTE (0x20)`: Route string
- `TAG_BODY (0x22)`: Message payload
- `TAG_ID (0x21)`: Correlation/message ID
- `TAG_TOKEN (0x10)`: Auth token
- `TAG_ERR_CODE/Msg`: Error reporting

---

## Security Model

### Authentication
- JWT tokens in `CONN_OPEN` or headers
- Signature validation via JWKS
- `realm` claim required for tenant routes

### Authorization
- Route-based permissions in JWT claims
- Examples: `pub:stream://acme/*`, `read:queue://acme/jobs/*`
- Broker enforces per-frame at dispatch

### Multi-Tenant Isolation
- Routes prefixed by realm
- Storage partitioned by tenant
- Cross-tenant access blocked

---

## Storage Architecture

### Provider Abstraction
```rust
trait StreamStore {
    async fn append(&mut self, route: &str, event: StreamEvent) -> Result<u64, StoreError>;
    async fn read(&self, route: &str, from_seq: u64, limit: usize) -> Result<Vec<StreamEvent>, StoreError>;
    async fn peek(&self, route: &str) -> Result<Option<StreamEvent>, StoreError>;
    async fn consume(&self, prefix: &str, from_seq: u64, limit: usize) -> Result<Vec<StreamEvent>, StoreError>;
}

trait QueueStore {
    async fn enqueue(&mut self, route: &str, message: QueueMessage) -> Result<String, StoreError>;
    async fn lease(&mut self, route: &str, visibility_ms: u32) -> Result<Vec<QueueMessage>, StoreError>;
    async fn complete(&mut self, route: &str, id: &str, token: &str) -> Result<(), StoreError>;
}
```

### Supported Backends
- **Local**: Embedded KV store + WAL + SST/manifest
- **Azure**: Blob + Table storage
- **AWS**: Kinesis + S3

### WAL & Recovery
- Append-only segments with CRC
- Crash-safe replay on startup
- Deterministic index reconstruction

---

## Control Plane Integration

### Registration
Broker announces capabilities on startup:
```json
{
  "broker_id": "node-01",
  "realm_support": ["acme", "test"],
  "stream_backend": "azure",
  "queue_backend": "kv",
  "capabilities": ["peek", "consume_prefix"]
}
```

### Heartbeats
Periodic health signals (default 30s):
```json
{
  "uptime": 3600,
  "clients": 42,
  "streams_appended": 1500,
  "queue_depth": 25,
  "errors_last_min": 0
}
```

### Configuration
Runtime config updates via `control://config/update`

---

## Operational Model

### Configuration
```yaml
version: 1
broker:
  listen: ":8080"
  realm: "dev1"
  control_plane: "wss://control.dev1.mesh.local"

storage:
  stream_backend: "azure"
  queue_backend: "kv"

limits:
  max_payload_bytes: 1048576
  max_inflight: 512

security:
  jwks_url: "https://auth.dev1/jwks.json"
```

### Startup Sequence
1. Load config and manifests
2. Initialize storage providers
3. WAL recovery and index rebuild
4. Register with control plane
5. Accept client connections

### Metrics
- `broker_clients_total{realm}`
- `stream_appends_total{route}`
- `queue_lease_duration_seconds{route}`
- `transport_frames_total{type}`

---

## Implementation Status

### ✅ Completed
- Core routing and subscription system
- WebSocket transport with multiplexing
- TLV framing and parsing
- JWT authentication framework
- Local storage backend (MemStore)
- Basic domain handler stubs

### 🚧 In Progress
- Domain implementations (Queue, Stream, RPC, etc.)
- Cloud storage backends
- Control plane integration
- Comprehensive test coverage

### 📋 Next Priorities
1. Complete queue domain implementation
2. Stream domain with transaction semantics
3. RPC with reply routing
4. Control plane heartbeat/metrics
5. Cloud storage adapters

---

## Development Guidelines

### Testing
- **Naming**: `should_<outcome>_given_<context>_when_<action>`
- **Structure**: Arrange/Act/Assert (AAA)
- **Coverage**: Unit tests for all domains
- **Meta-tests**: Guideline compliance validation

### Code Organization
- `src/core/`: Domain handlers and engine
- `src/protocol/`: Framing and TLV utilities
- `src/transport/`: Transport implementations
- `src/storage/`: Backend adapters
- `tests/`: Integration and unit tests

---

## Glossary

- **Realm**: Tenant or namespace identifier
- **Route**: URI identifying a resource or destination
- **TLV**: Type-Length-Value binary encoding
- **WAL**: Write-ahead log for durability
- **Lease**: Timed claim on a queue message
- **Watermark**: Highest contiguous committed sequence
- **Domain**: Per-scheme handler (queue, stream, etc.)

---

*See domain-specific specifications for detailed implementation details.*</content>
<parameter name="filePath">d:\repos\cntryl\fitz\docs\OVERVIEW.md