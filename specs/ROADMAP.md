# Fitz v2 Implementation Roadmap

**Version:** 1.0  
**Status:** Active Development  
**Last Updated:** December 11, 2025  

---

## Architecture Foundation

### Route Family = Isolation Boundary

Fitz v2 uses **Route Family** as the top-level isolation and storage partition:

```
Route Family (e.g., acme-prod)
    ↓
Route: {scheme}://{realm}/{area}/{resource}/{operation}
    ├─ scheme: domain type (stream, queue, kv, etc.)
   ├─ realm: logical grouping (NOT an isolation boundary!)
    ├─ area: subsystem
    ├─ resource: entity
    └─ operation: verb
```

**Storage Mapping:**
- `acme-prod.streams` → All stream routes in acme-prod family
- `acme-prod.queues` → All queue routes in acme-prod family
- `acme-prod.kv` → All KV routes in acme-prod family

---

## Phase 1: Core Actor Runtime ✅

**Status:** Complete  
**Duration:** Completed  

### Deliverables

- [x] Actor trait and message passing
- [x] ActorRef with manual Clone (no M: Clone bound)
- [x] Bounded mailbox (crossbeam-channel)
- [x] Scheduler (cooperative, synchronous)
- [x] ActorSystem and ActorContext
- [x] Timer support
- [x] Error handling

### Files

- `src/actor/mod.rs` - Core traits
- `src/actor/actor_ref.rs` - Message passing
- `src/actor/mailbox.rs` - Bounded queues
- `src/actor/scheduler.rs` - Actor execution
- `src/actor/system.rs` - System lifecycle
- `src/actor/timers.rs` - Scheduled messages

### Validation

- ✅ Builds successfully
- ✅ Examples execute
- ✅ Zero clippy warnings

---

## Phase 2: Message Types & Protocol ✅

**Status:** Complete  
**Duration:** Completed  

### Deliverables

- [x] TLV protocol implementation
- [x] Frame encoding/decoding
- [x] Streaming codec
- [x] 9 domain message types
- [x] Reply type enums

### Files

- `src/transport/protocol.rs` - TLV implementation
- `src/messages/*.rs` - 9 message type definitions

### Validation

- ✅ TLV encode/decode tests pass
- ✅ Streaming codec handles partial frames
- ✅ All message types defined

---

## Phase 3: Storage Bridge ✅

**Status:** Complete (Stubs)  
**Duration:** Completed  

### Deliverables

- [x] MidgeActor with placeholder implementations
- [x] All durable operation stubs
- [x] Message handler skeleton

### Files

- `src/storage/midge_actor.rs`

### Next Steps

- [ ] Integrate real Midge API
- [ ] Implement AppendStream
- [ ] Implement ReadStream
- [ ] Implement Enqueue/Dequeue
- [ ] Implement KV operations

---

## Phase 4: Route Family & Routing 🚧

**Status:** In Progress  
**Priority:** HIGH  
**Duration:** 1-2 weeks  

### Deliverables

- [ ] Route Family resolution
- [ ] Family → Midge column family mapping
- [ ] ParsedRoute with route_family field
- [ ] RouterActor implementation
- [ ] String interning (GlobalInternTable)
- [ ] Scheme-based dispatch
- [ ] Pattern matching for notifications

### Implementation Tasks

1. **Define RouteFamily Type**
   ```rust
   pub struct RouteFamily(InternedString);  // "acme-prod", "acme-dev"
   ```

2. **Update ParsedRoute**
   ```rust
   pub struct ParsedRoute {
      route_family: RouteFamily,      // NEW: isolation boundary
       scheme: RouteScheme,             // stream, queue, kv, etc.
      realm: InternedString,           // logical (not an isolation boundary!)
       area: InternedString,
       resource: InternedString,
       operation: Option<InternedString>,
   }
   ```

3. **Implement GlobalInternTable**
   - DashMap-based string deduplication
   - Arc<str> for zero-copy sharing
   - Thread-safe concurrent access

4. **Implement RouterActor**
   - Parse routes with family context
   - Dispatch to domain actors by scheme
   - Cache parsed routes

### Files to Create/Modify

- `src/routing/family.rs` - RouteFamily type
- `src/routing/intern.rs` - GlobalInternTable
- `src/routing/parser.rs` - Route parsing
- `src/personas/router_actor.rs` - RouterActor

### Validation Criteria

- [ ] Parse route with family prefix
- [ ] Intern strings efficiently
- [ ] Dispatch to correct domain actor
- [ ] Pattern matching for wildcards

---

## Phase 5: Sessions & Transport 📋

**Status:** Next Up  
**Priority:** HIGH  
**Duration:** 2-3 weeks  

### Deliverables

- [ ] SessionActor implementation
- [ ] WebSocket transport layer
- [ ] Connection lifecycle management
- [ ] Identity binding (JWT)
- [ ] Message correlation
- [ ] Heartbeat/keepalive

### Implementation Tasks

1. **SessionActor**
   - Per-connection state (session_id, identity, conn_tx)
   - Register/Authenticate/Disconnect
   - Route message → RouterActor
   - Send response → client

2. **WebSocket Transport**
   - Accept connections
   - TLV frame parsing
   - Read task → SessionActor
   - Write task ← SessionActor (via channel)

3. **Identity Binding**
   - Parse JWT from AUTH frame
   - Validate via AuthEvalActor
   - Cache identity in SessionState

### Files

- `src/personas/session_actor.rs`
- `src/transport/websocket.rs`
- `src/transport/multiplexer.rs`

### Validation Criteria

- [ ] WebSocket connections accepted
- [ ] TLV frames parsed correctly
- [ ] JWT authentication works
- [ ] Messages routed to domains
- [ ] Responses returned to client

---

## Phase 6: Ephemeral Domains 📋

**Status:** Planned  
**Priority:** MEDIUM  
**Duration:** 3-4 weeks  

### Domain Implementation Order

1. **RealmActor** (Logical grouping tracking)
   - Track realms within families
   - Resource counting
   - Stats aggregation

2. **LeaseActor** (Ephemeral coordination)
   - Acquire/renew/release
   - Token generation
   - Expiry handling (timer messages)

3. **RpcActor** (Request-reply)
   - Register handlers
   - Route requests
   - Correlation and timeout

4. **NoticeActor** (Pub/sub)
   - Subscribe with patterns
   - Publish to matched subscribers
   - Pattern matching

5. **AuthEvalActor** (Runtime authz)
   - Validate JWTs
   - Check permissions
   - Cache policies

6. **ControlRuntimeActor** (Live state)
   - Track connections
   - Health monitoring
   - Family registry

7. **DiscoveryActor** (Introspection)
   - List families
   - List schemes
   - System info

### Files

- `src/personas/realm_actor.rs`
- `src/personas/lease_actor.rs`
- `src/personas/rpc_actor.rs`
- `src/personas/notice_actor.rs` (rename from notification)
- `src/personas/auth_eval_actor.rs`
- `src/personas/control_runtime_actor.rs`
- `src/personas/discovery_actor.rs`

### Validation Criteria

- [ ] Each actor handles all message types
- [ ] State managed correctly
- [ ] Timer-based expiry works (leases)
- [ ] Pattern matching works (notices)
- [ ] Authorization checks pass

---

## Phase 7: Durable Domains 📋

**Status:** Planned  
**Priority:** MEDIUM  
**Duration:** 4-5 weeks  

### Domain Implementation Order

1. **StreamActor** (Durable event streams)
   - Append to Midge
   - Read from Midge
   - Subscribe to updates
   - Sequence number tracking

2. **QueueActor** (Durable message queues)
   - Enqueue to Midge
   - Dequeue with lease semantics
   - Ack message
   - Dead letter handling

3. **KvActor** (Key-value via Midge)
   - Put/Get/Delete
   - Scan ranges
   - TTL support

4. **MetricsActor** (Optional durability)
   - In-memory aggregation
   - Periodic flush to Midge
   - Query current metrics

5. **AuthConfigActor** (Auth config storage)
   - Store/retrieve JWKS
   - Store/retrieve policies
   - Store/retrieve roles

6. **ControlConfigActor** (System config storage)
   - Store/retrieve system settings
   - Store/retrieve realm quotas
   - Store/retrieve feature flags

### Midge Integration

- Implement real Midge API calls
- Column family per route family
- Efficient batching
- Error handling

### Files

- `src/personas/stream_actor.rs`
- `src/personas/queue_actor.rs`
- Complete `src/storage/midge_actor.rs` (remove stubs)
- `src/personas/metrics_actor.rs`
- `src/personas/auth_config_actor.rs`
- `src/personas/control_config_actor.rs`

### Validation Criteria

- [ ] Data persists across restarts
- [ ] Family isolation works
- [ ] Midge writes succeed
- [ ] Midge reads return correct data
- [ ] TTL expiry works

---

## Phase 8: Bootstrap & Configuration 📋

**Status:** Planned  
**Priority:** MEDIUM  
**Duration:** 1-2 weeks  

### Deliverables

- [ ] FitzSystemBuilder enhancements
- [ ] Route family configuration
- [ ] Actor startup ordering
- [ ] Configuration file support
- [ ] Environment variable overrides

### Configuration Example

```yaml
system:
  max_connections: 10000
  log_level: info

route_families:
  - name: acme-prod
    max_connections: 1000
    storage:
      midge_path: /data/acme-prod
      
  - name: acme-dev
    max_connections: 100
    storage:
      midge_path: /data/acme-dev

auth:
  jwks_url: https://auth.acme.com/.well-known/jwks.json
  enable_jwt: true

transport:
  websocket:
    bind_addr: 0.0.0.0:8080
    max_frame_size: 1048576
```

### Files

- `src/bootstrap/system_init.rs`
- `src/bootstrap/config.rs`
- `src/config/loader.rs`

---

## Phase 9: Testing & Benchmarking 📋

**Status:** Ongoing  
**Priority:** HIGH  
**Duration:** Continuous  

### Test Categories

#### Unit Tests

- [ ] Actor message handling
- [ ] Route parsing
- [ ] TLV encoding/decoding
- [ ] String interning
- [ ] Authorization logic

#### Integration Tests

- [ ] End-to-end WebSocket flow
- [ ] Multi-domain workflows
- [ ] Authentication/authorization
- [ ] Midge persistence

#### Benchmarks

##### Tier 1: Hotpath (< 1s runtime)
- [ ] Route parsing throughput
- [ ] String interning performance
- [ ] Actor message dispatch
- [ ] TLV encode/decode

##### Tier 2: Subsystem (< 3s runtime)
- [ ] Lease acquire/renew/release
- [ ] RPC request-reply
- [ ] Notice publish-subscribe
- [ ] Metrics recording

##### Tier 3: System (< 10s runtime)
- [ ] WebSocket throughput
- [ ] End-to-end latency
- [ ] Concurrent sessions
- [ ] Mixed workload

### Files

- `tests/integration/*.rs`
- `benches/tier1_hotpath/*.rs`
- `benches/tier2_subsystem/*.rs`
- `benches/tier3_system/*.rs`

---

## Phase 10: Documentation & Examples 📋

**Status:** Partial  
**Priority:** MEDIUM  
**Duration:** Ongoing  

### Deliverables

- [ ] API documentation (rustdoc)
- [ ] Architecture diagrams
- [ ] Usage examples
- [ ] Migration guides
- [ ] Performance tuning guide

### Examples to Create

- [x] Basic system initialization
- [x] Custom actor
- [ ] WebSocket client
- [ ] Stream producer/consumer
- [ ] Queue worker
- [ ] Lease-based coordination
- [ ] RPC service
- [ ] Pub/sub notifications
- [ ] Multi-family setup

---

## Success Criteria

### MVP (Minimum Viable Product)

- [ ] WebSocket transport works
- [ ] Route family isolation functional
- [ ] Streams: append + read
- [ ] Queues: enqueue + dequeue
- [ ] KV: put + get + delete
- [ ] Leases: acquire + renew + release
- [ ] JWT authentication
- [ ] Basic authorization
- [ ] Data persists across restarts
- [ ] Single-node deployment

### Performance Targets

- Throughput: 10,000 msg/sec per core
- Latency (p99): < 10ms end-to-end
- Connections: 10,000+ concurrent
- Memory: < 100MB base + 1KB per connection

### Quality Targets

- Zero clippy warnings
- 80%+ test coverage
- All benchmarks < target runtime
- Documentation complete

---

## Future Phases (Post-MVP)

### Multi-Node Coordination

- Cluster membership
- Lease coordination across nodes
- Distributed queue consumers
- Stream replication

### Advanced Features

- Compression
- TLS/mTLS
- Rate limiting
- Circuit breakers
- Observability integrations

### Performance Optimizations

- Zero-copy where possible
- SIMD for parsing
- Sharding within families
- Connection pooling

---

## Current Status Summary

| Phase | Status | Progress |
|-------|--------|----------|
| 1. Core Actor Runtime | ✅ Complete | 100% |
| 2. Message Types & Protocol | ✅ Complete | 100% |
| 3. Storage Bridge | ✅ Stubs | 50% |
| 4. Route Family & Routing | 🚧 In Progress | 10% |
| 5. Sessions & Transport | 📋 Planned | 0% |
| 6. Ephemeral Domains | 📋 Planned | 0% |
| 7. Durable Domains | 📋 Planned | 0% |
| 8. Bootstrap & Config | 📋 Planned | 20% |
| 9. Testing & Benchmarking | 🚧 Ongoing | 30% |
| 10. Documentation | 🚧 Ongoing | 40% |

---

## Next Actions

1. **Immediate (This Week)**
   - Implement RouteFamily type
   - Update ParsedRoute structure
   - Create GlobalInternTable
   - Begin RouterActor implementation

2. **Short Term (Next 2 Weeks)**
   - Complete RouterActor
   - Start SessionActor
   - Implement WebSocket transport basics

3. **Medium Term (Next Month)**
   - Complete Sessions & Transport
   - Implement ephemeral domains
   - Begin durable domain work

---

**Legend:**
- ✅ Complete
- 🚧 In Progress
- 📋 Planned
- ⏸️ Blocked
- ❌ Cancelled

---

*Last Updated: December 11, 2025*
