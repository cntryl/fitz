# Fitz Code Architecture Domain Patterns

**Status**: Source-structure reference
**Scope**: Domain implementation layout under `src/domains`

Fitz domains share implementation patterns, but each domain owns only its own
guarantees. Shared code shape must not blur domain semantics.

## Generic Domain Sink Pattern

```mermaid
flowchart TB
    ENVELOPE["runtime::Envelope"]
    MAILBOX["MailboxSink impl<br/>deliver or deliver_high_priority"]
    CLEANUP["SessionCleanup fast path"]
    REQUEST["Client request frame"]
    CODEC["protocol parse and encode"]
    SINKSTATE["domain sink state<br/>session indexes, warm actors, routes"]
    ACTOR["domain actor or state machine"]
    STORE["storage store when explicit"]
    ADMIN["AdminReadModel projection"]
    RESPONSE["client response envelope"]
    NOTIFY["live notification envelope"]

    ENVELOPE --> MAILBOX
    MAILBOX --> CLEANUP
    MAILBOX --> REQUEST
    REQUEST --> CODEC --> SINKSTATE
    SINKSTATE --> ACTOR
    ACTOR --> STORE
    ACTOR --> SINKSTATE
    SINKSTATE --> ADMIN
    SINKSTATE --> RESPONSE
    SINKSTATE --> NOTIFY
```

Pattern notes:

- Cleanup is handled before normal client request parsing where possible.
- Protocol code parses wire payloads; domain sinks and actors apply mechanics.
- Admin projections describe current state; they are not correctness inputs.
- Live notifications are best-effort delivery through the router unless a domain
  explicitly defines a stronger guarantee.

## Domain Module Families

```mermaid
flowchart TB
    subgraph COMMON["Common files repeated across most domains"]
        MOD["mod.rs<br/>contract comments and exports"]
        PROTOCOL["protocol.rs<br/>typed client messages"]
        SESSION["session.rs<br/>session-facing actor helpers"]
        EVENTS["events.rs<br/>admin or domain event records"]
        METRICS["metrics.rs<br/>domain counters and gauges"]
        PROJECTION["projection.rs<br/>admin projection helpers"]
        SINK["sink/<br/>MailboxSink and domain sink impls"]
    end

    subgraph ACTORS["Actor or state-machine modules"]
        KVA["kv/actor.rs"]
        QUEUEA["queue/actor/*"]
        NOTICEA["notice/actor.rs"]
        STREAMA["stream/actor.rs<br/>area_actor.rs<br/>realm_actor.rs"]
        RPCA["rpc/actor.rs<br/>reply_inbox.rs"]
        LEASEA["lease/actor.rs<br/>guard.rs"]
        SCHEDULEA["schedule/actor/*"]
    end

    subgraph STORES["Explicit storage modules"]
        STREAMSTORE["stream/store/*<br/>stream/storage/*"]
        QUEUESTORE["queue/actor/storage*"]
        SCHEDULESTORE["schedule/store/*"]
        KVSTORE["kv/actor.rs<br/>Midge transaction wrapper"]
    end

    SINK --> KVA
    SINK --> QUEUEA
    SINK --> NOTICEA
    SINK --> STREAMA
    SINK --> RPCA
    SINK --> LEASEA
    SINK --> SCHEDULEA
    KVA --> KVSTORE
    QUEUEA --> QUEUESTORE
    STREAMA --> STREAMSTORE
    SCHEDULEA --> SCHEDULESTORE
```

Notice and RPC do not have storage modules because their primary state is live
broker-local state. Lease also has no storage module; lease ownership, waiters,
and fencing tokens are ephemeral process-local coordination state.

## Storage-Backed Versus Ephemeral Code Paths

```mermaid
flowchart LR
    subgraph STORAGEBACKED["Storage-backed domains"]
        KV["KV<br/>Midge transactions"]
        QUEUE["Queue<br/>durable backlog by write policy"]
        STREAM["Stream<br/>committed history and watermarks"]
        SCHEDULE["Schedule<br/>timing intent and pending claims"]
    end

    subgraph EPHEMERAL["Ephemeral domains"]
        NOTICE["Notice<br/>subscriptions and fanout indexes"]
        RPC["RPC<br/>workers, pending requests, reply routing"]
        LEASE["Lease<br/>holders, waiters, process-local tokens"]
    end

    KV --> STORAGE["FitzStorageEngine / Midge"]
    QUEUE --> STORAGE
    STREAM --> STORAGE
    SCHEDULE --> STORAGE
    NOTICE --> MEMORY["in-memory sink or actor state"]
    RPC --> MEMORY
    LEASE --> MEMORY
```

The storage-backed group can still own ephemeral adjunct state, such as KV
transactions, Stream append sessions, Queue inflight ownership, or Schedule live
subscriptions. The ephemeral group must not grow hidden durable recovery paths.

## Admin And Observability Flow

```mermaid
flowchart TB
    DOMAIN["domain sink or actor"]
    EVENTS["domain event/projection data"]
    ADMINMODEL["control/admin/read_model.rs"]
    ADMINAPI["api/admin handlers"]
    MCP["api/mcp"]
    METRICS["domain metrics"]
    COLLECTOR["observability metrics collector"]
    CLIENT["operator or tool client"]

    DOMAIN --> EVENTS --> ADMINMODEL
    ADMINMODEL --> ADMINAPI --> CLIENT
    ADMINMODEL --> MCP --> CLIENT
    DOMAIN --> METRICS --> COLLECTOR
```

Admin and observability surfaces are read-side views. They can expose topology,
metrics, troubleshooting summaries, and current domain snapshots, but they must
not control retries, recovery, routing, ownership, scheduling, or domain
correctness decisions.
