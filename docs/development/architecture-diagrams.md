# Fitz Architecture Diagrams

**Status**: Repo-native architecture reference
**Scope**: Core broker architecture
**Authority**: These diagrams summarize the current architecture. If they conflict with
[architectural-laws.md](architectural-laws.md),
[domain-boundaries-spec.md](domain-boundaries-spec.md), or source, treat the conflict
as a documentation or implementation defect and resolve it explicitly.

These diagrams intentionally do not imply durability, replay, ownership continuity,
recovery, exactly-once delivery, or fused domain semantics beyond the guarantees in
the authoritative documents.

## Layered Broker Architecture

```mermaid
flowchart TB
    subgraph L1["Layer 1: src/api transport edge (async I/O)"]
        WS["WebSocket listener"]
        TCP["TCP listener"]
        HTTP["HTTP upgrade and admin probes"]
    end

    subgraph L2["Layer 2: session ingress (async boundary, sync dispatch)"]
        ING["RuntimeIngress"]
        AUTH["CONNECT auth and route-family resolution"]
        PERM["SessionActor permission checks"]
        DISPATCH["Manifest-backed domain frame dispatcher"]
    end

    subgraph L3["Layer 3: src/runtime core (sync)"]
        ROUTER["Router"]
        ADDR["RouteAddress = (RouteFamily, Route)"]
        MANIFEST["Exact message manifest: ID, direction, scheme, auth"]
        ACTORS["Family actor pools: fixed affinity and bounded lanes"]
    end

    subgraph L4["Layer 4: src/domains (sync domain sinks and actors)"]
        KV["KV"]
        QUEUE["Queue"]
        NOTICE["Notice"]
        STREAM["Stream"]
        RPC["RPC"]
        LEASE["Lease"]
        SCHEDULE["Schedule"]
    end

    subgraph L5["Layer 5: Midge-backed storage where explicitly used"]
        MIDGE["Midge engine"]
        KVSTORE["KV committed state"]
        QSTORE["Queue durable backlog"]
        SSTORE["Stream committed history"]
        TSTORE["Schedule timing intent"]
    end

    WS --> ING
    TCP --> ING
    HTTP --> ING
    ING --> AUTH --> PERM --> DISPATCH
    DISPATCH --> ADDR --> ROUTER
    MANIFEST --> ROUTER
    ROUTER --> ACTORS
    ACTORS --> KV
    ROUTER --> QUEUE
    ROUTER --> NOTICE
    ROUTER --> STREAM
    ROUTER --> RPC
    ROUTER --> LEASE
    ROUTER --> SCHEDULE
    KV --> KVSTORE --> MIDGE
    QUEUE --> QSTORE --> MIDGE
    STREAM --> SSTORE --> MIDGE
    SCHEDULE --> TSTORE --> MIDGE
```

## Request Lifecycle

```mermaid
sequenceDiagram
    participant Client
    participant Transport as Async transport in src/api
    participant Ingress as RuntimeIngress
    participant Auth as Auth and SessionActor
    participant Router
    participant Domain as Sync domain sink
    participant Store as Midge when domain is durable

    Client->>Transport: Binary frame
    Transport->>Ingress: on_frame(session_id, channel, type, payload)
    Ingress->>Auth: Authenticate CONNECT if needed
    Auth-->>Ingress: route_family and permission snapshot
    Ingress->>Auth: Authorize route-shaped permission
    Auth-->>Ingress: Allowed or ERR_UNAUTHORIZED response
    Ingress->>Router: Envelope(RouteFamily, Route, typed request)
    Router->>Domain: deliver or deliver_high_priority
    alt Durable domain operation
        Domain->>Store: Synchronous storage operation
        Store-->>Domain: Persisted or read result
    else Ephemeral domain operation
        Domain->>Domain: Update broker-local live state
    end
    Domain-->>Router: Domain response or live notification
    Router-->>Ingress: Response routed to session inbox
    Ingress-->>Transport: Accept, close, or backpressure decision
    Transport-->>Client: Encoded response frame
```

The session route family comes from authenticated session state. The request
realm comes from the route string. They are carried together as a route address
and are not inferred from each other.

## Disconnect Cleanup

```mermaid
sequenceDiagram
    participant Transport
    participant Ingress as RuntimeIngress
    participant Registry as Session registry
    participant Router
    participant Domains as Cleanup domains

    Transport->>Ingress: on_close(session_id, reason)
    Ingress->>Registry: Read current route_family
    Ingress->>Router: SessionCleanup to KV
    Ingress->>Router: SessionCleanup to Notice
    Ingress->>Router: SessionCleanup to RPC
    Ingress->>Router: SessionCleanup to Stream
    Ingress->>Router: SessionCleanup to Schedule
    Ingress->>Router: SessionCleanup to Lease
    Ingress->>Router: SessionCleanup to Queue
    Router->>Domains: Remove session-owned live state
    alt Cleanup delivery failed
        Ingress->>Ingress: Store retry ticket for same session and route_family
    else Cleanup delivered
        Domains-->>Ingress: Cleanup accepted
    end
    Ingress->>Registry: Finalize close and remove session
```

Cleanup removes only session-owned live state:

| Domain | Removed on disconnect | Explicitly retained if already durable |
| --- | --- | --- |
| KV | open transactions, resource locks, live watches | committed current state |
| Notice | subscriptions and queued live notifications | nothing |
| RPC | worker registrations, pending live requests, reply routing | nothing |
| Stream | live subscriptions and uncommitted append sessions | committed records, offsets, watermarks |
| Schedule | live subscriptions | definitions, next-fire state, pending fire claims |
| Lease | held leases, waiters, lease watches | nothing |
| Queue | live inflight ownership and queue watches | durable backlog, delayed entries, DLQ state under the configured write policy |

A reconnect creates a new session. Clients must explicitly re-authenticate,
re-subscribe, re-register, reopen transactions, reacquire leases, or resume from
client-managed offsets where the domain supports explicit durable reads.

## Domain Semantics

```mermaid
flowchart TB
    subgraph EP["Ephemeral primary guarantees"]
        NOTICE["Notice<br/>live fanout only<br/>no replay or recovery"]
        RPC["RPC<br/>live request and response<br/>no durable pending work"]
        LEASE["Lease<br/>single-broker ownership<br/>process-local fencing tokens"]
    end

    subgraph DU["Durable primary guarantees with live session adjuncts"]
        KV["KV<br/>durable committed current state<br/>ephemeral transactions and watches"]
        STREAM["Stream<br/>durable committed history and replay<br/>ephemeral append sessions and live subscriptions"]
        QUEUE["Queue<br/>durable backlog by write policy<br/>ephemeral inflight ownership"]
        SCHEDULE["Schedule<br/>durable timing intent<br/>ephemeral subscriptions and delivery"]
    end

    NOTICE -. "may hint" .-> STREAM
    NOTICE -. "may hint" .-> KV
    SCHEDULE -. "may explicitly compose with" .-> QUEUE
    RPC -. "results may explicitly be written to" .-> STREAM
    LEASE -. "may guard client workflow around" .-> KV
```

Dotted edges are optional explicit composition. They do not transfer guarantees
between domains.

## Route Isolation

```mermaid
flowchart LR
    CLAIM["JWT identity claim<br/>for example tid or org_id"]
    MAP["FITZ_ROUTE_FAMILY_MAP"]
    RF["Session route_family<br/>broker-internal isolation key"]
    ROUTE["Request route<br/>scheme://realm/area/resource"]
    REALM["realm<br/>application-visible opaque namespace"]
    ADDR["RouteAddress<br/>(RouteFamily, Route)"]
    ROUTER["Router and domain indexes"]
    NOALIAS["No inference, aliasing, substitution, or fallback"]

    CLAIM --> MAP --> RF
    ROUTE --> REALM
    RF --> ADDR
    ROUTE --> ADDR
    ADDR --> ROUTER
    RF --> NOALIAS
    REALM --> NOALIAS
```

```mermaid
flowchart TB
    subgraph F1["RouteFamily 1"]
        F1R["notice://acme/orders/created"]
    end

    subgraph F2["RouteFamily 2"]
        F2R["notice://acme/orders/created"]
    end

    F1R -. "same route string, isolated delivery and state" .-> F2R
```

The comparison edge above documents isolation, not delivery. A route lookup,
subscription match, queue operation, stream read, KV transaction, lease
operation, RPC dispatch, or schedule operation in one route family must not
observe state from another route family.
