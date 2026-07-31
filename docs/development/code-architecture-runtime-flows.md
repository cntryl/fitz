# Fitz Code Architecture Runtime Flows

**Status**: Source-structure reference
**Scope**: Boot, ingress, dispatch, and disconnect cleanup paths

These diagrams describe code flow. They intentionally avoid adding domain
guarantees beyond the authoritative domain documents.

## Boot Wiring

```mermaid
sequenceDiagram
    participant Main as main/bootstrap caller
    participant Boot as src/boot/mod.rs
    participant Config as BootConfig
    participant Runtime as runtime init
    participant HTTP as HTTP listener
    participant Storage as Midge storage
    participant Domains as src/boot/domains.rs
    participant Router as runtime Router
    participant Background as api/background.rs

    Main->>Boot: boot(config)
    Boot->>Config: validate environment and limits
    Boot->>Runtime: create Router, RuntimeIngress, runtime state
    Boot->>HTTP: start HTTP listener early for standby orchestration health
    Boot->>Storage: open configured Midge engine
    Boot->>Runtime: mark storage ready
    Boot->>Domains: construct all domain sinks
    Domains->>Router: register domain patterns
    Domains->>Domains: preload persisted schedule families
    Domains-->>Boot: DomainHandles
    Boot->>Runtime: attach domains and mark domains ready
    Boot->>Background: start domain background tasks
    Boot->>Runtime: mark startup complete
```

`/targetz` can come up before storage is ready for observation by a separate
orchestration path. It is not a customer traffic-admission signal. Data-plane
readiness still requires storage, domain registration, and startup completion.

## Frame Dispatch Path

```mermaid
sequenceDiagram
    participant Transport as src/api/handlers
    participant Ingress as RuntimeIngress
    participant Auth as session_authenticator.rs
    participant Registry as session_registry.rs
    participant Policy as dispatch_policy.rs
    participant Dispatcher as domain_frame_dispatcher.rs
    participant Codec as src/protocol
    participant Router as src/runtime/router.rs
    participant Sink as domain sink

    Transport->>Ingress: on_frame(session, channel, type, payload)
    Ingress->>Auth: authenticate_frame
    Auth->>Registry: read or update SessionInfo
    Auth-->>Ingress: route_family
    Ingress->>Policy: identify domain authorization policy
    Policy-->>Ingress: DomainAuthorizationSpec
    Ingress->>Dispatcher: dispatch_if_domain
    Dispatcher->>Codec: extract auth route and parse request
    Dispatcher->>Registry: fetch SessionActor
    Dispatcher->>Dispatcher: check permission target
    Dispatcher->>Router: route Envelope(RouteAddress, request)
    Router->>Sink: deliver
    Sink-->>Router: response routed to session inbox when needed
    Ingress-->>Transport: Accept, Backpressure, or Close
```

The dispatcher is the code choke point where typed domain routing, permission
policy, and `RouteAddress` construction meet.

## Disconnect Cleanup Path

```mermaid
flowchart TB
    CLOSE["transport on_close"]
    INGRESS["RuntimeIngress::on_close"]
    RETRY["retry pending cleanup tickets"]
    FAMILY["read session route_family"]
    CLEANUP["SessionCleanup { session_id }"]
    ORDER["DomainRegistry::cleanup_order"]
    ROUTER["Router route cleanup envelope"]
    DOMAINS["domain cleanup handlers"]
    FAILURE["store PendingSessionCleanup"]
    FINALIZE["session_registry.finalize_close"]
    EVENT["optional SessionEvent::Close"]

    CLOSE --> INGRESS --> RETRY --> FAMILY --> CLEANUP --> ORDER --> ROUTER --> DOMAINS
    ROUTER -- failed domain delivery --> FAILURE
    DOMAINS --> FINALIZE --> EVENT
    FAILURE --> FINALIZE
```

Cleanup retry tickets are only for finishing cleanup delivery. They do not
restore a session, subscriptions, workers, transactions, leases, stream append
sessions, or queue inflight ownership.

## Domain Registration And Cleanup Manifest

```mermaid
flowchart LR
    MANIFEST["src/runtime/domain_manifest.rs"]
    ALL["DomainKind::ALL<br/>KV, Queue, Notice, Stream, RPC, Lease, Schedule"]
    CLEANUP["SESSION_CLEANUP_ORDER<br/>KV, Notice, RPC, Stream, Schedule, Lease, Queue"]
    DESCRIPTOR["DomainDescriptor<br/>scheme, inbound route, cleanup route"]
    BOOT["src/boot/domains.rs"]
    INGRESS["src/api/runtime_ingress"]

    MANIFEST --> ALL
    MANIFEST --> CLEANUP
    MANIFEST --> DESCRIPTOR
    DESCRIPTOR --> BOOT
    CLEANUP --> INGRESS
    BOOT -->|"register_sink"| ROUTER["Router"]
    INGRESS -->|"SessionCleanup envelopes"| ROUTER
```

Adding a domain requires updating the manifest, boot registration, ingress
authorization/dispatch metadata, protocol codecs, admin/read-model surfaces, and
tests that prove cleanup coverage.
