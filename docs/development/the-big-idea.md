# BIG IDEA: World‑class Single-Node Multi‑Modal Broker on an Actor-Based Runtime

## TL;DR ✅
Build a deterministic, ultra-high-performance, single-node multi-modal broker centered on a synchronous actor-based runtime and thin async transports. Target use cases: real-time messaging, RPC, streaming, queues, and hybrid multimodal integrations. Focus on **realm isolation**, **route‑based addressing**, **pluggable codecs**, and **predictable, low-latency behavior**.

This document describes direction within Fitz's single-node boundary. It does
not promise replication, distributed consensus, cross-node recovery, or
exactly-once delivery.

---

## Vision 🎯
- A broker that is simple to integrate with any protocol and payload format, but opinionated internally for performance and operational clarity.
- Provide predictable latency, strong isolation (per-realm), explicit domain durability semantics, and observability out of the box.
- Developer-friendly primitives (routes, actors, codecs) so domain logic remains synchronous and deterministic.

---

## Core Principles 🔧
- **Async at the edges, sync in the core** — transport adapters are async; engine, runtime, and domains are fully synchronous. No .await in domain code.
- **Actor-based runtime** — small deterministic actors, per-actor state, mailboxes, and a scheduler for fair, low-latency execution.
- **Route-first addressing** — use the pair `(RouteFamily, Route)`. The `Route` itself follows the pattern `{scheme}://{realm}/{area}/{resource}` with domain-specific suffixes where documented, and the `RouteFamily` is a separate broker-internal isolation id (for example: RouteFamily=1 with route `kv://acme/app/users`).
- **Realm isolation** — realms are first-class, opaque application-defined isolation boundaries. A realm may model a tenant, department, cost center, user, environment, or any other developer-chosen partition, but Fitz does not assign one business meaning.
- **Pluggable codecs & adapters** — TLV, JSON, Protobuf, binary, gRPC/HTTP/WS/TCP adapters, and streaming bridges.
- **Performance by construction** — microbenchmarks first, no allocations in hot paths, precomputed buffers, and deterministic routing.

---

## High-level Architecture 🏗️

1. Transport Layer (async)
   - Protocol adapters: WebSocket, HTTP, TCP, gRPC, and connectors
   - Responsibility: framing, connection lifecycle, TLS, authentication handshakes
   - Output: raw frames → queued to runtime via SPSC/crossbeam channels

2. Session Layer (sync middleware)
   - TLV/codec parsing, permission checks, route extraction, session state (realm, claims)
   - Produces typed messages for the Runtime

3. Runtime (actor engine, 100% sync)
   - Router, scheduler, matcher, subscription index
   - Enforces RouteFamily isolation and routes messages to domain actors
   - Actor mailboxes are lightweight; scheduling is deterministic and observable

4. Domains (business logic, 100% sync)
   - Built-in domains: `kv`, `notice` (pub/sub), `rpc`, `queue`, `lease`, `stream`
   - Domains must obey sync-only rules: no async, no tokio types, synchronous operations only

5. Storage & Durability
   - Pluggable storage backends (WAL + key-value store, optional disk-backed volumes)
   - Per-operation durability levels (in-memory fast path, buffered, fsync)

6. Observability & Ops
   - Metrics (prometheus), traces (OpenTelemetry), structured audit logs
   - Integrated microbench harnesses and CI performance checks

---

## Multi‑Modal Support (What "multi-modal" means) 🔀
- Accept and normalize messages from different transports and protocols to the internal message model.
- Support payload formats: TLV (native), JSON, Protobuf, CBOR, raw binary.
- Bridge patterns: protocol translation, fanout to multiple transports, content transformation plug-ins.
- Streaming first-class: long-running streams with back-pressure and offset/ack semantics.

---

## Runtime Design Details ⚙️
- RouteFamily is a broker-internal isolation key selected from verified JWT claims. Example: a full address is the pair `(RouteFamily=1, route="kv://acme/app/users")`.
- RouteFamily is never a public or business namespace label. It must never be treated as a realm alias or realm fallback.
- Anonymous mode always uses RouteFamily `1`; authenticated families must be provisioned before readiness.
- Actors are single responsibility, synchronous objects with clearly typed messages/responses.
- Interop boundary: async transport enqueues frames → runtime dequeues and calls actor handlers synchronously → response serialized and forwarded to transport writer.

---

## Reliability & Delivery Semantics 🛡️
- Delivery semantics stay domain-specific: Notice is live ephemeral fanout, Stream is durable history and replay, Queue is durable at-least-once work delivery, and RPC is live request and response.
- Subscription semantics: wildcards (`*`, `**`) and pattern matching.
- Ordering and partitioning guarantees configurable per resource (per-route).
- Durable Queue messages and Stream history survive restart where the configured storage backend supports persistence.

---

## Security & Multi‑Realm Isolation 🔐
- AuthN/ AuthZ integrated at the session layer (scope/claims -> realm/area permissions).
- Per-realm quotas and rate-limits; audit logs and fine-grained permissions (read/write/subscribe/publish).
- Encryption in transit (TLS); optional per-realm secrets for signing and encryption of persisted blobs.

---

## Performance, Testing & Quality 📈
- Benchmarks: criterion-style microbenchmarks with hot path isolation, deterministic seeded workloads.
- Tests: strong meta-tests for test naming and AAA structure (stay consistent: `should_*` names and AAA comments).
- CI gates: clippy, formatting, unit tests, benchmarks and a perf budget per release.

---

## Deployment & Scalability 📦
- Fitz is a single-node broker.
- Route families are provisioned local isolation namespaces, not cross-node ownership ranges.
- Scale-up work must preserve the async-edge and sync-core rule without implying coordinated failover or state transfer.

---

## Roadmap & Milestones 🛣️
- MVP (v0.1): core engine, transport adapters (WS/TCP), `notice` (pub/sub), `kv` (in-memory), TLV codec, tests & benchmarks
- v0.2: durable storage + WAL, `queue` and `rpc` domains, authN/Z, metrics and tracing
- v0.3: single-node operational hardening, stream domain improvements, and multi-modal bridges
- v1.0 GA: explicit domain credibility evidence, stable operations, and carefully scoped extension points

---

## Developer Experience & Governance 💡
- Clear invariants: domain code is sync-only, tests follow `should_*` naming and AAA structure, benchmarks follow microbench rules.
- Plug-in model for codecs and protocol adapters; documented extension points.
- Capture operational runbooks and a health-check API.

---

## Appendix – Key Terms
- **realm**: opaque application-defined isolation boundary for resources; it may represent a tenant, department, cost center, user, environment, or another developer-chosen partition
- **area**: namespace within a realm
- **resource**: specific entity
- **route**: route address, formatted as `{scheme}://{realm}/{area}/{resource}/{operation}`. Note: the full address is always the pair `(RouteFamily, Route)`; `RouteFamily` is not part of the route string.
- **RouteFamily**: broker-internal numeric isolation id provisioned on the single node; separate from realm and never a substitute for it

---

## Next steps ✅
- Turn this doc into a short RFC and identify owner(s) for each milestone
- Implement MVP checklist and execute microbench baselines


*Document authored to align with Fitz architecture principles: async edges, sync core, deterministic runtime, realm-first isolation.*
