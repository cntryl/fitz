# Fitz v2 Domain Model (Canonical)

**Version:** 1.0  
**Status:** Authoritative  
**Last Updated:** December 11, 2025

This document defines the **canonical Fitz v2 domain model**, aligning:

- Domains (what Fitz exposes)
- Personas (who implements it)
- Route Families (where it lives)
- Routing Schemes (how it is addressed)
- Durability (Midge vs ephemeral)

---

## 1. Domains (What Fitz Exposes)

Fitz has exactly **six** user-facing messaging domains:

- `stream`  – append-only logs with replay
- `queue`   – work queues with ack/redelivery
- `kv`      – durable key-value
- `notice`  – fire-and-forget pub/sub
- `rpc`     – request/response
- `lease`   – ephemeral locks/coordination

These are the **only things** we call “domains”.

---

## 2. Domain → Persona → Durability

| Domain | Persona        | Durable? | Storage | Scheme prefix | Description |
|--------|----------------|---------:|---------|---------------|-------------|
| stream | `StreamActor`  |   yes    | Midge   | `stream://`   | Append-only event streams with replay and subscriptions |
| queue  | `QueueActor`   |   yes    | Midge   | `queue://`    | At-least-once work queues with leases and redelivery |
| kv     | `MidgeActor` / `KvActor` facade | yes | Midge | `kv://` | Durable key-value for config, offsets, metadata |
| notice | `NoticeActor` / `RealmActor` fanout | no | Memory | `notice://` | Pub/sub fanout, no replay |
| rpc    | `RpcActor`     |   no     | Memory  | `rpc://`      | Request/response with correlation and timeouts |
| lease  | `LeaseActor`   |   no     | Memory  | `lease://`    | Ephemeral exclusive locks with TTL |

Notes:

- Durable domains (`stream`, `queue`, `kv`) **always** go through `MidgeActor` to Midge.
- Ephemeral domains (`notice`, `rpc`, `lease`) **never** write to Midge.

---

## 3. Route Families (Where Domains Live)

A **Route Family** is the **physical boundary**:

- isolation / environment / partition
- maps to Midge column families
- defines which actors & storage a request hits

Examples:

- `acme-prod`
- `acme-dev`
- `customer-42`
- `internal-core`

For durable domains, each family maps to Midge like:

- `<family>.streams`
- `<family>.queues`
- `<family>.kv`

Same domains, different families → **different physical data**.

---

## 4. Routing Scheme (How Domains Are Addressed)

All domains use the same route structure *inside* a Route Family:

```text
{scheme}://{realm}/{area}/{resource}/{operation}
```

Where:

- `scheme`   = one of `stream`, `queue`, `kv`, `notice`, `rpc`, `lease`
- `realm`    = logical grouping (NOT an isolation boundary)
- `area`     = subsystem label
- `resource` = concrete entity name
- `operation`= verb (append, enqueue, get, publish, invoke, acquire, …)

Examples (within family `acme-prod`):

- `stream://orders/events/created/append`
- `queue://orders/jobs/email/enqueue`
- `kv://auth/tokens/session:123/get`
- `notice://chat/rooms/42/publish`
- `rpc://billing/payments/refund/invoke`
- `lease://orders/locks/reconcile/acquire`

---

## 5. Infrastructure (What Is *Not* a Domain)

These are **not** domains; they are system subsystems that apply across all domains:

- `auth`            – token validation, RBAC
- `control-plane`   – manage streams/queues, discovery
- `metrics`         – internal counters and histograms
- `routing`         – route parsing and persona dispatch
- `realms`          – logical grouping / membership
- `sessions`        – connection lifecycle (WS/TCP)
- `transport`       – TLV, TCP, WebSocket
- `actor-runtime`   – scheduler, mailboxes, timers

Each of these has one or more personas (actors), but they are **never** called domains.

---

## 6. Apples-to-Apples View

- **Domains** = what Fitz does (messaging models)
- **Personas** = who does the work (actors per domain)
- **Route Families** = where the work lives (storage/isolation boundary)
- **Routing Schemes** = how work is addressed (uniform URI structure)
- **Infrastructure** = what supports the work (auth, control, routing, etc.)
- **Durability** = whether work is persisted (Midge) or ephemeral (memory)

This model must stay stable and consistent across all docs and code.

For deeper diagrams and system-level context, see:

- `ARCHITECTURE.md`
- `ROUTING_ARCHITECTURE.md`
- `ROADMAP.md`
