# Fitz v2 — Self-Driving Implementation Guide (Canonical)

**Status:** Authoritative for implementation automation  
**Last Updated:** January 1, 2026

This document is the single entry point for an automated “self-driving” implementation script/agent. It identifies which specs are authoritative, defines hard invariants, and lays out an ordered build plan with acceptance criteria.

## 1) Authoritative Specs

Use these as the canonical source of truth:

- `specs/ARCHITECTURE.md` — system architecture, boundaries, and personas
- `specs/DOMAIN_MODEL.md` — domain set + durability rules
- `specs/ROUTING_ARCHITECTURE.md` — route family vs realm model
- `specs/domains/*.md` — domain behavior and operations
- `specs/infrastructure/*.md` — cross-cutting subsystems
- `specs/ROADMAP.md` — phased implementation ordering

Non-authoritative / legacy:

- `specs/OVERVIEW.md` — legacy overview; do not use for v2 routing or isolation semantics
- `specs/DESIGN.md` — forward-looking design notes; align to canonical docs before implementing against it

## 2) Terminology (Hard Rules)

These terms are used consistently in code, tests, and docs:

- **route family**: physical isolation boundary + storage partition + actor-set boundary
- **realm**: logical grouping inside a route family
- **area**: grouping within a realm
- **resource**: entity name
- **operation**: optional action
- **route**: `${scheme}://${realm}/${area}/${resource}[/${operation}]`

## 3) Routing & Isolation Invariants (Hard Rules)

1. **Route family is the isolation boundary.**
   - Different route families must never share durable storage.
   - Durable column families are derived from the route family (e.g., `${route_family}.streams`).
2. **Realm is not an isolation boundary.**
   - Realm is part of the route string and is purely organizational.
3. **Route family is not encoded into the route URI.**
   - The route URI begins with the **scheme** (`stream://`, `queue://`, etc.).
   - The route family arrives via session/connection context (envelope).
4. **Dispatch is driven by scheme, not by route family.**

## 4) Domain Set & Durability (Hard Rules)

Exactly six user-facing messaging domains:

- Durable: `stream`, `queue`, `kv`
- Ephemeral: `notice`, `rpc`, `lease`

Durability boundary:

- Only durable domains touch storage, and they do so via the storage bridge (Midge integration).

## 5) Architecture Boundary: Async Edges, Sync Core

Implementation must preserve the async/sync split:

- Async only at transport and storage edges.
- Synchronous core logic for routing and domain semantics.

(If a design doc proposes a different model, it must be reconciled before implementation.)

## 6) Implementation Plan (Script/Agent Oriented)

### Phase A — Route parsing and types

**Goal:** Define the canonical in-memory representation of a request.

Acceptance criteria:

- A `ParsedRoute` (or equivalent) contains:
  - `route_family` (from context)
  - `scheme`
  - `realm`, `area`, `resource`, `operation`
- Route parsing accepts the route string as `scheme://...` and does not infer route family from it.

### Phase B — Router dispatch

**Goal:** Ensure scheme-based dispatch with route-family context.

Acceptance criteria:

- Router logic dispatches by `scheme`.
- Route family is carried through to any durable storage operations.

### Phase C — Session handshake and identity binding

**Goal:** Bind a connection/session to a route family and permissions.

Acceptance criteria:

- Session state stores `route_family`.
- Authorization checks can be applied before domain handling.

### Phase D — Implement domains (ordered)

Suggested implementation order (matches roadmap intent):

1. `notice` (pattern match + fanout)
2. `rpc` (correlation + reply routing)
3. `lease` (TTL + ownership)
4. `kv` (Midge-backed)
5. `stream` (Midge-backed, append + subscribe + replay)
6. `queue` (Midge-backed, enqueue + lease/ack + redelivery)

Each domain acceptance criteria:

- Route format matches canonical route segments.
- Operations match the corresponding domain spec.
- Durable domains route all persistence through the storage bridge.

### Phase E — Control/auth configuration

**Goal:** Implement infrastructure schemes for configuration and management.

Acceptance criteria:

- `authcfg://...` and `ctrlcfg://...` are treated as infrastructure routes.
- Their data is durable (stored via `kv`/storage bridge).

## 7) “Self-Driving” Output Expectations

An automation script/agent should be able to:

- Identify which file(s) to edit for each phase (routing/session/domains).
- Add focused tests per component.
- Keep terminology consistent with this guide.
- Avoid implementing against non-authoritative docs.
