# Fitz Domain Violations (Code-Backed)

This file tracks **confirmed** (code-backed) violations and consistency breaks against Fitz’s published domain invariants and cross-domain architectural model.

**Scope rules for this document**

- Only list items that are directly evidenced in code.
- Prefer describing the mismatch/violation over proposing redesigns.
- Use Fitz terminology: **realm**, **area**, **resource**, **operation**, **route**, **RouteFamily**.

---

## V-001 — Queue `enqueue_batch` is not a single Midge transaction

**Invariant / intent being violated**

- Queue domain docs and protocol describe `enqueue_batch` as doing **exactly one** Midge write-batch/transaction per call (“All succeed or all fail”, “ONE Midge write batch”).

**Evidence**

- `handle_enqueue_batch` allocates message IDs by calling `next_message_id()` once per message.
- `next_message_id()` persists `next_id` using its **own Midge transaction per ID**, then `handle_enqueue_batch` performs another Midge transaction for the batch.

**Code paths**

- ID allocation and per-ID persistence: [src/domains/queue/queue_actor.rs](src/domains/queue/queue_actor.rs#L266-L306)
- Batch write transaction: [src/domains/queue/queue_actor.rs](src/domains/queue/queue_actor.rs#L394-L485)

**Why it matters**

- This breaks the stated atomicity/“single batch” semantics and creates extra transaction overhead on the hot path.

---

## V-002 — Queue delayed-visibility persistence uses non-epoch time

**Invariant / intent being violated**

- Queue durable record field `visible_at_ms` is documented as “milliseconds since UNIX epoch”.

**Evidence**

- `visible_at_ms` is computed from `Instant`-based deltas, not from an epoch clock.

**Code paths**

- Field documentation: [src/domains/queue/queue_actor.rs](src/domains/queue/queue_actor.rs#L65-L67)
- Computation: [src/domains/queue/queue_actor.rs](src/domains/queue/queue_actor.rs#L420-L426)

**Why it matters**

- Persisted timestamps based on `Instant` are not stable across process restarts and cannot represent wall-clock epoch time.

---

## V-003 — Queue restart recovery is not implemented (tests simulate recovery manually)

**Invariant / intent being violated**

- Queue domain describes “durable storage” and includes tests claiming restart recovery.

**Evidence**

- Actor startup only recovers `next_id`; there is no scan to rebuild `ready` / `delayed` state from persisted message records.
- Integration test manually re-enqueues a message ID into `ready` to simulate recovery.

**Code paths**

- Startup recovery is limited to `next_id`: [src/domains/queue/queue_actor.rs](src/domains/queue/queue_actor.rs#L211-L241)
- Test “manual recovery”: [tests/queue_e2e_basic.rs](tests/queue_e2e_basic.rs#L82-L85)

**Why it matters**

- The observable “durable queue” semantics across restart are incomplete relative to the documented behavior.

---

## V-004 — Schedule emits notice routes without an `area` segment

**Invariant / intent being violated**

- Across domains, identity is consistently modeled as `(realm, area, resource, operation)` in routes (and often in keyspace prefixes).

**Evidence**

- Schedule constructs notice routes as `notice://{realm}/{resource}/{operation}` (no area), diverging from the general realm/area/resource model.

**Code paths**

- Notice route construction: [src/domains/schedule/actor.rs](src/domains/schedule/actor.rs#L186-L195)

**Why it matters**

- Cross-domain route/identity consistency is weakened, and it becomes less obvious how schedule-driven notices map into a consistent area namespace.

---

## V-005 — Schedule hard-codes durability (`WriteOptions::sync`) (hidden default)

**Invariant / intent being violated**

- “No hidden defaults (especially persistence or routing).”
- Several domains make durability intent explicit by API shape (e.g., caller-selected or mode-selected).

**Evidence**

- Schedule persistence always commits with `WriteOptions::sync()`.

**Code paths**

- Store insert: [src/domains/schedule/store.rs](src/domains/schedule/store.rs#L37-L41)
- Store delete: [src/domains/schedule/store.rs](src/domains/schedule/store.rs#L51-L55)

**Why it matters**

- Schedule durability differs from Queue/Stream/KV patterns and is not caller-visible, making it a “hidden default” semantic.

---

## V-006 — NoticeRouteActor `Default` uses RouteFamily(0) (hidden default isolation boundary)

**Invariant / intent being violated**

- “No hidden defaults (especially persistence or routing).”

**Evidence**

- `Default` implementation constructs `NoticeRouteActor` with `RouteFamily::new(0)`.

**Code paths**

- Default implementation: [src/domains/notice/route_actor.rs](src/domains/notice/route_actor.rs#L173-L176)

**Why it matters**

- RouteFamily(0) is a meaningful routing/isolation value; providing it implicitly can lead to accidental cross-family coupling if `Default` is used outside narrowly-scoped contexts.

---

## V-007 — KV “resource/table” identity is not encoded into keys; CF mapping ignores `resource`

**Invariant / intent being violated**

- “Domains are logical namespaces in a shared keyspace” and “Isolation is via binary key prefixes” (and/or explicit domain keyspace partitioning).

**Evidence**

- KV enforces “transaction scoped to a single resource” at the API layer, but stored keys are raw user keys, and column family selection depends only on RouteFamily.
- `resolve_column_family` explicitly ignores `resource`.

**Code paths**

- Writes use caller-provided key directly: [src/domains/kv/actor.rs](src/domains/kv/actor.rs#L207-L208)
- CF resolution ignoring `resource`: [src/domains/kv/actor.rs](src/domains/kv/actor.rs#L381-L389)

**Why it matters**

- Two logical resources in the same RouteFamily can collide unless callers self-prefix keys, which is a UX/invariant enforcement gap compared to domains that encode identity into the keyspace.

---

## V-008 — KV error taxonomy exists but is not realized in mapping

**Invariant / intent being violated**

- “Error semantics: explicit, typed, and domain-appropriate; retryable vs fatal distinguished consistently.”

**Evidence**

- KV defines typed error variants including retryable categories (`Conflict`, etc.), but `map_midge_error` maps all Midge errors to `BackendError`.

**Code paths**

- Mapping function: [src/domains/kv/actor.rs](src/domains/kv/actor.rs#L392-L396)

**Why it matters**

- Callers cannot reliably distinguish retryable vs fatal storage failures via KV’s advertised error surface.

---

## V-009 — Stream store “single-record read” fast-path opens a transaction twice

**Invariant / intent being violated**

- Hot-path discipline: minimal overhead and avoid unnecessary work.

**Evidence**

- `read_resource_single` begins a read-only transaction, then begins a second read-only transaction before calling `get`.

**Code paths**

- Duplicate begin_tx in `read_resource_single`: [src/domains/stream/store.rs](src/domains/stream/store.rs#L473-L513)

**Why it matters**

- Extra transaction setup work in the stated fast path; likely unintended.
