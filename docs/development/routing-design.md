# Fitz Routing and Stream Ordering Design

Status: **authoritative design and implementation contract**

This document is the single design contract for route patterns used by Fitz
reads, subscriptions, watches, and worker registrations. It also defines the
storage and execution design required to support the complete Stream selector
surface with correct ordering and bounded cost.

The authoritative domain meanings remain in
`docs/development/domain-boundaries-spec.md`, and the architectural constraints
remain in `docs/development/architectural-laws.md`. This design does not turn a
live registration into replay, give an ephemeral domain durability, or merge
the application-visible `realm` with the broker-internal `RouteFamily`.

## 1. Decisions

The design makes these decisions explicitly:

1. Fitz route wildcards are whole-segment `*` and `**` matchers.
2. KV, Queue, and Schedule use the generic fixed-depth pattern language.
   Notice and RPC use the generic flexible-depth language. Lease remains exact
   only. Stream uses an explicit, typed selector grammar.
3. Authorization proves that one grant covers the request's complete concrete
   match set. Intersection alone is insufficient.
4. Stream supports all eight literal-or-`*` selectors over
   `{realm}/{area}/{resource}`, plus two deliberate `**` aliases.
5. A Stream selector is ordered by the narrowest scope containing all of its
   possible matches. Literal segments below that scope are filters, not cursor
   dimensions.
6. Global Stream selectors use one real, contiguous, family-global offset
   space. Sorted traversal of independent realms is not global order.

Stream request completion is ordered within one resource, not across
independent resources. Different resources in the same `RouteFamily` may be
executed concurrently, so their client responses may arrive in either order.
Durable resource, area, realm, and family-global offsets plus captured
watermarks are the only cross-resource ordering authority.
7. One family-keyed ordering coordinator serializes only exact global-range
   assignment. Area and realm offsets are assigned by the resource data
   transaction and their counters commit atomically with the records. Resource
   transactions remain concurrent and retry bounded storage conflicts; no
   broader-scope process lock is held through a resource commit.
8. Area, realm, and global watermarks are visibility frontiers. Area and realm
   frontiers follow their atomically committed allocation heads, so failed
   transactions cannot leave gaps. The global completion tracker accepts
   out-of-order committed or skipped ranges and advances only across the
   highest contiguous resolved prefix.
9. Resource and global fragments are the two payload views. Area and realm
   fragments plus sparse filtered selectors use compact locators that hydrate
   global fragments by logical offset. Payloads through 16 KiB therefore occur
   exactly twice; larger payloads occur once in a checked immutable blob.
10. Global Stream continuations are selector-bound snapshots that resume after
    the last examined scope offset, not merely the last returned record.
    Resource-, area-, and realm-scoped READ and LAST responses retain their
    established wire layouts; extending them requires a separately versioned or
    negotiated operation.
11. The new Stream storage layout is a clean break. A broker does not silently
    omit pre-layout history, lazily backfill it, or mix cursor models. Existing
    Stream data must be exported/replayed into a fresh store or intentionally
    reset before activation.

### 1.1 Authority and current implementation

This document supersedes the former `docs/development/route-design.md` and is
the sole routing design contract. The table below records the implemented
state and the remaining validation work.

| Surface | Current implementation | Target status |
| --- | --- | --- |
| Queue concrete route depth | Rejects trailing segments | Aligned |
| Lease concrete route depth | Uses the first three segments and tolerates a trailing suffix | Gap: reject trailing segments |
| Stream selector classification | All eight literal/`*` kinds and two aliases | Aligned |
| Resource, area, and realm reads | Dedicated offset spaces and visibility frontiers | Aligned |
| Global and global-filter reads | Direct family-global pages in assigned commit order | Aligned |
| Global storage citizens | Durable counter, watermark, discriminator, immutable pages, four postings, and writer epoch | Aligned |
| Read continuation | Global selectors use captured-watermark cursors with keyed integrity tokens; existing scopes retain the legacy cursor wire layout | Aligned |
| Filtered selector execution | Sparse posting fragments with exact parent-fragment locators | Aligned |
| Subscription delivery | Bounded pending delivery gated by selector frontier | Aligned |
| Performance validation | Correctness and work bounds implemented | Contention and amplification benchmarks remain release evidence, not a semantic gap |

## 2. Terms

| Term | Meaning |
| --- | --- |
| Concrete route | A scheme plus literal path segments naming one domain identity. |
| Exact selector | A concrete route used to select exactly one identity. |
| Pattern | A route containing `*` or `**` and describing concrete routes. |
| Match set | Every concrete route that a pattern can match. |
| Read scope | The ordered namespace whose offsets govern a read. |
| Route filter | A realm, area, or resource predicate applied within a broader ordered scope. |
| Record filter | A predicate over record metadata or discriminator values. |
| Watermark | A contiguous durable visibility frontier. Area and realm retain their inclusive frontier representation and have no reserved-only gaps; the global plane uses an exclusive frontier and may contain explicitly resolved skipped offsets. |
| Cursor | A versioned continuation position in one read scope and selector. |
| Live registration | Session-owned state matching future concrete events; it provides no replay. |
| Route family | Broker-internal routing and isolation key, orthogonal to `realm`. |

## 3. Shared route grammar

| Syntax | Meaning |
| --- | --- |
| Literal | Matches that complete segment exactly. |
| `*` | Matches exactly one complete segment. |
| `**` | Matches zero or more complete segments. |

All domains follow these validation rules:

- The scheme is mandatory and must match the operation's domain.
- Empty path segments are invalid.
- Wildcards occupy an entire segment. `user*`, `**suffix`, and
  `prefix*tail` are invalid.
- Routes and patterns are limited to 4 KiB and 64 non-empty path segments.
- Concrete operations reject every wildcard, except for the legacy Stream
  `LAST`/metadata wildcard no-op described in Section 8.
- Matching never crosses a `RouteFamily` boundary.
- A missing or unknown `realm` remains missing or unknown; it is never inferred
  from `RouteFamily`.
- Adjacent `**` segments add no expressive power and must be rejected or
  canonicalized to one `**` before retention.
- The broker compiles retained patterns once at registration time.

### 3.1 Generic fixed-depth patterns

For a concrete route of depth `D`:

- Without `**`, a pattern has exactly `D` segments, each literal or `*`.
- With `**`, the non-`**` segments number at most `D`, and the pattern must be
  capable of matching a concrete route of exactly depth `D`.

Separated double-stars remain meaningful where they express ordered literal
subsequences. Stream is excluded from this generic rule and accepts only the
explicit selectors in section 8.

The complete literal-or-`*` basis for a three-segment identity is:

| Realm | Area | Resource | Shape |
| --- | --- | --- | --- |
| Literal | Literal | Literal | `{realm}/{area}/{resource}` |
| Literal | Literal | `*` | `{realm}/{area}/*` |
| Literal | `*` | Literal | `{realm}/*/{resource}` |
| Literal | `*` | `*` | `{realm}/*/*` |
| `*` | Literal | Literal | `*/{area}/{resource}` |
| `*` | Literal | `*` | `*/{area}/*` |
| `*` | `*` | Literal | `*/*/{resource}` |
| `*` | `*` | `*` | `*/*/*` |

### 3.2 Flexible-depth patterns

Notice subscriptions and RPC worker registrations accept any non-empty
sequence of literals, `*`, and `**` within the shared size bounds. A published
Notice route and an RPC request route are still concrete.

### 3.3 Registration limits

KV, Queue, Notice, Stream, RPC, and Schedule cap retained wildcard
registrations at 128 per session. Exact registrations do not count against that
wildcard cap, but production safety also requires explicit limits for total
registrations, matching fanout, queued deliveries, and active route caches.

Duplicate registrations are idempotent only when the operation's public
identity says they are the same registration. Distinct overlapping patterns
remain independent.

## 4. Authorization

Authorization operates on concrete-route languages:

- A grant must cover every concrete route that the requested pattern can
  select.
- A grant that only intersects the requested pattern is rejected.
- Multiple partial grants are not silently unioned into one covering grant.
- Fixed-depth containment is evaluated after restricting both languages to the
  domain's concrete depth.
- Deliberate aliases are canonicalized before authorization or compared as the
  same domain-restricted language.
- Permission patterns and client selectors are related languages but not the
  same API. A permission may authorize many exact Lease operations even though
  Lease rejects wildcard subscriptions.

Permission strings serialize as `{route_pattern}#{access}`, where access is
`read`, `write`, or `*`; the suffix is optional and defaults to `*`. The final
`#...` suffix is authorization metadata: it is parsed and removed before the
route pattern is compiled, and it is never a route segment or part of a Stream
selector. Examples include
`stream://acme/**#read`, `stream://*/orders/*#read`, and
`stream://**#read`. The remaining route language is checked for containment
using the requested operation's required access.

Ingress parsing, authorization classification, dispatch, read planning, and
subscription matching must derive from the same selector classification or
equivalent shared source of truth. A pattern accepted by one layer and
interpreted differently by another is a correctness defect.

## 5. What support means

A pattern is supported only when both correctness and performance contracts are
complete.

### 5.1 Correctness contract

1. Grammar and alias behavior are unambiguous in every layer.
2. Authorization covers the entire match set.
3. Matching, indexes, cursors, and cleanup are isolated by `RouteFamily`.
4. Every multi-route result and live delivery identifies its concrete route.
5. Overlapping retained registrations behave independently unless the domain
   explicitly defines single-delivery selection.
6. The original registration identity remains usable for unsubscription even
   if its execution form is canonicalized.
7. Disconnect removes all session registrations, worker credit, waiters,
   matching-cache references, and ephemeral cursors.
8. Live registrations observe future events only. Replay remains an explicit
   operation of Stream.
9. Invalid patterns, unavailable indexes or watermarks, inventory failures,
   and cursor mismatches return errors instead of narrowing results silently.
10. Restart reconstructs every durable catalog, ordered index, counter, and
    watermark required by the contract.

### 5.2 Performance contract

1. Concrete routes use direct hash/keyed lookups.
2. Wildcard registrations use a segment trie or equivalent index, never a full
   registration scan on each event.
3. Client-controlled work is bounded separately by returned items, examined
   items, matched identities, bytes, fanout, queued delivery, and execution
   slice where applicable.
4. Resumable reads return a cursor after the last examined position, including
   zero-result pages. A mutating operation without continuation rejects an
   over-broad selector before changing state.
5. Storage, actor-map, and inventory guards are not held across delivery,
   flushing, or unrelated I/O.
6. Concrete route values are parsed/formatted once per source run and shared
   across its items.
7. Metrics expose patterns examined, identities examined, records examined,
   results returned, fanout, cursor resumes, and budget exhaustion.

## 6. Domain overview

| Domain | Concrete identity | Wildcard read | Live pattern operation | Historical replay |
| --- | --- | --- | --- | --- |
| KV | `kv://{realm}/{area}/{resource}` | No | `SUBSCRIBE` / `UNSUBSCRIBE`, generic depth 3 | No; current state only |
| Queue | `queue://{realm}/{area}/{resource}` | `RESERVE`, generic depth 3 | `WATCH` / `UNWATCH`, generic depth 3 | Durable work, not history |
| Notice | Flexible non-empty route | N/A | `SUBSCRIBE`, flexible depth | No; live fanout |
| Stream | `stream://{realm}/{area}/{resource}` | `READ`, typed matrix | `SUBSCRIBE` / `UNSUBSCRIBE`, same matrix | Yes |
| RPC | Flexible non-empty route | No | Worker `REGISTER` / `UNREGISTER`, flexible depth | No; live request/response |
| Lease | `lease://{realm}/{area}/{resource}` | No | Exact `SUBSCRIBE` / `UNSUBSCRIBE` only | No; ephemeral coordination |
| Schedule | `schedule://{realm}/{area}/{resource}/{operation}` | No route selector for `LIST` | `SUBSCRIBE` / `UNSUBSCRIBE`, generic depth 4 | Durable definitions, live fires |

## 7. Non-Stream domains

### 7.1 KV

| Operation | Route form |
| --- | --- |
| `BEGIN` | Exact resource |
| `GET`, `SCAN`, mutations, commit/rollback | The transaction's exact resource |
| `SUBSCRIBE`, `UNSUBSCRIBE` | Exact or generic depth-3 pattern |

Route wildcards never apply to KV keys. `SCAN` is an ordered key range inside
one concrete resource. An omitted limit resolves to a bounded server default,
and an explicit limit is capped at ingress.

Committed mutation notices carry the concrete resource route and subscription
identity. Failed, rolled-back, or read-only transactions emit no mutation
notice. Exact watches use a concrete-route map; wildcard watches use the shared
matcher. Fanout must scale with matches rather than total registrations.

### 7.2 Queue

| Operation | Route form |
| --- | --- |
| `SEND`, `EXTEND`, `ACK` | Exact queue |
| `RESERVE` | Exact or generic depth-3 selector |
| `WATCH`, `UNWATCH` | Exact or generic depth-3 pattern |

Queue inventory must include every live or persisted queue that may contain
reservable work. Inventory failure is an error, not an empty result. Each
wildcard reservation returns its concrete queue route and remains scoped to
that queue for token validation, extension, and acknowledgement.

Reserve batch size is capped at 1,024. Exact reserve is a direct actor lookup.
A wildcard pass inspects a selected queue at most once, asks it for up to the
remaining batch capacity, skips queues found empty for the rest of the pass,
and rotates the starting queue to avoid lexical starvation.

Catalog work is bounded separately from the message batch. Because reserve
mutates state and its response has no selector continuation, a selector that
exceeds the matched-queue or inventory work cap is rejected before reserving
anything. Delayed promotion and inflight expiration run before an inspected
queue is declared empty. Watches are readiness hints, not delivery or replay.

### 7.3 Notice

| Operation | Route form |
| --- | --- |
| `PUBLISH` | Concrete flexible-depth route |
| `SUBSCRIBE` | Exact or flexible-depth pattern |

Notice is ephemeral fanout. Exact subscriptions use a direct map and wildcard
subscriptions use the shared trie. Publishing is proportional to matching
registrations plus delivery cost. Per-session backlog and total fanout are
bounded; slow-consumer policy is explicit. No catalog, cursor, watermark, or
replay state is created.

### 7.4 RPC

| Operation | Route form |
| --- | --- |
| `REQUEST` | Concrete flexible-depth route |
| Worker `REGISTER`, `UNREGISTER` | Exact or flexible-depth pattern |

One request is assigned to at most one eligible worker. Registration credit is
owned per registration even when one worker has overlapping patterns. Fairness
must cover exact and wildcard matches without an undocumented priority rule.

The broker matches a newly seen route through an indexed registration trie and
caches the resulting registration IDs for that concrete route. Adding a broad
registration uses a reverse index or bounded incremental reconciliation rather
than scanning an unbounded active-route cache synchronously. Cache invalidation,
pending requests, correlations, worker queues, and active route entries are all
bounded and cleaned on disconnect.

### 7.5 Lease

| Operation | Route form |
| --- | --- |
| `QUERY`, `ACQUIRE`, `EXTEND`, `RELEASE` | Exact depth-3 route |
| `SUBSCRIBE`, `UNSUBSCRIBE` | Exact depth-3 route only |

Wildcard Lease subscriptions remain invalid. The exact route, including
`RouteFamily`, identifies the lease and its fencing sequence. Queries and
notifications account for expiration before reporting ownership. Exact watch
lookup needs no matcher. Timers and wait queues use indexed, bounded state.
Subscriptions do not make ownership durable across restart or reconnect.

### 7.6 Schedule

| Operation | Route form |
| --- | --- |
| `CREATE`, `CANCEL` | Exact depth-4 route |
| `CREATE_BATCH` | Exact route per entry; one atomic batch |
| `LIST` | No client route selector; requires global `schedule://**` read permission |
| `SUBSCRIBE`, `UNSUBSCRIBE` | Exact or generic depth-4 pattern |

Schedule subscriptions accept the complete 16-shape literal-or-`*` basis and
generic depth-4 `**` compositions. Fire notifications contain the concrete
route. Broadcast attempts each matching registration; single mode selects at
most one accepted live handoff using bounded, pruned per-route fairness state.
Neither mode turns a fire notification into durable downstream delivery.

`LIST` reads durable definitions, not notification history. It uses a stable
ordered index, a bounded default/maximum limit, and a versioned snapshot cursor
if snapshot-consistent pagination is promised. A numeric position alone cannot
prevent duplicates or omissions across concurrent create/cancel operations.

## 8. Stream selector contract

Stream concrete identity has exactly three dimensions:

```text
stream://{realm}/{area}/{resource}
```

Append, `LAST`, metadata mutation, and metadata lookup require a concrete
route. `READ`, `SUBSCRIBE`, and `UNSUBSCRIBE` accept the selectors below.
Every `READ` item, including an exact-resource item, carries its resolved
concrete route. For wire compatibility, `LAST` and `GET_METADATA` retain their
older empty-success response for area and realm selectors whose area or
resource segment is `*`; they never create wildcard-named state. A wildcard
realm paired with concrete area/resource segments is rejected by the concrete
actor-key guard.

### 8.1 Complete selector matrix

| Kind | Selector | Match set | Order and watermark | Route filter |
| --- | --- | --- | --- | --- |
| `Resource` | `{realm}/{area}/{resource}` | One resource | Resource offset | None |
| `Area` | `{realm}/{area}/*` | All resources in one area | Area offset | None |
| `RealmFilterResource` | `{realm}/*/{resource}` | Resource name across one realm | Realm offset | Resource |
| `Realm` | `{realm}/*/*` | All records in one realm | Realm offset | None |
| `GlobalFilterAreaResource` | `*/{area}/{resource}` | Area/resource pair across all realms | Global offset | Area and resource |
| `GlobalFilterArea` | `*/{area}/*` | Area name across all realms | Global offset | Area |
| `GlobalFilterResource` | `*/*/{resource}` | Resource name across all realms/areas | Global offset | Resource |
| `Global` | `*/*/*` | Every record in the family | Global offset | None |

Two `**` aliases are deliberately supported:

| Alias | Canonical execution kind | Equivalent fixed-depth selector |
| --- | --- | --- |
| `{realm}/**` | `Realm` | `{realm}/*/*` |
| `**` | `Global` | `*/*/*` |

The alias and expanded spelling have the same authorization language and read
plan. Registration identity retains the original spelling so an unsubscribe
can remove the registration the client created.

Other Stream `**` forms are rejected by policy. Examples include
`{realm}/{area}/**`, `{realm}/**/{resource}`, `**/{resource}`, and adjacent
`**/**`. They add aliases without adding a new match set to the eight-shape
matrix and make canonical authorization and cursor binding harder.

Reserved internal Stream segments such as `__realm__` and `__area__` are not
valid client area/resource literals where the implementation uses them for
coordination. The family-keyed global coordinator should use internal keyed
actor addressing rather than consume a client route segment, so this design
introduces no `__family__` route.

### 8.2 Watermark rule

The first wildcard dimension determines the ordered scope:

1. No wildcard: resource order.
2. Wildcard resource within a literal area: area order.
3. Wildcard area within a literal realm: realm order.
4. Wildcard realm: family-global order.

A literal below that scope is only a filter. For example,
`stream://*/orders/created` is globally ordered and filtered by area and
resource. It never switches between per-area or per-resource cursors.

## 9. Stream physical design

### 9.1 Typed planning

Parsing produces one typed selector enum shared by authorization, reads, and
subscriptions. Reads select one physical plan:

| Selector kind | Primary execution plan | Governing frontier |
| --- | --- | --- |
| `Resource` | `CompactResourcePage` | Resource committed offset |
| `Area` | `CompactAreaPage` | Area watermark |
| `RealmFilterResource` | Realm-resource posting index, then realm pages | Realm watermark |
| `Realm` | `CompressedCompactRealmPage` | Realm watermark |
| `GlobalFilterAreaResource` | Global area-resource posting index, then global pages | Global watermark |
| `GlobalFilterArea` | Global area posting index, then global pages | Global watermark |
| `GlobalFilterResource` | Global resource posting index, then global pages | Global watermark |
| `Global` | `CompactGlobalPage` | Global watermark |

Exact resource and global fragments are payload-bearing. Area and realm
fragments carry checked global locators, and posting indexes carry checked
parent-scope locators. Readers cache each distinct parent fragment during a
read slice. Bodies plus metadata totaling at most 16 KiB are inline in the two
payload views; larger payloads are stored once in an immutable blob keyed by
family-global offset, with checksum-verified references in both views.

### 9.2 Key registry

Existing resource, area, and realm tags remain unchanged. This design reserves
the following free tags, subject to a final registry collision check during
implementation:

| New citizen | Prefix | Key scope |
| --- | ---: | --- |
| Global counter | `0x12` | `[family]` |
| Global watermark | `0x13` | `[family]` |
| Global discriminator | `0x14` | `[family][global_offset]` |
| Family writer epoch | `0x15` | `[family]` |
| Compact global fragment | `0xEB` | `[family][bucket][first_global_offset][generation]` |
| Realm-resource posting fragment | `0xEC` | `[family][realm][resource][bucket][first_realm_offset][generation]` |
| Global-area posting fragment | `0xED` | `[family][area][bucket][first_global_offset][generation]` |
| Global-resource posting fragment | `0xEE` | `[family][resource][bucket][first_global_offset][generation]` |
| Global-area-resource posting fragment | `0xEF` | `[family][area][resource][bucket][first_global_offset][generation]` |
| Large payload blob | `0xF0` | `[family][global_offset]` |

The family is encoded as storage partition/isolation state, never as a route
segment and never derived from `realm`.

### 9.3 Global page record

The global page key supplies the global page start; entry position supplies the
global offset. Each entry contains enough data to return the record without
opening a resource actor:

```text
GlobalPageRecord {
    realm,
    area,
    resource,
    resource_offset,
    area_offset,
    realm_offset,
    body,
    metadata,
    created_at
}
```

The exact concrete route is reconstructed once per contiguous source run and
shared by routed response items. A global read exposes the global offset in
addition to narrower offsets that are stable at the visibility boundary.

### 9.4 Posting entries

Posting indexes preserve the order of their parent scope:

- A realm-resource posting page stores sparse realm offsets and the realm page
  locator needed to fetch their records.
- Global posting pages store sparse global offsets and global page locators.
- Resource, wider-scope data, and posting writes are immutable commit fragments
  split at fixed 64-offset bucket boundaries and keyed by their exact first
  assigned scope offset. Appending never reads or rewrites historical payload
  fragments, and concurrent resource commits write distinct keys instead of
  contending on a mutable tail page.
- Background compaction may merge adjacent fragments into larger pages after
  they are below the governing watermark. Readers accept both representations.
- One synchronous maintenance slice examines at most eight buckets or 4 MiB.
  Successful commits enqueue only their touched bucket prefixes. The first
  maintenance slice after restart rebuilds pending work with one lazy family
  scan; later slices consume the queue without rescanning the family history.
  `fitz_stream_maintenance_attempts_total`, `_failures_total`,
  `_retries_total`, and `_buckets_compacted_total` expose process-local
  maintenance progress. A retry is counted only when this process previously
  observed the failed attempt; restart discovery is a fresh attempt.
  The Tokio-owned background loop only enqueues an internal Stream command;
  bucket discovery, merging, and transactions execute synchronously on the
  domain actor. Failures are logged and retained for a later command rather
  than failing the actor or emitting a client notification.
  Each replacement uses one more than the highest source generation and fails
  closed on generation exhaustion. Replacement and source deletion commit
  atomically, and maintenance emits no client notification.
- Posting fragments are batched and compact; they contain no body or metadata
  copy.
- Every posting entry is written in the same transaction as its parent page.
- A posting miss below the governing read frontier advances the cursor to that
  frontier, so an empty suffix can complete.

The public filtered selectors are performance-complete only when these posting
indexes exist. A bounded parent-page scan can be useful during internal rollout
or recovery tooling, but it is not the intended steady-state plan for sparse
production reads.

## 10. Stream commit, concurrency, and global order

### 10.1 Watermarks do not serialize resources

Resource actors remain the owners of resource-local append order. Commits to
different resources may write storage concurrently, including resources in the
same area, realm, or `RouteFamily`.

The broader ordered scopes use two allocation mechanisms:

1. The family ordering coordinator briefly assigns a non-overlapping global
   range before the data transaction.
2. The data transaction reads, conditionally updates, and commits the area and
   realm heads together with all record views.

Use one family-keyed synchronous coordinator as the global ordering authority.
A reservation identifies the batch size, advances only the global allocation
head, and returns one exact global range plus the persisted writer epoch. The
resource data transaction then assigns exact area and realm ranges from their
current committed heads. It uses storage write-conflict detection and bounded
retry when another resource wins either shared counter.

For a batch of `N` records, the resource path reserves this exact global range:

```text
[first_global_offset, first_global_offset + N)
```

The coordinator serializes only this global counter operation. After the grant
returns, the resource transaction holds no area, realm, or family process
guard. Two resources can therefore encode and submit transactions concurrently.
Area/realm write conflicts are retried from the newly committed heads. Global
order is reservation order, not wall-clock completion order; area and realm
order is successful data-transaction commit order.

Exact-sized reservations are the baseline. Speculative 10,000-offset leases at
realm or global scope can leave unused holes that stall a broad watermark. A
block lease is valid only if rollover, actor shutdown, and disconnect resolve
every unused tail as skipped before later offsets become visible.

### 10.2 Durable allocation heads

The global counter is a reservation head and may be ahead of durable records.
Area and realm counters are committed heads: their updates live in the same
atomic transaction as the records and postings using those offsets. A failed or
abandoned data transaction therefore advances neither counter and cannot freeze
an area or realm frontier.

Every global reservation carries the current persisted family writer epoch.
The data transaction verifies the value and writes the unchanged epoch back
into its conflict-checked write set. A recovery transaction that advances the
epoch therefore either follows the old writer's commit or forces that old
writer to abort; a stale writer cannot commit after the durable fence.

Counter persistence is the unavoidable serialization point for one global
order, but it does not include body writes, page encoding, posting writes, or
the resource transaction. Batching amortizes it per commit, not per event.

Each global reservation has a stable identity and exactly one terminal outcome:

- `Committed`: its data transaction became visible under the selected write
  policy.
- `Skipped`: the transaction failed or was abandoned and no record will ever
  occupy the range.

Retries for the same logical commit reuse its unresolved reservation. A caller
must not allocate a fresh range while the earlier range can still commit. If a
retry changes the logical commit identity, including its event count, the old
reservation is first resolved as skipped and only then may the replacement
range be allocated.

### 10.3 Concurrent data transaction

Once the global offset is reserved, one atomic resource transaction writes:

1. Reads and transactionally verifies the persisted family writer epoch, then
   includes that key in conflict validation.
2. Reads the current area and realm counters and assigns exact ranges.
3. Resource, area, realm, and global immutable page fragments.
4. Resource, area, realm, and global discriminator rows where present.
5. Realm-resource and applicable global posting fragments.
6. Area and realm counters plus resource metadata; the atomic scope fragments are the durable commit
   evidence used by recovery.

The transaction does not advance a broader watermark. It also does not update a
shared wider-scope tail page: every fragment is keyed by the reservation's
first scope offset, so concurrent commits do not overwrite one another.

On success, the resource path reports the exact area, realm, and global ranges
as committed. On terminal failure only the already-reserved global range is
resolved as skipped; no area or realm range exists to repair. Reporting the
global outcome is part of commit cleanup, not best-effort observability.

### 10.4 Contiguous completion tracking

The global tracker retains resolved ranges ordered by first offset. Given the
exclusive global watermark `W`, it consumes the next range beginning at `W`
and stops at the first unresolved reservation.

For example, if global range `[100, 110)` is slow and `[110, 120)` commits
first, global reads remain bounded below 100. When `[100, 110)` commits or is
durably skipped, the tracker can advance through both ranges in one step.

The global tracker persists the new watermark before treating it as visible or
publishing a watermark notification. Persistence failure leaves the old
watermark active and retains the completed range for retry. Once the event
transaction is durable, a later watermark-persistence failure does not turn
the append into a client-visible failure: reads and later completions retry the
pending frontier so the client cannot duplicate an already-durable batch.
Notifications, metrics, and admin projections observe the persisted frontier
but never define it.

For area and realm reads, `next_offset - 1` is a safe inclusive frontier because
the counter and its records commit atomically. Coordinator actors may persist a
coalesced copy and publish ephemeral watermark notices, but reads and
subscription gating use the maximum of that advisory row and the committed
counter. Coordinator capacity is bounded without evicting live actors. A full
coordinator pool or mailbox can lose an ephemeral notice; it cannot discard an
existing coordinator or hide durable history.

A skipped offset is below the watermark but has no record. Readers advance over
it exactly as they advance over an expired/tombstoned offset.

### 10.5 Failure and restart rules

No global reserved range may remain permanently unresolved:

- Normal transaction failure resolves the reservation as skipped.
- Actor/session teardown resolves every reservation that can no longer commit.
- A family coordinator failure fails the family closed until recovery; a new
  coordinator is not allowed to overlap old in-flight commits.
- Recovery first stops new reservations, atomically advances the family writer
  epoch with synchronous durability, and installs that epoch in the new
  coordinator. Every old-epoch transaction must fail its transactional epoch
  guard even if it began before the fence was advanced.
- Only after the fence is durable may recovery inspect global page-fragment
  evidence, classify every absent reservation below the allocation head as
  skipped, and persist the allocation head as the repaired resolved frontier
  before accepting Stream traffic. The repaired frontier may be beyond the
  highest durable record precisely because fenced missing reservations are now
  terminal skipped ranges.

The storage engine must provide a conditional/serializable epoch check whose
success cannot race a committed epoch increment. If it cannot, recovery fails
closed; a process-local generation check is insufficient. Epoch exhaustion also
fails closed. Missing rows from a transaction with durable commit evidence are
corruption, not an aborted reservation.

### 10.6 Capacity consequence

True global order requires one cheap allocation sequence per family. It does
not require one body-write sequence per family. A sharded global counter is not
equivalent because merging shards removes the single contiguous cursor and
recreates the composite-order problem.

Capacity work therefore focuses on batch size, allocation-head latency,
immutable fragment density, completion-map size, watermark persistence
coalescing, and parallel resource transactions. Independent `RouteFamily`
values remain fully parallel.

## 11. Stream read semantics

### 11.1 Snapshot boundary

The first page of a global read captures the family-global watermark. Every
global continuation is bounded by that captured watermark, producing a stable
finite snapshot. Records committed later require a new read or live
subscription. Resource reads remain ungated. Area and realm reads retain their
legacy wire contract and use the current committed frontier on each request;
they do not claim snapshot pinning without a versioned cursor.

An initial `from_offset` is inclusive. A continuation resumes strictly after
the last examined scope offset. The two forms are never interpreted
interchangeably. A zero-limit global read examines nothing, leaves
`last_global_offset` absent, and binds its integrity token to the unchanged
request offset so resuming the returned cursor cannot skip a record.

### 11.2 Global structured cursor and wire boundary

The structured snapshot cursor is encoded only for selectors governed by the
family-global watermark: `stream://**`, `stream://*/*/*`, and the three global
filtered forms. Their event records add `global_offset` after `realm_offset`,
and their cursor adds `last_global_offset`, the integrity token, and the
captured watermark.

Existing resource-, area-, and realm-scoped READ responses keep the prior
record and cursor byte layout. LAST also keeps the prior record layout. The
broker must not insert an absent `global_offset` presence byte into those
records or append snapshot fields to those cursors: even an absent optional
value changes field positions for existing decoders. Extending those operations
requires a new message type or explicit protocol-version negotiation.

A cursor is versioned, integrity-protected, and binds at least:

```text
ReadCursor {
    version,
    route_family,
    canonical_selector_kind,
    canonical_selector_values,
    read_scope,
    last_examined_scope_offset,
    captured_watermark,
    stable_record_filter_digest,
    page/work-budget version
}
```

The filter digest uses a stable specified encoding/hash, not a process-random
or implementation-default hasher. The broker rechecks current authorization on
every page. It rejects a cursor used with another family, selector, filter,
scope, or incompatible budget/version.

The cursor advances across:

- route-filter nonmatches,
- records suppressed by TTL/compaction metadata,
- emitted record-filter markers,
- byte-limit boundaries,
- examined-work exhaustion, and
- pages that return zero records.

### 11.3 Route and record filtering

Route filters are pushed into posting indexes and never emitted as response
items. Record filters run after the route-selected record is fetched and retain
the existing Stream semantics: a filtered record or compact filtered range is
emitted when the protocol requires it, consumes result budget, and advances in
the governing scope.

Permission filtering is never performed record by record. The complete selector
must be authorized before reading.

### 11.4 Work bounds

Every read enforces server defaults and maxima for:

- returned items,
- returned bytes,
- examined parent offsets,
- posting entries examined,
- storage pages fetched,
- elapsed synchronous work slice, and
- cursor size/version.

Area, realm, and direct global fragment reads start at the containing 64-offset
bucket but fetch only the fragments needed to cover the skipped bucket slots,
the requested result budget, and one `has_more` sentinel, subject to the hard
examined-work cap. A small result limit therefore does not decode the full
1,024-fragment safety window.

For a global read, `has_more` means more work may remain below the captured
watermark. For a legacy-scope read, it means more work may remain below the
frontier used by that request. On
work exhaustion, the response can contain no records and still return a
forward-moving continuation. Repeated continuation must eventually reach the
watermark even for a selector with no matches.

## 12. Stream subscriptions

`SUBSCRIBE` and `UNSUBSCRIBE` use the same typed selector matrix as reads, but
subscriptions remain live and session-scoped:

- Exact registrations use a direct route map.
- Wildcard registrations use the shared `RoutedSubscriptionSet` segment trie.
- A concrete committed route is parsed once and matched against retained
  registrations.
- The original selector string/registration identity is retained for removal.
- Syntactically distinct aliases may remain independent registrations even
  though they share one compiled execution selector.
- Overlapping registrations each receive their eligible delivery.
- Notification queues and fanout are bounded.

Delivery waits only for the selector's governing visibility frontier:

| Selector kinds | Delivery frontier |
| --- | --- |
| `Resource` | Resource commit visibility |
| `Area` | Area watermark |
| `RealmFilterResource`, `Realm` | Realm watermark |
| All global kinds | Global watermark |

A narrow registration must not wait behind an unrelated broader-scope gap.
Each delivery carries the concrete route and only offsets that are readable at
that frontier. A broader offset is not advertised as resumable before its page,
posting entry, and watermark are visible. Visibility advancement caused by a
terminally skipped or explicitly abandoned reservation also drains pending
deliveries; it does not depend on a later publish event arriving by chance.

Historical-to-live handoff needs an explicit checkpoint protocol. Until one is
defined, clients subscribe and read with documented race handling and
deduplication; the broker must not imply an atomic handoff that it does not
persist.

The existing shared trie is the initial subscription design. Eight specialized
subscription maps would duplicate matcher state and are justified only by
profiled evidence that the shared trie is the bottleneck.

## 13. Storage activation, recovery, and compaction

### 13.1 Clean-break activation

The global page, postings, counter, discriminator, watermark, and cursor model
form one new Stream storage generation. Activation follows Fitz's existing
clean-break policy:

- A fresh/empty family initializes the new marker and all citizens lazily.
- A family containing an older Stream generation is rejected with
  `ResetRequired` before reads or writes are accepted.
- The broker performs no compatibility scan, hydration, mixed-layout read,
  lazy backfill, or silent “global order starts now” behavior.
- Operators preserve history by exporting/replaying source events into a fresh
  store before cutover, or intentionally clear and rebuild Stream state.
- Rollback restores the previous broker together with its matching store
  snapshot.

This is deliberately stronger than allowing global reads to omit old history.
If preservation requirements later demand an automated conversion, it must be
an explicit offline migration with a separately reviewed order-assignment
contract, not a hidden activation path.

### 13.2 Boot recovery

For the new generation, boot validation loads the global reservation head and
watermark, then fences old writers by advancing the persisted family epoch.
Atomic global page fragments identify durable records; missing reservations
below the head become terminal skipped ranges only after that fence. A
missing/stale global counter is repaired to at least one past the highest
durable global fragment. Recovery finds that tail with a reverse, single-row
fragment scan rather than materializing the family history. Area and realm
counters require no reservation
recovery because they commit atomically with their records. Posting indexes
must not claim offsets without their parent transaction. Recovery completes
before Stream traffic for the family is accepted.

Recovery never derives `realm` from the family, never uses observability as a
correctness signal, and never advances a watermark across a reservation whose
outcome could still change.

### 13.3 TTL and compaction

TTL and page compaction update all ordered views consistently. Removing an
expired body must not leave a posting entry that causes an invalid fetch or a
cursor that can no longer advance. D4 persists the original absolute
expiration on every data record and posting.
Readers apply that deadline before payload or blob hydration. A compacted Midge
row receives a physical TTL ending at the latest contained deadline, so
fragment merging cannot extend a logical record lifetime or reclaim a live
record early. Uncompacted positional fragments retain logical deadlines but no
independent physical TTL; otherwise an older fragment could disappear while a
newer fragment in the same bucket remains, making an expired prefix
indistinguishable from corruption. Compaction retains a body-free positional
range tombstone when every record has expired; sparse postings can be removed
because they carry explicit offsets. Implementations may remove corresponding
postings transactionally or retain compact tombstone/range metadata, provided:

- parent-scope offset continuity remains explicit,
- a reader can advance to its governing frontier,
- no expired record is returned,
- restart reconstruction reaches the same state, and
- compaction work is bounded and benchmarked.

## 14. Implemented sequence

1. The typed Stream selector grammar is the sole classifier used by ingress,
   authorization, dispatch, reads, subscriptions, codecs, and tests.
2. The new Stream storage generation provides a durable global reservation
   head, atomic area/realm committed heads, exact global reservations, and a
   family-keyed global completion tracker.
3. Immutable global page/discriminator/posting fragments are written atomically with
   existing record views, then report committed or skipped range outcomes.
4. Direct global-page reads and the structured cursor replace sorted-realm
   traversal; composite realm continuation state has been removed.
5. Four posting indexes serve all filtered selector plans.
6. Subscriptions are gated on their governing visibility frontier and expose only
   readable offsets.
7. Examined-work limits, zero-result continuations, cursor integrity, and
   parent-fragment reuse bound sparse reads.
8. Recovery and activation validation cover every new storage citizen.
9. Correctness tests are part of the workspace suite. Contention, sparse-read,
   storage-amplification, and end-to-end benchmarks remain release evidence.

## 15. Required tests

### 15.1 Shared grammar and authorization

- Exact routes, every independent `*`, and beginning/middle/trailing `**` in
  generic-pattern domains; adjacent `**` is rejected or canonicalized.
- Empty segments, wrong schemes, partial wildcards, excessive depth/size, and
  patterns incapable of matching the required concrete depth.
- Full grant coverage, insufficient intersection rejection, alias equivalence
  in both directions, permission-suffix stripping, and no accidental grant union.
- Route-family isolation, duplicates, overlaps, registration caps, and cleanup.

### 15.2 Domain behavior

- KV transaction resource isolation, bounded scans, and committed-only notices.
- Queue exact/wildcard reserve, routed results, batch cap, cross-queue fairness,
  delayed/inflight processing, restart inventory, and pre-mutation over-cap
  rejection.
- Notice flexible-depth matching, fanout bounds, slow consumers, and no replay.
- RPC wildcard/exact selection, overlaps, credit, FIFO/correlation, incremental
  reconciliation, unregistration, timeout, and disconnect cleanup.
- Lease wildcard rejection, expiration, fencing, exact watches, and restart
  loss of ephemeral state.
- Schedule depth-4 matching, global `LIST` authorization, bounded and stable
  pagination, broadcast, single fairness, and cursor pruning.

### 15.3 Stream selectors and reads

- All eight selector kinds and both aliases through parser, authorization,
  codec, planner, storage, and subscription paths.
- Every noncanonical Stream `**` near-miss and reserved literal is rejected;
  every multi-route result and live delivery carries its concrete route.
- Initial-offset inclusivity, continuation exclusivity, and a stable captured
  global watermark while concurrent commits continue.
- Cursor advancement across route nonmatches, record-filter markers, TTL
  tombstones/ranges, byte limits, work limits, and zero-result pages.
- Global cursor rejection for family, selector, filter, scope, integrity, or
  version mismatch; legacy scopes reject structured snapshot cursor fields.
- Sparse posting reads whose last match is far below the watermark still
  complete at the watermark.

### 15.4 Global order and recovery

- Interleaved multi-realm commits receive contiguous, non-overlapping batch
  ranges, while two route families retain independent sequences.
- Concurrent commits to different resources can overlap after range assignment
  without a broader-scope lock or shared tail-page write.
- A later range may commit first while the watermark stops at the earlier gap;
  resolving the earlier range advances across both.
- Transaction failure writes no page, posting, discriminator, metadata, area
  counter, or realm counter row; only its already-advanced global reservation
  resolves as skipped so no watermark can stall.
- Retry of one logical commit reuses its unresolved reservation.
- Advancing the family writer epoch rejects a slow old-epoch transaction even
  when it began before the fence transaction.
- Restart repairs a missing/stale global counter from durable pages and may
  advance the watermark through fenced, absent reservations classified as
  skipped.
- Direct global pagination remains strictly ordered across realm boundaries.
- Storage activation rejects an older generation before traffic.
- TTL/compaction keeps global pages, postings, sidecars, and cursors coherent.

### 15.5 Performance and capacity

- Pattern match cost as total registrations grow with fixed match count.
- Fanout cost as matches grow.
- Queue wildcard reserve over many empty queues and sparse hot queues.
- Stream sparse realm/global filters with zero or few matches.
- Stream allocation-head latency plus commit throughput and tail latency under
  same-family concurrent resources, different-family parallelism, and varying
  batch sizes.
- Read amplification, write amplification, page density, posting density, and
  route allocations for every Stream plan.
- RPC new-route matching and new-registration reconciliation at cache limits.
- Schedule fairness state and cleanup under high route cardinality.
- Memory retained after mass disconnect.

Tests should assert work counters or complexity proxies as well as timing.
Wall-clock benchmarks alone cannot prove bounded examination.

A pattern is public only when it satisfies section 5 and the applicable tests
in section 15. Grammar, authorization, indexed execution, ordering, identity,
work bounds, lifecycle, recovery, and migration must all be complete.
