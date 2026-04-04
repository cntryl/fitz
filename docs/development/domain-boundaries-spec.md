# Fitz Domain Boundaries Specification

**Status**: Authoritative
**Audience**: Fitz maintainers, reviewers, and domain implementers
**Purpose**: Define the strict responsibilities, limits, and interaction rules for Fitz domains so the system stays coherent, predictable, and maintainable as it grows.
**Scope**: Notice, Stream, KV, Queue, RPC, Lease, and Schedule. This is an internal architectural contract, not end-user documentation.

## Intent

Fitz domains are intentionally narrow. Each domain owns one class of distributed systems problem and must stay narrow enough that its guarantees remain legible years from now. This document exists to stop domain drift before it becomes implementation drift.

This document defines the intended architectural contract. Current implementation, current tests, and the detailed domain contract files under `docs/todos/` remain the proof surface. If they diverge from this specification, treat that as a contract bug and resolve it explicitly.

This specification is prescriptive:

- clarity outranks convenience
- separation outranks clever abstraction
- explicit guarantees outrank inferred behavior
- long-term maintenance outranks short-term ergonomics
- domain boundaries are safety rails, not suggestions

## System Rules

These rules apply to every domain in this document:

- Sessions are ephemeral. Disconnect destroys session-owned state.
- Broker-side recovery exists only for explicitly persisted committed state.
- RouteFamily is a hard isolation boundary. Cross-family delivery, replay, or state bleed is a contract violation.
- Durable and ephemeral behavior must never be described with the same language.
- A domain may compose with another domain, but it must not silently inherit the other domain's guarantees.
- If a workflow needs multiple guarantees, it must compose multiple domains explicitly.
- If a guarantee is not stated here, clients must not infer it from implementation convenience.

## 1. Domain Responsibility

### Notice

Notice provides low-latency ephemeral fanout of live events to connected subscribers. It is optimized for live awareness, lightweight change signaling, and current-process delivery, and it does not attempt durability, replay, or recovery.

### Stream

Stream provides durable ordered append and replay for committed history. It is the Fitz recovery surface for clients that need rebuild, catch-up, auditability, or deterministic rereads from persisted offsets.
Clients resume from offsets they persist themselves; live Stream subscriptions are change notifications, not broker-managed replay cursors.

### KV

KV provides transactional storage for current authoritative state. It is the system of record for what a value is now within one resource scope, not a historical record of how that value changed over time.

### Queue

Queue provides durable competing-consumer work delivery with reservation, retry, redelivery, and optional dead-letter handling. It exists to manage work backlog and work lifecycle, not to expose durable history or live request-response semantics.

### RPC

RPC provides live request and response dispatch to currently registered workers. It is optimized for interactive execution with explicit correlation, bounded pending state, and immediate worker participation, not for restart-safe buffering or replay.

### Lease

Lease provides single-broker ownership coordination with TTL expiry and process-local fencing tokens. It exists to make explicit ownership and contention decisions inside one running broker, not to provide durable lock recovery or cross-node consensus.

### Schedule

Schedule provides durable timing intent for future work. It persists schedule definitions and due-state across restart so the broker can remember when something should happen later, without turning timing intent into workflow orchestration or durable downstream delivery.

## 2. Primary Use Cases

### Notice

- live UI updates
- cache invalidation hints
- presence updates
- operational alerts
- state change notifications for connected clients
- live subscription fanout for current observers

### Stream

- event sourcing
- audit logs
- rebuild read models
- analytics ingestion
- compliance history
- catch-up after disconnect
- backfill for derived systems

### KV

- configuration storage
- metadata storage
- authoritative current state
- current resource properties
- transactional key and value updates
- lookup of current truth for one resource scope

### Queue

- background jobs
- retries with bounded redelivery
- worker distribution
- delayed durable work
- dead-letter workflows
- backlog smoothing between producers and consumers

### RPC

- service execution
- request-response calls
- interactive operations
- worker dispatch that requires immediate answers
- bounded live task routing
- streaming responses for one live call

### Lease

- leader election inside one broker deployment unit
- distributed coordination at the client workflow layer
- ownership fencing
- singleton task ownership
- contention resolution for one logical owner
- guarded writes when the client wants explicit ownership first

### Schedule

- delayed jobs
- retries with delay when paired with Queue
- durable timers
- cron-style future triggers
- recurring task intent
- persisted time-based wakeups

## 3. Explicit Non-Responsibilities

### Notice must NOT:

- support replay
- support durability
- support backfill
- support recovery after disconnect
- become a stream
- become CDC history
- guarantee delivery after subscriber disconnect
- store broker-managed consumer positions

### Stream must NOT:

- become a queue
- become RPC
- become pub-sub for live awareness
- provide work distribution
- provide acknowledgements for mutable work lifecycle
- provide broker-managed consumer groups
- silently recover live writer sessions after disconnect

### KV must NOT:

- become CDC history
- become event replay
- become pub-sub
- become a queue
- become workflow storage
- imply cross-resource transactions
- imply durable recovery of open transactions

### Queue must NOT:

- become pub-sub
- become stream replay
- become RPC
- become event storage
- imply exactly-once delivery
- turn live lease tokens into durable recovery handles
- hide work backlog as generic messaging

### RPC must NOT:

- become background job distribution
- become pub-sub
- become event streaming history
- become a queue replacement
- imply durable pending work
- use correlation ids as replay or dedup semantics
- silently retry timed-out or lost work

### Lease must NOT:

- become a lock abstraction for arbitrary KV rows
- become a queue ownership system
- become workflow orchestration state
- imply crash-safe or cross-restart fencing
- imply cross-node consensus
- accumulate durable waiter state

### Schedule must NOT:

- become workflow orchestration
- become a queue replacement
- become unreliable best-effort timers
- become cron replacement without durability guarantees
- imply durable downstream delivery
- replay every missed interval after downtime
- become execution history storage

## 4. Core Guarantees

### Notice

Notice guarantees:

- live delivery attempts to subscriptions that are active when fanout is computed
- subscription isolation by RouteFamily and live subscription identity
- low-latency current-process fanout
- duplicate subscribe registration does not create duplicate logical deliveries

Notice does NOT guarantee:

- delivery after disconnect
- durable delivery
- replay or backfill
- stable cross-subscriber ordering
- recovery of missed events

### Stream

Stream guarantees:

- ordered append within a resource
- durable committed history according to the selected write mode
- replay from client-supplied offsets
- wildcard area and realm reads gated by committed watermarks
- monotonic committed offsets and watermarks
- committed history is readable after restart

Stream does NOT guarantee:

- queue-style reservation or acknowledgement
- broker-managed consumer positions
- replay through live subscriptions or durable subscription recovery
- durable live subscription recovery
- recovery of abandoned append sessions
- multi-node sequencing guarantees beyond the current broker contract

### KV

KV guarantees:

- transaction correctness inside one resource scope
- committed current-state persistence according to the selected write policy
- rollback discards uncommitted mutations
- stale or invalid transaction handles do not mutate committed state
- RouteFamily isolation for committed values

KV does NOT guarantee:

- event history
- replay
- cross-resource atomicity
- durable restoration of open transactions
- broker-managed change feeds

### Queue

Queue guarantees:

- durable backlog according to the selected write policy
- at-least-once delivery semantics
- exclusive live reservation per active lease token
- retry and redelivery after lease expiry
- optional dead-letter transition when retry policy is exhausted

Queue does NOT guarantee:

- exactly-once delivery
- strict global FIFO under competing consumers
- durable continuation of live lease ownership after restart
- stream-style immutable replay
- live request-response semantics

### RPC

RPC guarantees:

- request correlation for one live call
- dispatch to one live worker when capacity exists
- bounded pending queues with explicit backpressure
- FIFO pending order within one route
- explicit timeout behavior
- in-order chunk sequencing for one streaming response

RPC does NOT guarantee:

- durable pending requests
- worker survival after dispatch
- retries after timeout or worker loss
- replayable response history
- queue-like backlog across restart

### Lease

Lease guarantees:

- one live holder per lease identity in the running broker
- fencing tokens that are monotonic within the local broker lifetime
- explicit held, waiting, renewed, released, and expired outcomes
- eventual reacquisition after expiry or release
- explicit waiter ordering inside the local actor contract

Lease does NOT guarantee:

- crash-safe lock recovery
- cross-restart token monotonicity
- cluster consensus
- durable wait queues
- KV or Queue correctness by implication

### Schedule

Schedule guarantees:

- persisted schedule definitions survive restart
- execution does not occur before due time
- overdue schedules normalize forward rather than replaying every missed interval
- durable pending fire claims survive broker restart until resolved
- cancel and upsert produce one durable definition outcome per route

Schedule does NOT guarantee:

- durable subscriber delivery
- replay of every missed run after downtime
- workflow orchestration semantics
- durable execution history by itself
- queue-style work reservation or acknowledgement

## 5. Domain Interaction Rules

### Notice + Stream

- Notice may tell clients that new Stream data exists.
- Notice must not attempt replay, backfill, or recovery.
- Stream is authoritative for rebuild, catch-up, and historical rereads.

### KV + Notice

- KV-backed applications may emit lightweight invalidation or change hints through Notice.
- Notice fanout does not make KV changes durable as events.
- If durable change history matters, the application must also write to Stream.

### KV + Stream

- KV is authoritative for current state.
- Stream may record how that state changed.
- KV must not depend on Stream being present to remain authoritative.
- Stream must not be treated as a substitute for current-value lookup.

### Queue + Schedule

- Schedule may enqueue work into Queue when due.
- Schedule must not execute work directly as if it were a worker runtime.
- Queue remains authoritative for reservation, retry, acknowledgement, and dead-letter handling.

### Queue + RPC

- Queue distributes durable work.
- RPC executes work that a live worker must answer now.
- Queue must not become RPC with hidden backlog.
- RPC must not become a durable retry system.

### Queue + Notice

- Notice may publish queue availability hints or operational signals.
- Notice hints do not change queue durability or lease state.
- Queue remains authoritative for actual work ownership and backlog.

### Lease + Queue

- Lease may coordinate which worker group is allowed to consume or manage a queue.
- Queue must not depend on Lease correctness to preserve message durability.
- Queue visibility leases are not Lease-domain fencing tokens.

### Lease + KV

- Lease may guard a workflow that later writes to KV.
- Lease does not make KV transactions restart-safe or cross-resource atomic.
- KV remains authoritative for state; Lease remains authoritative for current ownership.

### Schedule + Notice

- Schedule may emit live notifications when a schedule fires.
- Durable schedule state does not make Notice durable.
- Notice delivery failure does not erase the durable schedule definition.

### Schedule + Stream

- If execution history matters, schedule-triggered work should also write to Stream.
- Schedule owns timing intent, not durable historical record of every downstream effect.
- Stream is authoritative for audit and replay.

### Stream + RPC

- RPC may produce results that are also written to Stream if durable history is required.
- RPC streaming sequence numbers must not be treated as Stream offsets.
- Stream remains the only durable replay surface.

## 6. Domain Selection Guide

If you need:

- live notifications -> Notice
- durable history -> Stream
- state authority -> KV
- background work -> Queue
- service execution -> RPC
- coordination -> Lease
- future execution -> Schedule

If you need one of these, do NOT choose the adjacent domain:

- replay -> not Notice -> use Stream
- retries -> not RPC -> use Queue
- durability for messages -> not Notice -> use Stream or Queue depending on whether the problem is history or work
- current authoritative state -> not Stream -> use KV
- coordination or fencing -> not Queue -> use Lease
- durable future intent -> not Notice or RPC -> use Schedule
- immutable history -> not Queue -> use Stream
- live request-response -> not Queue -> use RPC

Quick decision rule:

- If the client asks what happened, use Stream.
- If the client asks what is true now, use KV.
- If the system asks who owns this now, use Lease.
- If the system asks who should work on this item, use Queue.
- If the caller asks who can answer this now, use RPC.
- If the system asks who is listening right now, use Notice.
- If the system asks when should this happen later, use Schedule.

## 7. Domain Complexity Budget

### Notice

Allowed complexity:

- route matching
- wildcard subscription indexing
- live fanout efficiency
- session cleanup correctness

Not allowed:

- persistence
- replay
- consumer position tracking
- durable subscriber recovery

### Stream

Allowed complexity:

- append sessions
- durable storage
- replay
- watermarks and offsets
- retention and ordering rules

Not allowed:

- worker dispatch
- ack tracking
- queue reservation semantics
- broker-managed consumer groups that change the current contract

### KV

Allowed complexity:

- transaction boundaries
- committed reads and writes
- single-resource isolation
- write-policy clarity

Not allowed:

- history feeds
- event replay
- cross-resource workflow engines
- broker-restored transaction sessions

### Queue

Allowed complexity:

- reservation state
- retry logic
- visibility timeouts
- dead-letter handling
- fair competing-consumer behavior

Not allowed:

- replay logs
- pub-sub fanout
- interactive request-response semantics
- exactly-once illusions

### RPC

Allowed complexity:

- dispatch
- correlation
- bounded pending state
- timeout handling
- streaming chunk assembly for one response

Not allowed:

- retry queues
- durability
- event history
- background backlog management

### Lease

Allowed complexity:

- ownership state
- fencing tokens
- wait queues
- expiry and renewal rules

Not allowed:

- distributed consensus
- durable lock history
- queue semantics
- workflow orchestration state

### Schedule

Allowed complexity:

- persisted schedule definitions
- due-time computation
- pending fire claims
- boot-time preload
- overdue normalization rules

Not allowed:

- worker execution runtime
- durable downstream delivery
- execution history storage
- orchestration graphs

## 8. Future Feature Admission Rules

Place a new feature in the domain that owns its primary guarantee:

- if it requires replay -> Stream
- if it requires durable ordering of committed history -> Stream
- if it requires current authoritative state -> KV
- if it requires retries or durable backlog -> Queue
- if it requires live request execution -> RPC
- if it requires ownership or fencing -> Lease
- if it requires timing intent -> Schedule
- if it requires live awareness or ephemeral fanout -> Notice

Admit a feature only if it preserves the receiving domain's existing contract. A feature is a bad fit when it needs another domain's guarantee to make sense.

Reject features that:

- blur domain boundaries
- duplicate another domain's primary purpose
- require hidden cross-domain coupling
- add implicit guarantees that the domain does not already own
- turn ephemeral state into pseudo-durable state without saying so
- turn durable state into live coordination state without clear boundaries
- require the reviewer to explain them with "it is kind of like two domains at once"

Require architecture review when a proposal:

- adds replay to a non-Stream domain
- adds durable backlog to a non-Queue domain
- adds durable delivery claims to Notice or RPC
- adds queue-style reservation to Stream
- adds workflow or orchestration semantics to Schedule
- adds durable ownership claims to Lease
- adds history semantics to KV

## 9. Boundary Violation Warning Signs

Trigger architecture review immediately if any of these appear:

- Notice gains replay buffers, resume tokens, or missed-message recovery
- Stream gains worker reservation, ack, or redelivery semantics
- KV gains CDC or event-history obligations as a primary contract
- Queue gains pub-sub fanout or subscriber identity tracking
- Queue starts advertising exactly-once delivery
- RPC gains durable retries, restart-safe pending queues, or backlog recovery
- RPC sequence numbers start being described as replay cursors
- Lease tokens start being treated as durable or cross-cluster fencing guarantees
- Schedule starts promising orchestration, dependency graphs, or durable downstream delivery
- Schedule starts replaying all missed intervals by default
- Any domain starts depending on another domain's internal state instead of its explicit contract
- Docs start describing multiple domains with the same promise using different words

## 10. Fitz Domain Philosophy

Fitz domains are intentionally narrow.

Each domain solves one class of distributed systems problem.

Domains compose instead of merging.

Durable and ephemeral messaging remain separate.

Execution and storage remain separate.

Coordination is explicit.

Time is explicit.

Recovery belongs to Stream.

Work belongs to Queue.

Execution belongs to RPC.

State belongs to KV.

Coordination belongs to Lease.

Future work belongs to Schedule.

Live awareness belongs to Notice.

When Fitz adds features, it should add them by strengthening the right domain or by composing domains more clearly, never by letting one domain quietly absorb another. Boundary discipline is what keeps semantics stable, implementation choices aligned, and future maintenance affordable.