# Realm-Isolated Queueing — Design Spec

## 0. Intent

Define a simple, fast, and predictable competing-consumer queue architecture that **isolates workload by `realm`**. Each realm has its own lock-free command path and single-threaded dispatcher that arbitrates state and persists tiny merge deltas to the KV. Publishers never block each other; consumers pull in batches up to **N** with best-effort FIFO (per lane).

---

## 1. Scope & Non-Goals

**In-scope**

- Realm as the primary multitenancy boundary (perf, faults, quotas).
- Competing consumers with visibility timeouts, retries, DLQ, delays.
- “Small meta merges” for control plane; payload written once.
- Single node (today) with a clean path to multi-node (future).

**Non-goals**

- Exactly-once end-to-end (best-effort via dedup + idempotent consumers only).
- Cross-realm transactions and broadcast semantics.
- Global FIFO across all queues.

---

## 2. Identity & Routing

- Queue URIs: `queue://{realm}/{area}/{resource}` and `.../dlq`.
- **Realm** is the isolation unit. All control and data for a realm map to a **realm dispatcher**.
- Broker request router: parse URI → `realm` → enqueue command into that realm’s command ring.

---

## 3. High-Level Architecture

```
[Producers/Consumers] ──(commands)──▶ [Bounded MPMC→SPSC Ring (per REAM)]
                                         │
                                         ▼
                                [Realm Dispatcher (single thread)]
                                         │  (batched merge ops)
                                         ▼
                                   [KV Store (Midge)]
                                         │  (payload gets)
                                         ▲
                           Consumers fetch payload via get_batch
```

**Separation of planes**

- **Data plane (payload):** `/q/{realm}/{area}/{res}/msg/{id}` — immutable, written once, deleted once.
- **Control plane (meta):** small **MERGE** deltas that encode ready/delay/inflight/attempts — folded by compaction.

---

## 4. KV Layout (realm-prefixed)

Let `R=realm`, `A=area`, `X=resource`.

```
/q/{R}/{A}/{X}/msg/{id}                 -> payload (PUT once; DEL on ACK/DLQ)
/q/{R}/{A}/{X}/meta/{id}                -> merged state (MERGE deltas)

/q/{R}/{A}/{X}/idx/ready/{lane}/{ts}_{seq}_{id}   -> "" (discoverability)
/q/{R}/{A}/{X}/idx/delay/{lane}/{ts}_{id}         -> "" (due later)
/q/{R}/{A}/{X}/idx/infl/{lane}/{deadline}_{id}    -> {token, consumer}

/q/{R}/{A}/{X}/dlq/msg/{id}             -> payload copy or pointer
/q/{R}/{A}/{X}/dlq/idx/{ts}_{id}        -> meta for DLQ browsing
```

**Notes**

- All **index values are tiny**; heavy bytes live only under `/msg/*`.
- Realm prefix co-locates ranges and scans.

---

## 5. Realm Dispatcher

### 5.1 Responsibilities

- Drain the **realm’s command ring** (lock-free).
- Serialize state transitions; write **batched MERGE** ops to KV:

  - Publish: `AddReady`
  - Claim: `SetInflight`
  - Ack: `Remove/Acked`
  - Nack/Timeout: `Requeue{next_visible, attempts+1}`
  - Delay promotion & lease expiry reaping

- Fill consumer batches up to **N** (respect caps).
- Maintain light hints (lane cursors; per-tick budgets). No heavy in-memory queues required.

### 5.2 Tick Loop (scan-first)

Per tick:

1. **Promote delays**: scan `delay[..now]` (budgeted) → MERGE to ready.
2. **Reap inflight**: scan `infl[..now]` (budgeted) → MERGE requeue/DLQ.
3. **Collect candidates**: for each **lane**, scan `ready[cursor..]` to build assignments honoring consumer credits.
4. **Claim**: write one **batch** of `SetInflight` merges for winners (and implicit ready removals).
5. **Respond**: return **IDs** to consumers. Consumers (or broker) call `get_batch` on `/msg/*`.

**Cadence knobs**

- `flush_interval_us` (e.g., 100–500 µs) or `flush_batch_size` merges.
- Per-tick scan budgets for `delay`/`inflight` to prevent storm amplification.

---

## 6. Command Queue (per realm)

- **Medium:** bounded **MPMC→SPSC ring** (fixed size; cache-aligned slots).
- **Producers:** publishers, consumer RPC handlers, reaper timers — all enqueue tiny commands.
- **Consumer:** realm dispatcher only (single thread).
- **Backpressure:** if full, apply per-command policy (drop/yield/block). Never block publishers against each other.

_Command classes (fixed-size):_ `Publish`, `PollRequest{consumer, credit, vt}`, `Ack`, `Nack{next_visible}`, `Extend`, `SysTick`.

---

## 7. Lanes & Ordering

- Each queue is assigned **L lanes** (e.g., 16–32) via rendezvous/consistent hash on routing key (or message id).
- **Best-effort FIFO** per lane: ordered by `(visible_at, seq)`. No global ordering across lanes.
- Realm dispatcher interleaves lanes fairly (round-robin with small per-lane caps).

---

## 8. Consumer Semantics

### 8.1 Poll (batch up to N)

- Consumer declares **credit (N)** and preferred **VT**.
- Dispatcher fills from lane scans until credit met or no work.
- Returns **IDs + lease tokens + deadlines**.
- **Long-poll:** optional hold (100–300 ms) to coalesce work and reduce empty polls.

### 8.2 Ack / Nack / Extend

- **Ack:** MERGE remove inflight + DELETE payload. Credit restored immediately.
- **Nack:** MERGE remove inflight + enqueue delay with backoff. Attempts++.
- **Extend:** MERGE move deadline forward (cap enforced).
- **Fencing:** lease token must match inflight record; stale acks ignored.

---

## 9. Retry, Backoff, DLQ

- **Backoff policy** per queue (fixed / exponential / full jitter, capped).
- **Mix control:** e.g., aim for **80% fresh / 20% retry** assignment by budgeting scans.
- **Poison detection:** `attempts >= max_attempts` → DLQ (`.../dlq/*`).
- DLQ is a first-class route: `queue://{realm}/{area}/{resource}/dlq`.

---

## 10. Publisher Path (never blocks each other)

- **Fast path:** `PUT msg/{id}` + `MERGE AddReady{lane, ts}`; return after WAL append or group-commit depending on ack mode.
- No contact with dispatcher required to accept writes.
- Optional **durable-ack** mode: wait for next KV group-commit (still batched).

---

## 11. Failure, Recovery, & HA (single node now)

- Dispatcher crash/restart:

  - On boot, **no rebuild required** beyond resetting cursors.
  - First ticks naturally re-promote `delay[..now]` and re-reap `infl[..now]`.
  - Merges are idempotent; double-promotion safe.

- Future multi-node:

  - **Lease key per realm** (fenced epoch) to ensure single active dispatcher.
  - Takeover: new owner resumes tick loop with same scan-first semantics.

---

## 12. Backpressure & Quotas (per realm)

- **Ring capacity:** bounded. Full ring surfaces pressure quickly.
- **Consumer credits:** enforce max in-flight per consumer.
- **Rate limits:** token buckets per realm, per queue, or per consumer.
- **Retry throttling:** per-tick budgets on `infl[..now]` promotions.

---

## 13. Config & Defaults (per realm unless noted)

- `lanes_per_queue`: 16 (raise for hot keys).
- `ring_capacity`: 16k–64k commands.
- `flush_interval_us`: 200 µs; `flush_batch_size`: 1–2k merges.
- `delay_promotion_budget`: 1–5k items/tick (realm-wide).
- `inflight_reap_budget`: 1–5k items/tick (realm-wide).
- `default_vt_ms`: p95 observed processing × 2 (min 5s, max 2m).
- `backoff`: exp + full jitter; cap 5m; `max_attempts`: 10.
- `long_poll_ms`: 150 ms (adaptive down under heavy load).

---

## 14. Observability

**Per realm**

- Gauges: `ready_count`, `delayed_due`, `inflight_count`, `ring_depth`, `waiters`, `credits_used`.
- Rates: `assigns/s`, `acks/s`, `nacks/s`, `lease_expired/s`, `dlq/s`, `publish/s`.
- Latency: `poll_wait`, `assign_latency`, `ack_to_delete_payload`, `end_to_end`.
- Error counters: `producer_drops`, `stale_ack`, `merge_failures`.

**Per queue & lane** (sampled)

- `ready`, `retry_share`, `hot_lane_ratio`.

Tracing:

- Span per dispatcher flush; attributes: merges count by type, KV latency, budgets hit.

---

## 15. Security & Multitenancy

- **AuthZ** uses URI components and JWT claims; dispatcher only handles commands for its realm.
- **Quotas** (msgs/sec, storage bytes, in-flight caps) enforced at realm dispatcher.
- **Data isolation**: realm prefixes at KV level; easy to export/bill per realm.

---

## 16. Capacity & Sizing (rules of thumb)

- One dispatcher thread per **active realm**; pin hot realms to dedicated cores.
- Throughput driver = KV batch rate × merges/flush. With small merges, 1–2k merges per 200 µs yields very high control throughput.
- If a realm saturates a core: **increase lanes**, tighten flush interval, or split the realm into **sub-realms** (administrative), or move realm to another node (future).

---

## 17. Testing Matrix

- **Correctness:** publish→ready→claim→ack; nack backoff; token fencing; DLQ on poison.
- **Concurrency:** many producers; many consumers; saturated ring; long-poll interactions.
- **Storms:** retry wall; hot lane; empty queue scans; timer burst on large delayed set.
- **Durability:** crash between merge batch and reply; restart idempotency.
- **Isolation:** noisy neighbor realm; ensure metrics/latency of other realms unaffected.

---

## 18. Migration & Lifecycle (realm)

- **Create realm:** allocate ring + dispatcher lazily on first command; warm KV cursors.
- **Idle:** park thread; wake on ring enqueue (eventfd/condvar).
- **Delete realm:** drain ring, flush, stop dispatcher (admin only).
- **Future move:** detach lease, drain, checkpoint cursors (optional), re-attach on target node.

---

## 19. Why this design

- **Isolation by design:** noisy neighbors contained within realm.
- **Lock-free producers:** all ingress flows are atomic appends to the realm ring.
- **Deterministic control:** single-thread dispatcher per realm; tiny, batched merges.
- **Scan-first simplicity:** KV is the truth; minimal in-memory state.
- **Payload once, meta as merges:** minimal write amplification; easy recovery.

---

If you want, we can follow up with a **state transition table** (which merge deltas are emitted on each transition) and a **realm dispatcher runbook** (alerts, SLOs, and what to tweak when a realm runs hot).
