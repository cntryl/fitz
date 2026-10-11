# Stream Commit, Concurrency, and Global Order

Section 10 moved from [routing-design.md](routing-design.md) unchanged. Sections 8, 9 and 11-13 of the stream design remain there.

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
