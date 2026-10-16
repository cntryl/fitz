# Runtime and storage boundaries

## Delivery failures

`ActorRef::send_detailed` and the `Context::*_detailed` sending methods return
`RouteError` with the exact destination and delivery failure. The actor-facing
`send`, `send_untracked`, `publish_event`, and `reply` methods preserve those
same semantic categories in `SendError`: timeout, actor stop, sink panic,
invalid payload, and unsupported payload are distinct. Invalid-payload errors
retain the actual size and wire limit.

`MailboxSink::deliver_high_priority` remains required. Every sink explicitly
chooses its handling: family actor runtimes use their separate bounded control
lane, while single-lane sinks such as session outbound transport explicitly
forward to `deliver`. The trait provides no automatic priority fallback. Code
requiring reserved control capacity must use an implementation that provides it.

## KV and Stream commit persistence

KV BEGIN selects scope and transaction mode without a persistence choice.
KV and Stream COMMIT require `domains::CommitPersistence`: flag 0 is Buffered,
and flag 1 is Sync. Missing, unknown, and trailing values are rejected. There
is no default choice or begin-time policy stored on a KV transaction.

The configured storage backend fixes the mapping: local commits use Buffered
or Sync; cloud commits use CloudAsync or CloudStrict. Background cloud durability
does not weaken an explicit Sync choice. Client COMMIT accepts only these two
choices, rather than internal BestEffort or cloud policy variants.

Conversion from the resolved Fitz policy to Midge `WriteOptions` lives in
`src/storage/write_policy.rs`. Domain stores use that resolved policy when
committing engine transactions. Midge remains the concrete storage engine
behind those adapters. Queue persistence is always best effort.

## Queue recovery

`QueueRecoveryStore` owns recovery transactions, index queries, row decoding,
and atomic index replacement. One store is created per actor; both normal writes
and recovery reuse its cached keys and reference-counted scan prefixes. Recovery
clones one store handle, without copying queue identity strings or rebuilding
prefixes on its error and fallback paths.

Index metadata, ID reservation, ready/delayed/dead-letter rows, and fallback
headers all use the same read snapshot. If index metadata is invalid, the
reserved-ID row supplies the fallback floor; invalid index metadata is not an
authority for that floor. Ordinary typed iterators decode each scan lazily and
borrow the snapshot. Header recovery no longer retains a vector of complete
header records. The live recovered state and ready-ID sorting still scale with
the recovered queue; this is not a constant-memory recovery guarantee.

`QueueActor` owns index-counter validation, fallback selection, live ready and
delayed state reconstruction, and the recovered ID boundary. `QueuePersistence`
groups the concrete engine, cached persistence keys, and recovery store. All
Queue writes, including index rebuilds and startup repair, use Midge
`WriteOptions::best_effort()`; Queue has no configurable persistence policy.
The recovery store receives
a borrowed index-rebuild description and commits stale-index deletion, new
entries, and metadata together. A failed replacement commit does not publish a
partially replaced index. Recovery assumes the existing single-owner queue
lifecycle; snapshot consistency does not authorize concurrent queue writers.

Queue acknowledgements are always best effort: a crash can lose accepted
messages, ACKs, or ID reservations. Persisted messages may be redelivered, and
IDs from lost reservations may be reused. Storage formats, RouteFamily
isolation, and ephemeral inflight ownership are unchanged.

Family actors submit dirty column families to one bounded
Queue-domain flush worker. A family has at most one flush in flight; new writes
stay dirty for a later pass, and a full worker queue leaves work pending for
retry. The storage flush no longer occupies the Queue request actor, but slow or
failed storage can delay persistence without a fixed loss-window bound.
