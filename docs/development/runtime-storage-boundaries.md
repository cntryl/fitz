# Runtime and storage boundaries

## Delivery failures

`ActorRef::send_detailed` and the `Context::*_detailed` sending methods return
`RouteError` with the exact destination and delivery failure. A timeout does not
establish that an actor stopped. A payload rejection does not establish that a
sink panicked. Invalid-payload errors retain the actual size and wire limit.

The existing `send`, `send_untracked`, `publish_event`, and `reply` methods retain
the original four-variant `SendError` and its legacy classification for source
and behavior compatibility. Callers that need to distinguish timeout, invalid
payload, and unsupported payload must use the detailed methods.

`MailboxSink::deliver_high_priority` remains required. Every sink explicitly
chooses its handling: managed actor mailboxes use their separate bounded control
lane, while single-lane sinks such as session outbound transport explicitly
forward to `deliver`. The trait provides no automatic priority fallback. Code
requiring reserved control capacity must use an implementation that provides it.

## KV write policy

`KvMessage::Begin` carries `domains::WritePolicy`, a Fitz-owned guarantee type.
The wire codec produces `Buffered` for flag 0 and `Sync` for flag 1. The domain
sink maps those requests to the broker's configured local or cloud policies;
explicit `BestEffort`, `CloudAsync`, and `CloudStrict` requests retain their
meaning. No policy has a default. The wire inventory and configuration resolver
live together in `domains/kv/write_policy.rs`; the codec and sink share them.

Conversions to and from Midge `WriteOptions` live in `src/storage/write_policy.rs`.
Existing engine-based construction and broker configuration methods continue
accepting Midge options. The actor converts the resolved policy when creating
its engine transaction state. Midge remains the concrete storage engine.

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
delayed state reconstruction, and the recovered ID boundary. The store receives
a borrowed index-rebuild description and commits stale-index deletion, new
entries, and metadata together. A failed replacement commit does not publish a
partially replaced index. Recovery assumes the existing single-owner queue
lifecycle; snapshot consistency does not authorize concurrent queue writers.

Storage formats, acknowledgement guarantees, RouteFamily isolation, and
ephemeral inflight ownership are unchanged.
