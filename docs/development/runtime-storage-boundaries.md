# Runtime and storage boundaries

## Delivery failures

`ActorRef::send` and `Context` sending methods preserve timeout, invalid-payload,
and unsupported-payload failures as distinct `SendError` variants. A timeout
does not establish that an actor stopped. A payload rejection does not establish
that a sink panicked. Invalid-payload errors retain the actual size and wire
limit so callers can correct the message instead of retrying it unchanged.

`MailboxSink::deliver_high_priority` requests priority where the sink supports
it. Its default calls ordinary `deliver`, preserving the same envelope and
result. Single-lane sinks, including session outbound transport, need only
implement `deliver`. Managed actor mailboxes override priority delivery to use
their separate bounded control lane. Code requiring reserved control capacity
must use those concrete mailbox implementations, not assume that every
`MailboxSink` provides it.

## KV write policy

`KvMessage::Begin` carries `domains::WritePolicy`, a Fitz-owned guarantee type.
The wire codec produces `Buffered` for flag 0 and `Sync` for flag 1. The domain
sink maps those requests to the broker's configured local or cloud policies;
explicit `BestEffort`, `CloudAsync`, and `CloudStrict` requests retain their
meaning. No policy has a default.

Conversions to and from Midge `WriteOptions` live in `src/storage/write_policy.rs`.
Existing engine-based construction and broker configuration methods continue
accepting Midge options. The actor converts the resolved policy when creating
its engine transaction state. Midge remains the concrete storage engine.

## Queue recovery

`QueueRecoveryStore` owns recovery transactions, index queries, row decoding,
and atomic index replacement. It reuses the Queue storage-key codecs so normal
writes and recovery read the same persisted format. Recovery index metadata and
lazy decoded index iterators share one read transaction. Iterators borrow that
transaction, preventing it from being dropped while they are consumed.

`QueueActor` owns index-counter validation, fallback selection, live ready and
delayed state reconstruction, and the recovered ID boundary. The store receives
a borrowed index-rebuild description and commits stale-index deletion, new
entries, and metadata together. A failed replacement commit does not publish a
partially replaced index. Authoritative header fallback still loads the header
set before rebuilding; this refactor does not introduce bounded fallback memory.

Storage formats, acknowledgement guarantees, RouteFamily isolation, and
ephemeral inflight ownership are unchanged.
