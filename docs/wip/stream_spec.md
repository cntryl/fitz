# Stream semantics specification

This document defines the semantics, APIs, engine/store responsibilities and tests for the Stream scheme in Fitz. Streams are ordered, potentially long-lived sequences of messages with resume and optional replay capabilities. Streams differ from queues in that they are primarily append-and-subscribe (not reserve/ack) and often require ordering, cursors, and resumption.

## Summary contract
- Inputs: Producers append opaque bytes to a named stream route.
- Outputs: Consumers subscribe and receive ordered messages, can peek by cursor, and may resume from a cursor/token.
- Success criteria: messages are delivered in append order to subscribers, clients can resume after disconnect using cursors, and system provides bounded retention or configurable retention policies.

Optimistic concurrency and event sourcing:
- Streams support conditional appends using an "expected revision" to enable optimistic concurrency control (OCC). This allows Fitz Streams to serve as the backing store for event-sourced systems (similar to EventStoreDB).
- Each append can specify an ExpectedRevision that must match the current stream head (or existence) for the append to succeed; otherwise the append fails with a specific error and returns the actual current revision.

## Core features
1. Publish (append) — producers append body to stream with optional id and metadata
2. Subscribe (live) — clients open a subscription to receive new items as they arrive
3. Consume/Peek by cursor — clients can fetch items starting at a cursor or sequence number without removing them
4. Resume — clients can resume a subscription from a saved cursor/token
5. Ordering — stream preserves append order and exposes sequence numbers
6. Cursors/tokens — opaque resume tokens or explicit sequence numbers are supported
7. Retention — configured per-stream (time or size-based)
8. Replay — allow replay from older offsets/cursors while respecting retention
9. Backpressure — server should support flow control (windowing, per-subscriber buffer limits) and error with TAG_ERROR:Backpressure if subscriber cannot keep up
10. Delivery modes — at-most-once (push-only), at-least-once (with explicit ack per-message), and exactly-once (not supported without external coordination)
11. Sharding/partitioning — optional per-stream partition to scale producers/consumers
12. Observability and metrics

## TLV conventions
- TAG_ROUTE: stream route
- TAG_BODY: message body
- TAG_ID: optional producer-supplied id
- TAG_SEQ: sequence number (server-assigned) for ordered streams
- TAG_CURSOR / TAG_RESUME_TOKEN: opaque cursor for resuming
- TAG_NOTIFICATION: used for server->client push of stream items
- TAG_ERROR: error frames

Additional TLVs for concurrency control:
- TAG_EXPECTED_REV (u8=0xA0): carries the expected revision for the append. Encoding:
  - 0xFF... special sentinel values for modes:
    - 0xFFFFFFFFFFFFFFFF (u64::MAX): Any (no concurrency check)
    - 0xFFFFFFFFFFFFFFFE: NoStream (stream must not exist)
    - 0xFFFFFFFFFFFFFFFD: StreamExists (stream must exist)
  - Otherwise: u64 exact expected revision number
- TAG_ASSIGNED_REV (u8=0xA1): on successful append, server echoes the assigned revision of the last record in the batch
- TAG_FIRST_ASSIGNED_REV (u8=0xA2): if appending a batch, the first assigned revision in the batch
- TAG_METADATA (u8=0xA3): optional per-record metadata map (CBOR/JSON-encoded) for event headers

## Record model (store-side)
- Each record contains:
  - id: String (client-provided or server-generated)
  - seq: u64 (server-assigned, strictly increasing per-stream)
  - body: Vec<u8>
  - created_at: u64 (epoch seconds)
  - metadata: Option<HashMap<String,String>>
  - revision: u64 (alias of seq for single-partition streams; stored explicitly for clarity)

## API proposals (high-level)
- Producer helpers
  - async fn stream_publish(route: &str, id: Option<&str>, body: &[u8], expected: ExpectedRevision) -> Result<AppendResult, StreamError>
    - Returns assigned sequence number `seq` (aka revision) in `AppendResult`
  - async fn stream_publish_batch(route: &str, events: &[Event], expected: ExpectedRevision) -> Result<AppendResult, StreamError>
    - Appends a batch atomically with OCC check applied to the first event; returns first and last assigned revisions
- Consumer helpers
  - async fn stream_subscribe(route: &str, from_seq: Option<u64>, on_message: impl Fn(Message)) -> SubscriptionHandle
  - async fn stream_peek(route: &str, from_seq: u64, limit: usize) -> Result<Vec<Message>, StreamError>
  - fn make_resume_token(route: &str, seq: u64) -> String // opaque token the client can persist

Types:
```rust
pub enum ExpectedRevision {
    Any,         // accept regardless of current state
    NoStream,    // only if stream does not exist
    StreamExists,// only if stream exists (at any revision)
    Exact(u64),  // only if current revision equals this
}

pub struct AppendResult {
    pub first_assigned: u64, // first revision assigned in this append (batch aware)
    pub last_assigned: u64,  // last revision assigned (== first for single append)
}
```

## Delivery semantics
- On publish: store appends record with monotonic `seq`, notifies live subscribers for the stream.
- For live subscriptions: server pushes TAG_NOTIFICATION frames containing TAG_SEQ and TAG_BODY.
- For peek/resume: client can request items starting at a sequence number or resume token; server returns matching records up to `limit`.
 - For conditional appends: server compares `TAG_EXPECTED_REV` to current head revision and either appends the event(s) atomically or fails with `TAG_ERROR:WrongExpectedVersion`, echoing `TAG_ASSIGNED_REV` (current head) to help clients reconcile.

## Ordering guarantees
- Per-stream ordering: seq numbers are monotonic and reflect append order. Subscribers receive messages in increasing seq order.
- For partitioned streams, ordering is per-partition (clients choose partition key).
 - Atomicity: when a batch append succeeds, all events are assigned contiguous revisions with no interleaving by other writers; when it fails due to WrongExpectedVersion, none are appended.

## Resume and cursor format
- Cursor: opaque token encoding { route, seq } plus HMAC to avoid tampering (optional).
- Resume semantics: if seq < earliest retained seq, server should return TAG_ERROR:OutOfRange; client must handle by seeking to earliest or giving up.

## Backpressure and buffering
- Server maintains a per-subscription buffer with a configurable maximum (e.g., 1024 messages or bytes).
- If a subscriber cannot accept messages (transport slow), server may:
  - Drop the subscription (close connection) with TAG_ERROR:Backpressure
  - Pause delivery until the subscriber drains (preferred if resource allows)
  - Offer windowed delivery: the client acknowledges window progress

## Retention
- Configure per-stream retention policy: time-based (e.g., 7 days) or size-based (e.g., last 10GB), or compaction policies for key-based streams.
- When requested seq < earliest retained seq, server returns TAG_ERROR:OutOfRange on resume.

## Replay
- Client may request historical messages by calling `stream_peek(route, from_seq, limit)`; server responds with messages within retention window.

## Delivery modes and acks
- At-most-once (default): server pushes messages to subscribers; no ack required, messages may be lost on transport failure.
- At-least-once: server pushes messages and expects transports/clients to ack progress (e.g., via TAG_ACK with last-seen seq); server can attempt redelivery to clients reconnecting from earlier seq.
- Exactly-once: not supported natively; requires idempotent consumer logic or external coordination (e.g., transactional sinks).

## Interactions with streams and queues
- Streams are append-only; messages remain and can be replayed. Queues remove messages on consume.
- For RPC reply-queue pattern, a reply route may be a transient queue (use queue semantics) rather than a stream.

## Engine / store responsibilities
- Store must maintain monotonic seq per-stream and append-only logs with retention and efficient random access by seq.
- Engine should notify live subscribers on append and persist records for peers that later resume.
- Optional: engine provides per-subscription flow-control primitives (windowing) to avoid unbounded buffering.
 - OCC implementation details:
   - The store keeps a per-stream head revision (u64). On append with ExpectedRevision:
     - Any: ignore check, append at head+1..
     - NoStream: succeed only if stream does not exist (head is None)
     - StreamExists: succeed only if stream exists (head is Some)
     - Exact(N): succeed only if head == N
   - On failure, return WrongExpectedVersion(head) so clients can retry with the correct expected revision.
 - Idempotency: Clients may send a producer-supplied id per record; the store may maintain a dedupe index per stream (id -> assigned rev) with a TTL to make retries idempotent.

## Tests to add
- Publish/subscribe happy path: publish messages and verify subscribers receive in order with matching seq.
- Resume happy path: publish N messages, subscribe from seq=K, verify next messages match expected
- Out-of-range resume: attempt resume from seq older than earliest retention → expect TAG_ERROR:OutOfRange
- Backpressure: simulate slow subscriber and verify server either pauses delivery or closes with Backpressure
- Partition ordering: publish to partitioned stream and verify per-partition ordering
 - OCC happy path: append with ExpectedRevision::NoStream then successive Exact(head) appends; assert assigned revisions
 - OCC failure: attempt append with wrong ExpectedRevision::Exact(N); expect TAG_ERROR:WrongExpectedVersion and return current head via TAG_ASSIGNED_REV
 - Batch atomicity: concurrent writers where one batch succeeds fully and others fail/ retry; ensure no interleaving within accepted batch

## Metrics and admin APIs
- Provide counters: published_count, subscriber_count, bytes_published, retained_bytes
- Admin APIs: list streams, stream_stats(route) returning head_seq, earliest_seq, retained_count

## Implementation roadmap (priority)
1. Support append-only store with monotonic seq and retention config.
2. Implement Publish -> notify subscribers in engine.
3. Add server-side per-subscription buffers and basic backpressure handling.
4. Provide `stream_peek` and resume token helpers.
5. Add tests and admin APIs.
 6. Add conditional append with ExpectedRevision and WrongExpectedVersion error mapping; expose TLVs TAG_EXPECTED_REV, TAG_ASSIGNED_REV in protocol.

---

End of stream spec.
