# Queue semantics specification

This document captures the required behaviors, APIs, error modes, edge cases and a prioritized implementation plan to support typical message queue semantics for the Fitz engine + store.

Scope and positioning:
- This spec describes the durable queue subsystem ("queue queue"). Messages appended here are intended to be persisted according to the selected storage backend. Delivery semantics target at-least-once with leases and acknowledgements.
- The RPC request path uses a separate, in-memory, non-durable queue ("rpc queue") optimized for latency. Do not conflate the two; if you need persistence, use the durable queue.
  - Backpressure for RPC is applied via in-memory caps and channel-level ERRs; durable queues may use different policies (e.g., disk-based limits, producer throttling).

## Contract (tiny)
- Inputs: client requests to produce/reserve/extend/consume/peek on a named route (string). Bodies are opaque bytes.
- Outputs: success / typed errors, or reserved item tuple (id, body, delivery_token).
- Success criteria: consumers can safely reserve and ack messages with lease semantics; expired leases cause redelivery; duplicates can be addressed by idempotency rules; system exposes DLQ and observability.

## Core features required
1. Produce (append)
2. Reserve (visibility timeout / lease) — returns delivery token
3. Extend lease — verify token, extend TTL
4. Consume / Acknowledge — verify token, remove message (ack)
5. Peek (inspect next available without reserving)
6. Redelivery on lease expiry
7. Dead-letter queue (DLQ) for repeatedly failing messages (poison)
8. Deduplication support (optional): dedupe by client-provided id
9. Ordering (optional per-route): preserve append order for consumers that require it
10. Visibility: an item reserved is invisible to other consumers until lease expires or consumed
11. At-least-once by default; provide guidance for at-most-once via immediate removal on deliver if desired
12. Delivery tokens: HMAC-signed opaque token (already exists in MemStore)
13. Error model: typed errors (NotFound, InvalidToken, LeaseExpired, NoAvailable, PermissionDenied, Backpressure)
14. Observability: metrics (reserve_count, consume_count, lease_extend_count, redelivery_count, dlq_count), and admin listing APIs
15. Tests: unit tests for basic flows + failure/edge tests (timeout, invalid token, concurrent reserves)

## Current store status (from `src/storage/mem.rs`)
- Present: `append`, `reserve_next` (returns `(id, body, delivery_token)`), `extend_lease`, `read_all`.
- Missing/insufficient:
  - No `consume`/remove API to ack and delete a message by id + token.
  - No per-record delivery_count / retry_count to implement DLQ.
  - `append` signature uses `&mut self` and async; check callsites where the store is behind `Arc<Mutex<MemStore>>` (engine currently locks `store.lock().await` and calls methods).
  - Error handling uses plain strings; prefer a typed error enum.
  - No `list_resources` / `list_areas` APIs for admin queries.
  - No persistence (acceptable for prototype, but note tradeoffs and tests will be ephemeral).

## Concrete API proposals (Rust signatures)
- Errors:
```rust
pub enum StoreError {
    NotFound,
    InvalidToken,
    LeaseExpired,
    NoAvailable,
    PermissionDenied,
    Backpressure,
    Other(String),
}
```
- `MemStore` methods (suggested):
```rust
pub async fn append(&mut self, route: String, id: String, body: Vec<u8>) -> Result<(), StoreError>;
pub async fn reserve_next(&mut self, route: &str, lease_secs: u32) -> Result<(String, Vec<u8>, String), StoreError>;
pub async fn extend_lease(&mut self, route: &str, id: &str, delivery_token: &str, add_secs: u32) -> Result<u32, StoreError>;
pub async fn consume(&mut self, route: &str, id: &str, delivery_token: &str) -> Result<(), StoreError>; // remove/ack
pub async fn peek(&self, route: &str) -> Result<Option<(String, Vec<u8>)>, StoreError>;
pub async fn read_all(&self, route: &str) -> Result<Vec<Record>, StoreError>;
// optional helpers
pub async fn bump_delivery_count(&mut self, route: &str, id: &str) -> Result<u32, StoreError>; // returns new delivery_count
pub async fn move_to_dlq(&mut self, route: &str, id: &str) -> Result<(), StoreError>;
pub async fn list_resources(&self, route_prefix: &str) -> Result<Vec<String>, StoreError>;
pub async fn list_areas(&self) -> Result<Vec<String>, StoreError>;
```
- Record additions:
  - `delivery_count: u32`
  - `created_at: u64`
  - `attempts: u32`
  - optional `metadata: Option<HashMap<String,String>>`
  - optional `sequence/offset` if ordering required

## Engine command implications
- `EngineCommand` already contains `Publish`, `Reserve`, `ExtendLease`, `Consume`, `Peek`, `ListResources`, `ListAreas` — these map naturally to the store APIs above.
- Implement engine-side handling for `Consume` that calls `store.consume(route, id, token)` with the token and returns success.
- DLQ workflow options:
  - When `reserve_next` increments delivery_count and it exceeds a threshold, `MemStore` or engine moves the message to `<route>.dlq` and removes it from the main queue.
  - Alternatively expose `EngineCommand::MoveToDlq` if you want engine-triggered DLQ moves.
- Engine must publish notifications to subscribers on `Publish` so Subscribe-based workers can receive messages (unblocks reply-queue RPC patterns).

## Behavioral semantics (detailed)
- Reserve semantics:
  - Return the first item where `lease_expiry` is `None` or `lease_expiry <= now` (available).
  - When reserved: set `lease_expiry = now + lease_secs`; set `lease_owner = token`; increment `delivery_count/attempts`.
  - Reserved messages are invisible to other `reserve()` calls until lease expiry or `consume`.
- ExtendLease semantics:
  - Verify `lease_owner` token matches; allow extension up to a sensible maximum (or unlimited depending on policy).
- Consume semantics:
  - Verify `lease_owner` token; remove the record or mark as consumed; optionally return the consumed record for auditing.
- Redelivery:
  - If lease expires and not consumed, the message becomes available again for `reserve_next`.
- DLQ and poison messages:
  - Track `delivery_count`; if `delivery_count > DLQ_THRESHOLD`, move message to `<route>.dlq` and remove it from main queue, optionally adding metadata about failures.
- Ordering:
  - For strict ordering: allow a route-level single consumer mode (serialize delivery), or maintain sequence numbers and disallow concurrent reservations that would violate ordering.
- De-dup:
  - If client provides `id` on produce, engine/store should dedupe by id when appending (if dedupe enabled). Keep a route-level dedupe index with TTL (store seen ids for a configurable window).

## Edge cases and race conditions
- Concurrent `reserve()` calls must be serialized by engine owning the store lock (current single-engine-task model provides this).
- Clock skew: use server-side monotonic time for lease durations; be careful with time conversions.
- Token replay: tokens are opaque HMACs; ensure token includes a timestamp/nonce so stolen tokens have limited lifetime.
- Crash during processing: workers using Reserve+ExtendLease+Consume pattern are resilient; Subscribe-only workers are not unless they manage idempotency.
- Backpressure: if append rate exceeds memory or store capacity, return `Backpressure` to producers; implement per-route size limits.

## Testing matrix (minimum tests to add)
- Happy path: produce -> reserve -> extend -> consume (assert message removed)
- Lease expiry: produce -> reserve(1s) -> wait>1s -> reserve again -> message available
- Invalid token: produce -> reserve -> attempt extend/consume with wrong token -> `InvalidToken`
- DLQ: produce -> reserve and repeatedly let leases expire until attempts > threshold -> ensure message moves to DLQ route
- Dedup: produce id=A twice -> dedupe enabled -> only one copy appended
- Concurrent reserves: multiple consumers attempt reserve concurrently -> only one gets the message (engine serializes)
- Peek: produce then peek -> returns body without reserving

## Admin / observability APIs
- `list_resources` / `list_areas`: implement store-side scanning of keys, return last N resources, counts per-route
- `fetch_status` / `fetch_resource_status`: return counts: total, in-flight (leased), ready, dlq_count
- Expose Prometheus-compatible counters or provide a tiny metrics facade

## Configuration hierarchy
- Configs can be set at different scopes; more specific scopes override general ones:
  - `queue://realm/**` → realm scope (applies to all queues in a realm)
  - `queue://realm/area/**` → area scope (overrides realm for queues in area)
  - `queue://realm/area/resource` → resource scope (overrides area and realm for that queue)
- Initial knobs: `dlq_threshold` (default 5). Later: message size limits, per-route memory caps, ordering flags.

## Priority implementation plan (small, safe steps)
1. Implement a typed `StoreError` enum and convert existing string-based errors to typed errors.
2. Add `consume(&mut self, route, id, token)` to `MemStore` and corresponding engine handling for `EngineCommand::Consume` to actually remove/ack messages.
3. Add `delivery_count`/`attempts` to `Record` and bump on reserve; add `move_to_dlq`.
4. Implement `Publish -> notify subscribers` in `src/core/engine.rs` so Subscribe workers actually receive messages (unblocks reply-queue RPC).
5. Add simple DLQ logic: when `delivery_count > threshold`, move to `<route>.dlq`.
6. Add unit tests for the flows above; run `cargo test`.
7. Add `list_resources` / `list_areas` implementations.
8. (Optional) Add dedupe index and ordering flags.

## Small tasks I can do next
- Implement `MemStore::consume` + adjust `reserve_next` to increment attempts and store delivery_count, and add a minimal DLQ movement.
- Implement `Publish -> notify` in the engine loop so Subscribe pathways work (this is necessary for reply-queue RPC).
- Replace string errors with `StoreError` enum across `mem.rs` and `engine.rs`.

---

End of queue semantics specification.
