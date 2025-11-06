# Notice semantics specification

This document defines the semantics and API for the Notice scheme in Fitz. Notices are fire-and-forget informational events that the server delivers to active subscribers. They prioritize low-latency and low-overhead delivery over guaranteed durability.

## Summary contract
- Inputs: Producers publish opaque bytes to a named notice route.
- Outputs: Active subscribers receive TAG_NOTIFICATION frames containing the body and route.
- Success criteria: Subscribers connected at publish time should receive notifications; disconnected subscribers are not guaranteed delivery.

## Core features
1. Publish — append or emit notification to active subscribers
2. Subscribe/Unsubscribe — clients express interest in a route and receive notifications
3. Best-effort delivery — no required acks or leases by default
4. Optional reliable mode — delivery tokens/acks may be layered on top for stronger guarantees
5. Subscription lifetime — subscriptions are session-scoped (tied to transport connection) or may be server-persistent depending on config
6. Filtering/routing — route patterns and wildcards to limit notification fanout
7. Backpressure handling — when subscribers cannot keep up, server may drop notifications or apply per-subscriber buffering limits

## TLV conventions (implemented values)
- 0x20 TAG_ROUTE: route being notified (UTF-8)
- 0x22 TAG_BODY: opaque notification payload (bytes)
- 0x92 TAG_NOTIFICATION: marker TLV indicating a server->client notice inside a DAT frame
- 0x90 TAG_SUBSCRIBE: present in REG to request a subscription
- 0x91 TAG_UNSUBSCRIBE: present in REG to request unsubscription

Notes:
- Notifications are carried in a DAT frame whose payload includes TAG_NOTIFICATION, TAG_ROUTE, and TAG_BODY.
- REG frames manage subscriptions. A REG request MUST include TAG_ROUTE and either TAG_SUBSCRIBE or TAG_UNSUBSCRIBE.

## Delivery semantics
- On Publish: the engine triggers notification dispatch to matching active subscribers. If the subscriber is connected via a transport, the server sends a DAT frame containing TAG_NOTIFICATION (0x92), TAG_ROUTE (0x20), and TAG_BODY (0x22).
- No persistent store is required for notice-only mode; this codebase opportunistically appends to an in-memory store used by other features and immediately fans out.

## Subscription models
- Session-scoped subscriptions (default, implemented): when a transport disconnects, its subscriptions are removed.
- Persistent subscriptions (optional, not implemented): subscriptions survive disconnects and the server buffers notifications to be delivered on reconnect (requires storage/backlog and increases complexity).

## Delivery modes
- Best-effort (default, implemented): server sends notifications to active subscribers; no acks expected.
- Reliable (optional, not implemented): would include delivery tokens and require explicit ack; engine would track pending deliveries and retries/DLQ.

## Backpressure and buffering
- Server maintains a small per-subscriber buffer for in-flight notifications.
- Implemented policy: best-effort drop. Each subscription uses a bounded async channel; if full, the server drops the notification for that subscriber and continues without blocking.
- Optional policies (not implemented): close the subscription with an ERR:Backpressure; or buffer until drain (not recommended due to memory risk).

## Filtering and routing
- Implemented matching:
  - Exact match (e.g., "a/b/c")
  - Trailing '*' prefix wildcard (e.g., "a/b/*" matches "a/b/x" and "a/b/x/y")
  - Hierarchical prefix (e.g., pattern "a/b" matches "a/b" and "a/b/..." but not "a/c")
- Global wildcard '*' matches all routes.
- Engine maintains an in-memory registry and fans out to matching subscribers on publish.

## Framing details (concrete)
- Subscribe: client sends a REG frame with TLVs TAG_ROUTE + TAG_SUBSCRIBE. Server replies with an ACK echoing TAG_ROUTE.
  - 20 00 00 00 05 61 2F 62 2F 63   // route "a/b/c"
  - 90 00 00 00 00                   // subscribe op (no value)

- Unsubscribe: client sends a REG frame with TLVs TAG_ROUTE + TAG_UNSUBSCRIBE. Server replies with an ACK echoing TAG_ROUTE.
  - 20 00 00 00 05 61 2F 62 2F 63
  - 91 00 00 00 00

- Notification (server->client): server sends a DAT frame with TLVs TAG_NOTIFICATION + TAG_ROUTE + TAG_BODY.
  - 92 00 00 00 00                   // notification marker
  - 20 00 00 00 05 61 2F 62 2F 63   // route "a/b/c"
  - 22 00 00 00 04 64 61 74 61      // body "data"

ACK/ERR semantics:
- On successful subscribe/unsubscribe, server replies with ACK (frame 0x0A) echoing TAG_ROUTE.
- On protocol/auth errors, server replies with ERR (frame 0x0B) including TAG_ERR_CODE and TAG_ERR_MSG; subscribe requests without required TLVs are rejected.

## Tests
- Subscribe -> Publish -> notification received by active subscriber
- Disconnect: subscriber unsubscribed on transport close -> publish -> subscriber doesn't receive
- Backpressure: slow subscriber experiences dropped notifications (implemented policy)
- Optional reliable mode: (not implemented) would test delivery token/ack behavior

## Engine responsibilities
- Maintain subscription registry keyed by route pattern and per-transport session
- On Publish: find matching subscriptions and route notifications to transports
- Optionally support persistent subscriptions and storage/backlog (not implemented)

## Current implementation status (this repo)
- Subscribe/Unsubscribe via REG with TAG_SUBSCRIBE/TAG_UNSUBSCRIBE: implemented.
- Wildcards/prefix routing: implemented.
- Best-effort delivery (drop on backpressure): implemented.
- Session-scoped subscriptions and automatic cleanup on disconnect: implemented.
- Reliable mode with delivery tokens/acks: not implemented (future work).
- Persistent subscriptions: not implemented (future work).

## Implementation roadmap (priority)
1. Implement in-memory subscription registry and Publish -> notify dispatch in `src/core/engine.rs`.
2. Add route matching (exact + prefix) and simple wildcard support.
3. Implement per-transport subscription lifecycle (remove on disconnect).
4. Add per-subscriber buffering and backpressure policy.
5. (Optional) Add reliable notice mode with DLQ and ack semantics.

---

End of notice spec.

## Test Coverage

Consolidated test coverage notes and inventory for the Notice domain.

### Overview
Comprehensive test coverage for pub/sub operations (extracted from older test drafts and `tests/notice.rs`). The suite focuses on delivery, subscription lifecycle, metadata handling, error handling, and backpressure.

### Test Inventory (representative)
- Basic Pub/Sub
  - `should_deliver_notice_to_single_subscriber`
  - `should_deliver_notice_to_multiple_subscribers`
  - `should_support_hierarchical_route_matching`

- Subscription Management
  - `should_unsubscribe_successfully`
  - `should_subscribe_with_channel_id_one`
  - `should_subscribe_with_channel_id_two`
  - `should_cleanup_channel_subscriptions`
  - other subscription lifecycle tests

- Metadata
  - `should_deliver_notice_with_metadata`

- Error Handling
  - `should_not_deliver_notice_to_unsubscribed_route`
  - `should_handle_publish_when_no_subscribers_exist`
  - `should_not_receive_notices_after_unsubscribe`
  - `should_handle_invalid_subscription_route`
  - `should_handle_unsubscribe_with_invalid_id`
  - `should_handle_channel_cleanup_for_nonexistent_channel`

- Backpressure
  - `should_handle_subscriber_channel_full_backpressure`

### Implementation status (at consolidation time)
- Total tests (inventory): ~16
- Tests implemented in this repo: core matching & publish/subscription unit tests exist in `src/core/notice/*` (route table and service)
- Blockers: none for unit tests; some integration-style tests that exercise engine wiring may remain.

### Next steps
1. Ensure unit tests cover each listed behavior (small, focused tests per the repo test guidelines).
2. Add integration tests that exercise `NoticeDomain` handling in the engine path (subscribe via REG frames, publish via DAT frames).
3. Add CI gating for notice tests and keep coverage notes updated here.

