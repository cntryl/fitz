# Notice

This file defines Notice-specific contract detail and proof points. For Fitz-wide domain ownership, interaction rules, complexity budgets, and future feature admission, use [../development/domain-boundaries-spec.md](../development/domain-boundaries-spec.md) together with [todo-all.md](todo-all.md).

## A. Domain Purpose Statement

Notice provides live fanout to subscribers that are connected now.

- Problem solved: low-latency publish and wildcard subscribe for current listeners.
- Optimized for: simple live delivery, low coordination cost, and bounded in-memory subscription matching.
- Not trying to do: replay, missed-message recovery, durable subscriber state, or durable publish history.
- Adjacent overlap: Stream also distributes events, but Stream owns durable history and recovery.
- Strict boundary: if a client needs rebuild, catch-up, audit history, or deterministic replay, that belongs to Stream, not Notice.

## B. Semantic Contract

Clients can rely on the following:

- A successful subscribe creates live in-memory subscription state on the current broker process only.
- Exact routes and wildcard patterns are matched against publishes in the same RouteFamily only.
- A publish is fanned out once to each matching live subscription in the current process snapshot.
- Repeating the same effective subscription must not create duplicate deliveries for that subscription.
- Disconnect removes session-owned subscriptions.
- Broker restart removes all Notice subscriptions. Clients must re-subscribe.

Server guarantees:

- Notice is explicitly ephemeral and session-scoped.
- Publish acknowledgement means the broker accepted the publish request. It does not mean any subscriber received it.
- Delivery is attempted only for subscriptions alive when publish fanout is computed.
- No replay is attempted on reconnect or restart.
- No durable recovery of missed deliveries exists.
- Wildcard subscriptions are bounded by a per-session limit.

Best-effort only:

- Subscriber delivery after publish acceptance.
- Delivery under downstream routing failure or backpressure.
- Relative fanout timing between different subscribers.

Intentionally unsupported:

- Replay by offset, time, cursor, or last N.
- Durable publish logs.
- Broker-managed consumer positions.
- Recovery of messages missed while disconnected.

Disconnect semantics:

- When a subscriber disconnects, its Notice subscriptions are removed.
- Messages published after disconnect are missed permanently by that subscriber unless the producer also wrote the event to Stream.

Ordering and duplicates:

- Notice guarantees match correctness, not durable sequencing.
- Fitz does not expose a Notice sequence number, resume token, or replay cursor.
- Fitz must not emit duplicate deliveries for the same live subscription because of duplicate subscribe registration.
- Fitz does not promise a stable cross-subscriber delivery order.

Subscription semantics:

- Subscribe is session-scoped.
- Unsubscribe removes only the targeted live subscription.
- Unsubscribe-all on cleanup removes all Notice subscriptions owned by the session.
- Wildcard support is part of the domain contract, but wildcard count is bounded to protect memory.

## C. Non-Negotiable Invariants

- Invariant: Notice never implies replay or recovery semantics.
	- Why it matters: this is the primary boundary between Notice and Stream.
	- How it fails: docs, admin APIs, or reconnect behavior silently pretend missed Notice traffic can be rebuilt.
	- How to test it: [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_remove_notice_subscription_when_subscriber_disconnects` and `should_require_resubscribe_after_broker_restart`.

- Invariant: no subscription receives a message after unsubscribe or disconnect cleanup.
	- Why it matters: leaked subscriptions create false fanout and break trust.
	- How it fails: cleanup misses a session-owned subscription, or unsubscribe removes the wrong row.
	- How to test it: [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_retain_other_notice_subscription_after_unsubscribe` and `should_remove_notice_subscription_when_subscriber_disconnects`.

- Invariant: duplicate subscribe registration does not create duplicate delivery for one logical subscription.
	- Why it matters: duplicate fanout turns at-most-one live delivery attempt into accidental amplification.
	- How it fails: the index inserts duplicate entries for the same session and pattern.
	- How to test it: [tests/notice_basics.rs](../../tests/notice_basics.rs) `should_not_duplicate_delivery_for_same_subscription` and [tests/notice_advanced.rs](../../tests/notice_advanced.rs) `should_not_duplicate_deliveries_for_duplicate_subscriptions`.

- Invariant: Notice fanout never crosses RouteFamily boundaries.
	- Why it matters: RouteFamily is a hard isolation boundary.
	- How it fails: the matcher or router looks up subscriptions without family scoping.
	- How to test it: required missing focused regression such as `should_not_fanout_notice_across_route_families_given_same_pattern`. Current auth and routing coverage is strong, but a direct delivery-isolation proof should exist.

- Invariant: wildcard subscription state stays bounded per session.
	- Why it matters: unbounded wildcard growth turns an ephemeral domain into a memory leak.
	- How it fails: the sink accepts unlimited wildcard patterns for one session.
	- How to test it: [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_reject_wildcard_subscription_when_session_limit_is_exceeded`, `should_allow_exact_subscription_when_wildcard_limit_is_reached`, and `should_allow_wildcard_subscription_after_unsubscribe_releases_session_budget`.

- Invariant: matching correctness outranks subscriber ordering convenience.
	- Why it matters: Notice is trustworthy only if the right live subscribers are matched every time.
	- How it fails: wildcard resolution misses or over-matches routes.
	- How to test it: [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_match_single_wildcard_pattern`, `should_match_double_wildcard_pattern`, `should_match_multiple_subscribers_on_overlapping_patterns`, and `should_deliver_to_exact_match_before_wildcard`.

## D. Anti-Goals / What This Domain Must Not Become

- Notice must not become pseudo-durable pub/sub with vague replay promises.
- Notice must not accumulate broker-side resume tokens, cursors, or consumer groups.
- Notice must not silently depend on Stream-like storage while still advertising itself as ephemeral.
- Notice must not hide missed deliveries behind reconnect magic.
- Notice must not become a work queue with ack, retry, or dead-letter semantics.

## E. Failure Semantics

- Client disconnect: all session-owned subscriptions are removed; subsequent messages are missed permanently.
- Server restart: all Notice subscriptions are lost; clients must re-subscribe.
- Storage failure: Notice has no durable Notice-state recovery path. Publish and subscribe are not backed by a Notice history store.
- Backpressure or downstream routing failure: publish acceptance does not imply subscriber delivery. Current delivery is best-effort and per-subscriber routing failures are not recoverable via Notice.
- Invalid request: malformed patterns or invalid subscription operations are rejected.
- Stale subscription id: the id is meaningful only while the live subscription exists on the current broker process.

## F. Observability Requirements

Operators must be able to inspect:

- active live subscriptions
- active routes with subscriber counts
- publish rate for the current broker process
- wildcard-limit rejections
- delivery-drop or route-failure counters

Current surface:

- Admin read model exposes live Notice subscriptions and routes.
- Global stats include `subscriptions_active` and `publishes_per_second`.
- Prometheus currently exports `fitz_notice_subscriptions_active`.

Current gaps to keep explicit:

- [src/api/admin/stats.rs](../../src/api/admin/stats.rs) has a stub route for the per-domain Notice stats endpoint; it currently returns not_implemented. The domain data is not yet populated.
- The metrics surface does not yet expose publish-failure, wildcard-limit-reject, or dropped-delivery counters.
- Admin views are live current-process state only and must never be described as durable history.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/notice_basics.rs](../../tests/notice_basics.rs) `should_deliver_notification_to_exact_matching_subscription`
	- [tests/notice_basics.rs](../../tests/notice_basics.rs) `should_not_duplicate_delivery_for_same_subscription`
	- [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_deliver_to_exact_match_before_wildcard`
- Restart and recovery tests:
	- [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_require_resubscribe_after_broker_restart`
- Race and cleanup tests:
	- [tests/notice_e2e.rs](../../tests/notice_e2e.rs) `should_remove_notice_subscription_when_subscriber_disconnects`
	- required missing regression such as `should_not_deliver_notice_after_disconnect_cleanup_given_publish_race`
- Integration tests:
	- [tests/notice_e2e.rs](../../tests/notice_e2e.rs) TCP and WebSocket publish and wildcard cases
- Benchmark and stress tests:
	- [tests/notice_advanced.rs](../../tests/notice_advanced.rs) `should_handle_1k_subscriptions_end_to_end`
	- [tests/notice_advanced.rs](../../tests/notice_advanced.rs) `should_handle_5k_subscriptions_without_failure_end_to_end`

## H. Cross-Domain Boundaries

- Notice versus Stream: Notice is live-only; Stream owns history, replay, and catch-up.
- Notice versus Schedule: Schedule may use live fanout for notifications, but schedule durability does not make Notice durable.
- Notice versus Queue: Queue is for work reservation and acknowledgement; Notice is for live observation only.

## I. Ambiguity Risks

- Broader docs that say Fitz supports generic durable pub/sub can accidentally blur Notice into Stream.
- Operators may assume publish counters imply durable Notice history. They do not.
- Current implementation ignores per-subscriber routing failures during fanout; this must remain documented as best-effort rather than hidden as a guaranteed delivery path.
- Lack of Notice ordering tokens can tempt clients to infer recovery from arrival order. That inference is invalid.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `Notice provides live, ephemeral fanout to subscribers connected now. It does not provide replay, durable subscriber recovery, or missed-message catch-up.`
- Use this sentence wherever recovery is discussed: `If a client must recover after disconnect or rebuild state from history, the producer must also write the event to Stream; Notice alone is insufficient by design.`
- Remove any wording in broader docs that suggests Notice publish acknowledgement means end-to-end delivery.
