# Queue

## A. Domain Purpose Statement

Queue provides durable competing-consumer work delivery with visibility leases, redelivery, and optional dead-lettering.

- Problem solved: durable work backlog that consumers reserve, extend, complete, and retry.
- Optimized for: at-least-once delivery, durable queue state, fair enough competing-consumer distribution, and bounded hot in-memory coordination.
- Not trying to do: exactly-once delivery, durable inflight lease recovery, or durable event history.
- Adjacent overlap: RPC also routes work to workers, but RPC is live-only. Stream also stores records, but Stream is immutable history rather than mutable work state.
- Strict boundary: if the system needs durable work lifecycle, use Queue; if it needs immutable replayable history, use Stream.

## B. Semantic Contract

Clients can rely on the following:

- Enqueue persists a message into durable queue state according to the queue's configured write policy.
- Reserve grants exclusive live processing ownership for a message using a lease token and visibility timeout.
- Complete removes the leased message when the correct live token is supplied.
- Extend prolongs the current live lease when the correct token is supplied.
- Expired leases return messages to availability for redelivery.
- Optional `max_attempts` moves terminal failures into durable dead-letter state.

Server guarantees:

- Queue committed messages and primary indexes survive restart according to the configured write policy.
- Inflight lease ownership, lease tokens, and warm actor state are ephemeral.
- Delivery is at-least-once, not exactly-once.
- Ready-queue ordering is maintained inside the durable ready sequence, but competing consumers do not imply a strict global consumption order.
- Duplicate completion with the same live token is handled explicitly rather than mutating state twice.
- Dead-letter replay is an explicit admin action on retained DLQ rows, not a hidden replay system.

Fairness and ordering expectations:

- Queue uses simple fair competing-consumer distribution over ready work.
- Queue is not a strict end-to-end FIFO guarantee once multiple consumers compete.
- Ready order is meaningful; consumer completion order is not guaranteed to match enqueue order.

Crash and disconnect semantics:

- Committed messages remain durable.
- Old inflight tokens and live lease ownership do not survive reconnect or restart.
- A client cannot safely continue an old lease after reconnect.

Long-poll semantics:

- Long poll behavior is handled at the RPC layer using availability hints.
- Queue actor state remains synchronous and does not store durable waiting callers.

Intentionally unsupported:

- Exactly-once delivery.
- Durable inflight lease recovery across restart.
- Turning queue leases into cluster-wide lease-domain tokens.
- Stream-style history replay for normal queue traffic.

## C. Non-Negotiable Invariants

- Invariant: one message cannot be leased to two consumers at the same time.
	- Why it matters: competing-consumer correctness depends on exclusive live reservation.
	- How it fails: reserve returns the same message to multiple consumers before expiry.
	- How to test it: [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_isolate_leases_between_consumers` and `should_support_multiple_concurrent_consumers`.

- Invariant: valid completion removes the message exactly once.
	- Why it matters: completion must not double-delete or leave ghost backlog.
	- How it fails: repeated completion mutates state twice, or correct completion leaves the message visible.
	- How to test it: [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_support_complete_operation_with_lease_token`, `should_deduplicate_complete_for_same_lease_token`, and `should_persist_message_until_completed`.

- Invariant: expired lease returns the message to availability unless DLQ policy takes over.
	- Why it matters: abandoned work must not disappear.
	- How it fails: expired messages remain stuck inflight or are lost.
	- How to test it: [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_return_message_to_queue_on_lease_expiry` and [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_redelivery_message_on_lease_expiration`.

- Invariant: DLQ transition happens exactly once on the terminal failure path.
	- Why it matters: dead-lettering must not duplicate or resurrect terminal messages silently.
	- How it fails: a message dead-letters twice or continues normal redelivery after crossing `max_attempts`.
	- How to test it: [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_dlq_message_after_max_attempts`.

- Invariant: restart does not lose committed messages or durable indexes.
	- Why it matters: Queue is the durable work backlog surface.
	- How it fails: committed messages, delayed visibility, or next-id state regress after restart.
	- How to test it: [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_redelivery_messages_after_crash`, `should_preserve_fifo_order_after_recovery`, `should_preserve_delayed_visibility_across_restart`, and `should_prevent_id_collisions_across_crash`.

- Invariant: wrong or expired lease token cannot extend or complete a message.
	- Why it matters: stale ownership must not mutate queue state.
	- How it fails: an old token can complete or extend after expiry or redelivery.
	- How to test it: [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_reject_complete_with_wrong_lease_token`, `should_reject_extend_with_expired_lease`, and `should_use_4013_for_invalid_lease_token`.

## D. Anti-Goals / What This Domain Must Not Become

- Queue must not become vague pub/sub with ack semantics.
- Queue must not imply exactly-once delivery.
- Queue must not turn inflight lease tokens into durable recovery handles.
- Queue must not impersonate Stream replay for normal backlog consumption.
- Queue must not hide buffered write-policy tradeoffs behind synchronous durability language.

## E. Failure Semantics

- Client disconnect: Fitz does not promise durable continuation of the old lease token. Redelivery follows expiry or restart recovery.
- Server restart: committed messages and durable indexes recover; inflight lease ownership and tokens do not.
- Storage failure during durable mutation: enqueue, complete, or DLQ mutation fails and must not be reported as committed success.
- Empty queue reserve: explicit empty result.
- Invalid batch size or invalid token: explicit error.
- Backpressure: transport or RPC-layer backpressure is separate from queue durability.
- Admin DLQ replay: explicit destructive operation that moves one retained dead-letter row back to ready state.

## F. Observability Requirements

Operators must be able to inspect:

- messages ready
- messages delayed
- messages pending total
- messages dead-lettered
- active live queue leases
- DLQ rows per queue
- retry and redelivery counts
- lease expiry counts

Current surface:

- Global stats include `messages_ready`, `messages_delayed`, `messages_pending`, `messages_dead_lettered`, `leases_active`, and `operations_per_second`.
- Prometheus currently exports `fitz_queue_messages_pending` and `fitz_queue_leases_active`.
- Admin APIs expose warm queue detail, live leases, retained dead letters, and DLQ replay and purge actions.
- Queue is the only domain in [src/api/admin/stats.rs](../../src/api/admin/stats.rs) with a currently implemented per-domain stats response.

Current gaps to keep explicit:

- Metrics do not yet expose redelivery count, DLQ transition count, complete-token reject count, or backlog age histograms.
- Admin queue views are partly warm-actor views and must not be described as a full durable catalog of every queue without traffic.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_support_enqueue_operation`
	- [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_support_complete_operation_with_lease_token`
	- [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_distribute_messages_fairly_among_consumers`
- Restart and recovery tests:
	- [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_redelivery_messages_after_crash`
	- [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_preserve_fifo_order_after_recovery`
	- [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_preserve_delayed_visibility_across_restart`
- Race and cleanup tests:
	- [tests/queue_basics.rs](../../tests/queue_basics.rs) `should_deduplicate_complete_for_same_lease_token`
	- [tests/queue_advanced.rs](../../tests/queue_advanced.rs) `should_distribute_messages_fairly_among_competing_consumers`
- Integration tests:
	- [tests/queue_e2e.rs](../../tests/queue_e2e.rs) enqueue, dequeue, empty, concurrent enqueue, and mixed flow cases
	- [tests/admin_api.rs](../../tests/admin_api.rs) DLQ replay admin coverage
- Benchmark and stress tests:
	- tier3 Queue sustained-load, mixed lifecycle, and backlog-depth benches
	- tier4 Queue transport benches

## H. Cross-Domain Boundaries

- Queue versus RPC: Queue is durable backlog with reservation and redelivery; RPC is live request and response.
- Queue versus Stream: Queue models mutable work lifecycle; Stream models immutable history.
- Queue versus Lease: queue lease tokens govern message visibility only; they are not lease-domain fencing tokens.
- Queue versus Notice: Notice availability signals are hints around queue behavior, not durable queue state.

## I. Ambiguity Risks

- The word `lease` can make Queue sound like Lease domain coordination. It is not the same contract.
- FIFO language can be overstated if docs ignore competing-consumer effects.
- DLQ `replay` can be misread as Stream-like replay. It is only an admin move from retained DLQ state back to ready state.
- Buffered commits can be misdescribed as sync durability if docs are careless.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `Queue provides durable at-least-once work delivery. Committed messages survive restart; live lease ownership does not.`
- Use this sentence when comparing Queue and RPC: `Queue is for work that may wait durably. RPC is for work that must be answered by a live worker.`
- Use this sentence when comparing Queue and Stream: `Queue is mutable work state. Stream is immutable history.`
