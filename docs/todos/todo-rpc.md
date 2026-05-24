# RPC

This file defines RPC-specific contract detail and proof points. For Fitz-wide domain ownership, interaction rules, complexity budgets, and future feature admission, use [../development/domain-boundaries-spec.md](../development/domain-boundaries-spec.md) together with [todo-all.md](todo-all.md).

## A. Domain Purpose Statement

RPC provides live request and response dispatch to currently registered workers.

- Problem solved: low-latency request routing to live workers with correlation and optional streaming responses.
- Optimized for: interactive call and response, bounded pending queues, and in-memory worker pools.
- Not trying to do: durable request backlog, durable worker registration, or replayable work recovery.
- Adjacent overlap: Queue also routes work, but Queue owns durable backlog and redelivery; RPC owns live request and response.
- Strict boundary: if the caller needs durable buffering or restart-safe pending work, that belongs to Queue or Stream-backed workflow design, not RPC.

## B. Semantic Contract

Clients can rely on the following:

- Each accepted request is associated with one correlation id and one live pending request slot.
- Responses must carry the original correlation id.
- A route has a bounded pending queue.
- If a live worker is available, a request is dispatched to one worker.
- If no live worker is immediately available and queue capacity remains, the request may wait in the per-route pending queue.
- Streaming responses use sequence numbers and an explicit final-chunk signal.

Server guarantees:

- RPC is explicitly ephemeral.
- Worker registrations, pending requests, and reply routing live only in memory.
- Broker restart drops all workers and all pending requests.
- Request timeout and backpressure are explicit contract outcomes, not hidden retries.
- Pending request order is FIFO within one route's queue.
- Streaming chunks for one response must follow the strict sequence contract.

Best-effort only:

- Work completion after a request is accepted.
- Worker survival after dispatch.
- Recovery of a caller or worker after disconnect.

Intentionally unsupported:

- Durable request queues.
- Durable worker inventory.
- Broker-side replay or deduplication by correlation id.
- Exactly-once request execution across restart.

Worker registration semantics:

- Workers register live routes on the current broker process.
- Disconnect or broker restart removes the registration.
- A worker must re-register after reconnect.

Timeout and no-worker semantics:

- If no suitable worker becomes available before timeout, Fitz returns an RPC timeout error.
- If pending capacity is exhausted, Fitz returns backpressure rather than silently growing memory.
- An unregistered or unknown route is rejected rather than stored as durable future work.

Streaming response semantics:

- Chunks are correlated by correlation id.
- Sequence numbers must be in-order.
- Final chunk closes the live stream state.
- RPC sequence numbers are response-assembly state, not replay cursors.

## C. Non-Negotiable Invariants

- Invariant: one correlation id maps to exactly one live pending request at a time.
	- Why it matters: caller identity and response routing depend on it.
	- How it fails: duplicate pending entries or reused correlation ids route a response to the wrong call.
	- How to test it: [tests/rpc_basics.rs](../../tests/rpc_basics.rs) `should_correlate_response_with_request` and `should_match_response_to_request_by_correlation_id`, plus [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_reject_wrong_correlation_response_after_accept_tcp` and `should_reject_wrong_correlation_response_after_accept_ws`. Sink-level regressions in [src/boot/domains/rpc_sink.rs](../../src/boot/domains/rpc_sink.rs): `should_reject_duplicate_live_correlation_given_rpc_sink`, `should_reject_worker_response_from_non_owner_session_given_rpc_sink`, and `should_reject_worker_ack_from_non_owner_session_given_rpc_sink`.

- Invariant: a response cannot be delivered to the wrong caller or wrong worker route.
	- Why it matters: misrouting is worse than a visible failure.
	- How it fails: reply route ownership or worker registration cleanup leaks across sessions.
	- How to test it: [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_reject_wrong_correlation_response_after_accept_tcp`, `should_reject_wrong_correlation_response_after_accept_ws`, `should_return_worker_disconnect_error_after_unsubscribe_tcp`, and `should_return_worker_disconnect_error_after_unsubscribe_ws`. Sink-level regressions in [src/boot/domains/rpc_sink.rs](../../src/boot/domains/rpc_sink.rs): `should_reject_worker_response_from_non_owner_session_given_rpc_sink` and `should_reject_worker_ack_from_non_owner_session_given_rpc_sink`.

- Invariant: a timed-out request never becomes live again.
	- Why it matters: timeout must be a terminal caller-visible outcome.
	- How it fails: late worker response resurrects a request after timeout.
	- How to test it: [tests/rpc_advanced.rs](../../tests/rpc_advanced.rs) `should_drop_late_response_after_lease_expired` and `should_reject_late_worker_response_after_timeout_given_rpc_sink`, plus [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_return_rpc_timeout_error_after_accept_tcp` and `should_return_rpc_timeout_error_after_accept_ws`.

- Invariant: backpressure is explicit and bounded.
	- Why it matters: RPC must not become an unbounded backlog.
	- How it fails: queue-full requests are silently accepted or hidden in memory growth.
	- How to test it: [tests/rpc_basics.rs](../../tests/rpc_basics.rs) `should_reject_request_when_queue_is_full` and [tests/rpc_advanced.rs](../../tests/rpc_advanced.rs) `should_handle_backpressure_when_queue_full` and `should_reject_rpc_request_when_pending_capacity_reached_given_rpc_sink`.

- Invariant: worker and pending-request cleanup happens exactly once.
	- Why it matters: double cleanup or missed cleanup breaks route ownership and leaks live state.
	- How it fails: disconnect or restart leaves live registrations or pending ownership behind.
	- How to test it: [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_require_worker_reregistration_after_broker_restart_tcp`, `should_require_worker_reregistration_after_broker_restart_ws`, `should_drop_pending_requests_on_broker_restart_tcp`, and `should_drop_pending_requests_on_broker_restart_ws`.

- Invariant: invalid streaming sequence cannot corrupt live RPC state.
	- Why it matters: sequence corruption must fail the stream, not poison future responses.
	- How it fails: out-of-order or duplicate chunks are accepted as valid continuation.
	- How to test it: [tests/rpc_advanced.rs](../../tests/rpc_advanced.rs) `should_fail_out_of_order_chunks`, `should_fail_when_gap_appears_mid_stream`, `should_fail_when_final_chunk_arrives_before_gap_is_closed`, `should_drop_duplicate_chunks`, and [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_reject_invalid_sequence_response_after_accept_tcp` and `should_reject_invalid_sequence_response_after_accept_ws`.

## D. Anti-Goals / What This Domain Must Not Become

- RPC must not become a durable request queue.
- RPC must not hide worker loss behind fake durability language.
- RPC must not use correlation ids as if they were replay or dedup tokens.
- RPC must not blur live streaming responses into Stream-style durable history.
- RPC must not silently retry or resurrect timed-out work.

## E. Failure Semantics

- Caller disconnect: live pending state may be cleaned up; Fitz does not promise durable caller recovery.
- Worker disconnect before response: the request fails with a worker-disconnect-style error once the domain detects the loss.
- Server restart: all worker registrations, pending requests, and reply routes are lost.
- Timeout: request completes with explicit timeout error code rather than staying pending forever.
- Backpressure: request is rejected with explicit backpressure error when pending capacity is exhausted.
- Invalid request: unknown service, unknown method, invalid sequence, wrong worker, and wrong correlation are explicit failures.
- Late response after timeout or cleanup: dropped; it must not reopen the request.

## F. Observability Requirements

Operators must be able to inspect:

- registered workers
- pending requests
- timeout rate
- backpressure rejects
- wrong-worker and wrong-correlation rejects
- late-response drops
- active streaming response count

Current surface:

- Admin APIs expose live workers and live pending requests for the current broker instance.
- Global stats include `workers_registered`, `requests_pending`, and `operations_per_second`.
- Per-domain admin stats include live workers, pending requests, oldest pending age, pending route count, worker latency buckets, timeout and backpressure counters, duplicate-correlation and wrong-worker rejects, late-response/missing-pending drops, invalid-sequence response handling, and diagnostics.
- Prometheus exports `fitz_rpc_workers_registered`, `fitz_rpc_requests_pending`, worker latency buckets, request timeouts, backpressure rejects, duplicate-correlation rejects, wrong-worker rejects, late-response drops, missing-pending responses, and invalid-sequence response/error counters.
- Sink-level counters are now emitted for wrong-worker and duplicate-correlation rejects: `rpc_requests_rejected_duplicate_correlation_total`, `rpc_responses_rejected_wrong_worker_total`, and `rpc_acks_rejected_wrong_worker_total`.
- Error codes 6007 (`ERR_RPC_DUPLICATE_CORRELATION`) and 6008 (`ERR_RPC_WRONG_WORKER`) are defined in [src/protocol/error_codes.rs](../../src/protocol/error_codes.rs) and asserted in [tests/rpc_basics.rs](../../tests/rpc_basics.rs) via `should_define_error_code_6007_rpc_duplicate_correlation` and `should_define_error_code_6008_rpc_wrong_worker`.

Current gaps to keep explicit:

- Admin views are current-process only and must not be described as durable backlog state.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/rpc_basics.rs](../../tests/rpc_basics.rs) `should_route_request_to_available_worker`
	- [tests/rpc_basics.rs](../../tests/rpc_basics.rs) `should_maintain_request_order_in_queue`
	- [tests/rpc_basics.rs](../../tests/rpc_basics.rs) `should_handle_streaming_response_with_multiple_chunks`
- Restart and recovery tests:
	- [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_require_worker_reregistration_after_broker_restart_tcp`
	- [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) `should_drop_pending_requests_on_broker_restart_tcp`
- Race and cleanup tests:
	- [tests/rpc_advanced.rs](../../tests/rpc_advanced.rs) `should_drop_late_response_after_lease_expired`
	- [tests/rpc_advanced.rs](../../tests/rpc_advanced.rs) `should_reject_late_worker_response_after_timeout_given_rpc_sink`
	- [src/boot/domains/rpc_sink.rs](../../src/boot/domains/rpc_sink.rs) `should_reject_duplicate_live_correlation_given_rpc_sink`
	- [src/boot/domains/rpc_sink.rs](../../src/boot/domains/rpc_sink.rs) `should_reject_worker_response_from_non_owner_session_given_rpc_sink`
	- [src/boot/domains/rpc_sink.rs](../../src/boot/domains/rpc_sink.rs) `should_reject_worker_ack_from_non_owner_session_given_rpc_sink`
- Integration tests:
	- [tests/rpc_e2e.rs](../../tests/rpc_e2e.rs) worker disconnect, timeout, wrong correlation, and invalid sequence cases
- Benchmark and stress tests:
	- tier3 RPC pending-cardinality and worker-pool benches
	- tier4 transport-path RPC benches

## H. Cross-Domain Boundaries

- RPC versus Queue: RPC is live request and response; Queue is durable backlog and acknowledgement.
- RPC versus Stream: RPC streaming chunks are not durable replay.
- RPC versus Notice: Notice is broadcast fanout; RPC is single-request correlation.

## I. Ambiguity Risks

- Correlation ids can be misread as durable dedup tokens. They are not.
- Pending queues can be misread as durable backlog if docs blur restart behavior.
- Streaming response support can be misread as durable replay if sequence language is too loose.
- Route-not-registered, timeout, and backpressure semantics must stay visibly different.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `RPC is a live, in-memory request and response facility. Worker registrations and pending requests do not survive broker restart.`
- Use this sentence when comparing RPC and Queue: `Use RPC when a live worker must answer now; use Queue when work must wait durably.`
- Use this sentence when describing correlation ids: `Correlation ids match live requests to live responses. They do not create broker-side replay, recovery, or dedup semantics.`
