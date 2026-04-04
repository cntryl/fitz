# Lease

## A. Domain Purpose Statement

Lease provides single-broker ownership coordination with TTL expiry and process-local fencing tokens.

- Problem solved: current-process mutual exclusion and wait/renew/release coordination.
- Optimized for: low-latency in-memory ownership decisions and explicit contention handling.
- Not trying to do: durable lock recovery, distributed consensus, or cross-node fencing.
- Adjacent overlap: Queue has visibility leases, but Queue leases are queue-delivery state, not lease-domain ownership tokens.
- Strict boundary: if a workflow needs durable lock recovery or cluster-wide fencing, Lease is the wrong domain.

## B. Semantic Contract

Clients can rely on the following:

- One live holder exists per lease identity within the running broker process.
- Acquire returns ownership state plus a fencing token when the lease is granted.
- Renew and release require the correct live token.
- Query reports current live state for the current broker process.
- Wait semantics are explicit: immediate failure or queued wait depending on request parameters.

Server guarantees:

- Lease is explicitly ephemeral and in-memory.
- Disconnect cleanup removes session-owned lease state.
- Broker restart removes all lease state.
- Fencing tokens are monotonic only within the lifetime of the current actor or broker process.
- Waiters follow the actor's intended FIFO queueing behavior.
- Expired leases eventually become acquirable again.

Contention semantics:

- If a lease is held and wait is disabled, the caller gets an explicit held-by-other style result.
- If waiting is requested and the queue has capacity, the caller is queued.
- If the same owner is already queued, the server reports that state rather than duplicating the waiter.
- If the same owner already holds the lease, reacquire does not mint a new independent ownership state.

Failover semantics:

- Failover is local and TTL-based only.
- Expiry or explicit release makes the lease available again.
- Restart is not a recovery path; it is loss of lease state.

Intentionally unsupported:

- Crash-safe lease recovery.
- Cross-node fencing guarantees.
- Persistent wait queues.
- Durable handoff after restart.

## C. Non-Negotiable Invariants

- Invariant: only one live holder exists for one lease identity at a time.
	- Why it matters: ownership ambiguity defeats the purpose of the domain.
	- How it fails: concurrent acquires both succeed.
	- How to test it: [tests/lease_basics.rs](../../tests/lease_basics.rs) `should_grant_lease_to_first_requester` and `should_reject_second_requester_when_lease_is_held`.

- Invariant: fencing tokens are monotonic only within the local live contract.
	- Why it matters: callers must know exactly how much trust to place in a token.
	- How it fails: token reuse within one actor lifetime or docs implying cross-restart monotonicity.
	- How to test it: [tests/lease_basics.rs](../../tests/lease_basics.rs) `should_issue_monotonically_increasing_tokens` and [tests/lease_advanced.rs](../../tests/lease_advanced.rs) `should_reset_fencing_tokens_when_actor_is_recreated`.

- Invariant: wrong token cannot renew or release.
	- Why it matters: stale ownership must not mutate live state.
	- How it fails: renew or release accepts an invalid token.
	- How to test it: [tests/lease_e2e.rs](../../tests/lease_e2e.rs) `should_prevent_renew_with_invalid_token` and [tests/lease_advanced.rs](../../tests/lease_advanced.rs) `should_fail_renew_with_invalid_token`.

- Invariant: expired or released leases eventually become acquirable again.
	- Why it matters: progress depends on ownership turnover.
	- How it fails: expired leases remain stuck or hidden.
	- How to test it: [tests/lease_e2e.rs](../../tests/lease_e2e.rs) `should_release_lease_and_allow_reacquisition`, `should_grant_waiting_acquire_when_holder_releases`, and [tests/lease_advanced.rs](../../tests/lease_advanced.rs) `should_handle_renew_on_expired_lease`.

- Invariant: queued waiters preserve the intended ordering semantics.
	- Why it matters: contention behavior must be predictable.
	- How it fails: later waiters leapfrog earlier ones or duplicate queue entries appear.
	- How to test it: [tests/lease_advanced.rs](../../tests/lease_advanced.rs) `should_grant_fifo_order_verified_via_query`, `should_preserve_fifo_order_through_single_waiter_lifecycle`, and `should_block_new_acquirers_while_waiter_is_being_granted`.

- Invariant: disconnect cleanup and restart-loss are explicit parts of the contract.
	- Why it matters: Lease must never sound durable when it is not.
	- How it fails: state survives unexpectedly or docs imply it should.
	- How to test it: [tests/lease_e2e.rs](../../tests/lease_e2e.rs) `should_remove_session_owned_leases_when_client_disconnects` and `should_lose_all_leases_on_broker_restart`.

## D. Anti-Goals / What This Domain Must Not Become

- Lease must not become a hidden durable lock service.
- Lease must not imply cross-node or cross-restart fencing safety.
- Lease must not blur queue visibility leases with lease-domain ownership.
- Lease must not hide restart-loss behind graceful wording.
- Lease must not accumulate durable waiter state.

## E. Failure Semantics

- Client disconnect: session-owned live lease state is cleaned up.
- Server restart: all lease state is lost.
- Timeout while waiting: acquire returns timeout rather than preserving a durable waiter.
- Invalid token: renew and release fail.
- Queue depth exhausted: waiting acquire fails explicitly.
- Invalid request: malformed or out-of-scope operations are rejected.

## F. Observability Requirements

Operators must be able to inspect:

- active live leases
- current holder identity
- current fencing token
- expiry window
- pending waiter count
- acquire, expire, and forced-release counters

Current surface:

- Admin APIs expose live in-memory lease rows for the current broker process.
- Global stats include `leases_active` and `operations_per_second`.
- Prometheus currently exports `fitz_lease_active`.

Current gaps to keep explicit:

- [src/api/admin/stats.rs](../../src/api/admin/stats.rs) does not yet implement the per-domain Lease stats endpoint.
- Current metrics do not expose waiter depth, timeout count, invalid-token count, or forced-release count.
- Admin views are current-process only and must not be described as durable recovery state.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/lease_basics.rs](../../tests/lease_basics.rs) `should_grant_lease_to_first_requester`
	- [tests/lease_basics.rs](../../tests/lease_basics.rs) `should_issue_monotonically_increasing_tokens`
	- [tests/lease_advanced.rs](../../tests/lease_advanced.rs) `should_grant_fifo_order_verified_via_query`
- Restart and recovery tests:
	- [tests/lease_e2e.rs](../../tests/lease_e2e.rs) `should_lose_all_leases_on_broker_restart`
- Race and cleanup tests:
	- [tests/lease_e2e.rs](../../tests/lease_e2e.rs) `should_remove_session_owned_leases_when_client_disconnects`
	- [tests/lease_advanced.rs](../../tests/lease_advanced.rs) `should_block_new_acquirers_while_waiter_is_being_granted`
- Integration tests:
	- [tests/lease_e2e.rs](../../tests/lease_e2e.rs) acquire, renew, release, queued wait, timeout, disconnect, and restart coverage over TCP and WebSocket
- Benchmark and stress tests:
	- tier3 Lease acquire, renew, query, and mixed contention benches
	- tier4 Lease transport benches

## H. Cross-Domain Boundaries

- Lease versus Queue: queue lease tokens control message visibility; lease-domain tokens control explicit ownership.
- Lease versus KV: Lease can guard a workflow around KV, but it does not make KV transactions durable across disconnect.
- Lease versus Schedule: Schedule can trigger work, but Lease is the explicit local ownership surface if one run must exclude another.

## I. Ambiguity Risks

- Fencing tokens can be misread as durable identifiers if restart-loss is not stated every time.
- Wait queue behavior can be misread as durable reservation if timeout and restart semantics are vague.
- Queue and Lease both use the word lease; the docs must keep their meanings separate.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `Lease is an in-memory single-broker coordination primitive. Ownership and fencing tokens disappear on disconnect cleanup or broker restart.`
- Use this sentence when comparing Lease and Queue: `Queue visibility leases protect message delivery state; Lease protects explicit coordination state.`
- Remove any wording that suggests Lease provides crash-safe or cross-node fencing guarantees.
