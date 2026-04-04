# KV

## A. Domain Purpose Statement

KV provides transactional reads and writes for current authoritative key and value state.

- Problem solved: current-state storage with explicit transaction boundaries over one resource at a time.
- Optimized for: direct Midge-backed committed reads and writes, explicit transaction control, and RouteFamily isolation.
- Not trying to do: event history, replay, cross-resource transactions, or durable recovery of open transactions.
- Adjacent overlap: Stream records history; KV stores current authoritative values.
- Strict boundary: if a workflow needs history or replay, write to Stream; if it needs current authoritative value lookup, use KV.

## B. Semantic Contract

Clients can rely on the following:

- KV operations execute within an active transaction.
- A transaction is bound to one `(realm, area, resource)` scope.
- Committed data survives restart according to the transaction's selected write options.
- `tx_id` is a live handle for the current session only.
- RouteFamily maps to an explicit storage column family; the same logical key can have independent values in different families.

Server guarantees:

- Open transactions, uncommitted writes, and live lock ownership are broker-local session state only.
- Disconnect or restart aborts open transactions.
- `commit` makes the transaction's writes visible according to the configured write policy.
- `rollback` discards uncommitted changes.
- Operations using an invalid or stale `tx_id` must not mutate committed state.

Durability expectations:

- Durability is chosen at `Begin` by `WriteOptions`.
- Sync-style write options are the correctness-first path.
- Buffered write options are throughput-first and must be described as buffered durability rather than per-operation fsync.
- Durability promises apply only to committed transactions.

Isolation expectations:

- Transaction scope is one resource only.
- Fitz does not provide cross-resource atomicity.
- Fitz does not provide cross-session transaction recovery.

Intentionally unsupported:

- Durable transaction logs.
- Cross-resource distributed transactions.
- Replayable history from KV itself.
- Broker-restored transaction handles after reconnect.

## C. Non-Negotiable Invariants

- Invariant: transaction scope never leaks across resource, area, realm, or RouteFamily boundaries.
	- Why it matters: state authority is trustworthy only if scope is exact.
	- How it fails: a live `tx_id` can read or write a different bound resource or family.
	- How to test it: [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_isolate_transactions_across_resources`, [tests/kv_basics.rs](../../tests/kv_basics.rs) realm and area authorization checks, and [tests/kv_advanced.rs](../../tests/kv_advanced.rs) RouteFamily isolation cases.

- Invariant: committed state survives restart according to the selected write policy.
	- Why it matters: KV is the current-state durable surface.
	- How it fails: committed values disappear after restart.
	- How to test it: [tests/kv_advanced.rs](../../tests/kv_advanced.rs) `should_commit_durable_kv_transaction`, `should_restore_committed_kv_value_on_engine_restart`, and the family-isolation restart cases.

- Invariant: rolled-back or uncommitted state is never visible as committed data.
	- Why it matters: disconnect or crash must not leak partial writes.
	- How it fails: restart resurrects uncommitted data, or rollback leaves visible mutation behind.
	- How to test it: [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_rollback_transaction_successfully` and [tests/kv_advanced.rs](../../tests/kv_advanced.rs) `should_discard_uncommitted_kv_write_on_engine_restart`.

- Invariant: invalid or stale `tx_id` never mutates committed state.
	- Why it matters: reconnect cannot safely continue a dead transaction.
	- How it fails: stale handles after reconnect still write data.
	- How to test it: [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_reject_operations_on_invalid_transaction` and `should_reject_stale_transaction_id_after_client_reconnect`.

- Invariant: disconnect or restart aborts open transactions instead of attempting recovery.
	- Why it matters: KV must stay honest about transaction lifetime.
	- How it fails: docs or runtime imply live transaction recovery.
	- How to test it: [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_handle_connection_drop_during_transaction` and [tests/kv_advanced.rs](../../tests/kv_advanced.rs) `should_discard_uncommitted_kv_write_on_engine_restart`.

- Invariant: RouteFamily is a hard committed-data isolation boundary.
	- Why it matters: persistent data isolation depends on it.
	- How it fails: the same logical key leaks value across column families.
	- How to test it: [tests/kv_advanced.rs](../../tests/kv_advanced.rs) `should_return_family_one_value_given_same_key_in_multiple_route_families`, `should_return_family_two_value_given_same_key_in_multiple_route_families`, and the matching restart variants.

## D. Anti-Goals / What This Domain Must Not Become

- KV must not become an implicit event log.
- KV must not imply durable transaction recovery after reconnect.
- KV must not blur single-resource transactions into cross-resource atomic scope.
- KV must not hide write-policy differences behind generic durability language.
- KV must not become a queue or replay system.

## E. Failure Semantics

- Client disconnect: open transaction is lost; the client must begin again.
- Server restart: committed values remain according to write policy; open transactions are lost.
- Storage failure during commit: commit fails; Fitz must not report durable success.
- Invalid request or malformed frame: request is rejected and must not mutate committed state.
- Stale `tx_id` after reconnect: rejected; caller must start a new transaction.
- Backpressure or transport timeout: affects response delivery, not the definition of committed state.

## F. Observability Requirements

Operators must be able to inspect:

- active live transactions
- key count
- operations rate
- committed resource detail
- live transaction inventory for one resource
- commit failure and invalid-transaction counters

Current surface:

- Global stats include `transactions_active`, `keys_total`, and `operations_per_second`.
- Prometheus currently exports `fitz_kv_transactions_active` and `fitz_kv_keys_total`.
- Admin APIs expose committed resource detail and current-process live transaction rows.

Current gaps to keep explicit:

- [src/api/admin/stats.rs](../../src/api/admin/stats.rs) does not yet implement the per-domain KV stats endpoint.
- Metrics do not yet expose commit-failure, rollback, or invalid-transaction counters.
- Admin transaction views are current-process only and must not be described as durable recovery handles.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_complete_begin_put_commit_over_transport`
	- [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_isolate_transactions_across_resources`
	- [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_reject_operations_on_invalid_transaction`
- Restart and recovery tests:
	- [tests/kv_advanced.rs](../../tests/kv_advanced.rs) `should_restore_committed_kv_value_on_engine_restart`
	- [tests/kv_advanced.rs](../../tests/kv_advanced.rs) `should_discard_uncommitted_kv_write_on_engine_restart`
- Race and cleanup tests:
	- [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_handle_connection_drop_during_transaction`
	- [tests/kv_e2e.rs](../../tests/kv_e2e.rs) `should_reject_stale_transaction_id_after_client_reconnect`
- Integration tests:
	- [tests/kv_e2e.rs](../../tests/kv_e2e.rs) TCP and WebSocket transaction flows
- Benchmark and stress tests:
	- tier3 KV contention and mixed read/write benches
	- tier4 transport-path KV transaction benches

## H. Cross-Domain Boundaries

- KV versus Stream: KV is current authoritative state; Stream is history.
- KV versus Lease: Lease can coordinate who writes, but it does not make KV transactions restart-safe.
- KV versus Queue: Queue stores work backlog; KV stores state.

## I. Ambiguity Risks

- Older high-level docs can imply non-transactional KV operations even though the current domain contract is transaction-scoped.
- Buffered writes can be oversold as hard durability if docs collapse them with sync writes.
- Live transaction rows in admin can be misread as recoverable broker state.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `KV provides durable committed current-state storage and ephemeral live transaction handles. Disconnect or restart drops open transactions.`
- Use this sentence when comparing KV and Stream: `KV answers what the state is now. Stream answers how it got there.`
- Remove wording that suggests Fitz restores open KV transactions after reconnect.
