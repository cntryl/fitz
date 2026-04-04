# Stream

This file defines Stream-specific contract detail and proof points. For Fitz-wide domain ownership, interaction rules, complexity budgets, and future feature admission, use [../development/domain-boundaries-spec.md](../development/domain-boundaries-spec.md) together with [todo-all.md](todo-all.md).

## A. Domain Purpose Statement

Stream is Fitz's durable append, replay, and catch-up domain.

- Problem solved: committed ordered history that survives restart and can be read again from offsets.
- Optimized for: append throughput, durable committed sequencing, and deterministic client-driven resume.
- Not trying to do: broker-managed consumer groups, durable writer sessions, or queue-style work reservation.
- Adjacent overlap: Notice also emits change notifications, but Stream owns durable history and recovery.
- Strict boundary: if a client needs rebuild, replay, or backfill, it must use Stream rather than Notice.

## B. Semantic Contract

Clients can rely on the following:

- Stream append is a two-phase live session: `Begin`, one or more `Append`, then `Commit` or `Rollback`.
- Only one active append session may exist for a given resource at a time.
- Committed records survive broker restart according to the selected stream write mode.
- Reads are offset-based.
- Resource reads are exact-history reads for one resource stream.
- Area and realm reads are gated by committed watermarks.
- `ReadCursor` is response metadata only. The client owns resume persistence.
- Stream subscriptions are live change notifications only. They are not durable replay cursors.

Server guarantees:

- Resource offsets are monotonically increasing within a resource.
- Area offsets are monotonically increasing within an area and reflect commit order.
- Realm offsets are monotonically increasing within a realm and reflect commit order.
- Area and realm sequencing follow commit order, not begin order.
- Uncommitted staged appends are not visible as committed history.
- Disconnect or restart aborts live append sessions and removes live subscriptions.
- Reading past the committed watermark returns empty success rather than fabricating speculative records.

Replay and catch-up semantics:

- Replay starts from a client-supplied offset.
- The server does not store consumer positions.
- A reconnecting client must resume from its last known committed offset.
- Resource replay is durable exact-history replay for committed resource records.
- Area and realm replay are durable committed-history replay up to the respective watermark.

Tail semantics:

- `Last` is an exact-resource tail operation.
- Wildcard area or realm routes do not expose a wildcard tail contract through `Last`.
- Stream subscriptions are live notify hints about committed change, not a substitute for reading committed history.

Durability modes:

- `StreamWriteMode::Sync` maps to synchronous Midge commit.
- `StreamWriteMode::Buffered` maps to buffered Midge commit and may lose very recent committed work on crash according to the storage policy.
- The contract must always say `durable according to selected write mode`, not `fsync on every append`.

Intentionally unsupported:

- Broker-side consumer groups.
- Durable broker-managed replay cursors.
- Timestamp, beginning, end, or last-N replay APIs beyond the existing offset-based wire contract.
- Durable stream subscription recovery.
- Multi-node sequencing coordination.

## C. Non-Negotiable Invariants

- Invariant: committed resource offsets are monotonic and never reused.
	- Why it matters: the client resume model depends on stable offsets.
	- How it fails: restart or conflict handling reuses an offset or moves the next offset backward.
	- How to test it: [tests/stream_basics.rs](../../tests/stream_basics.rs) `should_recover_next_offset_from_store_after_restart` and [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_preserve_monotonic_stream_resource_offsets_after_restart`.

- Invariant: area and realm ordering follow commit-time monotonic sequencing.
	- Why it matters: cross-resource replay must be deterministic within the contract actually offered.
	- How it fails: begin order leaks into higher-level offsets, or restart rebuild regresses the counters.
	- How to test it: [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_preserve_monotonic_stream_area_offsets_after_restart` and `should_preserve_monotonic_stream_realm_offsets_after_restart`.

- Invariant: replay of committed history does not skip visible committed messages.
	- Why it matters: Stream is the recovery surface.
	- How it fails: reads skip committed records, or watermark logic exposes gaps incorrectly.
	- How to test it: [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_read_appended_data`, `should_preserve_append_order`, `should_maintain_fifo_order_with_multiple_appends`, `should_read_committed_area_history_given_wildcard_route_tcp`, `should_read_committed_realm_history_given_wildcard_route_tcp`, and [tests/stream_advanced.rs](../../tests/stream_advanced.rs) `should_return_empty_success_when_reading_past_committed_stream_watermark`.

- Invariant: one active append session per resource is enforced.
	- Why it matters: this is the current concurrency contract for predictable expected-offset conflict handling.
	- How it fails: two writers append concurrently into one resource session path.
	- How to test it: [tests/stream_basics.rs](../../tests/stream_basics.rs) `should_reject_second_active_session_on_same_resource`, `should_allow_new_session_after_commit`, and `should_allow_new_session_after_rollback`.

- Invariant: uncommitted staged data never becomes durable history after disconnect or restart.
	- Why it matters: partial writer loss must not corrupt future replay.
	- How it fails: abandoned staged writes leak into reads or future offsets.
	- How to test it: [tests/stream_basics.rs](../../tests/stream_basics.rs) `should_abort_append_session_on_owner_cleanup`, [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_abort_uncommitted_stream_session_on_disconnect`, and `should_drop_uncommitted_stream_batch_on_restart`.

- Invariant: `ReadCursor` remains client-owned response metadata, not a broker recovery feature.
	- Why it matters: reconnect behavior must stay explicit and deterministic.
	- How it fails: docs or admin APIs imply broker-side cursor tracking or consumer-group semantics.
	- How to test it: contract audit against [src/domains/stream/protocol.rs](../../src/domains/stream/protocol.rs), [src/domains/stream/mod.rs](../../src/domains/stream/mod.rs), restart tests proving clients resume from offsets rather than restored sessions, and [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_not_treat_stream_subscription_as_replay_cursor_given_shared_route_tcp`.

- Invariant: reads past the current committed boundary return empty success instead of speculative data.
	- Why it matters: a replay client must never confuse not-yet-committed with lost data.
	- How it fails: area or realm reads leak records beyond watermark, or resource reads fabricate tail data.
	- How to test it: [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_handle_read_past_end` and [tests/stream_advanced.rs](../../tests/stream_advanced.rs) `should_return_empty_success_when_reading_past_committed_stream_watermark`.

## D. Anti-Goals / What This Domain Must Not Become

- Stream must not become an ad hoc work queue with reservation and ack semantics.
- Stream must not pretend to offer broker-managed consumer groups when the current contract is client-managed offsets only.
- Stream must not hide commit-mode tradeoffs by describing buffered writes as synchronous durability.
- Stream must not blur live notify subscriptions into durable replay.
- Stream must not silently recover append sessions after disconnect or restart.

## E. Failure Semantics

- Client disconnect: active append session is aborted; live stream subscriptions are removed; committed history remains.
- Server restart: committed records, counters, and watermarks remain; append sessions and subscriptions are lost.
- Storage failure during commit: commit fails and must not be described as durable success. The caller must resolve the error explicitly.
- Stale expected offset: begin is rejected as a concurrency conflict.
- Read beyond end on an exact resource route: empty success.
- Read beyond watermark on area or realm routes: empty success.
- Backpressure on live subscriber notifications: live notify delivery is best-effort and does not change committed stream history.

## F. Observability Requirements

Operators must be able to inspect:

- committed stream resources
- last committed resource offset
- area watermark and realm watermark
- live append sessions
- stream subscription count
- events total and operations rate
- commit failures and conflict counters

Current surface:

- Admin stream views rebuild durable committed metadata plus live append-session counts.
- Admin now exposes dedicated realm and area watermark views for Stream committed history inspection.
- Global stats include `streams_active`, `events_total`, `operations_per_second`, and `subscriptions_active`.
- Prometheus exports `fitz_stream_active`, `fitz_stream_events_total`, `fitz_stream_operations_per_second`, and `fitz_stream_subscriptions_active`.
- Stream conflict and notify-drop counters are emitted through the observability metrics collector as `fitz_stream_append_conflicts_total` and `fitz_stream_notify_drops_total`.

Current gaps to keep explicit:

- The metrics surface still does not expose replay lag or watermark series as first-class labeled metrics.
- The admin read model remains resource-centric; realm and area watermark inspection uses dedicated Stream endpoints rather than per-resource rows.
- Admin does not expose broker-side replay cursors because broker-side replay cursors do not exist.

## G. Highest-Value Tests

- Invariant tests:
	- [tests/stream_basics.rs](../../tests/stream_basics.rs) `should_reject_second_active_session_on_same_resource`
	- [tests/stream_basics.rs](../../tests/stream_basics.rs) `should_reject_stale_expected_offset_after_commit`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_preserve_append_order`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_read_committed_area_history_given_wildcard_route_tcp`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_read_committed_realm_history_given_wildcard_route_tcp`
- Restart and recovery tests:
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_preserve_monotonic_stream_resource_offsets_after_restart`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_preserve_monotonic_stream_area_offsets_after_restart`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_preserve_monotonic_stream_realm_offsets_after_restart`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_drop_uncommitted_stream_batch_on_restart`
- Race and concurrency tests:
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_handle_concurrent_appends_from_multiple_clients`
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_not_treat_stream_subscription_as_replay_cursor_given_shared_route_tcp`
- Integration tests:
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) TCP and WebSocket append, wildcard read, unsubscribe, and disconnect cleanup coverage
	- [tests/stream_e2e.rs](../../tests/stream_e2e.rs) `should_remove_stream_subscription_when_subscriber_disconnects_tcp`
- Benchmark and stress tests:
	- tier3 and tier4 Stream benches should keep append/read separate from live notify fanout because these are different contract surfaces

## H. Cross-Domain Boundaries

- Stream versus Notice: Stream owns durable history and replay; Notice owns live-only fanout.
- Stream versus KV: KV is current authoritative state; Stream is the historical record of committed events.
- Stream versus Queue: Stream records immutable history; Queue manages mutable work lifecycle.
- Stream versus RPC: RPC sequence numbers are live response assembly, not durable stream offsets.

## I. Ambiguity Risks

- Stream subscriptions can be misread as durable replay subscriptions. They are not.
- Buffered commit mode can be oversold as hard durability if docs collapse it with sync mode.
- Area and realm watermark behavior can be misread as skipped data if empty-success semantics are not documented clearly.
- Broader docs may blur client-managed offsets into broker-managed recovery if they describe `ReadCursor` too casually.

## J. Recommended Wording For Fitz Docs / ADRs

- Use this sentence in broader docs: `Stream is Fitz's durable append and replay domain. Clients resume from offsets they persist themselves; Fitz does not store broker-side consumer cursors.`
- Use this sentence when comparing Notice and Stream: `Notice is live-only fanout. Stream is the recovery surface.`
- Use this sentence when describing commit modes: `Committed stream data survives according to the selected write mode; Sync and Buffered are different durability contracts and must not be described as equivalent.`
