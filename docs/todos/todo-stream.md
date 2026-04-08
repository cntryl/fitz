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
- Admin now persists dedicated realm and area watermark snapshots for Stream committed history inspection.
- Global stats include `streams_active`, `events_total`, `operations_per_second`, and `subscriptions_active`.
- Prometheus exports `fitz_stream_active`, `fitz_stream_events_total`, `fitz_stream_operations_per_second`, and `fitz_stream_subscriptions_active`.
- Prometheus also exports first-class labeled Stream watermark series through `fitz_stream_realm_watermark{realm,family}` and `fitz_stream_area_watermark{realm,area,family}`.
- Stream conflict and notify-drop counters are emitted through the observability metrics collector as `fitz_stream_append_conflicts_total` and `fitz_stream_notify_drops_total`.

Current gaps to keep explicit:

- The metrics surface still does not expose replay lag because broker-side replay cursors do not exist.
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

## K. Storage Redesign Research Todo

Any attempt to reduce Stream's current triple-write event body storage must stay in research until it proves that the client-visible replay contract does not regress.

For this redesign track, backward compatibility with the current on-disk Stream row format is explicitly out of scope. The work may assume a clean storage epoch break, but it must still define the operator cutover/reset procedure.

- Add separate benchmark coverage for exact resource replay, wildcard area replay, and wildcard realm replay. The current Tier 3 read row exercises a concrete route and is not enough to justify deduplicating the higher-scope covering indexes.
- Capture a baseline for commit throughput, publish-fanout throughput, exact-resource read throughput, area wildcard read throughput, realm wildcard read throughput, and approximate bytes written per committed event before changing the storage shape.
- Define candidate storage layouts that keep one canonical durable event body while preserving enough locator data for area and realm replay to recover the right committed record deterministically.
- Reject any design that turns wildcard replay into an unmeasured per-row lookup cliff. If area or realm replay requires indirection, prove the batched hydration cost with benchmarks before shipping it.
- The first local proof bench for a canonical-body plus locator layout now exists in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs). Its initial local Midge results reject the naive per-stream multi-scan hydration plan: area replay dropped from about 178-184 Kelem/s on the current covering layout to about 38-41 Kelem/s on the hydrated prototype, and realm replay dropped from about 164-181 Kelem/s to about 20-23 Kelem/s.
- Treat that result as a design constraint, not a benchmark footnote. Any viable deduplicated layout now has to remove or hide most of that locator-to-canonical lookup overhead before the storage cutover proceeds.
- A second local prototype in the same bench now uses shared realm-ordered replay pages plus tiny area locators. That materially improved realm replay versus the naive locator layout, landing around 125-145 Kelem/s versus about 20-23 Kelem/s for naive hydration and versus about 142-161 Kelem/s for the current covering layout. But area replay still landed only around 25-27 Kelem/s versus about 199-217 Kelem/s for the covering layout and even below the naive per-stream hydration result.
- Treat that split result as the current frontier: shared replay pages look credible for realm-scope recovery, but area-scope recovery still needs either a cheaper direct read surface or a substantially cheaper area locator-to-page decode path.
- A follow-on local prototype kept the area path direct and replaced the realm page encoding with a compact manual format in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs). On the latest local run, direct area replay stayed fast at about 236-258 Kelem/s while area-paged replay remained poor at about 34-36 Kelem/s, which reinforces that area should keep a direct read surface. Realm replay, however, improved further: the compact realm-page path landed around 305-348 Kelem/s, slightly ahead of the bincode page variant at about 290-349 Kelem/s and clearly ahead of the current covering realm path at about 235-253 Kelem/s.
- Treat the current leading candidate as: exact resource rows remain canonical, area replay keeps a direct covering read surface, and realm replay moves to compact shared pages. Future experiments should focus on the write-path and storage economics of that hybrid rather than reopening area indirection ideas.
- Preserve the current ordering and visibility contract: resource offsets remain exact-history offsets, area and realm replay remain commit-ordered and watermark-gated, and reads past the committed boundary still return empty success.
- Define the storage epoch break and operator cutover procedure for any row-shape change. This track may choose delete-and-reseed or other non-compatible cutover mechanics, but the reset semantics must be explicit.

## L. Performance Acceptance Budgets

The redesign budget is now explicit: Stream does not need identical throughput across resource, area, and realm replay, but it does need bounded slowdown and no wildcard replay cliff.

- The machine-readable acceptance targets live in [../../config/perf_targets.json](../../config/perf_targets.json).
- Resource exact replay is the anchor path. It should remain the fastest consume path at every layer.
- Area wildcard replay may be slower than exact-resource replay, but it must stay in the same throughput class and above its explicit operational floor.
- Realm wildcard replay may be slower than area replay, but it must stay in the same throughput class and above its explicit operational floor.
- The redesign fails if write amplification drops by pushing area or realm replay below the new operational floors, even if append throughput improves.
- The redesign fails if wildcard replay devolves into an unbounded per-row lookup pattern, regardless of whether the average throughput still looks acceptable on tiny benches.
- Tier 3 engine-core consume floors are: exact resource replay >= 850K records/s, area wildcard replay >= 650K records/s, realm wildcard replay >= 625K records/s.
- Tier 4 direct consume floors are: resource replay >= 400K records/s, area wildcard replay >= 325K records/s, realm wildcard replay >= 200K records/s.
- Tier 4 TCP consume floors are: resource replay >= 350K records/s, area wildcard replay >= 180K records/s, realm wildcard replay >= 180K records/s.
- Tier 4 WebSocket consume floors are: resource replay >= 300K records/s, area wildcard replay >= 180K records/s, realm wildcard replay >= 180K records/s.
