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
- Read responses carry the committed record envelope that was actually read: resource offset, available area or realm offsets, body, optional metadata, and server timestamp.
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
- `Last` returns the same exact-resource record envelope as `Read` for the tail record when one exists.
- Wildcard area or realm routes do not expose a wildcard tail contract through `Last`.
- Stream subscriptions are live notify hints about committed change, not a substitute for reading committed history.

Metadata semantics:

- Exact-resource `GetMetadata` returns first readable resource offset, last readable resource offset, readable record count, batch limits, TTL, and current area or realm watermarks.
- Wildcard routes do not currently expose a wildcard metadata contract.

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
- The dedicated write-economics proof bench in [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs) now quantifies that tradeoff. On the latest local run, the current layout wrote about 648.00 bytes per event while the hybrid compact-realm layout wrote about 625.39 bytes per event, only a 3.49% reduction. The entire gain came from shrinking the realm plane; resource and area planes remained unchanged.
- Treat that write result as a second hard constraint: the current hybrid is a strong realm-read improvement, but it is not yet a meaningful write-amplification fix. If the redesign goal remains materially lower write cost, a later iteration still has to eliminate at least one duplicate body plane rather than only repacking realm history.
- A follow-on two-body experiment now does exactly that: realm pages store only compact area references and hydrate from the direct area plane. The write-economics bench shows this layout is finally material on the write side, cutting approximate write volume from about 648.00 bytes per event to about 451.39 bytes per event, roughly a 30.34% reduction.
- The current read-path proof for that same design is not viable yet. In [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), the realm area-reference page variant only reached about 4.9-5.2 Kelem/s on the latest local run, far below both the covering realm path and the compact realm-body page variant. Treat this as evidence that the two-body hybrid has the right storage economics but the wrong hydration mechanism.
- Current implication: the redesign target is still plausible, but only if realm-reference hydration stops depending on the current batched area-scan strategy. Future work should focus on a materially cheaper realm-reference hydration primitive rather than further tweaking row-shape bytes alone.
- A follow-on page-aligned two-body experiment now replaces the batched area-scan hydrator with exact compact area-page lookups. On the latest local replay run in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), compact paged area replay landed around 229-248 Kelem/s, which is strong enough to beat the same-run covering area path at about 130-160 Kelem/s. But the corresponding realm area-page-reference path still only reached about 18.7-21.2 Kelem/s.
- Treat that result as progress, not a solution. The page-aligned layout materially improves the earlier realm area-reference result at about 4.4-4.9 Kelem/s, but it is still far below the same-run covering realm path at about 162-177 Kelem/s and the compact realm-body page path at about 199-226 Kelem/s. Exact page lookup removes one hydration cliff, but it does not make the two-body realm path competitive yet.
- The same candidate now has write-economics proof in [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs): about 423.89 bytes per event versus about 648.00 for the current layout, roughly a 34.58% reduction and about 6% lower than the earlier two-body area-offset-reference variant. Current implication: area-page compaction is a real write and area-read win, but future work still has to solve realm-local body access rather than only shrinking rows.
- A hydration-only follow-on in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs) keeps that same page-aligned layout but replaces the random compact-page gets with a sequential compact-area-page scan before realm replay. On the latest local run, realm replay improved to about 86-96 Kelem/s versus about 18.7-21.2 Kelem/s for random page gets on the same layout.
- Treat that jump as evidence that the lookup primitive is a major part of the remaining loss. But the same scanned-page variant still trails the same-run covering realm path at about 162-177 Kelem/s and the compact realm-body page path at about 199-226 Kelem/s. Current implication: a two-body design may still be viable if it preserves scan-like page locality, but it still has not matched the simpler realm-body candidate.
- A final reference-encoding follow-on in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs) keeps the same scanned compact-area-page strategy but replaces each realm reference tuple with a compact page id plus slot. On the latest local run, that page-id variant reached about 82-105 Kelem/s for realm replay versus about 82-98 Kelem/s for the scanned page-offset variant.
- The same page-id variant now measures about 417.89 bytes per event in [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs), slightly below the page-offset scanned variant at about 423.89 bytes per event and about 35.51% below the current layout. Treat that as a cleanup-level improvement, not a breakthrough: narrowing the reference encoding helps a little, but the dominant remaining cost is still realm-to-area locality rather than tuple width alone.
- The first production Loop A slice is now in [../../src/domains/stream/storage.rs](../../src/domains/stream/storage.rs) and [../../src/domains/stream/store.rs](../../src/domains/stream/store.rs): realm replay rows are written as compact shared pages while area and resource planes remain unchanged. Focused validation is green again across stream library tests, stream integration tests, and the Tier 2 replay and write-shape benches.
- Treat one implementation constraint as proven, not optional: compact realm-page merge reads must happen before unrelated resource or area rows are staged in the same Midge write transaction. A first pass that scanned page state after resource writes surfaced pending resource values during the realm scan and corrupted page classification; the fixed production write shape now merges realm pages first and only then stages resource and area rows.
- Treat a second implementation constraint as proven too: when a compact realm-page append spills from one page into the next inside the same read-write transaction, the page-local merge scan can see earlier compact realm-page rows already written in that transaction. `load_realm_page_for_write()` must ignore any scanned row whose decoded realm offset is below the requested page start; otherwise the slot calculation underflows on cross-page batches and higher-tier wildcard consume paths fail with partial visibility or a panic.
- The latest acceptance reruns are green after that fix. New direct-sink regression coverage in [../../src/boot/domains/stream_sink.rs](../../src/boot/domains/stream_sink.rs) proves two 50-event resource commits remain fully visible through area and realm wildcard reads. The latest stress artifacts landed Tier 3 exact / area / realm consume at about 934.76K / 721.30K / 1.37M records/s, and Tier 4 direct at about 831.72K / 749.93K / 1.60M, TCP at about 438.65K / 431.68K / 763.30K, and WebSocket at about 385.15K / 432.23K / 757.91K records/s for resource / area / realm consume. All current consume rows are above the explicit operational floors in [../../config/perf_targets.json](../../config/perf_targets.json).
- A follow-on fallback candidate now combines compact area pages with the already-accepted compact realm-body pages instead of moving to a two-body realm-reference layout. The latest local write-economics run in [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs) measured about 595.89 bytes per event versus about 648.00 for the current layout and about 625.39 for the current production hybrid, an 8.04% reduction versus current and about 4.72% lower than the existing compact-realm-only production shape.
- The same-run replay proof stays monotonic on reads. In [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), compact paged area replay landed around 174-194 Kelem/s versus about 138-156 Kelem/s for the covering-area path, while compact paged realm replay landed around 196-214 Kelem/s versus about 155-178 Kelem/s for the covering-realm path.
- Treat that result as a credible fallback production slice, not the redesign end-state. Compacting the area plane on top of the current realm-body pages improves wildcard replay and trims write bytes without reopening the realm hydration cliff, but the overall write reduction is still modest compared with the roughly 30-35% two-body layouts. Current implication: area-page compaction is safe leverage, but it is not yet the material write-amplification breakthrough the redesign is still seeking.
- The next aggressive follow-on keeps the two-body page-aligned layout but compresses each realm page from per-record page references into run-encoded page slices. The latest local write-economics run in [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs) measured about 412.89 bytes per event, slightly below the page-id reference variant at about 417.89 bytes per event and about 36.28% below the current layout.
- Encoding alone did not rescue realm replay. In [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), the straightforward run-ref replay path landed around 154-167 Kelem/s, only roughly matching the scanned page-ref and page-id variants on the same run.
- A clustered replay follow-on then changed the read order rather than the row shape: it scans the same run-ref realm rows, groups assignments by compact area page, reads the body pages sequentially, and fills realm order afterward. That moved realm replay to about 161-169 Kelem/s on the latest local run.
- Treat that as an important negative result. The clustered run-ref path proves the two-body layout can regain much of the loss from naive point lookups while keeping the best write-economics result so far, but it still trails the same-run compact realm-body page path at about 291-326 Kelem/s and even the same-run covering realm path at about 309-359 Kelem/s. Current implication: the remaining gap is not just reference tuple width or page access order; the dream layout still needs a more radical realm-local body access mechanism if it is going to keep the 35%+ write win without accepting a roughly 2x realm replay penalty.
- A compression-based follow-on now attacks the same goal from the opposite direction: keep the fast realm-local compact body pages, but compress them instead of replacing them with realm-to-area indirection. In [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs), the area-paged plus compressed realm-body layout measured about 433.46 bytes per event, roughly 33.11% below the current layout and materially below the uncompressed area-paged realm-body fallback at about 595.89 bytes per event.
- The corresponding replay proof is strong on the current synthetic dataset. In [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), compressed compact realm-page replay landed around 372-397 Kelem/s on the latest local run, essentially preserving the same throughput class as the uncompressed compact realm-body page path at about 372-387 Kelem/s and ahead of all current two-body variants.
- Treat that result as the first bench-only candidate that appears to retain realm-local reads while also recovering a large share of the desired write reduction. But treat it as provisional, not promoted: the current stream payload generators in those Tier 2 benches use deterministic repeated-byte bodies and metadata, so the compression ratio is an upper-bound result on low-entropy data rather than a proven production distribution. The next follow-on should test compression sensitivity with less compressible payloads before any production port.
- That compression-sensitivity follow-on now exists in both Tier 2 benches. The latest local write-shape rerun measured the compressed realm-body path at about 431.48 bytes per event on the original low-entropy payloads but about 540.12 bytes per event on deterministic high-entropy payloads, versus about 595.89 for the uncompressed area-paged realm-body fallback and about 648.00 for the current layout.
- The matching replay sensitivity rerun shows that entropy changes the write win far more than the read path. In [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), the same-run high-entropy realm replay rows landed at about 194-207 Kelem/s for covering, about 188-222 Kelem/s for uncompressed compact realm-body pages, and about 182-203 Kelem/s for compressed compact realm-body pages. Those bands overlap the same-run low-entropy rows at about 187-213, about 193-211, and about 175-200 Kelem/s respectively, so the compressed realm-body design still preserves realm-local replay throughput under the harsher payload mix.
- Treat that as the refined verdict on the compression path: compression is still the strongest bench-only option for preserving fast realm replay while reducing bytes written, but the earlier ~33% write reduction is not robust across payload entropy. On the current high-entropy synthetic mix the gain shrinks to about 16.65% versus the current layout and about 9.36% versus the uncompressed area-paged realm-body fallback, so any production promotion now needs either production-like payload evidence or an explicit decision that a moderate write win is sufficient.
- That production-like evidence now exists as a deterministic mixed corpus in both Tier 2 benches. The new generator is not captured traffic, but it deliberately combines the shapes the repo actually suggests today: tiny literal event bodies like the stream tests use, structured JSON-like records, log-like text, and binary-ish blobs while keeping the same average 128-byte body plus 24-byte metadata budget.
- On that mixed corpus, the latest write-shape rerun in [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs) measured the compressed realm-body path at about 526.14 bytes per event, versus about 595.89 for the uncompressed area-paged realm-body fallback and about 648.00 for the current layout. That is still about 18.81% below current and about 11.71% below the same-corpus uncompressed fallback, so compression remains meaningful even after leaving the best-case low-entropy regime.
- The matching replay rerun in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs) shows a bounded read tax rather than a replay cliff: the same-run production-like realm rows landed at about 276-333 Kelem/s for covering, about 301-330 Kelem/s for uncompressed compact realm-body pages, and about 240-289 Kelem/s for compressed compact realm-body pages. Treat that as the current frontier: compression is still the strongest replay-preserving write-reduction candidate and remains far ahead of the two-body reference layouts, but the mixed corpus proves the replay cost is not free and the write win is now a mid-teens optimization rather than a transformational redesign outcome.
- A more structural follow-on now attacks the real duplicated-body problem instead of the realm-plane encoding: keep compact area pages as the only body plane, move realm replay onto the existing run-ref rows, and downgrade exact-resource history into compact refs that point back into the area pages. In [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs), that area-body canonical layout measured about 196.59 bytes per event, versus about 412.89 for the best earlier two-body run-ref layout and about 648.00 for the current layout. That is the first local result that looks like actual "meat and potatoes" leverage rather than gravy, at about 69.66% below current.
- The first exact-resource replay proof for that same layout is the new blocker. In [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs), an exact-resource path that lazily point-gets compact area pages only reached about 1.09-1.25 Kelem/s versus about 29-36 Kelem/s for the current covering resource path, which is not viable.
- A scan-local exact-resource follow-on improves that path materially but does not close the gap. Focused reruns of the scanned area-page variant landed around 14-20 Kelem/s versus about 29-36 Kelem/s for covering exact-resource replay. Realm replay for this candidate still inherits the current clustered run-ref result at about 128.98-142.94 Kelem/s, and the area path stays on the direct compact-area-page surface.
- Treat that as the new structural frontier: removing resource bodies is the first experiment that delivers a clearly material write-amplification win, but it immediately turns exact-resource replay into the dominant unsolved problem. Future work should focus on a more resource-local read surface or a resource-specific locator compression scheme that preserves enough page locality to keep exact replay in the same class as the current covering resource path.
- The next area-first follow-on is a compact resource mini-page surface layered on top of the same compact area pages plus realm run-refs. In [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs), that layout measured about 362.59 bytes per event versus about 196.59 for the no-resource-body canonical layout, about 412.89 for the earlier best run-ref two-body layout, and about 648.00 for the current layout. That means the mini-page version gives back some write savings to recover resource locality, but it still lands about 44.04% below current and about 12.18% below the earlier best two-body reference shape.
- The matching exact-resource replay rerun in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs) is the first structural result that appears to solve the new blocker cleanly: the compact resource mini-page path landed around 63.54-73.25 Kelem/s on the latest local run versus about 58.40-68.93 Kelem/s for the same-run covering exact-resource path, and far ahead of the area-page-ref variants at about 2.02-2.36 Kelem/s lazy and about 32.84-34.74 Kelem/s scanned.
- Treat this as the new bench frontier under the OCC plus area-first prompt: area remains the primary body/feed plane, realm can stay on cheap run-refs, and exact-resource replay can recover covering-class locality by owning a compact sequential mini-page surface instead of chasing area pointers per record. The remaining question is no longer whether exact replay can be recovered, but whether this write/read trade lands in the right promotion band once higher-tier consume benches are rerun.
- The next combined follow-on layers the earlier compression result back on top of the structural fix instead of treating compression as the whole answer. In [../../benches/tier2_subsystem_stream_write_shape.rs](../../benches/tier2_subsystem_stream_write_shape.rs), the resource-mini-page plus compressed-realm candidate measured about 381.18 bytes per event on the low-entropy corpus and about 475.84 bytes per event on the production-like mixed corpus, versus about 648.00 for the current layout. That lands at about 41.18% below current on the original synthetic data and about 26.57% below current on the production-like corpus while still keeping area as the primary body plane.
- The corresponding production-like exact-resource replay proof in [../../benches/tier2_subsystem_stream_replay.rs](../../benches/tier2_subsystem_stream_replay.rs) confirms the resource-local gain is not a low-entropy artifact: covering exact replay landed around 64.26-69.23 Kelem/s and the compact resource mini-page path landed around 73.36-75.80 Kelem/s on the same run.
- The same replay run also keeps the realm side in the compact-body class once compressed realm pages are restored: production-like realm replay landed around 352.70-374.65 Kelem/s for covering, around 350.13-370.11 Kelem/s for uncompressed compact realm-body pages, and around 352.18-369.60 Kelem/s for compressed compact realm-body pages. Treat that as the current promotion frontier: structural area-first layout for area plus resource, compressed realm pages as a secondary optimization layer, and no remaining exact-resource replay cliff.
- Added a new bench-only Tier 3 stress surface in [../../benches/tier3_system_stream_storage_model.rs](../../benches/tier3_system_stream_storage_model.rs) to measure the current promotion frontier under `cntryl_stress` without pretending the live Stream domain has already been ported. That file now covers both a direct storage-model path and a routed prototype path that reuses the real router and stream frame codec.
- Latest direct production-like Tier 3 prototype run: covering resource about 99.57 Kops/s, resource mini-pages about 106.19 Kops/s, covering area about 376.14 Kops/s, compact area pages about 684.79 Kops/s, covering realm about 700.17 Kops/s, and compressed realm pages about 711.71 Kops/s.
- Latest routed production-like Tier 3 prototype run: covering resource about 128.39 Kops/s, promotion-frontier resource about 137.00 Kops/s, covering area about 326.72 Kops/s, compact area pages about 664.75 Kops/s, covering realm about 679.04 Kops/s, and compressed realm pages about 659.91 Kops/s.
- Interpret those Tier 3 prototype surfaces carefully. They preserve the same ordering as Tier 2 for resource and area replay, and the routed realm path stays in the same throughput class with only a small tax, but they still are not directly comparable to the current live Tier 3 consume floors because they bypass `StreamDomainSink`, `StreamActor`, and transport. Treat them as stronger directional evidence, not yet a promotion gate.
- Added [../../benches/tier4_integration_stream_storage_model.rs](../../benches/tier4_integration_stream_storage_model.rs) plus [../../benches/support/stream_storage_model.rs](../../benches/support/stream_storage_model.rs) as a server-hosted follow-on. It boots a live `TestServer`, then overrides the `stream` domain registration after startup with the promotion-frontier prototype read sink so direct, TCP, and WebSocket consume paths can be measured against the same 100-record shapes as the current live Tier 4 stream suite.
- Latest live prototype Tier 4 run: direct resource/area/realm about 1.69M / 840.06K / 840.51K ops/s, TCP about 816.36K / 510.38K / 482.72K ops/s, and WebSocket about 806.06K / 530.43K / 508.20K ops/s.
- Treat that as the first server-hosted Stream redesign proof. It clears the current Tier 4 floor budgets and includes real boot, session, and transport cost, but it still is not a promotion gate because the router override swaps in a bench-only sink and therefore bypasses `StreamDomainSink` and `StreamActor` after dispatch.
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
