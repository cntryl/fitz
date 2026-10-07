# Stream history-growth pressure campaign

The existing seven-domain soak replays a fixed 16-event Stream history. This
opt-in diagnostic grows one concrete resource through increasing committed
history sizes, varies commit batch size and event bytes, and validates the full
history over TCP before and after clean broker shutdown/reopen.

Run from the repository root:

```bash
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
  cargo test --locked --release --features benchkit \
  --test stream_pressure_campaign \
  should_preserve_committed_history_under_stream_pressure \
  -- --ignored --exact --nocapture
```

Each invocation uses a fresh local-disk store, one writer, Sync commits, and
the concrete route `stream://stress-bench/history/growing`. It retains history
instead of recycling a bounded seed. This is a single-writer cardinality and
batch-pressure diagnostic; it does not measure independent offered rate,
multiwriter conflict behavior, or wildcard replay.

| Setting | Default | Bound |
| --- | --- | --- |
| `FITZ_STREAM_PRESSURE_TARGETS` | `100,1000,10000,100000` | Strictly increasing totals, 2 through 1,000,000; at most 16 stages. |
| `FITZ_STREAM_PRESSURE_BATCH_EVENTS` | `256` | 1 through 16,384. |
| `FITZ_STREAM_PRESSURE_PAYLOAD_BYTES` | `1024` | 8 through 32,768. |
| `FITZ_STREAM_PRESSURE_STAGE_SECS` | `300` | 1 through 600 per growth stage. |

The largest target plus two recovery events, times payload bytes, must not
exceed 512 MiB. The actual
store includes indexes, WAL, and metadata, so this guard is not a disk quota.
Partial final batches stop exactly at the target. Every operation has an
absolute 60-second deadline; each complete replay has a 600-second deadline.
A growth deadline does not turn an unfinished commit into a rejected write.

Before pressure and after load, a low-load append/Sync commit/replay proves
service on the same resource. After shutdown, a fresh session reads all known
committed events from the same store and performs another append/commit/replay.
The oracle verifies every route, contiguous resource offset, exact payload,
event kind, metadata absence, page count, and continuation flag. It requests a
final empty page, so unexpected extra events and leaked rolled-back batches
cannot silently pass. Scope offsets, timestamps, and global selector ordering are not
asserted by this concrete-resource diagnostic.

An APPEND acknowledgement only stages an event. `committed_events` advances
after a successful Sync COMMIT response. Accepted appends, commit requests,
acknowledged batches, pending staged events, and probe events remain separate.
Only the exact versioned backend response `2012: batch too large` emitted by
the actor, or the storage `2012: ERR_BATCH_TOO_LARGE:` prefix, is an expected
boundary: the campaign rolls back the whole batch, verifies the
previous history, and still requires clean restart and low-load recovery.
Other backend failures, deadlines, changed or missing events fail the run.
An unknown commit outcome remains unknown and is never retried automatically.

Normal Stream client commands use a bounded 60-second actor reply wait, aligned
with the explicit synchronous storage runtime response setting. Midge may raise
its effective budget to the provider I/O timeout plus 30 seconds. Admin/control
requests retain their one-second wait. Mailbox queueing consumes the client
budget too, so this does not guarantee that every accepted commit finishes
before expiry. A true post-dispatch timeout still reports an unknown outcome;
the 60-second external diagnostic request guard is unchanged.

JSON artifacts live under `target/fitz-stress/stream-pressure/` and identify
source SHA/dirtiness, configuration, storage path, per-stage actual elapsed
work, accepted and committed counts, complete readback, restart verification,
original failure, and cleanup. Completed batches checkpoint partial progress.
Successful shutdown/readback removes the fresh store; failed workloads retain
their store without attempting deletion.
No generated artifact belongs in a commit.

Clean restart evidence is limited to orderly shutdown of this local-disk
fixture. It does not establish crash recovery, power-loss durability, cloud
replication, recovery of live sessions, or throughput on an unloaded host.
Single diagnostic runs do not qualify an hour-long soak or a statistical
performance baseline. A passing configured envelope is not proof of an
unbounded capacity limit.

## Executed envelopes

Clean dev-profile runs used fresh local stores and 1 KiB values unless stated.
At `a61711c5`, targets 1,000/10,000/100,000 with batches of 256 passed full
readback, then verified 100,001 events after clean restart and committed the
final recovery event. At `91307161`, attempts above 10,000 staged events or
10 MiB of staged payload reached the documented batch rejection. Both rolled
back, verified unchanged history, and passed restart and recovery probes.

At `8d47811c`, targets 10,000/100,000/500,000, batches of 256 and a 300-second
stage guard failed after 158,880 acknowledged events. The next COMMIT returned
`2012: domain timeout: request outcome unknown, do not blindly retry` with
256 staged events and one more commit request than acknowledged commits.
The 100,000-event checkpoint readback passed. Shutdown completed, and the
failed store and JSON were retained. Restart verification and recovery did
not run; neither data loss nor the final batch's outcome is established.
A fresh matched repeat at `3cc49383` returned the same timeout after 106,400
acknowledged events, again with one uncertain 256-event batch. Its 100,000-event
checkpoint passed, shutdown completed, and the store remained retained.
These failures are tracked in [#399](https://github.com/cntryl/fitz/issues/399).
They establish a repeated liveness/indeterminate-outcome observation, not a
universal event-count boundary, data loss, or a crash-recovery result.

The #399 investigation later reopened copies of both retained stores without
retrying either commit. Exact full-history and final-empty-page readback passed
at 159,136 and 106,656 events: each uncertain 256-event batch had committed.
The original stores were preserved. A fresh unchanged 500,000-target diagnostic
with temporary phase timings failed at 160,672 acknowledgements. Its main Sync
transaction completed successfully in 5.3537 seconds; total actor delivery took
5.4179 seconds, exceeding the old four-second client reply wait. No measured
maintenance, reservation, or watermark operation exceeded 250 ms in that run.
This evidence motivates matching the client wait to storage's existing budget;
it does not diagnose an engine defect or establish a storage throughput limit.

At clean fix source `6632a5a1`, the unchanged 10,000/100,000/500,000 targets,
256-event batches, 1 KiB payloads, and 300-second stage guard passed in
425.13 seconds. All 1,955 growth COMMIT requests were acknowledged, every
checkpoint replayed exactly, and no batch remained pending. The final growth
stage took 254.47 seconds and its full replay took 33.14 seconds. The same-resource
post-load probe passed; orderly restart then verified 500,001 events and the
fresh-session recovery probe committed/replayed the final event (500,002 total).
Cleanup completed and removed the fresh store. This is one qualified local-disk
envelope with unchanged external guards, not a crash-recovery or throughput claim.

Twelve focused oracle regressions and full workspace/all-target/all-feature
strict Clippy passed. These diagnostic timings do not qualify throughput.
