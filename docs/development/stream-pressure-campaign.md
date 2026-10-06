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
