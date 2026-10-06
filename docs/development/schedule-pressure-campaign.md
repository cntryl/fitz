# Schedule due-wave pressure diagnostic

`schedule_pressure` grows persisted definition cardinality and drives actual
claim → live TCP delivery → durable claim acknowledgement for both broadcast
and single delivery modes. Two healthy wildcard subscribers drain concurrently.
Broadcast completes an occurrence only after both receivers validate its complete
1 KiB payload and route; single completes after exactly one observed recipient.
Duplicates, missing receipts within the declared window, changed payloads,
unresolved durable claims, and hidden acknowledgement failures fail the campaign.
A missing receipt does not by itself prove a broker drop: live delivery is best
effort and has no consumer acknowledgement.

Each stage verifies the exact persisted definition set after clean shutdown and
restart, then subscribes anew. It forces the due heap **once** through the existing
test hook. This is a forced-clock diagnostic, not natural cron latency evidence.
Ordinary broker scans finish subsequent bounded claim batches (currently 32).
Repeated force calls would incorrectly reset already-fired definitions and are
deliberately avoided. The hook itself has a one-second reply budget; receipts
and durable counters determine success even if that reply expires.

After firing, every definition is cancelled and an empty paginated definition
set is verified. A final one-definition lifecycle probes low-load recovery.
This establishes local-sync timing-intent readback after clean restart only,
not crash recovery, durable subscriber delivery, or downstream job completion.

```sh
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
FITZ_SCHEDULE_PRESSURE_COUNTS=1,32,128,512 \
cargo bench --locked --quiet --bench schedule_pressure \
  --features benchkit,stress-soak -- --workload should_measure_schedule_due_pressure
```

Defaults: counts `1,32,128,512`, two receivers, one writer, local disk storage,
60-second complete TCP request/startup/shutdown deadlines, 120-second receipt
and acknowledgement window per stage. `FITZ_SCHEDULE_PRESSURE_COUNTS` accepts
at most ten increasing values from 1 through 4096;
`FITZ_SCHEDULE_PRESSURE_FIRE_SECS` accepts 1 through 600. Retained workload
accounting is bounded by cardinality, the receive channel by 128 frames, and
each list page by 16 entries. Creation and clean restart time are reported
separately from firing time; completed counts come from receipts, never creates.

Atomic JSON under `target/fitz-stress/schedule-pressure/` records the source SHA,
dirty state, accepted definitions, completed occurrences, per-receiver counts,
timings, restart/cancellation verification, failure and retained store path.
Partial artifacts and failed stores are retained. Successful stores are removed
only after shutdown. Diagnostics use the single-invocation smoke exception;
they do not refresh release baselines or establish a capacity boundary merely
because the tested envelope passed. Run campaigns independently on the host so
other domain load does not contaminate the envelope.

Cancellation/upsert races, pending-claim crash recovery, disconnected or slow
subscribers, and sibling RouteFamily isolation need separate campaigns. They are
not established by this healthy-receiver definition-cardinality diagnostic.
