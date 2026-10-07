# Queue offered-rate pressure campaign

The `should_measure_queue_offered_pressure` Tier 5 row drives one shared Queue
through independent scheduled arrivals, increasing producer rates, and one fixed
consumer. It supplements the existing closed-loop lifecycle sweeps. It does not
change Queue behavior or qualify restart durability.

## Workload

One local-disk broker and store remain alive for the whole campaign. Every stage
uses the same concrete route. A baseline enqueue/reserve/ACK/empty probe must
pass before load. The campaign doubles configured arrival rates, then stops at
a broker admission rejection, a workload safety guard, or a stage whose generator
misses more than 1% of scheduled sends. If every rate completes, the tested range
has been exhausted; saturation has not thereby been established.

The default configuration is:

| Setting | Default | Environment variable |
| --- | --- | --- |
| Increasing arrivals per second | 100,200,400,800,1600,3200,6400,12800 | `FITZ_QUEUE_PRESSURE_RATES` |
| Active seconds per rate | 30 | `FITZ_QUEUE_PRESSURE_STAGE_SECS` |
| Producer TCP connections | 32 | `FITZ_QUEUE_PRESSURE_PRODUCERS` |
| Consumer TCP connections | 1, fixed | No override |
| Delay after each successful consumer ACK | 5 ms | `FITZ_QUEUE_PRESSURE_CONSUMER_DELAY_MS` |
| Outstanding sent work guard | 50,000 | `FITZ_QUEUE_PRESSURE_MAX_BACKLOG` |
| Sent requests per stage guard | 200,000 | `FITZ_QUEUE_PRESSURE_MAX_ATTEMPTS` |
| Drain deadline per stage | 600 seconds | `FITZ_QUEUE_PRESSURE_DRAIN_SECS` |

Each producer has at most one outstanding request. Arrival deadlines follow an
absolute schedule and do not depend on replies. If no producer connection is
free, the arrival is counted as a harness miss; it is not queued in unbounded
client memory or counted as a broker rejection. Scheduling slots missed when
an active window expires also count as harness misses. Enqueue latency includes
time since the scheduled arrival, with bounded power-of-two histograms. The
consumer processes one message at a time and retains the same delay during drain.
Payloads are 1 KiB and carry a sequence identity checked byte for byte.

The backlog guard conservatively counts sent work minus explicit rejections and
successful ACKs, including requests whose enqueue response has not yet arrived.
Reaching either guard is a workload safety boundary, not a broker capacity claim.
Rates, connections, stage duration, and guards have finite validated maxima.

After each rate, stop arrivals, settle every sent enqueue request, and drain
accepted messages through the same consumer. Require exact message-ID/payload
accounting and a successful empty Queue response before trying another rate.
The ledger reconciles a consumer ACK that arrives before the producer's enqueue
response, rejects repeated or changed message IDs, and fails if rejected work is
delivered, accepted work is missing, or an enqueue outcome is indeterminate.
A final low-load lifecycle and empty check establish return to useful service.

## Interpretation

- `broker_enqueue_rejection`: ENQUEUE returned the explicitly allowlisted Queue
  admission code `4005`; other error codes fail. The code alone does not identify
  whether pressure was on ingress, an actor mailbox, or another Queue limit.
- `offered_rate_not_met`: more than 1% of scheduled sends missed the generator's
  available capacity or scheduling window. Increase producer connections and
  rerun before attributing this rate to the broker.
- `backlog_safety_guard` or `accounting_safety_guard`: the declared workload
  envelope was reached. A drained run can pass correctness while the broker's
  actual capacity remains unknown.
- `configured_window`: the full active window completed. Compare acknowledged
  work during that window with offered/accepted work and retained backlog. A
  growing backlog identifies overload of the configured consumer workload;
  it does not by itself identify the broker's maximum throughput.
- `failed`: unexpected errors, malformed responses, timeout, duplicate/changed
  delivery, missing accepted work, unsuccessful drain, or cleanup failure.

Each operation has a 60-second absolute deadline. Only successfully acknowledged
work advances useful progress; rejection, retry, polling, and artifact writes do
not. Drain has its own deadline and a useful-progress check. Unknown outcomes
are never retried or reclassified as expected capacity pressure.

Queue ACK storage retries only Midge 0.3.1's typed, family-specific
`WriteStall` rejection for a missing L0 slot. That rejection occurs before WAL
submission. A fresh transaction restages the same actor-owned record/index
deletions, and the actor applies its in-memory plan only after commit succeeds.
All attempts share the existing 30-second storage-admission wait budget. This
does not bound transaction staging or Midge's separate commit response wait.
Unknown outcomes, conflicts, and other storage errors remain terminal; the
client must not blindly repeat the ACK.

This uses the existing fast Queue write policy. Accepted/ACKed work is checked
inside the running broker process. There is no broker restart, crash recovery,
strict durability, cloud qualification, or exactly-once claim.

## Commands and artifacts

Run the default campaign:

```bash
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
  cargo bench --locked --quiet --bench queue_pressure --features benchkit,stress-soak -- \
  --workload should_measure_queue_offered_pressure
```

Run a short diagnostic with a small backlog envelope:

```bash
FITZ_QUEUE_PRESSURE_RATES=20,100,200,400,800,1600,3200 \
FITZ_QUEUE_PRESSURE_STAGE_SECS=10 FITZ_QUEUE_PRESSURE_MAX_BACKLOG=3000 \
FITZ_QUEUE_PRESSURE_DRAIN_SECS=90 FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
  cargo bench --locked --quiet --bench queue_pressure --features benchkit,stress-soak -- \
  --workload should_measure_queue_offered_pressure
```

The existing single-sample smoke defaults apply. Larger duration/rate settings
may require increasing `STRESS_TIMEOUT_SECS`; a hard timeout is still a failed
campaign. This row is opt-in and does not extend the seven-domain scheduled
workflow matrix.

Raw stage and partial evidence is atomically written under
`target/fitz-stress/queue-pressure/`. Reports record scheduled, missed, sent,
accepted, rejected and ACKed counts; consumer admission rejections; known
accepted backlog and pending enqueue replies at load stop; enqueue latency;
active, producer-settling and drain durations; drain/empty/recovery checks; and
independent cleanup status. Process resource observations include clients and
broker together; unavailable RSS stays null, and process peak RSS is cumulative.
The per-message ledger is bounded by the sent-request guard and is not serialized.

Framework rows count validated messages through ACK over active plus settling
plus drain time. They are neither enqueue-admission throughput nor active-window
consumer throughput; use the raw phase counters for those comparisons. Expected
rejections and missed sends remain separate from canonical completed work.
Single-sample reports are correctness diagnostics, not performance baselines.

Workload failures are recorded before broker cleanup. Failed workload or
unconfirmed shutdown retains the temporary store for investigation. A successful
campaign removes its temporary store only after confirmed shutdown. Artifact
presence alone does not establish a pass.

To narrow an observed broker rejection bracket, rerun with rates between the
last completed rate and the first rejecting rate, retaining the same payload,
consumer delay, storage, producer pool and stage duration. For generator or
safety boundaries, change the limiting harness dimension explicitly and record
that change; do not label it a broker failure.

## Initial local evidence

On clean source `6ba0677abb1a27a406d9de9f0c6bbc6750f1db9e`, an optimized local
campaign used 128 producer connections, a 5 ms consumer delay, ten seconds per
rate, a 5,000 outstanding-work guard, and a 180-second drain budget. Requested
rates were `20,100,200,400,800,1600,3200`.

| Requested arrivals/s | Accepted and ACKed | Known accepted backlog at load stop | Drain seconds | Stop |
| --- | ---: | ---: | ---: | --- |
| 20 | 200 | 0 | 0.024 | Full ten-second window |
| 100 | 1,000 | 618 | 10.825 | Full ten-second window |
| 200 | 2,000 | 1,433 | 28.689 | Full ten-second window |
| 400 | 3,993 | 3,339 | 53.045 | Full ten-second window; seven missed sends |
| 800 | 5,472 | 4,998 | 78.175 | Outstanding-work guard after 6.853 seconds; eleven missed sends |

All 12,665 accepted messages were validated and ACKed. There were no broker
admission rejections; every stage drained, empty checks and the final recovery
probe passed, and shutdown/cleanup completed. Rates 1,600 and 3,200 were not
attempted after the guard. The guard includes pending enqueue replies, so known
accepted backlog can be slightly below its threshold.

This establishes successful accounting and recovery within the declared local
workload envelope. It does not establish the broker's maximum capacity, stable
performance, strict durability, or a full-duration endurance pass. Initial
shorter diagnostics also exercised a producer-limited stage and an intentionally
insufficient drain deadline; the latter failed with nine of eleven accepted
messages still outstanding and retained the failed fixture's store.
