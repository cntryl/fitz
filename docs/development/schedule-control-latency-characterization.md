# Schedule control latency during due bursts

Issue #343. This report records actor-level control latency while one family
claims and acknowledges 32 due occurrences. It preserves the serialized behavior;
no workload target or pass/fail latency threshold has been agreed.

## Reproduce

```sh
FITZ_LOG_LEVEL=off OTEL_ENABLED=false cargo bench --locked --bench tier2_subsystem_schedule_control_latency
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Schedule Control Latency Characterization"
```

Code revision: `a2d87c6b5843e4d3c426f68ecd7aae16a7fe42b3`.
The [raw cntryl-stress.v2 artifact](benchmark-data/schedule-control-latency-20261001.json)
retains the environment, default profile, all per-operation observations, counts
and diagnostics. It is minified for machine consumption. Rust 1.98.1, macOS arm64,
10 logical cores, release build; one warmup and five measured samples per row.
Each measured sample contains 128 control operations. Some concurrent local
verification ran during early measurements, so these values are characterization,
not an isolated-host release threshold.

## Workload and limits

Two real ScheduleActors share a local Midge engine with Sync writes and separate
family locks. A worker locks family 1, prepares 32 due schedules outside the timer,
then claims and ACKs all 32 while holding that lock. The timed operation starts
from the burst handshake and includes any wait for the selected family lock.
Create changes a payload on the same definition, ensuring a real upsert write;
Cancel's existing target is prepared outside the timer; List returns normal
paginated definitions. Every response must be successful. Every burst must claim
and ACK exactly 32 occurrences with no pending claims left. No transport/network
or live-subscriber handoff cost is included. This is an actor ownership model,
not a production mailbox queue-depth/load test.

`local_sync` measures real local transactions. `modeled_strict_cloud_5ms` adds an
assumed 5ms before each due claim/ACK transaction and each timed Create/Cancel
write, under its family lock. List adds no write delay. These are simulated
commit latencies over local Sync storage, not measurements from a cloud provider.
A representative deployment must supply measured commit latency, arrival rates
and acceptable control-operation tails before selecting an operational target.

## Results

Quantiles below use nearest rank over the 640 measured operation durations in
each row, excluding warmup. They are milliseconds; they are not quantiles of
five sample-average durations. Due counts include measured samples only.

| Storage | Control | Family | p50 ms | p95 ms | p99 ms | Controls | Claims / ACKs | Quality |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| local_sync | cancel | same_family | 35.022375 | 44.033750 | 50.012458 | 640 | 20480 / 20480 | acceptable |
| local_sync | cancel | sibling_family | 4.705750 | 17.621500 | 32.073667 | 640 | 20480 / 20480 | noisy |
| local_sync | create | same_family | 37.989667 | 50.014583 | 56.049458 | 640 | 20480 / 20480 | noisy |
| local_sync | create | sibling_family | 4.938875 | 10.504458 | 15.974000 | 640 | 20480 / 20480 | noisy |
| local_sync | list | same_family | 20.936917 | 28.123750 | 32.025792 | 640 | 20480 / 20480 | acceptable |
| local_sync | list | sibling_family | 0.001709 | 0.003708 | 0.007791 | 640 | 20480 / 20480 | acceptable |
| modeled_strict_cloud_5ms | cancel | same_family | 31.096292 | 34.422958 | 36.034625 | 640 | 20480 / 20480 | acceptable |
| modeled_strict_cloud_5ms | cancel | sibling_family | 10.939167 | 14.969958 | 16.572833 | 640 | 20480 / 20480 | acceptable |
| modeled_strict_cloud_5ms | create | same_family | 31.163875 | 33.975250 | 35.885875 | 640 | 20480 / 20480 | acceptable |
| modeled_strict_cloud_5ms | create | sibling_family | 10.982958 | 14.930833 | 15.025583 | 640 | 20480 / 20480 | acceptable |
| modeled_strict_cloud_5ms | list | same_family | 20.965833 | 25.819042 | 28.928750 | 640 | 20480 / 20480 | acceptable |
| modeled_strict_cloud_5ms | list | sibling_family | 0.001667 | 0.004125 | 0.011208 | 640 | 20480 / 20480 | noisy |

## Interpretation and next decision

Same-family control calls wait for the due burst's serialized claim/ACK work.
Sibling-family calls can proceed under their own actor lock, while sharing the
underlying local engine. Modeled writes include the configured 5ms delay, but
observed tails also include host/storage variation. In this run local same-family
Create/Cancel tails exceeded the modeled rows; these measurements do not establish
a monotonic cloud-cost effect. Concurrent validation and noisy rows require an
isolated repeat before making that comparison. All 7,680 measured controls and 245,760 claims/ACKs completed
without errors. Four rows were noisy under the default profile; no official
baseline or performance gate was changed.

The repository summarizer wrote its CSV, JSON manifest and Markdown report, then
exited 1 for the partial-run/full-baseline comparison: 1,063 unrelated baseline
rows were missing, with zero critical issues or measured regressions. This
report uses only the final raw artifact above. The partial summary is not a
successful full-suite baseline refresh.

Before changing serialization, agree the workload's active schedules per family,
due-burst frequency, control request arrival rate, provider commit-latency range,
and Create/List/Cancel p95/p99 targets. Repeat on an isolated representative host
and real strict-cloud storage. This issue supplies a reproducible characterization;
those deployment and target decisions remain separate from a code correctness fix.
