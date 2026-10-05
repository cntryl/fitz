# Tier 5 and Tier 6 benchmarks

Tier 5 measures scaling and observed saturation as concurrent load increases.
Tier 6 exercises sustained work for a declared endurance active-time budget.
Both use real domain operations, bounded logical workload state, explicit completed
operation counts, and semantic verification. Their rows are diagnostics, separate
from the existing release benchmark IDs, baselines, and performance targets.

## Targets and workload selectors

Both targets require `--features benchkit,stress-soak` and use `cntryl-stress`.
Their shared runner, domain drivers, and artifact helpers live in
`benches/stress_support/`.

| Domain | Tier 5: `tier5_saturation` | Tier 6: `tier6_endurance` |
| --- | --- | --- |
| Queue | `should_measure_queue_concurrency` | `should_soak_queue` |
| KV | `should_measure_kv_concurrency` | `should_soak_kv` |
| Stream | `should_measure_stream_concurrency` | `should_soak_stream` |
| Schedule | `should_measure_schedule_concurrency` | `should_soak_schedule` |
| RPC | `should_measure_rpc_concurrency` | `should_soak_rpc` |
| Notice | `should_measure_notice_concurrency` | `should_soak_notice` |
| Lease | `should_measure_lease_concurrency` | `should_soak_lease` |

Tier 5 runs concurrent load stages with 1, 2, 4, 8, 16, 32, and 64 lanes. The
configured duration is the **total active budget across the seven stages**. A
3,600-second sweep allocates approximately 514 seconds to each level; it does
not allocate one hour to every level. An explicit capacity rejection may end an
individual Tier 5 stage early; record that boundary and the actual elapsed work,
then require a successful recovery probe. Report each level separately so a higher
load's rejections or latency cannot disappear in a whole-sweep average. A sweep
that never observes capacity pressure establishes scaling within its tested
range; the target name does not prove the broker's saturation boundary was reached.

Tier 5 prepares all 64 lane drivers before the baseline and retains them through
the sweep; each level changes the number actively stepping, not the prepared
socket count. Tier 6 prepares eight drivers. Queue, KV, Stream, and Schedule use
one TCP connection per lane; RPC, Notice, and Lease use two. The framework's
`client_count` parameter counts active logical lane drivers, not TCP connections.

Tier 6 runs eight concurrent lanes until their accumulated workload batch time
reaches the full configured budget after fixture setup. The default active
budget is one hour. All separate verification, including periodic checks,
is excluded from this active time; its duration is recorded as
`verification_elapsed_ns`. The phase's `elapsed_ns` records active batch time,
and `wall_elapsed_ns` records wall time separately. Setup, cleanup, and low-load
recovery probes are also outside the endurance budget. A failed or interrupted
run that ends early is not a completed one-hour endurance run.

## Domain scope and completion

| Domain | Measured work and verification | Boundary of the claim |
| --- | --- | --- |
| Queue | Enqueue, reserve, validate delivery, and complete acknowledged work. | Fast write policy and acknowledgements within the running process; this workload does not qualify strict restart durability. |
| KV | Mutate and verify values using a bounded key ring. | Current authoritative values and transaction responses; key cardinality does not grow with elapsed time. |
| Stream | Read and replay a finite, prebuilt history, validating event identity, order, and payload. | History is finite; the workload does not mutate retention or accumulate an endless sequence of commits. |
| Schedule | Create, list, and cancel definitions, validating the definition lifecycle. | Definition churn; this workload does not claim that schedules were forced due or that fires were delivered. |
| RPC | Send a request, receive worker dispatch, and validate the caller's terminal response. | The full legacy request/response lifecycle; this workload does not qualify cooperative cancellation or cleanup acknowledgements. |
| Notice | Publish and count validated deliveries received by the intended live subscriber. | Live ephemeral fanout; publish acknowledgement or successful socket write is not a received delivery. |
| Lease | Exercise and validate live ownership operations and tokens. | Process-local ownership and fencing tokens; the workload does not establish ownership continuity across restart. |

## Storage profiles

`FITZ_STRESS_STORAGE_PROFILE` selects `local_disk` (the default) or `memory`.
The hosted workflows explicitly use `local_disk`. Each workload holds one
temporary local store for its entire campaign, including baseline, all active
stages, final verification and recovery. The directory is released after server
shutdown. The broker's existing storage flush and compaction paths can reclaim
old versions while Queue, KV and Schedule continually mutate bounded logical
state. The runner never resets the store or silently slows the workload to avoid
a storage failure.

`local_storage_path` records the temporary directory before startup. It is a
historical path after successful cleanup. Startup failure or cancellation and
unconfirmed shutdown retain that directory; cleanup failure also fails the run.

Memory is an opt-in resource-pressure diagnostic. In pinned Midge 0.3.1, memory
mode retains published versions and tombstones and skips flush/compaction.
Keeping Queue depth, KV key cardinality or Schedule definition count fixed does
not bound that physical history. The write-heavy fixture sets a 512 MiB memtable
flush threshold; its hard stall starts at twice that threshold. A long memory
write-churn campaign can therefore fail even with a bounded logical working set.
Such a backend error remains a failed run, including an error after work was
admitted. It is never converted into an expected admission-capacity rejection
or a successful one-hour soak.

Artifacts and framework parameters record the selected profile. Both profiles
use the existing fast Queue write policy. Running against local disk adds storage
flush/compaction coverage; this suite still makes no restart-recovery, strict
Queue durability or ownership-continuity claim. Ephemeral Notice, RPC and Lease
semantics remain ephemeral when the broker has local storage.

Every workload establishes a successful baseline probe before sustained load,
performs real semantic verification, and performs low-load probes after load
subsides. A verification counter must count checks actually performed, rather
than being assigned a constant to claim coverage. These recovery probes test
the running broker's return to useful service; they are not restart-recovery
qualification. `realm` and `route_family` remain separate identifiers.

## Timing and framework invocation

The shared runner resolves the active duration in this order:

1. `FITZ_TIER5_DURATION_SECS` or `FITZ_TIER6_DURATION_SECS` for the selected tier.
2. `FITZ_STRESS_DURATION_SECS` as a shared fallback.
3. 3,600 seconds when neither variable is set.

A configured duration must be an integer from 1 through 86,400 seconds.
Short overrides provide diagnostic feedback; they do not qualify the default
one-hour window. Record both the configured duration and the actual active
elapsed time in the workload artifacts.

The benchmark body owns its active window and records each measurement with
`ctx.record_external_outcome`, a `LogicalUnit`, and observed `OperationOutcome`
counts. Do not wrap the owned window in `ctx.measure_outcome`: its fixed-duration
mode can call a closure repeatedly and aggregate several owned windows into one
row. `--sample-duration-ms` controls framework timing, not the Fitz runner's
active window.

Tier 5 and Tier 6 deliberately pass
`--profile smoke --samples 1 --warmup-samples 0 --cooldown-samples 0` to execute
each long owned window once. This is an explicit exception to Tier 1 through
Tier 4, which retain the default profile and sample counts. The framework marks
a single-sample measurement statistically untrustworthy, and its default quality
gate rejects that result. Smoke permits correctness diagnostics while preserving
correctness failures, configured budget failures, and workload watchdogs.

Both targets initialize missing framework settings before it starts threads:
smoke, one measured sample, no warmup or cooldown, a 60-second progress watchdog,
and a 4,500-second hard timeout. A command without those flags therefore defaults
to one owned campaign per selected workload. Explicit CLI and environment values
remain in effect; increasing samples, warmup, or cooldown can repeat the full
campaign and requires an appropriate hard timeout. Changing the profile alone
does not make a single sample authoritative. Hosted workflows set smoke and
deadlines in their environment and use the same single-window startup defaults.
An unfiltered target selects all seven domain workloads, each with its
own default duration; use `--workload` to select one domain.

A Tier 6 full-duration pass requires the declared active time, semantic
verification, useful progress, and successful cleanup. Tier 5 may instead report
explicit capacity boundaries for stages that ended early, with successful
recovery; those stages do not claim their full allocated duration. Both supply
scaling or endurance evidence without a statistical performance-quality claim. The one
sample does not establish authoritative throughput, pass a performance regression
gate, or justify refreshing the release baseline. A short smoke run also cannot
qualify the declared one-hour window.

Run a full Tier 5 Queue sweep:

```bash
FITZ_STRESS_STORAGE_PROFILE=local_disk FITZ_TIER5_DURATION_SECS=3600 STRESS_NO_PROGRESS_TIMEOUT_SECS=60 STRESS_TIMEOUT_SECS=4500 \
  cargo bench --locked --quiet --bench tier5_saturation --features benchkit,stress-soak -- \
  --workload should_measure_queue_concurrency --profile smoke --samples 1 --warmup-samples 0 --cooldown-samples 0
```

Run a full Tier 6 Queue endurance window:

```bash
FITZ_STRESS_STORAGE_PROFILE=local_disk FITZ_TIER6_DURATION_SECS=3600 STRESS_NO_PROGRESS_TIMEOUT_SECS=60 STRESS_TIMEOUT_SECS=4500 \
  cargo bench --locked --quiet --bench tier6_endurance --features benchkit,stress-soak -- \
  --workload should_soak_queue --profile smoke --samples 1 --warmup-samples 0 --cooldown-samples 0
```

For a short diagnostic run, replace `3600` with a duration such as `7` and select
the desired domain from the table. A seven-second Tier 5 run divides that total
across all seven levels. Record any observed boundary with that short duration;
the run cannot qualify the default full-duration sweep or one-hour endurance.

## Saturation, correctness, and liveness

Count a logical operation as completed only after its required response or
delivery has been received and validated. Sent frames, queued attempts, rejected
requests, and setup actions are not throughput completions. Preserve a genuine
zero completion count; never replace it with one to satisfy a recording API.

Keep raw attempts, explicitly classified capacity rejections, Lease contention,
and Notice `delivery_window_misses` distinct from useful completions and correctness
failures. Only an operation's documented, allowlisted response may be classified
as expected saturation. Lease contention alone is not broker capacity pressure.
An expected rejection is an observed boundary, not evidence of successful work.
The framework requires canonical attempted and completed counts to match for a
correctness pass, even with smoke. The adapter therefore uses logical unit
`accepted_domain_cycle` and declares
`framework_attempt_scope=accepted_or_unexpected_failure`: canonical attempts are
validated completions plus unexpected failures, and canonical completions are
only validated successful cycles. True raw attempts remain in the workload JSON
and `raw_attempts` observation, including expected rejections, contention, and
delivery misses. This scope adapts the framework's correctness gate; it never
counts those expected outcomes as completed work.
Timeouts for operations requiring a response, malformed or mismatched responses,
unexpected errors, forbidden duplicates, and failed semantic verification fail
the workload. Notice records a missing live delivery within its bounded delivery
window as `delivery_window_misses`; this is not proof of a broker drop or a
capacity rejection.
A missing Notice baseline or recovery delivery fails verification.

Valid durable-domain error responses preserve their operation, available error code and
message and are classified as `DomainError`. Queue ACK and Schedule
CREATE/CANCEL/LIST_V2 use plain actor error envelopes and can also receive coded
ingress errors. Both forms must decode completely; an ambiguous or invalid error
envelope remains `InvalidResponse` with a bounded payload prefix for diagnosis.
Coded responses retain their code and message.
Neither classification permits a correctness pass.

The progress watchdog requires useful validated progress. Rejections, retries,
verification-only activity, log writes, and sleeps must not keep an otherwise
stalled workload healthy.
Each client operation also has a bounded wait: aggregate progress from other
lanes cannot excuse one lane remaining stuck. The runner stops peer work after
a terminal failure and preserves available partial evidence.

`cntryl-stress` 0.5.1 supplies a shared progress handle and
`STRESS_NO_PROGRESS_TIMEOUT_SECS`. Its hard timeout can report failure but cannot
cancel a blocked workload thread. Bounded operations and runner-owned stop and
cleanup behavior are still required. A timeout is a failed run, even when some
operations completed successfully before it.

## Artifacts and hosted runs

Framework reports remain under `target/stress`. Workload summaries, bounded
latency histograms, available process resource observations, partial JSON, and
runner logs are preserved under `target/fitz-stress`. Record unavailable resource
observations explicitly rather than presenting them as measured zeroes. Workload
keys, history, retained counters, and latency samples must remain bounded during
the active window. Periodically persisted partial JSON preserves evidence when
the framework cannot recover the active context from a timed-out worker.

Process RSS includes the server, clients, shared runtime, and every prepared
lane, including idle lanes. It does not isolate broker-only memory or measure
connection-memory growth at each load level.

Use stable parameters for workload dimensions. Record changing attempts,
completion counts, rejection counts, latency, and resource values as observations
or workload artifacts; dynamic values must not change measurement identity.

- [Tier 5 workflow](../../.github/workflows/bench-tier5.yml): weekly on Monday
  at 06:00 UTC, or manually dispatched.
- [Tier 6 workflow](../../.github/workflows/bench-tier6.yml): monthly on the first
  day at 07:00 UTC, or manually dispatched.

Each workflow uses seven independent domain jobs with `fail-fast: false`, a
90-minute job limit, a 60-second progress watchdog, and a 4,500-second framework
hard timeout. They follow Midge's workload-matrix layout, with one wildcard Cargo
command per job and concurrency per branch that queues overlapping campaigns.
The target startup defaults prevent warmup or repeated samples from multiplying
the owned hour. These workflows do not run on pushes
or pull requests and do not change the existing benchmark performance gates.

Artifact uploads run even after failure and include both artifact directories.
Names include the tier, workload, commit SHA, run ID, and attempt. The benchmark
command's exit status is preserved when its output is copied to a log. Interpret
partial evidence alongside the failure and elapsed window; uploading artifacts
does not turn an interrupted run into a pass.
