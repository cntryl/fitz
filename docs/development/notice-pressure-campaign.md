# Notice slow-receiver fanout pressure diagnostic

This opt-in Cargo integration diagnostic uses real TCP clients against one
memory-mode broker. Every publish fans out to two live registrations: one
receiver drains independently as fast as it can; one deliberately does not read
for five seconds. Notice is live ephemeral delivery. Missing observations or
pressure disconnects do not establish corruption, durable data loss, or replay.

Run on an otherwise idle host, from this checkout:

```sh
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
  cargo test --locked --test notice_pressure \
  should_push_notice_slow_receiver_limits -- --ignored --nocapture
```

`FITZ_NOTICE_PRESSURE_BURSTS` overrides the default `100,1000,10000,100000`.
Accept one through eight positive burst sizes, each at most 100000. Use `10`
for a small wiring smoke; this does not qualify larger bursts.

A stage creates fresh fast/slow subscriber sessions with exact registration
identities and one publisher. Every 1KiB publish body carries its sequence
identity; every observed delivery must match the concrete route, subscription,
sequence range, full bytes, and a previously unseen identity. Duplicate,
unknown, malformed, or corrupt observations fail the run. Retained observation
sets are bounded by the maximum burst size. Publisher writes have a whole-stage
30-second deadline, followed by a fixed six-second observation window. Receiver
joins have a two-second deadline and abort plus a bounded join on timeout.
Startup/shutdown have 60-second wrappers and full low-load probes ten seconds.
Closing
a receiver after its observation window discards that connection, so a partially
read frame can never be reused by another probe.

The artifact records attempted and fully sent writes and observed deliveries separately for each
receiver, with per-receiver observation-window misses and disconnect reasons.
Incomplete writes are explicitly indeterminate; the configured offered envelope
is planned work, not an inference that unsent work reached the broker.
There is no publish ACK in this completion ledger. Socket writes do not prove
broker acceptance, and an unobserved event is not proof of a broker drop. This
burst diagnostic measures no independent arrival rate or exact fanout capacity.
It does not assume global ordering, durability, or continued service for a
connection closed by pressure.

A baseline publish must be received in full before pressure. After each stage,
a fresh session subscribes and receives a new exact low-load payload. Missing
baseline/recovery delivery, unexpected publisher transport failure, corrupt
observation, or failed broker shutdown fails the diagnostic. Low-load probes
establish current-process service for a new live registration; they do not
recover old subscriptions or replay missed events.

Atomic JSON artifacts under `target/fitz-stress/notice-pressure/` include source
SHA/dirty state, payload size, receiver delay, actual offered/sent/observed/missed
counts, disconnects, elapsed time, partial failure, cleanup, and baseline/recovery
outcomes. The initial artifact precedes startup; failure checkpoints precede
shutdown, and artifact-save failures cannot bypass broker cleanup.
The test is intentionally ignored in ordinary workspace runs. It is
not a statistical baseline, endurance qualification, wildcard/family isolation
check, or restart/replay promise. At the largest stage, total published payload
is about 100MiB and there are three TCP clients plus the broker.
