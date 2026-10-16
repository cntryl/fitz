# KV cardinality and transaction pressure

This opt-in diagnostic grows unique 1KiB current-state values on one concrete
resource. The existing seven-domain saturation matrix remains unchanged.

Each batch-size/key-target pair adds fresh keys rather than overwriting a ring.
One TCP writer commits with local sync policy. Each accepted commit increments
committed counters only after the complete success response. Exact GET readback
checks every newly committed byte after each phase. A low-load write/read probe
then checks service after load. The broker shuts down cleanly, reopens the same
local store, and checks every committed key again. This proves this tested clean
restart path; it does not prove crash recovery, history, or exactly-once effects.

```sh
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
FITZ_KV_PRESSURE_KEYS=100,1000 FITZ_KV_PRESSURE_BATCHES=1,16 \
FITZ_KV_PRESSURE_STAGE_SECS=30 \
cargo bench --locked --bench kv_pressure --features benchkit,stress-soak -- \
  --workload should_measure_kv_cardinality_pressure
```

`FITZ_KV_PRESSURE_KEYS` defaults to `100,1000,10000`, specifying additional keys
per phase. `FITZ_KV_PRESSURE_BATCHES` defaults to `1,16,256,4096`. Both must contain
strictly increasing positive integers, with at most16 entries. A key target is
at most200,000 and a batch at most4,096. Combined configured growth must stay at
or below500,000 keys (~512MB raw payload, excluding indexes/WAL/maintenance).
The default grows at most44,401 keys including the recovery probe.
`FITZ_KV_PRESSURE_STAGE_SECS` defaults to120, at most600. The load checks the time
guard before each transaction, so a transaction already issued may settle beyond
that deadline. The time guard excludes separate semantic verification.

Every complete TCP operation has a60-second deadline; an expired COMMIT has an
indeterminate outcome and is never retried. This harness stops on unexpected
errors rather than assuming a storage error means valid admission rejection.
Configured time/key guards are experiment boundaries, not discovered capacity.
Only successful commits and validated values advance the progress watchdog.
Retained expected state uses deterministic key/payload generation, not an
unbounded in-memory value map. It does not test concurrent conflict behavior,
SCANS, watches, many routes or multiple writers yet.

JSON artifacts under `target/fitz-stress/kv-pressure/` record source SHA/dirty
state, config, accepted commits/keys, exact verification counts, separate load
and verification timing, failure and clean-restart results. Failed stores are
retained with their paths; successful stores are removed after confirmed shutdown.
Artifacts are partial on interrupted processes and are not statistical baselines.
When sharing a Cargo build directory, execute from the isolated worktree so git
identity and diagnostic artifacts remain associated with that source.
