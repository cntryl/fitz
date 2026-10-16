# Lease shared-route pressure

This opt-in Rust diagnostic stresses one concrete route with concurrent owners.
It leaves the existing seven-domain closed-loop benchmarks unchanged.

For each contender count, it first holds the route while every other connection
requests immediate acquisition concurrently. Only the declared Held error counts
as expected contention. Next, all contenders request a 30-second wait concurrently.
The campaign requires exactly `min(contenders,100)` provisional queued tokens and
separate QueueFull errors for the rest, matching the current 100-waiter contract.
A queued token is not counted as an ownership grant.

Every fourth queued session disconnects (except in the one-waiter baseline).
QUERY must then show precisely the surviving queue depth. The holder releases,
and surviving waiters drain in provisional-token order. Each deferred grant must
confirm its original token and advance the process-local fencing sequence. QUERY
checks the exclusive owner and remaining queue. A stale token RELEASE must be
fenced while the real holder remains intact; then a valid RELEASE advances the
queue. A final low-load acquisition/release checks service after pressure.
A separate 1-second TTL probe and holder-disconnect/new-session probe verify
that ephemeral ownership disappears and reacquisition advances the token.

```sh
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
FITZ_LEASE_PRESSURE_CONTENDERS=1,32,100,128 \
cargo bench --locked --bench lease_pressure --features benchkit,stress-soak -- \
  --workload should_measure_lease_contender_pressure
```

`FITZ_LEASE_PRESSURE_CONTENDERS` defaults to `1,32,100,128,256`. Values must
strictly increase, with 1..16 entries and at most256 connections per stage,
plus one holder and one observer. One broker uses memory storage because Lease
state is ephemeral. Setup connects clients before acquiring the holder TTL.
Requests/deferred responses have 10-second deadlines; disconnect/expiry cleanup
has a5-second observation budget. Individual network waits can outlast the
cleanup loop's observation guard. Unexpected errors, altered grant tokens,
unexpected queue counts, stale-token acceptance, and missing grants fail the
campaign. Saturation and contention rejections never count as completed grants.

JSON artifacts under `target/fitz-stress/lease-pressure/` contain git source and
dirty identity, declared counts, actual Held/QueueFull decisions, provisional
queues, disconnected sessions, validated grants, stale-token rejections, final
cleanup/probe results and failures. Interrupted processes leave partial artifacts.
These are finite correctness diagnostics, not statistical performance baselines.
No ownership continuity, persistent tokens, cross-node consensus, crash recovery,
or durable waiters are implied. Independent-route inventory pressure, wildcard
registration growth and LIST snapshots remain separate campaigns.
