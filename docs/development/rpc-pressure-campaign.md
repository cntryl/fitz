# RPC admission and cleanup pressure diagnostic

This opt-in Cargo integration diagnostic drives real TCP clients through one
running memory-mode broker. It deliberately offers more simultaneous calls than
one negotiated worker registration can execute. RPC remains live and ephemeral;
no outcome establishes durability, exactly-once execution, or retry safety.

Run with an otherwise idle host, from this checkout:

```sh
FITZ_LOG_LEVEL=off RUST_LOG=off OTEL_ENABLED=false \
  cargo test --locked --test rpc_pressure \
  should_push_rpc_admission_and_cleanup_limits -- --ignored --nocapture
```

`FITZ_RPC_PRESSURE_BURSTS` overrides the default `1,32,256,1024,8192`.
Accept one through eight positive burst sizes, each at most 8192. Use `1` for a
small wiring smoke; it does not qualify higher offered bursts.

One shared concrete route is registered with `max_concurrent=1` and cancellation
capability negotiation. At most 64 producer connections each pipeline at most
128 1KiB calls. The worker waits one second before reading dispatches, then waits
one millisecond before each echo. The shared route's current pending capacity
is 1000; the independent process-wide pending limit is 4096. This scenario
probes the concrete-route cap, not the global cap. Actual admission outcomes
are measured; do not infer an exact threshold from the offered burst count.

Each call has a 30-second request budget; each producer stage has an absolute
60-second deadline. Worker writes and joins have separate five-second deadlines,
followed by task abort and a bounded join on timeout. Startup/shutdown have
60-second wrappers; each complete lifecycle probe has a ten-second wrapper.
Successful terminal replies must match UUID, exact terminal
sequence/flags, and every payload byte. Unknown identities, duplicate terminal
replies, corrupt bodies, timeouts, or unexpected error codes fail the stage.
Only explicit `ERR_RPC_BACKPRESSURE` is classified as admission rejection.
Rejected calls never count as completed, and unknown outcomes are never retried.

After load drains, negotiated explicit cancellation, caller disconnect, and
budget expiry each receive validated worker cancellation controls. The handler
sends its cleanup acknowledgement; the same worker registration must then
execute and answer a new call using its single credit. Each stage and probe
waits for session transport state to reach zero after connections close.
Worker/pending admin snapshots are advisory: refresh failure can leave stale or
default-empty read models, so empty snapshots never prove actor cleanup.
A final low-load request proves current-process service after load.

Atomic JSON artifacts under `target/fitz-stress/rpc-pressure/` include current
source SHA/dirty state, fixed workload settings, planned burst envelopes,
actual attempted/sent/completed/
backpressure/dispatched/unresolved counts, elapsed time, failure, cleanup, and
lifecycle/recovery probe outcomes. A write attempt is counted before its TCP
write; incomplete writes remain explicitly indeterminate and unresolved.
Stage failures retain partial counts before broker shutdown. An initial artifact
precedes startup and a failure checkpoint precedes shutdown; artifact-save errors
cannot skip broker cleanup. This test is intentionally ignored by the ordinary workspace
suite; it is a bounded diagnostic, not a performance baseline or release gate.

The requested burst envelope is synchronous per producer socket, with concurrent
producers and independent worker service. Offered burst size is not an offered
rate, and socket writes are not broker acceptance acknowledgements. There is
no restart, side-effect rollback, cancellation-grace forced-close, wildcard
fairness, or cross-family isolation qualification in this scenario; those
remain separately named evidence boundaries.

## Initial clean-source diagnostic

On source `894b0234b94299dee4cbe072978f2bdb309eba9f`, with dirty state false,
the defaults passed in 13.71 seconds:

| Offered burst | Validated echoes | Explicit backpressure | Unresolved |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 0 | 0 |
| 32 | 32 | 0 | 0 |
| 256 | 256 | 0 | 0 |
| 1024 | 1001 | 23 | 0 |
| 8192 | 1001 | 7191 | 0 |

Worker dispatches matched validated completions at every stage. The observed
held-worker envelope matches the current 1000 queued plus one active call
route boundary. This is an observed admission boundary under the declared
burst/hold settings, not an independent arrival-rate or global capacity claim.
All negotiated lifecycle credit probes, final low-load call, and cleanup passed.
The separate one-call smoke passed in 3.78 seconds. No broker bug was reproduced.
The full artifact is `1791313739252040000.json` in the documented directory.
