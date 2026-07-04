# Recovery Internals

Recovery behavior is designed to preserve committed durability state and reject ambiguous partial state.

## Recovery Goals

1. Restore broker availability safely.
2. Preserve committed data according to configured durability level.
3. Prevent cross-realm contamination.

## Startup Recovery Flow

1. Validate configuration and startup resource preflight.
2. Start the HTTP listener so `/targetz` can participate in orchestrator handoff while the data plane remains closed.
3. Initialize the configured storage backend, acquire the active Midge writer lease, and ensure all configured route-family column families exist.
4. Register storage-backed domains and synchronously validate or preload durable domain state before marking domains ready. Queue validates persisted queue state for existing families. Schedule preloads persisted schedule families and pending fire claims. KV and Stream attach storage-backed sinks. Notice, RPC, and Lease intentionally start with empty live state.
5. Start the TCP listener, mark startup complete, and return `200` from `/healthz` or `/readyz` only when storage, auth configuration, durable domain initialization, startup completion, and traffic acceptance are all true.

`/targetz` is intentionally weaker than data-plane readiness. It can return `200` once the HTTP target is usable and the process is not draining, even while storage or domain preload is still pending. WebSocket upgrades and TCP sessions still reject data-plane traffic until the strict readiness gate passes.

Live session state is never recovered during startup. Notice subscriptions, Stream live subscriptions and append sessions, KV open transactions, Queue inflight ownership tokens, RPC worker registrations and pending calls, Lease ownership, and Schedule subscriptions are rebuilt only by reconnecting clients when their domain contract permits it.

## Failure Handling

- On unrecoverable corruption, remain not ready and require operator intervention.
- On recoverable partial state, isolate and report degraded components.

## Related Docs

- [storage-invariants.md](storage-invariants.md)
- [../operations/production-runbook.md](../operations/production-runbook.md)
