# Production Runbook

This runbook describes standard operating procedures for Fitz in production.

## Startup Procedure

1. Verify config and secrets are present. Use [production-auth.md](production-auth.md) for the auth/browser perimeter baseline and [cloud-setup.md](cloud-setup.md) for storage configuration.
2. Start Fitz and wait for `/startupz` plus `/healthz` or `/readyz` before sending data-plane traffic.
3. Use `/targetz` only for single-active handoff patterns where a replacement process must become target-eligible before it owns the storage writer lease.
4. Confirm Prometheus ingestion from the dedicated `FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT/metrics` listener; use `/api/v1/{family}/metrics` for authenticated operator JSON.
5. Validate one authenticated client round trip on `/ws`.

## Steady-State Signals

- Message latency histograms stay within target.
- No sustained growth in delivery failures.
- Mailbox depth and pending queue metrics remain bounded.
- Authentication failures remain near expected baseline.
- Queue write-policy and cloud durability settings match the operator's loss-window expectations.

See [observability.md](observability.md) for instrumentation details.

## Incident Response

1. Classify issue: auth, routing, storage, transport, or domain behavior.
2. Check probes and key counters first.
3. If impact is isolated to one realm, apply realm-scoped mitigation.
4. If impact is global, reduce load and start controlled rollback.
5. Record timeline and root cause data for postmortem.

## Planned Maintenance

For single-active rolling replacement:

1. Configure the traffic manager to use `/targetz` for target eligibility.
2. Keep `/readyz` or `/healthz` as the strict data-plane readiness check.
3. Set the external deregistration delay close to `FITZ_DRAIN_GRACE_SECONDS`.
4. Set process termination grace longer than the Fitz drain grace.
5. Let the replacement start. It may pass `/targetz` before it owns the writer lease; it must still reject WebSocket upgrades and TCP sessions until `/healthz` or `/readyz` succeeds.
6. Drain the old process with `POST /api/v1/runtime/drain` from an authenticated same-origin admin client, or let the orchestrator send the process termination signal.
7. Confirm `/targetz`, `/healthz`, and `/readyz` return `503` with `accepting_target_traffic` or `accepting_traffic` as `"draining"` while `/livez` remains `200`.
8. Allow Fitz to reject new TCP/WebSocket sessions, wait the configured drain grace, close active ephemeral sessions, and release the storage writer cleanly.
9. Confirm the replacement acquires the writer lease and `/healthz` or `/readyz` returns `200`, then run smoke checks.

If the target-eligible-before-ready retry window is unacceptable, use `/readyz` or `/healthz` for traffic admission and choose a rollout strategy that can stop the old writer before the replacement becomes target-eligible.

## Emergency Rollback

1. Stop rollout immediately.
2. Revert to the last known-good image and configuration.
3. Replay validation from the startup procedure.
4. Keep elevated monitoring for one full traffic cycle.
