# Operations Runbook

This runbook describes startup, observation, replacement, drain, and rollback
procedures exposed by Fitz. Operators remain responsible for validating these
procedures in their own environment.

## Startup Procedure

1. Verify config and secrets are present. Use [auth-browser-deployment.md](auth-browser-deployment.md) for the auth/browser perimeter baseline and [cloud-setup.md](cloud-setup.md) for storage configuration.
2. Start Fitz and wait for `/startupz` plus `/healthz` or `/readyz` before sending data-plane traffic.
3. Use `/targetz` only through a scheduling/orchestration path that can observe a lease-waiting standby without routing customer traffic to it. Never use it for the customer-facing ALB target-group health check.
4. Confirm Prometheus ingestion from the dedicated `FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT/metrics` listener; use `/api/v1/{family}/metrics` for authenticated operator JSON.
5. Validate one authenticated client round trip on `/ws`.

## Steady-State Signals

- Message latency histograms stay within target.
- No sustained growth in delivery failures.
- Mailbox depth and pending queue metrics remain bounded.
- Authentication failures remain near expected baseline.
- Queue write-policy and cloud durability settings match the operator's loss-window expectations.
- Active Midge writer-lease health remains healthy. Fitz withdraws orchestration health and readiness and terminates if lease renewal health is lost; investigate the provider and replacement event rather than expecting in-process reacquisition.

See [observability.md](observability.md) for instrumentation details.

## Incident Response

1. Classify issue: auth, routing, storage, transport, or domain behavior.
2. Check probes and key counters first.
3. If impact is isolated to one realm, apply realm-scoped mitigation.
4. If impact is global, reduce load and start controlled rollback.
5. Record timeline and root cause data for postmortem.

## Planned Maintenance

For single-active replacement, first choose a deployment topology. A standard
one-target-group ECS rolling deployment cannot provide zero-downtime handoff:
`/healthz` keeps the standby out of traffic but cannot succeed while the old
writer holds the lease, while `/targetz` would make ALB route traffic to a task
whose data plane is closed.

For a zero-downtime-capable custom handoff:

1. Start the replacement outside the customer-facing target group, or in a blue-green target group that is not yet on the production route.
2. Let a separate controller observe `/targetz`. Expected writer-lease contention retries indefinitely with capped exponential backoff.
3. Deregister and drain the old writer with `POST /api/v1/runtime/drain` from an authenticated same-origin admin client, or have the orchestrator send SIGTERM.
4. Confirm `/targetz`, `/healthz`, and `/readyz` on the old process return `503` while `/livez` remains `200`.
5. Allow the configured drain grace to complete and the old process to release the Midge writer lease.
6. Wait for `/healthz` or `/readyz` on the replacement to return `200`, then register it or shift the production route and run smoke checks.

Use `/healthz`, never `/targetz`, as the customer-facing ALB target-group health
check. Set the external deregistration delay close to
`FITZ_DRAIN_GRACE_SECONDS`. Use 90 seconds as the process-termination-grace
baseline with the default 25-second drain grace, then increase it for the
measured peak session cleanup, domain teardown, and configured storage backend
and provider.

If separate standby orchestration or blue-green/custom lifecycle control is not
available, use a stop-first ECS deployment: stop and drain the old task, accept
the resulting downtime, and start or admit the replacement only after the old
writer exits.

A standby waiting for the writer lease remains live until it acquires ownership
or receives Ctrl-C, SIGTERM, an authenticated runtime drain request, or a fatal
shutdown request. Shutdown while waiting cancels the backoff and closes the
early HTTP and metrics listeners without waiting the active-broker drain grace.
An in-flight Midge open or recovery attempt is always joined before exit so it
cannot acquire the lease later from a detached blocking task. Midge does not
cooperatively cancel that operation, so shutdown may take the backend or
provider's worst-case time and can exceed the 90-second baseline.

Shutdown modes are intentionally different:

- SIGTERM and `POST /api/v1/runtime/drain` gracefully withdraw traffic, wait `FITZ_DRAIN_GRACE_SECONDS` after full startup, and then clean up sessions, listeners, domains, and Midge.
- Ctrl-C and fatal actor or active writer-lease-health failures skip the drain delay and begin the same explicit cleanup immediately.

During active service, when Midge reports a lease-renewal health failure, Fitz
withdraws `/targetz`, `/healthz`, and `/readyz` and terminates through the fatal
path. Do not wait for a second signal and do not expect that process to reacquire
the lease; verify that the orchestrator starts or promotes a healthy replacement.

Provider-backed cloud failover has an upstream safety gate in the pinned Midge
revision: renewal health has no independent monotonic deadline while provider
I/O blocks, and malformed cloud lease expiration is treated as expired. Do not
approve cloud takeover as split-brain-safe until Midge adds a pre-TTL watchdog
and fail-closed expiration parsing.

## Emergency Rollback

1. Stop rollout immediately.
2. Revert to the last known-good image and configuration.
3. Replay validation from the startup procedure.
4. Keep elevated monitoring for one full traffic cycle.
