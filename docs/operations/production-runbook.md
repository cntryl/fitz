# Production Runbook

This runbook describes standard operating procedures for Fitz in production.

## Startup Procedure

1. Verify config and secrets are present. Use [production-auth.md](production-auth.md) for the auth/browser perimeter baseline and [cloud-setup.md](cloud-setup.md) for storage configuration.
2. Start Fitz and wait for `/startupz` then `/readyz` success.
3. Confirm metrics ingestion from `/metrics`.
4. Validate one authenticated client round trip on `/ws`.

## Steady-State SLO Signals

- Message latency histogram stable within target.
- No sustained growth in delivery failures.
- Mailbox depth and pending queue metrics remain bounded.
- Authentication failures remain near expected baseline.

See [operations/observability.md](observability.md) for instrumentation details.

## Incident Response

1. Classify issue: auth, routing, storage, or transport.
2. Check probes and key counters first.
3. If impact is isolated to one realm, apply realm-scoped mitigation.
4. If impact is global, reduce load and start controlled rollback.
5. Record timeline and root cause data for postmortem.

## Planned Maintenance

1. Drain or reject new sessions.
2. Complete in-flight write-sensitive operations.
3. Snapshot or backup durability-sensitive state.
4. Apply update using [operations/migration-guide.md](migration-guide.md).
5. Run smoke checks and restore normal traffic.

## Emergency Rollback

1. Stop rollout immediately.
2. Revert to last known-good image and configuration.
3. Replay validation checklist from startup procedure.
4. Keep elevated monitoring for one full traffic cycle.
