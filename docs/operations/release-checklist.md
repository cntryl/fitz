# Release Checklist

Use this checklist before approving a Fitz release.

## Build and Test

1. All CI checks green.
2. Required unit, integration, and benchmark suites completed.
3. No unresolved critical or high-severity regressions.

## Documentation

1. [docs/README.md](../README.md) links validated.
2. API and admin behavior changes documented.
3. Migration notes added for any compatibility impacts.
4. Broker and all supported client codecs validated against each breaking wire contract.

## Operations Readiness

1. [operations-runbook.md](operations-runbook.md) reviewed and current.
2. Alerts and dashboards updated for new signals.
3. Rollback plan verified.
4. Required storage export/replay or reset rehearsed for breaking persisted-format changes.

## Sign-off

1. Engineering sign-off.
2. Operations sign-off.
3. Security sign-off for auth or policy changes.

## Stream error envelope generation 2 release gate

For issue #238, record the released .NET, TypeScript, Go, Python, and Rust SDK
versions that decode status 2 before releasing the broker change. Verify legacy
status-1 decoding, APPEND and COMMIT code `2001` over a real broker, unrelated
wording with `2001`, misleading wording with another code, and backend `2012`.
Requalify both linked Portia assertions using the exact broker and client
artifacts, including original failure and pending-batch preservation through
cleanup failures. Follow the client-first upgrade and broker-first rollback in
[migration guidance](../operations/migration-guide.md).
