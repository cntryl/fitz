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

## Operations Readiness

1. [operations-runbook.md](operations-runbook.md) reviewed and current.
2. Alerts and dashboards updated for new signals.
3. Rollback plan verified.

## Sign-off

1. Engineering sign-off.
2. Operations sign-off.
3. Security sign-off for auth or policy changes.
