# Troubleshooting

Use this page for common Fitz failure modes.

## Connection Fails

1. Verify endpoint and transport path.
2. Confirm TLS and certificate configuration.
3. Confirm readiness probe is healthy.

## Authentication Errors

1. Validate JWT signature and expiration.
2. Confirm realm and scope claims match route intent.
3. Check permission denial counters.

## High Latency

1. Check mailbox depth and route mismatch rates.
2. Compare latency histograms before and after recent changes.
3. Apply steps from [../operations/performance-tuning.md](../operations/performance-tuning.md).

## Data Consistency Questions

1. Review [durability.md](durability.md).
2. Review [../development/storage-invariants.md](../development/storage-invariants.md).
3. Review [../development/recovery-internals.md](../development/recovery-internals.md).
