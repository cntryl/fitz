# Transaction Durability Contract

This page defines the external contract for KV and Stream transaction durability.
Queue operations always use best-effort persistence; their success responses are
not durability confirmations. See [durability.md](durability.md).

## Contract Summary

1. Successful commit response means the transaction reached the configured durability point.
2. Crash recovery may replay committed records according to backend policy.
3. Uncommitted transaction state is not guaranteed to survive restart.

## Client Responsibilities

1. Treat transaction IDs as session-scoped.
2. Use idempotency at application boundaries where required.
3. Handle retry behavior according to [troubleshooting.md](troubleshooting.md).

For implementation detail see [development/storage-invariants.md](../development/storage-invariants.md).
