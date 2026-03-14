# Recovery Internals

Recovery behavior is designed to preserve committed durability state and reject ambiguous partial state.

## Recovery Goals

1. Restore broker availability safely.
2. Preserve committed data according to configured durability level.
3. Prevent cross-realm contamination.

## Startup Recovery Flow

1. Initialize storage backend.
2. Validate manifests and metadata consistency.
3. Replay eligible committed records.
4. Rebuild runtime indexes and subscription state as needed.
5. Open readiness endpoint only after integrity checks pass.

## Failure Handling

- On unrecoverable corruption, remain not ready and require operator intervention.
- On recoverable partial state, isolate and report degraded components.

## Related Docs

- [storage-invariants.md](storage-invariants.md)
- [../operations/production-runbook.md](../operations/production-runbook.md)
