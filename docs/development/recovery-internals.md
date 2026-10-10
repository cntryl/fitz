# Recovery Internals

Recovery behavior is designed to preserve committed durability state and reject ambiguous partial state.

## Recovery Goals

1. Restore broker availability safely.
2. Preserve committed data according to configured durability level.
3. Prevent cross-realm contamination.

## Startup Recovery Flow

1. Validate configuration and startup resource preflight.
2. Start the HTTP listener so a separate control path can observe `/targetz` during orchestrated handoff while the data plane remains closed.
3. Initialize the configured storage backend, acquire the active Midge writer lease, and ensure all configured route-family column families exist. Before domain initialization, wait up to one shared 60-second budget for existing route-family column families to admit writes after replay; pressure guards remain enforced and timeout fails startup.
4. Register storage-backed domains and synchronously validate or preload durable domain state before marking domains ready. Queue validates persisted queue state for existing families; best-effort startup repair removes incomplete split message remnants and invalidates their derived queue indexes before continuing. A crash may lose that repair, so a later startup validates again. Schedule preloads persisted schedule families and pending fire claims. KV and Stream attach storage-backed sinks. Notice, RPC, and Lease intentionally start with empty live state.
5. Start the TCP listener, mark startup complete, and return `200` from `/healthz` or `/readyz` only when storage, auth configuration, durable domain initialization, startup completion, and traffic acceptance are all true.

Domain startup scans stream their rows rather than collecting a whole shared
column family. Recognized foreign realm/domain key ranges are skipped with a
range seek, so a large KV payload history does not have to be materialized by
Queue, Stream or Schedule initialization. Owned rows retain the same decoding
and validation. Unrecognized keys and binary-leading legacy keys remain visible
to validators; non-ASCII realm prefixes conservatively retain the full scan.

`/targetz` is intentionally weaker than data-plane readiness. It can return `200` once the HTTP listener is usable and the process is not draining, even while storage or domain preload is still pending. It is only for a separate orchestration path; a customer-facing ALB target group must use `/healthz`. WebSocket upgrades and TCP sessions still reject data-plane traffic until the strict readiness gate passes.

Schedule preload waits for the actor-owned preload result within the
`FITZ_SCHEDULE_PRELOAD_TIMEOUT_SECS` startup watchdog. The default 120-second
deadline replaces the former one-second actor reply deadline while preserving a
bounded, diagnosable startup failure. Preload logs its configured deadline,
discovered family count, per-family progress at debug level, elapsed completion
time, and timeout. Actor failure also disconnects the reply channel so boot
fails closed before the watchdog expires.

Live session state is never recovered during startup. Notice subscriptions, Stream live subscriptions and append sessions, KV open transactions, Queue inflight ownership tokens, RPC worker registrations and pending calls, Lease ownership, and Schedule subscriptions are rebuilt only by reconnecting clients when their domain contract permits it.

## Persistent Domain Partial-State Policy

| Domain | Persisted write shape | Startup treatment |
| --- | --- | --- |
| KV | User values are submitted in one Midge transaction using the explicit COMMIT choice: Buffered/Sync locally and CloudAsync/CloudStrict in cloud mode, independent of background cloud configuration. Buffered mode may lose a recent transaction, but does not make a policy-permitted half-transaction an accepted KV startup state. Inventory estimates (record count, storage bytes) are updated in a separate transaction after the data commit succeeds; if that update fails, the commit still succeeds, the failure is logged and counted in `fitz_kv_inventory_estimate_update_failures_total`, and the family queues the resource for repair. Later KV frames on that family retry persisting the estimate as incomplete (backing off after a failed attempt), which makes the admin inventory rescan exact counts. Pending repairs are held in memory per family: if the family restarts before a retry succeeds, the estimate stays stale until the resource is written again. | Committed rows attach directly; malformed engine state remains a storage failure. Inventory estimates are non-authoritative. |
| Queue | Queue always skips WAL; a crash before background persistence completes can lose accepted mutations or leave incomplete split header/body records. | Startup deletes incomplete remnants and derived indexes with best-effort writes, logs discarded rows, and rebuilds indexes. Repair may repeat after a crash. Complete records remain unchanged; other malformed authoritative state fails closed. |
| Stream | Event rows, discriminators, counters, metadata, and watermarks for a commit are submitted in one Midge transaction using the explicit COMMIT choice: Buffered/Sync locally and CloudAsync/CloudStrict in cloud mode, independent of background cloud configuration. Stream has no best-effort persistent mode. | Existing rows are decoded and layout-validated; invalid authoritative rows fail closed. |
| Schedule | Definitions and bodies are written together. Best-effort writes exist only in non-recoverable memory mode; persistent modes use sync writes locally, and cloud-async (the default `background` cloud durability) or cloud-strict writes in cloud mode. | Persistent definitions and pending claims preload before readiness. Missing definition bodies or malformed claims fail closed. Reconstructed definition, body, due-index, and pending-claim routes must satisfy the same concrete-route grammar as normal Schedule writes; valid orphan pending claims remain recoverable. |

Midge transactions are atomic at the Fitz storage boundary. Durability policy
controls whether a recent transaction can be lost after acknowledgement; it
does not give Fitz permission to accept malformed multi-row state. Queue fast
mode is the exception that needs explicit reconciliation because its WAL-free
flush can expose a policy-permitted remnant after restart.

## Failure Handling

- On unrecoverable corruption, remain not ready and require operator intervention.
- On recoverable fast-queue partial state, durably discard only the incomplete
  message remnants, invalidate the affected derived indexes, log the discarded
  message IDs, and continue startup.

## Related Docs

- [storage-invariants.md](storage-invariants.md)
- [../operations/operations-runbook.md](../operations/operations-runbook.md)
