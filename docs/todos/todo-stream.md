# Stream

- Classification: Durable committed events and committed sequencing, ephemeral append sessions and subscriptions.
- Outcome: store-authoritative, commit-time sequencing across resource, area, and realm with restart-safe metadata and honest admin/docs surfaces.

## Completed

- [x] Chose one authoritative production path: `StreamStore` now owns durable offset allocation and committed stream metadata, while the boot sink is a thin adapter over warm per-resource actors.
- [x] Moved resource, area, and realm sequencing into durable Midge-backed state with lazy upgrade and backfill for legacy metadata.
- [x] Kept consumer cursors client-managed only. `ReadCursor` remains response metadata, not a durable broker-side cursor feature.
- [x] Aborted live append sessions on disconnect cleanup and broker restart. Staged appends remain in-memory only and are dropped when the session disappears.
- [x] Removed split sink/store staged-append ownership from the production path. One active append session per resource is enforced by the warm actor.
- [x] Rebuilt stream admin snapshots from durable committed metadata plus live append-session counts so committed streams remain visible after restart.
- [x] Aligned client/admin/OpenAPI/architecture docs with the implemented contract: committed data survives restart, append sessions and subscriptions do not, and reads past the watermark return an empty success.

## Non-Goals Kept

- Durable consumer groups or broker-side replay cursors.
- Multi-node stream coordination.
- New public Stream API surface beyond the existing wire contract.

## Verification

- [x] Restart tests prove resource offsets remain monotonic.
- [x] Restart tests prove area and realm offsets remain monotonic.
- [x] Disconnect tests prove abandoned append sessions are cleaned up.
- [x] Crash/restart tests prove committed events stay readable and staged writes do not corrupt future appends.
- [x] Admin snapshot tests prove stream resources rebuild from durable metadata after restart.
