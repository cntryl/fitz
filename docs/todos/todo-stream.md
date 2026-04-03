# Stream

- Classification: Durable committed events and committed sequencing, ephemeral append sessions and subscriptions.
- Outcome: store-authoritative, commit-time sequencing across resource, area, and realm with restart-safe metadata and honest admin/docs surfaces.
- Status: Hardening complete. Live-notify performance follow-up may remain if that path becomes a first-class workload.

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

## Benchmark Findings

- 2026-04-03 refreshed summary keeps append/read throughput strong on the authoritative path: tier3 sustained append measured about 825k ops/s, batch write about 840k ops/s, multiarea writes about 811k ops/s, read scan about 889k ops/s, and tier4 direct append about 790k ops/s.
- Tier4 transport paths stayed healthy relative to the current contract: WebSocket append measured about 23.9k ops/s, TCP append about 17.2k ops/s, and multiclient concurrent appends about 51.4k ops/s after aligning the bench with the one-active-session-per-resource rule.
- The current weak spots are still outside the durable append core: tier3 `publish_fanout_with_subscribers` measured about 198 ops/s, while offset-tracking overhead measured about 218k ops/s.

## Performance Follow-Up

- [ ] If live subscriber notifications are a first-class stream workload, profile and optimize the commit-notify/subscriber delivery path separately from the durable append/read path.
