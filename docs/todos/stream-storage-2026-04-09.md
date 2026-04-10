# Stream Storage 2026-04-09

## Purpose

This is a quick snapshot of the Stream key and value layout as it exists now, plus the current bench-only redesign frontier.

## Production Layout

Current production Stream still writes three durable event-body planes:

- Exact resource history in the resource keyspace.
- Area wildcard history in the area keyspace.
- Realm wildcard history in compact realm pages.

The realm plane is no longer one row per event. It is page-packed, but it still stores full bodies.

## Production Keys

Primary production keys in [src/domains/stream/storage.rs](../../src/domains/stream/storage.rs):

| Prefix | Key shape | Value shape | Role |
| --- | --- | --- | --- |
| `0x01` | `[resource][realm][0][area][0][resource][0][resource_offset_be]` | `ResourceValue { resource_offset, area_offset, realm_offset, body, metadata, created_at }` | Exact resource replay |
| `0x02` | `[area][realm][0][area][0][area_offset_be]` | `AreaValue { resource_offset, body, metadata, created_at }` | Area wildcard replay |
| `0x03` | `[realm][realm][0][page_start_realm_offset_be]` | `CompactRealmPageValue { records: Vec<CompactRealmPageRecord> }` on new writes; legacy `RealmValue` still decodes on read | Realm wildcard replay |
| `0x04` | `[watermark][realm][0][area]` | `WatermarkValue { watermark }` | Area watermark |
| `0x05` | staging/session-scoped | staged event payloads | Live append session buffering |
| `0x06` | `[offset_counter][realm][0][area][0][resource]` | `OffsetCounterValue { next_offset }` | TTL-safe exact-resource next offset |
| `0x07` | `[realm_watermark][realm]` | `WatermarkValue { watermark }` | Realm watermark |
| `0x08` | `[resource_meta][realm][0][area][0][resource]` | `ResourceMetaValue { next_offset, committed_size_bytes }` | Durable resource metadata |
| `0x09` | `[area_counter][realm][0][area]` | `AreaCounterValue { next_offset }` | Durable area next offset |
| `0x0A` | `[realm_counter][realm]` | `RealmCounterValue { next_offset }` | Durable realm next offset |

## Production Value Notes

- `ResourceValue` is the exact read surface. It carries both offsets plus the full event body.
- `AreaValue` is also a covering row. Area reads do not hydrate from another plane.
- Realm writes now go through `write_compact_realm_records(...)` in [src/domains/stream/store.rs](../../src/domains/stream/store.rs), but they still use the existing realm keyspace via `encode_realm_key(...)`.
- New realm values are `CompactRealmPageValue` with up to `REALM_PAGE_RECORD_LIMIT = 64` records per row.
- Reads remain backward-compatible with legacy one-row `RealmValue` encoding through `RealmValue::try_decode(...)` in [src/domains/stream/storage.rs](../../src/domains/stream/storage.rs).

## Production Write Path

Per committed event, production currently writes:

1. One `ResourceValue` row with the full body.
2. One `AreaValue` row with the full body.
3. One realm-page entry inside `CompactRealmPageValue`, also with the full body.
4. Resource metadata and offset counters.
5. Area and realm counters.
6. Area and realm watermarks.

The important accepted production improvement is that the realm plane is compacted into shared pages instead of one legacy realm row per event. The important remaining cost is that the body is still duplicated across resource, area, and realm storage.

## Production Read Surfaces

- Exact resource replay scans the `0x01` resource prefix in [src/domains/stream/store.rs](../../src/domains/stream/store.rs).
- Area wildcard replay scans the `0x02` area prefix and reads covering `AreaValue` rows directly.
- Realm wildcard replay scans the `0x03` realm prefix and decodes either compact realm pages or legacy realm rows.

Current production shape in one sentence:

`resource covering bodies + area covering bodies + compact realm-body pages`

## Frontier And Research Keyspaces

The following prefixes exist for redesign work beyond the current covering live path:

| Prefix | Purpose |
| --- | --- |
| `0x0B` | Canonical resource body row for hydration experiments |
| `0x0C` | Area locator row |
| `0x0D` | Realm locator row |
| `0xE4` | Production frontier contract for compact area page rows, defined in `src/domains/stream/storage.rs` but not yet used by the live store |
| `0xE8` | Production frontier contract for compressed compact realm page rows, defined in `src/domains/stream/storage.rs` but not yet used by the live store |
| `0xEA` | Production frontier contract for compact resource mini-page rows, defined in `src/domains/stream/storage.rs` but not yet used by the live store |
| remaining `0xE0`-`0xEA` variants | Bench-only paged, ref, run-ref, compressed, and locator layouts still used only by the Stream redesign prototype benches |

The `0xE4`, `0xE8`, and `0xEA` contracts now exist in production storage code so the real store port can target a stable on-disk shape. The rest still live only in the Tier 2 and bench-only Tier 3 prototype benchmark files.

## Frontier Snapshot

### Current shipped production slice

- 2026-04-10: Added the first durable activation guard for the real Stream path. `StreamStore` now persists a per-family layout marker for legacy-covering access, rejects promotion-frontier selection against unmarked existing stream data with an explicit reset-required error, and causes live `StreamDomainSink` / `TestServer` promotion-frontier boot to fail fast with `ERR_STREAM_STORAGE_LAYOUT_UNSUPPORTED` until the real frontier read/write paths are implemented. Local restart mismatch is now explicit instead of silently reopening under the wrong layout.
- 2026-04-10: Promoted the frontier row contract into `src/domains/stream/storage.rs` by adding the real compact area page, compact resource mini-page, and compressed compact realm page key prefixes and codecs. This does not change live behavior yet; `StreamStore` and `StreamDomainSink` still block PromotionFrontier until the real read and write paths are ported.
- 2026-04-10: Refactored `StreamStore` into explicit layout dispatch at the public method boundary. Commit, exact-resource read, wildcard replay, tail, and resource-metadata entry points now route through `LegacyCovering` helpers, while `PromotionFrontier` still fails fast. This keeps the current client-visible behavior unchanged but makes the next write and read parity slices replace helper bodies instead of rewriting actor or sink callers.
This is the current safe production baseline.

### Bench-only fallback slice
- Meaning: safe but modest write reduction.

### Bench-only compression slice

- Compact area pages plus compressed realm-body pages.
- Mixed-corpus write shape: about `526.14 B/event`.
- Mixed-corpus realm replay: about `240-289 Kelem/s` versus `301-330 Kelem/s` for uncompressed compact realm-body pages.
- Meaning: useful optimization layer, but not the main structural answer.

### Bench-only structural frontier

- Area pages become the only body plane.
- Realm replay stays on run-ref rows pointing into area pages.
- Exact resource history becomes compact refs back into area pages.
- Write shape: about `196.59 B/event`.
- Exact resource replay: about `1.09-1.25 Kelem/s` with lazy page gets, improved to about `14-20 Kelem/s` with a scanned exact-resource variant, still below the current covering exact path at about `29-36 Kelem/s`.

Meaning:

- This is the first layout with clearly material write leverage.
- It also makes exact resource locality the new dominant blocker.

### Bench-only promotion frontier

- Area replay stays on direct compact area pages.
- Exact resource replay moves onto compact resource mini-pages.
- Realm replay restores compressed compact realm-body pages as a secondary plane.
- Low-entropy write shape: about `381.18 B/event`.
- Production-like write shape: about `475.84 B/event`.
- Production-like exact resource replay: about `73.36-75.80 Kelem/s` versus `64.26-69.23 Kelem/s` for covering exact replay.
- Production-like realm replay: about `352.18-369.60 Kelem/s` for compressed compact realm-body pages versus `352.70-374.65 Kelem/s` for covering realm replay.

Meaning:

- The structural fix and compression no longer compete. Compression now looks like an additive realm optimization layered on top of the area-first design.
- The exact-resource replay cliff is no longer the blocking issue on the current Tier 2 evidence.
- The remaining question is no longer Tier 2 viability. The first server-hosted proof now exists. The remaining question is whether the same layout can keep those bars once it runs through switchable live Stream domain code instead of a router-swapped prototype sink.

### Tier 3 prototype model

- A bench-only stress surface exists in [../../benches/tier3_system_stream_storage_model.rs](../../benches/tier3_system_stream_storage_model.rs).
- That file now measures the promotion frontier in two ways with production-like payloads: a direct storage-model path and a routed prototype path that reuses the real router and stream frame codec.
- Latest direct storage-model run: covering resource about `99.57 Kops/s`, resource mini-pages about `106.19 Kops/s`, covering area about `376.14 Kops/s`, compact area pages about `684.79 Kops/s`, covering realm about `700.17 Kops/s`, compressed realm pages about `711.71 Kops/s`.
- Latest routed prototype run: covering resource about `128.39 Kops/s`, promotion-frontier resource about `137.00 Kops/s`, covering area about `326.72 Kops/s`, compact area pages about `664.75 Kops/s`, covering realm about `679.04 Kops/s`, compressed realm pages about `659.91 Kops/s`.

Meaning:

- The relative ordering still holds on both prototype surfaces for resource and area replay, and routed realm replay stays in the same throughput class with only a small tax in the latest run.
- The routed prototype is stronger evidence than the storage-only surface because it includes router delivery and frame encode/decode, but it still is not a promotion-gate number for the live Stream service because it bypasses `StreamDomainSink`, `StreamActor`, and network transport.
- A switchable test-server path now exists in [../../benches/tier4_integration_stream_storage_model.rs](../../benches/tier4_integration_stream_storage_model.rs): it boots the live server, then overrides the `stream` domain registration with a promotion-frontier read sink.
- That changes the open problem. It is no longer finding any live routed surface. It is porting the same layout through switchable or production `StreamDomainSink` and `StreamActor` code so the live bars come from the real Stream implementation.

### Tier 4 live prototype model

- A server-hosted prototype surface now exists in [../../benches/tier4_integration_stream_storage_model.rs](../../benches/tier4_integration_stream_storage_model.rs), backed by [../../benches/support/stream_storage_model.rs](../../benches/support/stream_storage_model.rs).
- It boots `TestServer`, keeps the real boot/session/direct/TCP/WS paths, and swaps the router's `stream` registration to the promotion-frontier sink after startup.
- Latest run: direct resource about `1.69 Mops/s`, area about `840.06 Kops/s`, realm about `840.51 Kops/s`; TCP resource about `816.36 Kops/s`, area about `510.38 Kops/s`, realm about `482.72 Kops/s`; WS resource about `806.06 Kops/s`, area about `530.43 Kops/s`, realm about `508.20 Kops/s`.

Meaning:

- The promotion frontier now has a real server-hosted consume surface and it clears the current Tier 4 floor budgets on all three replay modes.
- This is stronger evidence than the Tier 3 routed prototype because it includes live boot, session, and transport overhead.
- It still is not a production promotion gate because the benchmark swaps in a bench-only sink after router registration, so it still bypasses `StreamDomainSink` and `StreamActor` after dispatch.

## Practical Conclusion

What we have now is:

- A stable production layout that still duplicates bodies three times, but with the realm plane already compacted into pages.
- A compression path that helps, but behaves like optimization gravy.
- A structural area-first candidate that solved exact-resource locality by introducing compact resource mini-pages.
- A combined area-first plus compressed-realm candidate that currently looks like the best promotion frontier on Tier 2 evidence.
- Bench-only Tier 3 storage-model and routed prototype surfaces that preserve the same candidate ordering, plus a Tier 4 server-hosted prototype surface that clears current floor budgets, but still stops short of the real Stream sink/actor path.

If Stream redesign continues from here, the next problem is no longer exact-resource locality first or finding any live path at all. It is porting the combined area-first plus compressed-realm shape through switchable live `StreamDomainSink` and `StreamActor` code so the current Tier 4 prototype bars become real promotion-gate numbers, and then mapping that same shape onto the resource-head-only OCC contract in production code.