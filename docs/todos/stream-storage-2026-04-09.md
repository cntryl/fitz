# Stream Storage 2026-04-09

## Purpose

This is the current runtime snapshot for shipped Stream storage. It replaces the older "bench-only promotion frontier" framing: Fitz already runs promotion-frontier in the live Stream store.

## Current Runtime Status

- `StreamStorageLayout::PromotionFrontier` is the only live runtime layout in [../../src/domains/stream/store.rs](../../src/domains/stream/store.rs).
- `FITZ_STREAM_STORAGE_LAYOUT=promotion-frontier` and `frontier` select the shipped path.
- Legacy aliases such as `legacy`, `legacy-covering`, and `covering` are normalized to `promotion-frontier` with a warning.
- Stored legacy layout markers and unmarked historical stream rows survive only for explicit failure detection:
	- `ERR_STREAM_STORAGE_LAYOUT_MISMATCH`
	- `ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED`
- There is no in-place upgrade path for old or unmarked stream stores.

## Shipped Production Keys

Primary production keys in [../../src/domains/stream/storage.rs](../../src/domains/stream/storage.rs):

| Prefix | Key shape | Value shape | Role |
| --- | --- | --- | --- |
| `0x04` | `[watermark][realm][0][area]` | `WatermarkValue { watermark }` | Durable area watermark |
| `0x07` | `[realm_watermark][realm]` | `WatermarkValue { watermark }` | Durable realm watermark |
| `0x08` | `[resource_meta][realm][0][area][0][resource]` | `ResourceMetaValue { next_offset, committed_size_bytes }` | Durable exact-resource metadata |
| `0x09` | `[area_counter][realm][0][area]` | `AreaCounterValue { next_offset }` | Durable next area offset |
| `0x0A` | `[realm_counter][realm]` | `RealmCounterValue { next_offset }` | Durable next realm offset |
| `0x0E` | `[layout_marker]` | `StreamLayoutMarkerValue { layout }` | Per-family layout activation marker |
| `0xE4` | `[compact_area_page][realm][0][area][0][page_start_area_offset_be]` | `CompactAreaPageValue { records: Vec<CompactAreaPageRecord> }` | Area wildcard replay |
| `0xE8` | `[compressed_compact_realm_page][realm][0][page_start_realm_offset_be]` | `CompressedCompactRealmPageValue { records: Vec<CompactRealmPageRecord> }` | Realm wildcard replay |
| `0xEA` | `[compact_resource_page][realm][0][area][0][resource][0][page_start_resource_offset_be]` | `CompactResourcePageValue { records: Vec<CompactResourcePageRecord> }` | Exact resource replay |

## Shipped Write Path

Per committed event, the live runtime writes:

1. One exact-resource mini-page entry in the `0xEA` keyspace.
2. One area wildcard page entry in the `0xE4` keyspace.
3. One compressed realm page entry in the `0xE8` keyspace.
4. Resource metadata plus area and realm counters.
5. Area and realm watermarks.
6. A per-family layout marker on first real write or boot activation.

Live append sessions and subscriptions remain in memory only. They are not durable storage rows.

The old `0x01`, `0x02`, and `0x03` covering-row layouts are not written by the current runtime.

## Shipped Read Surfaces

- Exact-resource replay uses `read_resource(...)` over compact resource mini-pages in [../../src/domains/stream/store.rs](../../src/domains/stream/store.rs).
- Area wildcard replay uses `read_area(...)` over compact area pages.
- Realm wildcard replay uses `read_realm(...)` over compressed compact realm pages.
- Admin metadata uses `list_resource_metadata(...)` over durable `0x08` resource metadata rows.
- Exact-resource `GetMetadata` and `Last` still reflect the exact stream only. Wildcard `Last` and wildcard `GetMetadata` currently return empty success payloads rather than wildcard aggregates.

Current production shape in one sentence:

`compact resource mini-pages + compact area pages + compressed compact realm pages + durable counters and watermarks`

## Legacy Detection And Operator Action

- Stored legacy markers fail fast with `ERR_STREAM_STORAGE_LAYOUT_MISMATCH`.
- Unmarked stream rows fail fast with `ERR_STREAM_STORAGE_LAYOUT_RESET_REQUIRED`.
- Operator action is explicit cutover or reset, not upgrade-in-place:
	1. Stop Fitz.
	2. Back up or delete the old stream family data.
	3. Restart into a clean promotion-frontier epoch.
	4. Re-seed or replay history from an upstream durable source if needed.

## Research-Only Keyspaces

The following prefixes still exist for redesign experiments and benchmark support, not for current production reads or writes:

| Prefix | Purpose |
| --- | --- |
| `0x0B` | Canonical resource body row for hydration experiments |
| `0x0C` | Area locator row |
| `0x0D` | Realm locator row |

Historical prototype evidence also lives in [../../benches/tier3_system_stream_storage_model.rs](../../benches/tier3_system_stream_storage_model.rs) and [../../benches/tier4_integration_stream_storage_model.rs](../../benches/tier4_integration_stream_storage_model.rs). Those benches are now research artifacts, not promotion gates for the shipped runtime.

## Current Promotion Evidence

Real acceptance now comes from the live Stream benches and contract suites:

- [../../benches/tier3_system_stream.rs](../../benches/tier3_system_stream.rs)
- [../../benches/tier4_integration_stream.rs](../../benches/tier4_integration_stream.rs)
- [../../config/perf_targets.json](../../config/perf_targets.json)
- [../../tests/stream_basics.rs](../../tests/stream_basics.rs)
- [../../tests/stream_advanced.rs](../../tests/stream_advanced.rs)
- [../../tests/stream_e2e.rs](../../tests/stream_e2e.rs)

If future storage redesign work resumes, it should start from this shipped baseline rather than treating promotion-frontier as a separate bench-only candidate.

## Practical Conclusion

What we ship today is a promotion-frontier-only Stream store with compact page surfaces for exact, area, and realm replay, durable counters and watermarks, and explicit operator-visible failure modes for old or unmarked data.

The remaining storage-model experiments are optional future work. They are no longer the story of how the live Stream domain currently behaves.