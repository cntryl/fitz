# Format Compatibility

This page defines compatibility expectations for serialized data, protocol payloads, and storage formats.

## Compatibility Rules

1. Backward-incompatible wire changes require explicit release notes and migration guidance.
2. Storage format changes require documented upgrade or rewrite path.
3. Message-type range assignments remain stable once released.

## Routed Queue reserve and Stream read payloads

Queue RESERVE preserves the established route-less item encoding for concrete
requests. The new wildcard request form returns routed items, each beginning
with its matched concrete route; clients choose the response decoder from the
selector they sent. Stream READ is an intentional clean wire break: every read
item now begins with its matched concrete route, so Stream clients must upgrade
with the broker and there is no negotiation shim. That routed-item generation
remains the compatibility baseline: resource-, area-, and realm-scoped READ
responses retain their established record and cursor layouts, and LAST retains
its established record layout. The newly introduced global selector family
uses a selector-specific extended READ layout that adds `global_offset`, the
cursor integrity token, and the captured watermark. Clients select that decoder
only after sending a global-scope selector; the broker does not insert those
fields into existing READ or LAST responses.

Stream compact area, compact realm, and compressed compact realm pages also use
new required-route formats (`0xE5`, `0xB3`, and `0xE9`). The broker does not read
the older route-less page markers (`0xE4`, `0xB2`, and `0xE8`). The
promotion-frontier layout generation marker advances from `0xD1` to `0xD2`.
Activation rejects `0xD1` immediately with reset guidance; it does not scan,
hydrate, decode, or migrate V1 pages. Before upgrading a broker with existing
Stream data, operators must export/replay the source events into a fresh store
or intentionally clear and rebuild persisted Stream state. Rollback requires
restoring the pre-upgrade store snapshot together with the old broker.

## Schedule delivery modes

Schedule CREATE and CREATE_BATCH now require a delivery-mode byte after the cron
string (`0` = `broadcast`, `1` = `single`), and LIST returns the byte in the
same position. This is a clean client wire break; older CREATE payloads are
rejected and there is no negotiation shim. New writes use versioned rows that
persist the mode.

## Single-generation storage formats

Every domain store now reads exactly the one on-disk generation it writes.
All prior-generation readers have been removed, so a store written by an
earlier broker is not upgraded, migrated, or detected — it is simply not
readable. Upgrading in place requires starting from a fresh storage path.

- **Queue records**: messages are written as a versioned split header (`0x02`,
  79 bytes) plus a separate body row. The former embedded-header encoding and
  the separate legacy message-key family (`0x05`) are gone, along with their
  recovery scan. The queue meta row holds a single little-endian `u64`
  reserved-id value; the wider 57-byte meta encoding is no longer read.
- **Queue index meta**: only version `0x02` is accepted. Version `0x01` rows no
  longer decode to a recoverable state.
- **Schedule definitions**: only the metadata-plus-body row pair is read
  (definition `V3`, body `V2`, pending-fire `V3`). Inline definition rows and
  the `sched:m` / `sched:idx:` key families are neither read nor rejected —
  they are ignored, so schedules stored that way are lost.
- **Stream layout**: `promotion-frontier` is the only layout. The
  `legacy-covering` layout and the `0xD1` generation marker are no longer
  recognized.

Rollback requires restoring the pre-upgrade store snapshot together with the
old broker.

## Change Categories

- Additive: generally safe with version negotiation or default handling.
- Behavioral: requires explicit release note and client guidance.
- Breaking: requires migration procedure and rollback plan.

## Required Artifacts For Breaking Changes

1. Updated [../operations/migration-guide.md](../operations/migration-guide.md)
2. Updated [release-policy.md](release-policy.md)
3. Updated [../operations/release-checklist.md](../operations/release-checklist.md)
