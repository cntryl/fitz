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
with the broker and there is no negotiation shim.

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
rejected and there is no negotiation shim. Existing persisted definition and
pending-claim versions decode as `broadcast`; new writes use versioned rows that
persist the mode, so no offline storage migration is required.

## Change Categories

- Additive: generally safe with version negotiation or default handling.
- Behavioral: requires explicit release note and client guidance.
- Breaking: requires migration procedure and rollback plan.

## Required Artifacts For Breaking Changes

1. Updated [../operations/migration-guide.md](../operations/migration-guide.md)
2. Updated [release-policy.md](release-policy.md)
3. Updated [../operations/release-checklist.md](../operations/release-checklist.md)
