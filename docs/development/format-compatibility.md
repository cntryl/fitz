# Format Compatibility

This page defines compatibility expectations for serialized data, protocol payloads, and storage formats.

## Compatibility Rules

1. Backward-incompatible wire changes require explicit release notes and migration guidance.

## Schedule delivery modes

Schedule CREATE and CREATE_BATCH now require a delivery-mode byte after the cron
string (`0` = `broadcast`, `1` = `single`), and LIST returns the byte in the
same position. This is a clean client wire break; older CREATE payloads are
rejected and there is no negotiation shim. Existing persisted definition and
pending-claim versions decode as `broadcast`; new writes use versioned rows that
persist the mode, so no offline storage migration is required.
2. Storage format changes require documented upgrade or rewrite path.
3. Message-type range assignments remain stable once released.

## Change Categories

- Additive: generally safe with version negotiation or default handling.
- Behavioral: requires explicit release note and client guidance.
- Breaking: requires migration procedure and rollback plan.

## Required Artifacts For Breaking Changes

1. Updated [../operations/migration-guide.md](../operations/migration-guide.md)
2. Updated [release-policy.md](release-policy.md)
3. Updated [../operations/release-checklist.md](../operations/release-checklist.md)
