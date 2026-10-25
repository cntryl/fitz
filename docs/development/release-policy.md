# Release Policy

Release changes must be documented, validated on the exact candidate commit,
and recoverable according to their compatibility impact.

## Requirements

- Add migration instructions for wire, stored-data, and deployment-configuration
  changes.
- Document changes to domain guarantees, routing, authorization, or recovery.
- Require matching client support before releasing a breaking wire change.
- Define rollback steps for stored-data changes and coordinated broker/client
  upgrades.
- Publish immutable versioned artifacts and verify them before announcement.

Use the [release checklist](../operations/release-checklist.md) for the release
sequence and [migration guide](../operations/migration-guide.md) for upgrade
details.
