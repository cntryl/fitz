# Release Policy

Release changes must be documented, validated on the exact candidate commit,
and recoverable according to their compatibility impact.

## Requirements

- Add migration instructions for wire, stored-data, and deployment-configuration
  changes.
- Document changes to domain guarantees, routing, authorization, or recovery.
- Require matching client support before releasing a breaking wire change.
- For the Wire 3 cutover, qualify the broker and all five supported SDKs as one
  release set from the deployed 0.1.0 / Wire 1 baseline; do not allow a mixed
  wire-generation fleet or claim release readiness from the server draft.
- Define rollback steps for stored-data changes and coordinated broker/client
  upgrades.
- Publish immutable versioned artifacts and verify them before announcement.

Use the [release checklist](../operations/release-checklist.md) for the release
sequence and [migration guide](../operations/migration-guide.md) for upgrade
details.
