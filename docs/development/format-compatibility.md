# Format Compatibility

This page defines compatibility expectations for serialized data, protocol payloads, and storage formats.

## Compatibility Rules

1. Backward-incompatible wire changes require explicit release notes and migration guidance.
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
