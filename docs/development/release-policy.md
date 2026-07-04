# Release Policy

Fitz release work is focused on explicit change communication and operator safety.

## Policy

1. Every release includes migration notes when compatibility risk exists.
2. Behavior changes affecting durability, routing, or auth require prominent documentation.
3. All production-impacting changes require rollback instructions.

## Release Gates

1. Core test suites pass.
2. Target benchmark checks pass for non-regression.
3. Required docs updates are merged.

Use [../operations/release-checklist.md](../operations/release-checklist.md) before final publish.
