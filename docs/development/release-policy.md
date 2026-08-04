# Release Policy

Fitz release work is focused on explicit change communication and operator safety.

## Policy

1. Every release includes migration notes when compatibility risk exists.
2. Behavior changes affecting durability, routing, or auth require prominent documentation.
3. Changes that affect stored data, wire behavior, or deployment configuration require rollback instructions.

## Release Gates

1. Core test suites pass.
2. Target benchmark checks pass for non-regression.
3. Required docs updates are merged.
4. Breaking wire releases include matching supported-client changes and a mixed-version prohibition when no negotiation exists.

Use [../operations/release-checklist.md](../operations/release-checklist.md) before final publish.
