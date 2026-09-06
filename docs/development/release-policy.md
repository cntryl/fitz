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

## Stream error envelope generation 2 release gate

For issue #238, record the released .NET, TypeScript, Go, Python, and Rust SDK
versions that decode status 2 before releasing the broker change. Verify legacy
status-1 decoding, APPEND and COMMIT code `2001` over a real broker, unrelated
wording with `2001`, misleading wording with another code, and backend `2012`.
Requalify both linked Portia assertions using the exact broker and client
artifacts, including original failure and pending-batch preservation through
cleanup failures. Follow the client-first upgrade and broker-first rollback in
[migration guidance](../operations/migration-guide.md).
