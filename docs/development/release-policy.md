# Release Policy

Fitz release work is focused on explicit change communication and operator safety.

An immutable stable SemVer tag identifies a release artifact; it is not a
declaration of general availability and does not alter the product's release
stage or support commitments. Fitz is pre-GA and is already used in production
deployments.

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

## Wire format 2 release gate (0.3.0)

The 0.3.0 release is a flag day with no negotiation against wire format 1, so
policy item 4 applies in full. Before tagging:

1. Every row in the
   [0.3.0 breaking-change ledger](release-0.3.0-breaking-changes.md) is
   `landed` or explicitly deferred with a reason.
2. The wire golden vectors (`tests/fixtures/golden/wire2_vectors.txt`) are
   committed and the golden contract test is green.
3. The .NET, TypeScript, Go, Python, and Rust SDK 2.0 lines pass the
   cross-language conformance runner against the release candidate broker, and
   the released SDK versions are recorded in the release checklist.
4. The [migration guide](../operations/migration-guide.md) 0.3.0 section
   states the mixed-version prohibition and the upgrade and rollback order.
5. Storage rows in the ledger carry their own migration notes and rehearsed
   export or reset steps.

## Historical wire 1: Stream error envelope generation 2 release gate

This gate applies to pre-0.3.0 releases. The wire-2 gate above supersedes it.

For issue #238, record the released .NET, TypeScript, Go, Python, and Rust SDK
versions that decode status 2 before releasing the broker change. Verify legacy
status-1 decoding, APPEND and COMMIT code `2001` over a real broker, unrelated
wording with `2001`, misleading wording with another code, and backend `2012`.
Requalify both linked Portia assertions using the exact broker and client
artifacts, including original failure and pending-batch preservation through
cleanup failures. Follow the client-first upgrade and broker-first rollback in
[migration guidance](../operations/migration-guide.md).
