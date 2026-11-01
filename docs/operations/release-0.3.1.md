# Fitz 0.3.1 Release Notes

Fitz 0.3.1 is a maintenance release following the Wire 2 transition in 0.3.0.

## Changes

- Updates the embedded `cntryl-midge` dependency to 0.5.2.
- Updates Rust and UI tooling dependencies and uses Rust 1.99.1 for CI and
  container builds.
- Corrects Stream and RPC benchmark fixtures so the workloads send valid
  routes, append offsets, and request payloads.

## Compatibility

The 0.3.0 Wire 2 contract remains unchanged. Fitz 0.3.1 adds no Fitz domain
storage format change. Midge 0.5.2 retains the 0.5.0 and 0.5.1 persisted
formats; the existing Midge 0.5 migration instructions still apply to FORMAT 8
or earlier cloud metadata. Preserve a verified store or prefix before an
upgrade and use it with the matching broker for rollback unless the exact
target engine has been qualified against candidate-written data. See the
[migration guide](migration-guide.md) and
[format compatibility](../development/format-compatibility.md).
