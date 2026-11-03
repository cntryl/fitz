# Fitz 0.3.1 Release Notes

The Fitz 0.3.1 source includes the Wire 3 broker implementation following the
Wire 2 transition in 0.3.0. It requires a coordinated broker-and-SDK cutover;
it is not wire-compatible with 0.3.0.

## Changes

- Updates the embedded `cntryl-midge` dependency to 0.5.2.
- Updates Rust and UI tooling dependencies and uses Rust 1.99.1 for CI and
  container builds.
- Corrects Stream and RPC benchmark fixtures so the workloads send valid
  routes, append offsets, and request payloads.
- Implements Wire 3 extension envelopes and capability-ID negotiation, with
  Queue TTL overrides and NACK gated by negotiated capabilities.

## Compatibility

This broker accepts only Wire 3; it has no Wire 1 or Wire 2 fallback codec.
Do not infer SDK compatibility or release qualification from the package
version. Qualify all five SDKs with the exact broker candidate and upgrade or
roll back the broker and clients together. See the
[Wire 3 cutover](migration-guide.md#wire-3-coordinated-cutover).

Fitz 0.3.1 adds no Fitz domain storage format change. Midge 0.5.2 retains the
0.5.0 and 0.5.1 persisted formats; the existing Midge 0.5 migration instructions
still apply to FORMAT 8 or earlier cloud metadata. Preserve a verified store or prefix before an
upgrade and use it with the matching broker for rollback unless the exact
target engine has been qualified against candidate-written data. See the
[migration guide](migration-guide.md) and
[format compatibility](../development/format-compatibility.md).
