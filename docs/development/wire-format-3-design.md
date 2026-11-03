# Wire Format 3 Design

Wire 3 is the planned final compatibility boundary for Fitz clients. The
normative contract is in [wire-routing-3.md](../clients/spec/wire-routing-3.md)
and the additive field assignments are in the
[extension registry](../clients/spec/extension-registry.md). The released Wire
2 contract remains in [wire-routing.md](../clients/spec/wire-routing.md) and
is not redefined by this design.

## Fixed Contract

- Retain the 15-byte big-endian header, one-message-per-frame transports,
  message IDs, correlation behavior, error codes, and existing domain core
  payloads.
- Assign flag bit 2 to `EXTENSIONS_PRESENT`; keep bits 0 and 1 unchanged and
  bits 3–7 reserved.
- With the flag clear, carry the core directly, adding no value bytes. With
  the flag set, carry `[u32 BE core_length][core][extension entries]`.
- Scope extension IDs by direction and message ID. Use explicit lengths,
  ascending field-ID order in encoders, any field order in decoders, and
  declared singleton/repeatable behavior.
- Skip unknown fields only after overflow-safe bounds validation and without
  copying their values. Validate the entire envelope before domain dispatch.
- Use numeric capability IDs in the extensible CONNECT and HELLO payloads.
  The two legacy `u32` capability fields remain zero. An empty set is an
  explicit two-byte zero count. The broker advertises capability IDs `1`
  (per-publish Queue TTL) and `2` (Queue NACK).
- Count the header, prefix, core, and extensions against `max_message_bytes`.
- Keep storage formats and stored-data migrations outside the wire change.

## Rollout Boundary

The deployed SDK cohort identified by issue #508 is Fitz 0.1.0 / Wire 1. Wire
2 is released, but is not the deployed client baseline for this coordinated
cutover. The migration is directly from that deployed 0.1.0 line to Wire 3;
there is no permanent Wire 1 or Wire 2 adapter and no rolling mixed-generation
fleet. An unsupported version receives structured non-retryable
`VERSION_MISMATCH`; broker and SDKs roll back together.

This server implementation is a draft of the broker side. It does not complete
the coordinated release: Rust, .NET, TypeScript, Go, and Python SDKs must all
adopt the same contract, run the shared vectors and conformance suite against
the exact broker candidate, and complete the performance comparison before
release. See the [release checklist](../operations/release-checklist.md).

## Qualification Requirements

The broker and each SDK must cover core-only and extension-bearing traffic,
unknown-field skipping, absent defaults, repeated fields, duplicate known
singletons, malformed and truncated lengths, overflow, size limits, and
capability-gated behavior. Shared vectors must include requests, responses,
notifications, handshake, errors, and extension cases.

On the same hardware, compare Wire 3 with the Wire 2 implementation for small
control frames, high-rate Notice/Queue/RPC messages, and larger KV/Stream
payloads. Record throughput, p50/p99 latency, allocation counts, and bytes per
frame. Require zero added payload bytes and allocations for core-only messages
and no more than a 5% throughput or p99 codec-latency regression on
representative hot paths. Include unknown-field and maximum-size scans.
