# Fitz 0.4.0 Release Candidate

Fitz 0.4.0 coordinates the Wire 3 broker and all five client SDKs. Upgrade or roll back the broker and SDKs together; the broker does not serve Wire 1 or Wire 2 clients.

- Retains the fixed header, message IDs, correlation rules, and domain core payloads.
- Adds bounded extension envelopes and numeric capability negotiation.
- Supports per-message Queue TTL in hours (capability 1) and Queue NACK with a visibility delay in seconds (capability 2).
- Validates extended requests before buffer-overflow refusal and preserves RPC UUIDs in BUSY responses.
- Rejects malformed cancellation controls without consuming response capacity.

Queue persistence remains best effort. Reconnect establishes a new ephemeral session. Wire 3 does not change storage formats or imply replay, ownership continuity, or exactly-once delivery.

The broker and SDKs must pass their checks and shared conformance against the exact candidate image. Container publication is permitted for qualification; package publication and release tags remain a separate explicit operation. See [migration guidance](migration-guide.md#wire-3-coordinated-cutover) and the [Wire 3 contract](../clients/spec/wire-routing-3.md).
