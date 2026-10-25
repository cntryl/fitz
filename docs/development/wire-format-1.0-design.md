# Wire Format 2 Design

Wire format 2 is the current protocol design. The normative client contract is
in [wire-routing.md](../clients/spec/wire-routing.md), the exact message IDs
are in [registry-acceptance.md](../clients/spec/registry-acceptance.md), and
error meanings are in [error-codes.md](../clients/spec/error-codes.md).

## Design decisions

- Each message has a fixed 15-byte header: `u16` type, `u8` flags, `u64`
  correlation, and `u32` value length, all big-endian.
- Each transport frame carries one message. TCP adds a four-byte big-endian
  frame length; WebSocket uses binary messages.
- `CONNECT` and `HELLO` negotiate the wire version, capabilities, and message
  limits. The server may answer with `CONNECT_REJECTED`; `GOAWAY` announces a
  drain, forced close, or limit change.
- Wire format 2 defines no capability bits. Message IDs and error codes are
  exact assignments; gaps are reserved.
- Errors carry a code, class, effect, and message. The effect distinguishes
  `NOT_APPLIED`, `APPLIED`, and `INDETERMINATE` outcomes.
- Domain payloads use explicit, length-checked fields. Their layouts and
  operation semantics are defined by the individual domain specifications.
- The default maximum message size is 1 MiB and is advertised in `HELLO`.
  Stream events have a separate 400 KiB body-plus-metadata limit.

## Evolution

Protocol changes must preserve existing meanings. Add a new message or error
assignment when the current contract cannot express the behavior. Do not reuse
an assigned identifier or introduce a capability dependency unless the broker
advertises that capability. Update the wire specification, domain contract,
golden vectors, and client conformance scenarios together.

The [migration guide](../operations/migration-guide.md) describes compatibility
and coordinated-upgrade requirements.
