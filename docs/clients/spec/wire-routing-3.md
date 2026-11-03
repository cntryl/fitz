# Wire Protocol 3

This is the normative Wire 3 contract. It keeps the fixed header, transport
framing, message IDs, correlation rules, and domain core payload layouts from
the released Wire 2 contract. Wire 2 remains documented in
[wire-routing.md](wire-routing.md); Wire 3 adds the extension envelope defined
here and in the [extension registry](extension-registry.md).

## Message Header and Frame Limits

Every frame carries one message with the existing 15-byte header:

```text
[u16 BE] message_type
[u8]     flags
[u64 BE] correlation
[u32 BE] value_length
[bytes]  value
```

Flag bit 0 is `ACK_REQUESTED`; bit 1 is `RESPONSE_FINAL` on correlated server
responses; bit 2 is `EXTENSIONS_PRESENT`. Bits 3–7 are reserved and MUST be
zero. The value length is the number of bytes after the header.

One binary WebSocket message or one TCP length-prefixed frame carries the
complete message. The configured `max_message_bytes` includes the 15-byte
header and every value byte, including the optional core-length prefix and all
extension entries. A truncated frame, a length mismatch, a reserved flag, or a
message exceeding the advertised limit is a protocol error.

When `EXTENSIONS_PRESENT` is clear, the value is the existing core payload
directly. This is the common domain-message path: it adds no payload bytes and
requires no extension scan. Existing domain payload field order and meanings
remain as defined in the [domain specifications](../client-spec.md).

When `EXTENSIONS_PRESENT` is set, the value is:

```text
[u32 BE core_length][core bytes][one or more extension entries]
```

`core_length` counts only the core bytes. Each extension entry is:

```text
[u16 BE field_id][u32 BE value_length][value bytes]
```

Field ID zero is reserved. The core and every entry must fit within the frame
value. An extension flag with no entries is malformed. Encoders write entries
in ascending field-ID order. Decoders accept entries in any order.

## Handshake and Capabilities

Wire 3 `CONNECT` and `HELLO` retain their Wire 2 core field layouts:

```text
CONNECT core:
  [u16 BE] min_version
  [u16 BE] max_version
  [u32 BE] client_caps       MUST be zero in Wire 3
  [string] jwt               compact JWT; empty in anonymous mode
  [string] service_name      display metadata; may be empty
  [string] client_build      SDK name/version; may be empty

HELLO core:
  [u16 BE] version           3
  [u32 BE] broker_caps       MUST be zero in Wire 3
  [u32 BE] max_message_bytes header and complete value included
  [u32 BE] max_in_flight
  [u64 BE] session_id        diagnostic only; not a credential
```

`string` is `[u32 BE byte_length][UTF-8 bytes]`. Their legacy `client_caps`
and `broker_caps` `u32` fields MUST be zero; Wire 3 capabilities use extension
field ID 1 instead. `CONNECT_REJECTED` and `GOAWAY` keep their existing core
layouts from the [released handshake contract](wire-routing.md).

Both handshakes MUST set `EXTENSIONS_PRESENT` and carry exactly one field ID 1
capability list, even when the list is empty. The field value is `[u16 BE
count][count × u32 BE capability_id]`. IDs are nonzero and strictly ascending.
An empty list is encoded as `[0x00 0x00]`. The capability registry currently
assigns no IDs, so this broker advertises the empty set.

The active capability set is the intersection of the client and broker sets.
Unknown numeric capability IDs are ignored. A request or server-initiated
message that depends on an optional capability MUST NOT be sent unless both
peers advertised that ID. The detailed handshake field registry and
assignment rules are in [extension-registry.md](extension-registry.md).

The broker accepts only a client version range containing Wire 3. An
unsupported range receives `CONNECT_REJECTED` with `VERSION_MISMATCH` (0050)
and closes the connection. The broker has no Wire 1 or Wire 2 fallback codec.
Clients send `CONNECT` first and wait for `HELLO` or `CONNECT_REJECTED` as
specified by the [connection lifecycle](wire-routing.md#connection-lifecycle).

## Extension Processing

Extension IDs are scoped by direction and message ID. A receiver validates the
core length and walks the extension entries once with overflow-safe bounds
checks before dispatching the core to a domain codec. Unknown values are
skipped without copying or retaining their bytes. Known singleton entries may
occur once. Repeated fields must be explicitly registered and preserve their
wire order; no repeatable field is currently assigned. Unknown entries are
ignored individually after bounds validation.

Malformed, truncated, overflowing, over-limit, or duplicate known singleton
fields close the connection before domain dispatch. They cannot partially
apply a domain operation. An unknown field is optional and cannot be required
for the meaning of an existing request.

New optional values MUST have a documented default. Additive fields do not
change the wire version. A change that requires new client behavior uses a
negotiated capability or a new message ID; it never changes an existing core
field or operation meaning.

## Compatibility and Migration

Wire 3 is the final planned compatibility cutover from the deployed Fitz
0.1.0 / Wire 1 client line. The published Wire 2 contract remains a separate,
unchanged historical contract; the Wire 3 broker does not serve either older
generation. Upgrade the broker and all five supported SDKs together, then roll
back both sides together if needed. Wire changes do not imply storage
migration. See the [migration guide](../../operations/migration-guide.md#wire-3-coordinated-cutover)
and [format compatibility ledger](../../development/format-compatibility.md).

## Performance Contract

Core-only messages add zero payload bytes and no extension-scan allocation.
Extension parsing is a bounded pass over borrowed frame bytes and does not
copy unknown values. Release qualification also measures throughput, p50/p99
latency, allocations, and bytes per frame on the broker and every SDK against
the Wire 2 baseline. It includes unknown-field and maximum-size scans and
requires zero added bytes and allocations on core-only messages, with no more
than a 5% throughput or p99 codec-latency regression on representative hot
paths.
