# Wire 3 Extension Registry

Extension field IDs are scoped by message direction and message ID. An ID is
assigned once, is never reused, and remains reserved after retirement. The
wire entry contains only the field ID, value length, and value; type and
meaning come from this registry.

Field ID zero is reserved. Encoders emit fields in ascending ID order;
decoders accept any order. A known singleton may occur once. Repeatable fields
must be declared as such and preserve their wire order. Unknown fields are
skipped after bounds validation and are not copied or retained.

## Assigned fields

| Direction | Message | Field ID | Name | Value | Presence and default | Repeatable | Capability dependency | Lifecycle |
|---|---|---:|---|---|---|---|---|---|
| Client → broker | CONNECT (1) | 1 | `capability_ids` | `[u16 count][count × u32 capability_id]` | Required; empty list means no client capabilities | No | None | Active |
| Broker → client | HELLO (2) | 1 | `capability_ids` | `[u16 count][count × u32 capability_id]` | Required; empty list means no broker capabilities | No | None | Active |
| Client → broker | ENQUEUE (200) | 1 | `ttl_hours` | Positive `[u32 BE]`, exactly 4 bytes | Optional; absent uses `FITZ_QUEUE_MESSAGE_TTL` | No | 1 | Active |

Capability IDs are nonzero and strictly ascending within the field. The active
set is the intersection of both peers' advertised sets. Unknown capability
IDs are ignored. The broker advertises IDs `1` and `2`. The legacy `client_caps` and `broker_caps` fields in
the handshake core remain zero and are not aliases for these lists.

## Assigned capabilities

| ID | Name | Required behavior |
|---:|---|---|
| 1 | `queue_message_ttl` | ENQUEUE may specify a positive TTL override in hours through field 1. Clients must fail clearly if requested TTL semantics cannot be negotiated. |
| 2 | `queue_nack` | Client may send NACK (210) to relinquish a current reservation with a visibility delay in seconds. |

TTL does not add fields to the ENQUEUE core or RESERVE response core. A
message without a TTL override needs no extension envelope. Broker defaults
apply independently of negotiated capabilities. Behavior tests live in
`tests/queue_delivery_controls.rs`, the Queue actor retention/NACK tests, and
the Wire 3 extension and session capability tests. Shared byte vectors are
in `tests/fixtures/golden/wire3_vectors.txt`.

## Future assignments

Every proposal must record direction, message ID, field ID, wire type, presence
and default behavior, repeatability, size limit, semantic meaning, capability
dependency, and lifecycle. It must also identify the behavior tests and shared
golden vectors that cover it. Changes to storage formats or stored-data
migrations are reviewed independently from this registry.
