# FTZ Frame Specification (header-v2 + TLV)

This document specifies the binary frame format used on the Fitz transports (FTZ frames). It is intended to be language-agnostic and detailed enough for implementers in other languages to parse and build frames interoperably.

## Overview

Each frame consists of a fixed-size header followed by a payload. The payload is a sequence of TLV (Tag-Length-Value) elements. The header (version 2) contains a channel identifier which allows multiple logical channels to be multiplexed over a single connection.

High-level framing rules:

- All integer fields are big-endian (network byte order).
- The wire format is binary; do not assume any text encoding except inside TLV values where a TLV may contain UTF-8 text (documented per-tag).
- Frames are self-delimiting: the header includes a payload length field so receivers can read exactly the payload bytes for a frame.
- TLV values are opaque byte arrays to the transport and server; higher-level semantics (JSON, protobuf, etc.) are contained inside the TLV value when applicable. Server implementations should avoid parsing application-level encodings unless explicitly required by the application logic.

## Frame Header (header-v2)

Total header length: 16 bytes.

Layout (bytes):

0..=3 : Magic / protocol identifier (4 bytes) — ASCII `FTZ\0` (0x46 0x54 0x5A 0x00)
4 : Version (1 byte). Current value: 0x02
5 : Frame type (1 byte). See Frame Types below.
6 : Flags (1 byte). Bitfield reserved for future use; currently 0x00.
7 : Reserved / padding (1 byte). Must be zero for forward compatibility.
8..=11 : Payload length (4 bytes, unsigned u32 BE) — length in bytes of the TLV payload that follows the header.
12..=15 : Channel ID (4 bytes, unsigned u32 BE) — logical channel identifier.

Field notes:

- Magic: allows quick rejection of non-FTZ frames. Implementations should verify the magic and version and reject frames that do not match expected values.
- Version: bump this when header layout changes incompatibly. Receivers SHOULD reject unknown versions unless a compatibility handling path is implemented.
- Payload length: a u32 allows up to 4 GiB payloads; implementations MAY impose lower practical limits (e.g., 16 MiB) and should document them.
- Channel ID: zero is reserved for the connection-control channel (control frames). Non-zero channel IDs are allocated by endpoints per the multiplexing protocol (client or server rules). Use u32 to allow many concurrent channels.

## Frame Types (normalized)

The protocol uses a small, orthogonal set of top-level frame types. Each is a single-byte code in the header (offset 5). The goal is to keep the framing simple and idiomatic across languages.

Recommended canonical set (byte codes chosen to be compact and backwards-friendly):

- 0x01: CONN_OPEN — open/establish a connection (HELLO/handshake). Use on channel 0.
- 0x02: CONN_CLOSE — orderly connection close. Use on channel 0.
- 0x03: CH_OPEN — open a logical channel (server or client may allocate channel ids separately).
- 0x04: CH_CLOSE — close a logical channel.
- 0x05: REG — register a subscription/handler on a channel. Semantics include notice/stream/rpc/queue registration; TLVs determine type and options.
- 0x06: REQ — request data (read/pull) or perform request semantics. Server streams responses (DAT) on same channel or per-REQ channel.
- 0x07: PUB — publish data (notice, rpc call, queue enqueue, or stream append) to a route; may contain `TAG_BATCH`/`TAG_SEQUENCE`.
- 0x08: DAT — data frame used to carry stream/queue entries or RPC responses. Supports `TAG_ENTRY` batching.
- 0x09: RELAY — relay/forward frame for bridging/proxying scenarios (contains a nested frame or route metadata).
- 0x0A: ACK — acknowledgement for durable delivery, sequence ack, or batch commit success.
- 0x0B: ERR — error frame. Contains `TAG_ERR_CODE` and `TAG_ERR_MSG` and optional context TLVs.
- 0x0C: BATCH_END — finalize/commit a batch identified by `TAG_BATCH`.

Design notes:

- `REG` replaces the earlier `SUB` framing for subscription/registration semantics and is intended to be flexible: clients register interest in notices, streams, RPC handlers, or queues using `REG` with appropriate TLVs (e.g., `TAG_SUB_OP`, `TAG_ROUTE`, `TAG_DURABLE`).
- `REQ` is the generic request/read frame: it can express read/pull semantics for streams or queue consumption and is distinct from `PUB` which is client-to-server publishing/enqueuing.
- `DAT` is the canonical server→client container for entries and responses. It supports batching via `TAG_ENTRY`.
- `RELAY` is optional — useful when building proxies or gateways. Its semantics are implementation-defined but should carry a nested frame payload or a route to forward to.
- `CONN_OPEN`/`CONN_CLOSE` are connection-level and should be used on channel 0; channel-level open/closes use `CH_OPEN`/`CH_CLOSE`.

Compatibility & mapping notes:

- The previous spec used `HELLO`, `AUTH`, `PUB`, `SUB`, `OPEN`, `CLOSE`, `ACK`, `ERR`. Map them as follows:

  - HELLO → CONN_OPEN
  - AUTH → part of CONN_OPEN or a subsequent REG/REQ as appropriate
  - PUB → PUB (0x07)
  - SUB → REG (0x05)
  - OPEN → CH_OPEN (0x03)
  - CLOSE → CH_CLOSE (0x04)
  - ACK → ACK (0x0A)
  - ERR → ERR (0x0B)

- Choose a single canonical mapping and update client/server implementations to the new codes; servers MAY accept legacy codes for backward compatibility by translating them to the canonical set.

## TLV Encoding (payload)

The payload is comprised of zero or more TLVs concatenated.

Each TLV element layout:

1 byte : Tag (u8)
4 bytes : Length (u32 BE) — number of bytes in Value
N bytes : Value (Length bytes)

Notes:

- Length is a 4-byte unsigned big-endian integer to allow large TLV bodies while keeping parsing simple.
- Tags are u8 values; the set of known tags is documented below. Unknown tags should be ignored by parsers unless the context requires strict validation.
- TLVs are ordered only when semantics require ordering; in general TLV ordering is not significant unless specified.

## Standard TLV Tags

This list includes commonly used tags. Implementations should tolerate unknown tags.

- 0x10: TAG_TOKEN — authentication token (UTF-8 text). Value: UTF-8 string of the token.
- 0x20: TAG_ROUTE — route/topic/topic-path (UTF-8 text). Value: UTF-8 string indicating the route.
- 0x21: TAG_ID — message identifier (UTF-8 text). Value: UTF-8 string representing a stable id (client-provided or server assigned).
- 0x22: TAG_BODY — message body (opaque bytes). Value: application payload (JSON, binary blob, etc.). Server storage treats this as opaque bytes.
- 0x30: TAG_WINDOW — flow-control window/credits (u32 BE). Value: 4-byte big-endian integer representing credits.
- 0x40: TAG_ERR_CODE — error numeric code (u32 BE). Value: 4-byte big-endian error code.
- 0x41: TAG_ERR_MSG — error text (UTF-8). Value: UTF-8 error message.
- 0x50: TAG_META — application metadata blob (opaque bytes).
- 0x50: TAG_META — application metadata blob (opaque bytes).
- 0x70: TAG_OFFSET — absolute offset/cursor for the message in the stream (u64 BE).
- 0x71: TAG_NOTIFY_META — optional small metadata in notifications (opaque bytes).
- 0x72: TAG_REQ_ID — request identifier (UTF-8 or u32 BE) used to correlate responses when multiplexing multiple REQs on a single channel.
- 0x73: TAG_OFFSETS — array of offsets (repeated u64 BE entries) — convenience for notifications that only need offsets.
- 0x74: TAG_SEQUENCE — sequence number for the stream entry (u64 BE). If present, the client may supply this when writing; otherwise the server assigns/infers a sequence when persisting the entry. Servers SHOULD echo the assigned sequence in DAT/ACK responses.
- 0x75: TAG_BATCH — batch identifier (UTF-8 or opaque bytes). When present on write entries, indicates the entry belongs to the named batch.
- 0x76: TAG_LEASE — lease/visibility extension (u32 BE seconds or optional alternative format). Used by queue consumers to request or convey visibility TTLs. Value format described below.
- 0x77: TAG_DELIVERY_TOKEN — opaque token used to correlate a specific delivery/lease instance for ack/extend operations.
- 0x20: TAG_ROUTE_REPLY — RPC reply route (UTF-8), where the worker should publish responses. Used in PUB frames for RPC requests and echoed in notifications.
- 0x24: TAG_SEQ — RPC streaming response sequence number (u32 BE), starting at 1 and incrementing per chunk. Included in DAT notifications to help clients order responses.
- 0x25: TAG_STREAM_END — RPC streaming end-of-stream marker (empty). Included on the final response message.
- 0x80: TAG_ENTRY — container TLV representing a single stream entry; value is itself a TLV-encoded sequence (ID, OFFSET, BODY, ROUTE, etc.). Multiple TAG_ENTRY TLVs may appear to represent N entries in a single DAT frame.
 - 0x90: TAG_SUBSCRIBE — used inside REG frames to request a subscription (notice/stream/etc. depending on context). Value: empty.
 - 0x91: TAG_UNSUBSCRIBE — used inside REG frames to request removal of a subscription. Value: empty.
 - 0x92: TAG_NOTIFICATION — marker TLV indicating a server->client notice contained within a DAT frame. Value: empty. When present, DAT SHOULD also include TAG_ROUTE and TAG_BODY.
  - RPC notifications: DAT frames for RPC workers/subscribers SHOULD include TAG_NOTIFICATION and carry TAG_ROUTE (request route), TAG_BODY (request body), TAG_ID (correlation id), and when present TAG_ROUTE_REPLY, TAG_SEQ and TAG_STREAM_END.

Tag semantics

- TAG_TOKEN (0x10): used in AUTH frames. Value is UTF-8. Servers validate tokens using configured auth backends.
- TAG_ROUTE (0x20), TAG_ID (0x21), TAG_BODY (0x22): used together in PUB frames. Route and ID are UTF-8 strings; BODY is opaque bytes.
- TAG_WINDOW (0x30): used for per-channel flow-control frames (ACK/OPEN/CREDIT adjustments).
- TAG*ERR*\* (0x40/0x41): used in ERR frames to convey error codes and human messages.

Optimistic concurrency (TAG_SEQUENCE)

When a client sets `TAG_SEQUENCE` (0x74) on an entry (for example inside a `TAG_ENTRY` when writing a stream), it signals an optimistic-concurrency intent: the client is requesting that its entry be persisted at the specified sequence number only if that sequence is available (no conflicting entry). Servers should implement the following recommended behavior:

- Sequence format: `TAG_SEQUENCE` is a u64 in big-endian representing the desired sequence number within the stream.
- If the client supplies `TAG_SEQUENCE`:
  - If `sequence == last_persisted_sequence + 1`: accept the entry and persist it at that sequence.
  - If `sequence <= last_persisted_sequence`:
    - If an entry already exists at `sequence` and the existing entry's ID and body are byte-for-byte identical to the incoming entry, treat the write as idempotent and respond with success (echo the sequence in DAT/ACK).
    - Otherwise, reject with a conflict error (see error codes below) and include the current sequence/offset in the ERR or in a supplemental TLV so the client can reconcile.
  - If `sequence > last_persisted_sequence + 1`: reject the write with an error (gap not allowed) unless the system explicitly supports sparse writes; rejecting prevents accidental gaps and simplifies ordering guarantees.
- If the client omits `TAG_SEQUENCE`, the server MUST assign `sequence = last_persisted_sequence + 1` when persisting and echo the assigned sequence back in the response (DAT/ACK) via `TAG_SEQUENCE`.

Error & response guidance

- On conflict, servers SHOULD return an `ERR` frame (frame_type=0x06) on the request channel and set `TAG_ERR_CODE` to `0x191` (401) or a more appropriate code (for example, `0x191` for auth, but for conflict use `0x19A` (410) or `0x193` (403) depending on your mapping). Recommendation: use `0x193` (403) for forbidden/permission, `0x194` (404) for not found, and define `0x19F` (415) or `0x18D` (397) for conflict — pick numbers and document them in your server's API. The important aspect is to include a human `TAG_ERR_MSG` and echo the current `TAG_SEQUENCE`/`TAG_OFFSET` so the client can retry correctly.
- On idempotent success (client re-sent the same entry), respond with the same DAT/ACK indicating the sequence and any other metadata.

Example

Server's last persisted sequence for `stream://realm/x` is 41.

Case A — client omits sequence:

- Client sends entry without `TAG_SEQUENCE`.
- Server assigns sequence 42, persists entry, and responds with DAT/ACK echoing `TAG_SEQUENCE=42`.

Case B — client supplies sequence 42 (optimistic append):

- If last_persisted_sequence == 41: server accepts, persists at 42, responds with `TAG_SEQUENCE=42`.
- If last_persisted_sequence >= 42 and existing entry matches incoming entry: treat as idempotent success and respond `TAG_SEQUENCE=42`.
- If last_persisted_sequence >= 42 and existing entry differs: respond with ERR (conflict) and include `TAG_SEQUENCE`/`TAG_OFFSET` of the conflicting entry.

Notes

- Choosing the exact numeric error codes is implementation-specific. The essential requirement: return an ERR with a clear numeric code and message, and include the current/authoritative sequence so the client can retry or reconcile.
- If your platform needs to support sparse client-assigned sequences (sequence > last+1), document that explicitly and define the semantics for gaps and how subscribers/consumers should interpret them.

## Visibility / Lease extension (queue)

Queue consumers often need more time to process a message after it is delivered. The protocol provides an explicit REQ/ACK flow to request a visibility/lease extension for an item. This section defines a small convention using an additional TLV, `TAG_LEASE` (0x76), and the `REQ` (0x06) / `ACK` (0x0A) frame pair.

Design principles

- Use `REQ` (0x06) from the consumer on the channel used for queue delivery to request an extension of the visibility timeout for a specific item.
- Server validates that the requester currently holds the lease (or has permission) and atomically updates the lease expiry.
- Server replies with `ACK` (0x0A) on the same channel, echoing the `TAG_REQ_ID` (if provided) and returning the new remaining TTL in `TAG_LEASE`.
- On failure the server replies with `ERR` (0x0B) including `TAG_ERR_CODE` / `TAG_ERR_MSG` and optionally the authoritative lease/offset information.

Tag semantics and formats

- `TAG_LEASE` (0x76): recommended default encoding is a 4-byte big-endian unsigned integer representing additional seconds to extend the visibility (relative extension). Example value: `0x00 00 00 1E` for 30 seconds.
- Alternative: servers may support an 8-byte u64 BE absolute expiry timestamp (epoch seconds or millis); if you support both, include a TLV sub-format version in `TAG_META` or define a small version byte in the value. Prefer the relative u32 form to avoid clock-skew semantics.

REQ usage (client -> server)

Required TLVs in the request (recommended):

- `TAG_REQ_ID` (0x72): client-chosen request id to correlate responses (optional but strongly recommended).
- `TAG_ROUTE` (0x20): the queue route/topic.
- `TAG_ID` (0x21): the message id to extend.
- `TAG_LEASE` (0x76): u32 BE seconds to extend the visibility.

Server response (success)

On success the server MUST send an `ACK` (0x0A) on the same channel with TLVs including:

- `TAG_REQ_ID` (echoed, if present)
- `TAG_ID` (echoed)
- `TAG_LEASE` (0x76) containing the new remaining TTL in seconds (u32 BE)
- optionally `TAG_META` with other metadata

Server response (error)

On failure the server SHOULD send an `ERR` (0x0B) on the request channel with TLVs including:

- `TAG_REQ_ID` (echoed, if present)
- `TAG_ERR_CODE` (0x40) — numeric error code (u32 BE)
- `TAG_ERR_MSG` (0x41) — human message explaining the rejection (UTF-8)
- Optionally include `TAG_LEASE` with the authoritative remaining TTL or `TAG_OFFSETS`/`TAG_SEQUENCE` to help the client reconcile state.

Example (extend by 30 seconds)

Client wants to extend `msg-007` on `queue/inbox` on channel 5 and sends a `REQ` with TLVs:

- `TAG_REQ_ID` (0x72) = "42"
- `TAG_ROUTE` (0x20) = "queue/inbox"
- `TAG_ID` (0x21) = "msg-007"
- `TAG_LEASE` (0x76) = 0x00 0x00 0x00 0x1E (30 seconds)

Payload TLV bytes (conceptual):

72 00 00 00 01 34
20 00 00 00 0B 71 75 65 75 65 2F 69 6E 62 6F 78
21 00 00 00 07 6D 73 67 2D 30 30 37
76 00 00 00 04 00 00 00 1E

Header example (REQ frame, channel 5):

46 54 5A 00 02 06 00 00 00 00 00 2B 00 00 00 05

Server ACK example (new TTL 60s):

ACK payload TLVs:
72 00 00 00 01 34
21 00 00 00 07 6D 73 67 2D 30 30 37
76 00 00 00 04 00 00 00 3C

Best-practices and notes

- Prefer relative extension (u32 seconds) to avoid clock-skew issues.
- Server MUST check ownership/reservation before extending; otherwise reject with `ERR`.
- Enforce server-side maximum extension and cumulative hold time to protect resources.
- Recommend clients include `TAG_REQ_ID` to enable idempotent retries and easy correlation.
- Use same channel for delivery and lease control for simple correlation; control channel (0) may be used for management, but server must then map consumer identity to reservation.

## Example Frame (PUB)

Client publishes a JSON body to route `auth/route` with id `3` on channel 1.

TLVs (conceptual):

- Tag=0x20, Length=10, Value=`auth/route` (10 bytes)
- Tag=0x21, Length=1, Value=`3`
- Tag=0x22, Length=17, Value=`{"msg":"withauth"}` (17 bytes)

Payload bytes (concatenate TLVs):

    [0x20][0x00 0x00 0x00 0x0A]['a' 'u' 't' 'h' '/' 'r' 'o' 'u' 't' 'e']
    [0x21][0x00 0x00 0x00 0x01]['3']
    [0x22][0x00 0x00 0x00 0x11]['{' '"' 'm' 's' 'g' '"' ':' '"' 'w' 'i' 't' 'h' 'a' 'u' 't' 'h' '"' '}']

Header (16 bytes):

    Magic: 0x46 0x54 0x5A 0x00 ('F' 'T' 'Z' '\0')
    Version: 0x02
    Frame type: 0x03 (PUB)
    Flags: 0x00
    Reserved: 0x00
    Payload len: 0x00 0x00 0x00 0x26 (38 decimal, total TLV payload bytes)
    Channel ID: 0x00 0x00 0x00 0x01 (1)

Full frame = header || payload (binary). When parsing, read 16 bytes then read payload_len bytes.

## Error Frame Example

A server replies with an ERR frame indicating not-authenticated (error code 401):

TLVs:

- Tag=0x40, Length=4, Value=0x00 0x00 0x01 0x91 (401 decimal)
- Tag=0x41, Length=15, Value=`not authenticated`

Header frame type: 0x06 (ERR), channel-id: 0 (control channel), payload_len accordingly.

## Parsing recommendations

1. Read the 16-byte header. Verify Magic == `FTZ\0` and Version == 0x02. If not, reject or close the connection.
2. Read the u32 payload length and ensure it is within allowed bounds (implementations may set a maximum). If payload_len is larger than allowed, close the connection with an error.
3. Read payload_len bytes into a buffer.
4. Parse TLVs from the buffer in a loop:
   - While bytes remain:
     - Read 1 byte tag (u8).
     - Read 4 bytes length (u32 BE).
     - If length > remaining bytes → protocol error.
     - Read `length` bytes as value and dispatch/store as required.

Be robust: ignore unknown tags (skip their value) unless the protocol context requires strict validation.

## Building frames (recommendations and pseudocode)

Building a frame should follow these steps:

1. Build TLV payload into a temporary byte buffer.
   - For each TLV to append:
     - Append tag (1 byte)
     - Append length (4 bytes, BE)
     - Append value bytes
2. Compute payload_len = payload_buffer.len()
3. Build header (16 bytes) with Magic, Version=0x02, FrameType, Flags, Reserved=0x00, payload_len (u32 BE), channel_id (u32 BE).
4. Concatenate header || payload and send as a single binary message over the transport.

Pseudocode (language-agnostic):

function build_tlv(tag: u8, value: bytes) -> bytes:
out = []
out.append(tag)
out.append(u32_to_be_bytes(len(value)))
out.append(value)
return out

function build_frame(frame_type: u8, flags: u8, channel_id: u32, payload: bytes) -> bytes:
header = []
header.append([0x46, 0x54, 0x5A, 0x00]) // 'FTZ\0'
header.append(0x02) // version
header.append(frame_type)
header.append(flags)
header.append(0x00) // reserved
header.append(u32_to_be_bytes(len(payload)))
header.append(u32_to_be_bytes(channel_id))
return header + payload

Note: The existing repository's Rust implementation uses the same order and BE encodings; follow the same mapping when implementing in other languages.

## Limits and Best Practices

- Implement a maximum payload limit (for example, 16 MiB) to protect against memory exhaustion. If payload_len exceeds the limit, close the connection with an ERR frame or close the socket.
- Validate TLV lengths while parsing to avoid integer overflows and ensure the parser does not read beyond buffer bounds.
- Treat channel 0 as the control/management channel. Use non-zero channel ids for logical data channels.
- Preserve TLV ordering if your higher-level semantics expect order; otherwise treat TLVs as an unordered set.
- For flow-control, implement TAG_WINDOW (0x30) TLVs carrying a u32 BE credit count and apply them per-channel.

## Test vectors

Include these hex-encoded examples to validate parsers.

PUB example from above (header + payload) as hex (spaces for readability):

Header (16 bytes):
46 54 5A 00 02 03 00 00 00 00 00 26 00 00 00 01

Payload TLVs (hex):
20 00 00 00 0A 61 75 74 68 2F 72 6F 75 74 65
21 00 00 00 01 33
22 00 00 00 11 7B 22 6D 73 67 22 3A 22 77 69 74 68 61 75 74 68 22 7D

Combined (concatenate header + payload):
46 54 5A 00 02 03 00 00 00 00 00 26 00 00 00 01 20 00 00 00 0A 61 75 74 68 2F 72 6F 75 74 65 21 00 00 00 01 33 22 00 00 00 11 7B 22 6D 73 67 22 3A 22 77 69 74 68 61 75 74 68 22 7D

ERR example (hex):

Header (16 bytes):
46 54 5A 00 02 06 00 00 00 00 00 13 00 00 00 00

Payload TLVs:
40 00 00 00 04 00 00 01 91
41 00 00 00 0F 6E 6F 74 20 61 75 74 68 65 6E 74 69 63 61 74 65 64

Combined:
46 54 5A 00 02 06 00 00 00 00 00 13 00 00 00 00 40 00 00 00 04 00 00 01 91 41 00 00 00 0F 6E 6F 74 20 61 75 74 68 65 6E 74 69 63 61 74 65 64

## Batching and representing N entries in a frame

When servers need to return multiple stream entries in a single frame (for efficiency or reduced per-message overhead) the DAT frame's payload may contain multiple logical entries. There are two supported approaches; the first is preferred because it is explicit and unambiguous:

1. Preferred: Use `TAG_ENTRY` (0x80) as a container TLV per entry.

   - Each `TAG_ENTRY`'s Value contains a TLV-encoded sequence for that single entry. Typical inner TLVs: `TAG_ROUTE` (0x20), `TAG_ID` (0x21), `TAG_OFFSET` (0x70), `TAG_BODY` (0x22).
   - Multiple `TAG_ENTRY` TLVs may be concatenated; their order represents the stream order within that frame.
   - This approach avoids ambiguity: each entry is self-contained and parsers need only iterate top-level TLVs and, when encountering `TAG_ENTRY`, parse its inner TLVs.

   Example structure:

   DAT payload = [TAG_ENTRY len=... [ TAG_OFFSET len=8 ... TAG_ID len=... TAG_BODY len=... ]] [TAG_ENTRY len=... [...]] ...

2. Compatibility / convenience: use `TAG_OFFSETS` (0x73) or repeated `TAG_OFFSET` TLVs for simple notification-like frames that only need offsets. This is less expressive because it doesn't pair offsets with bodies inside the same frame. Use it only when the frame contains only offsets (no bodies) or when client/server agree on positional correspondence between separate arrays (not recommended unless both sides are simple and fixed).

Recommendation: prefer `TAG_ENTRY` for DAT frames carrying one or more full entries. Use `TAG_OFFSETS` or repeated `TAG_OFFSET` only for notification/summary frames where bodies are intentionally omitted.

Parsing notes for `TAG_ENTRY`:

- When parsing the DAT payload, iterate TLVs top-level. On encountering a `TAG_ENTRY`, read its length L and parse L bytes as an inner TLV buffer using the same TLV parsing loop (tag, length, value). The inner TLVs are specific to that entry and may appear in any reasonable order (but include at least `TAG_OFFSET` or `TAG_ID`).

Binary example (conceptual) for DAT with two entries (high-level):

Header: frame_type=DAT (0x08), channel=M, payload_len = computed

Payload TLVs:
80 00 00 00 1A [ -- TAG_ENTRY length=26 bytes
70 00 00 00 08 00 00 00 00 00 00 00 2A -- TAG_OFFSET=42
21 00 00 00 02 34 32 -- TAG_ID='42'
22 00 00 00 06 01 02 03 04 05 06 -- TAG_BODY (6 bytes)
]
80 00 00 00 1B [ -- TAG_ENTRY length=27 bytes
70 00 00 00 08 00 00 00 00 00 00 00 2B -- TAG_OFFSET=43
21 00 00 00 02 34 33 -- TAG_ID='43'
22 00 00 00 07 11 12 13 14 15 16 17 -- TAG_BODY (7 bytes)
]

## Atomic batches

To support atomic batches of writes, clients can tag individual entries with `TAG_BATCH` (0x75). The server treats entries that share the same batch id as belonging to the same logical batch. The client then signals batch completion by sending a BATCH_END control frame which includes the same `TAG_BATCH` value. The server will commit all entries from that batch atomically (all become visible together) or abort the batch if validation fails.

Control framing options for BATCH_END:

- Option A (preferred): define a new control frame type `BATCH_END` (0x12) whose payload MUST include `TAG_BATCH` and optional summary TLVs (e.g., count, checksum). When server receives `BATCH_END`, it attempts to commit the batch.
- Option B: reuse `CLOSE` (0x11) or `ACK` (0x05) with `TAG_BATCH` present to indicate batch completion. This is less explicit but reduces frame types.

Recommended behavior:

1. Client streams a sequence of write entries (e.g., `TAG_ENTRY` TLVs or PUB frames) where each entry includes `TAG_BATCH` with the same batch id. Entries may include `TAG_SEQUENCE` if optimistic concurrency is desired.
2. Server buffers incoming batch entries (per-connection or persisted staging area) but does not make them visible to subscribers/reads until the batch is committed.
3. Client sends `BATCH_END` (frame_type=0x12) on the same channel or control channel including `TAG_BATCH` with the batch id. Optionally include a checksum TLV to validate integrity.
4. Server validates the batch (sequence continuity, conflicts, permissions, checksum). If validation passes, the server atomically commits all entries in the batch and responds with an ACK or DAT frames echoing committed sequences/offsets. If validation fails, server returns ERR referencing `TAG_BATCH` and may provide a reason TLV.
5. If the client disconnects before sending `BATCH_END` or the server times out waiting for `BATCH_END`, the server must either abort and discard the staged entries or persist them as an incomplete batch depending on policy. Servers SHOULD define a timeout and document the behavior.

Error handling and retries:

- On conflict detected during commit (e.g., a supplied `TAG_SEQUENCE` collides), server rejects the batch with ERR and includes an authoritative `TAG_SEQUENCE`/`TAG_OFFSET` sample for reconciliation.
- Clients may resubmit a batch with a new batch id if commit fails after resolving conflicts.

Example (client streams 3 entries, then ends batch):

- Client sends three PUB/DAT entries (or `TAG_ENTRY` inside frames) each with `TAG_BATCH='batch-xyz'`.
- Client sends BATCH_END (0x12) with payload: `TAG_BATCH='batch-xyz'`, `TAG_META='count=3'`, optional checksum.
- Server validates and commits the three entries atomically; server responds with ACK or DAT frames listing assigned offsets/sequences.

Notes:

- Servers SHOULD enforce maximum batch sizes and duration to prevent resource exhaustion.
- Implementations may support streaming commit acknowledgements (e.g., partial commit) but that breaks atomicity and should be avoided for strictly atomic semantics.

## Error handling

- If Magic or Version mismatch: close connection immediately.
- If payload length doesn't match TLV contents (e.g., TLV length exceeds remaining payload): close connection and/or send ERR.
- On unknown frame types or critical protocol violations, send ERR with TAG_ERR_CODE/TAG_ERR_MSG and close if unrecoverable.

## Appendix: Language-specific notes

- When implementing in languages where integers default to little-endian (e.g., x86 systems), ensure explicit big-endian conversions for all multi-byte integer fields.
- For languages without unsigned integer types (some older languages), treat u32 as non-negative numbers and use integer ranges big enough to hold 0..=2^32-1.
- When appending TLVs to a buffer, pre-allocate capacity when possible to avoid repeated reallocations for large payloads.

---

If you'd like, I can also add minimal example implementations in JavaScript (Node), Go, and C# to `docs/` showing parse/build functions for this spec.
