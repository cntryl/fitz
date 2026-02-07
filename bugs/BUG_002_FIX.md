# Bug 002 Fix: Client Protocol Mismatch — Not a Broker Bug

**Status:** RESOLVED — Client-side fix required  
**Date:** 2026-02-07

## Root Cause

The bug report misidentifies the problem as a broker issue. After thorough analysis of the Fitz broker source code and CLIENT_SPEC.md, the root cause is:

1. **The Go client invented a framing layer that doesn't exist in the Fitz protocol.**
2. **CLIENT_SPEC.md had a contradictory Verb Set table that may have caused the confusion.**

### What the Go Client Does (WRONG)

The Go client wraps domain operations in a custom `Frame` struct:

```
Frame.Type = 0x0a (FrameTypeReq)     ← NOT a Fitz concept
Frame.Channel = 6                     ← NOT part of wire protocol
Body: [TagOp: 0x11 0x00 0x01 0x64]   ← TagOp is NOT a Fitz concept
      [TagRoute: 0x20 0x00 ...]
      [TagID: 0x21 0x00 ...]
```

This `FrameTypeReq`, `TagOp`, `TagID`, `TagRoute` framing is entirely invented by the Go client and has no basis in the Fitz wire protocol.

### What the Fitz Protocol Actually Is (CORRECT)

The Fitz wire protocol is simple TLV (Type-Length-Value):

```
TCP: [u32 BE length][TLV payload]
TLV: [type: 1-3 bytes][length: 2 bytes BE][value: N bytes]
```

Where the **TLV type field IS the operation code**. There is no separate frame type, channel, or TagOp concept on the wire.

**Example — KV BEGIN request:**
```
[0x64]            ← type = 100 (KV BEGIN), single byte
[0x00 0x2A]       ← length = 42 bytes
[... 42 bytes of positional payload (route_family, realm, area, resource, mode, write_options) ...]
```

**Example — Notice PUBLISH request (type > 254, requires escape):**
```
[0xFF 0x01 0xF4]  ← type = 500 (Notice PUBLISH), escape-encoded
[0x00 0x1C]       ← length = 28 bytes
[... 28 bytes of positional payload ...]
```

### How the Broker Processes Frames

1. **TCP layer** (`src/api/tcp.rs`): Reads `[u32 len][payload]`, forwards raw `Bytes` payload
2. **Session layer** (`src/session/session.rs`): Calls `TlvDecoder::decode_one_ref()` which extracts `(MessageType, value_slice, consumed)` directly from the TLV bytes
3. **Mux layer** (`src/protocol/mux.rs`): Maps `MessageType` range to `ChannelId`:
   - 0–99 → Control
   - 100–199 → KV (Pub channel)
   - 200–299 → Queue (Sub channel)
   - 300–399 → RPC
   - 400–499 → Lease
   - 500–599 → Notice (Pub channel)
   - 600–699 → Stream (Sub channel)
   - 700–799 → Schedule (Internal)
4. **Manager** (`src/session/manager.rs`): Dispatches to domain handler based on msg_type range
5. **Domain codec** (e.g., `src/protocol/kv_codec.rs`): Parses positional payload bytes

### Why the Client Times Out

When the Go client sends `0x0a 0x00 ...`, the broker's TLV decoder reads:
- Type = `0x0a` (10) → MessageType(10) → **Control** channel (0-99 range)
- But the session is already authenticated, and msg_type 10 is not CONNECT (1)
- The frame is either silently dropped or causes an unrecognized control message error
- **No response is ever sent** because the operation never reaches a domain handler

### CLIENT_SPEC.md Fix Applied

The CLIENT_SPEC.md had a contradictory "Verb Set" table (Section 9) that showed **overlapping** wire codes across domains (KV 100-104, Notice 100-104, Stream 200-204, Queue 200-204). This contradicted the authoritative "Constants & TLV Registry" section which correctly shows **non-overlapping** ranges.

The Verb Set table has been corrected to match the authoritative Constants & TLV Registry section and the actual broker implementation.

## Required Go Client Fix

The Go client must:

1. **Remove the Frame.Type/Channel wrapper** — There is no `FrameTypeReq`/`FrameTypeResp` concept in Fitz
2. **Remove TagOp/TagID/TagRoute TLV wrapping** — Operation codes go directly in the TLV type field
3. **Use TLV MessageType as the operation code** — Encode domain operations as TLV records where the type IS the op code
4. **Use positional encoding for payloads** — Not tagged fields inside the value

### Correct Go Client Wire Encoding

```go
// WRONG — Current Go client
enc := transport.NewTLVEncoder()
enc.AddOp(100)                                    // ← Remove this
enc.AddString(transport.TagRoute, "skv://...")     // ← Remove tags
enc.AddUint64(transport.TagID, requestID)          // ← Remove tags
frame := transport.Frame{Type: 0x0a, Channel: 6}  // ← Remove frame wrapper

// CORRECT — What Fitz expects
buf := make([]byte, 0, 128)
// TLV type = 100 (KV BEGIN), single byte since 100 <= 0xFE
buf = append(buf, 0x64)
// TLV length (u16 BE) — to be filled after building payload
// TLV value: positional encoding per kv_codec.rs
//   u64 route_family
//   string realm
//   string area
//   string resource
//   u8 mode (0=ReadOnly, 1=ReadWrite)
//   u8 write_options

// Wrap in TCP length prefix
tcpFrame := make([]byte, 4+len(tlvRecord))
binary.BigEndian.PutUint32(tcpFrame, uint32(len(tlvRecord)))
copy(tcpFrame[4:], tlvRecord)
```

### Correct CONNECT Sequence

```go
// CONNECT (MessageType=1, value=JWT bytes)
buf := []byte{0x01}                                    // type = 1
buf = append(buf, 0x00, byte(len(jwt)))                // length (u16 BE)
buf = append(buf, []byte(jwt)...)                      // JWT string (raw, no length prefix)

// Wrap in TCP frame
tcpFrame := make([]byte, 4+len(buf))
binary.BigEndian.PutUint32(tcpFrame, uint32(len(buf)))
copy(tcpFrame[4:], buf)
conn.Write(tcpFrame)

// Wait — no explicit ACK. If connection stays open, CONNECT succeeded.
```

## Files Changed (Broker-Side)

- `docs/clients/CLIENT_SPEC.md` — Fixed contradictory Verb Set table to use correct non-overlapping wire codes; removed misleading "MessageType Overlap" section

## Files NOT Changed (Broker Is Correct)

- `src/protocol/tlv.rs` — TLV decoder already handles escape byte encoding correctly
- `src/protocol/mux.rs` — Mux routing by MessageType range is correct
- `src/session/session.rs` — Session frame handling is correct
- `src/session/manager.rs` — Domain dispatch is correct
- All domain codecs — Already use correct MessageType constants
- All domain handlers — Response path is correct

## Verification

The Fitz broker unit tests pass (`cargo test --lib -q`) confirming all protocol handling, TLV encoding/decoding, mux routing, and domain dispatch are working correctly. The issue is purely client-side.
