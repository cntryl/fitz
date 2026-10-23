## Wire Protocol

### Message Header

Every Fitz message is one transport frame (one WebSocket binary message, or
one `u32 BE` length-prefixed TCP frame) carrying a fixed 15-byte header
followed by exactly `length` bytes of value:

```
[u16 BE] type          verb or control identifier (see registry-acceptance.md)
[u8]     flags         bit 0 ACK_REQUESTED; bits 1..7 reserved, MUST be 0
[u64 BE] correlation   non-zero on requests and their responses; 0 on notifications and control
[u32 BE] length        byte count of value
[bytes]  value         domain or control fields, concatenated, exactly `length` bytes
```

A frame carries exactly one message. There is no record list, no variable-width
type, and no 64 KiB value cap: `length` is bounded only by the
`max_message_bytes` the broker states in `HELLO`. A frame whose header does
not match the bytes present, or a message longer than `max_message_bytes`, is
a protocol error and the broker closes the connection.

### Request Correlation

Every request carries a client-generated, non-zero `u64` correlation in the
header. The broker echoes it unchanged on every frame that answers the request,
including errors the broker synthesizes itself (busy, timeout, unavailable) and
answers deferred arbitrarily long (a parked Queue `RESERVE`, a contended Lease
`ACQUIRE` granted when the holder releases). Clients match responses by
correlation, never by arrival order or by message type.

**Client requirements**

1. Correlation MUST be unique among the requests a connection currently has in
   flight. Reusing a live identifier yields undefined response routing; the
   broker does not police it.
2. Zero is reserved for frames that answer nothing. The broker still answers a
   request sent with correlation 0, echoing 0, but the client then cannot tell
   that answer from any other zero-correlated frame, so conformant clients
   never send it.
3. **A correlation may receive more than one frame.** Most requests are
   answered once, but a domain may define a multi-phase answer, and every
   phase carries the same identifier. A contended Lease `ACQUIRE` is answered
   immediately with `Queued` and again later with the grant. A client MUST
   route a correlated frame whose identifier is no longer pending by message
   type instead of discarding it; dropping it loses the deferred grant.
4. **Notifications are never correlated.** `NOTIFY` frames carry correlation 0
   and answer no request. Clients MUST route them by message type and
   subscription id, and MUST NOT treat a non-zero correlation as a
   response-vs-notification discriminator.
5. RPC `REQUEST` (302) and `RESPONSE` (303) keep their 16-byte UUID inside the
   payload. That UUID is an end-to-end application identifier surviving
   broker, worker, and broker again; `RESPONSE` frames are delivered with
   correlation 0 and are matched by the UUID alone.

### Flags

| Bit | Name | Meaning |
|---:|---|---|
| 0 | `ACK_REQUESTED` | On Notice `PUBLISH`: the broker answers with the common response envelope. Success means accepted for fan-out and never means delivered. Ignored on every other message type. |
| 1–7 | reserved | MUST be 0. A later version assigns one only behind a capability bit. |

### Capabilities

`CONNECT` carries the capability bits the client implements and `HELLO` the
bits the broker implements. The set in force is the bitwise AND. No bits are
defined in wire version 2: `broker_caps` is 0 and a client sends
`client_caps` 0. Optional behavior added after 0.3.0 arrives as a capability
bit that changes what a value may contain, never the byte layout, so an
unknown bit is ignored rather than rejected.

#### Message Structure

```
[type (u16 BE)][flags (u8)][correlation (u64 BE)][length (u32 BE)][Value (length bytes)]
│              │           │                      │                 │
│              │           │                      │                 └─ Domain fields (concatenated)
│              │           │                      └─ Byte count of value
│              │           └─ Client-chosen, non-zero on requests
│              └─ 0 unless ACK_REQUESTED
└─ Verb wire code
```

#### Complete Message Examples

**Example 1: KV PUT (MessageType=104)**

Wire format specification:
```
[u64 BE]   tx_id
[u32 BE]   route_len
[bytes]    route
[u32 BE]   key_len
[bytes]    key
[u32 BE]   value_len
[bytes]    value
```

Actual bytes on wire:
```
[0x00 0x68]                              (type=104, KV PUT)
[0x00]                                   (flags=0)
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  (correlation=1)
[0x00 0x00 0x00 0x39]                    (length=57 bytes)
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  (tx_id=1, u64 BE)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [6b 76 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 75 73 65 72 73]  (route="kv://prod/app/users", 21 bytes)
  [0x00 0x00 0x00 0x03]                  (key_len=3)
  [62 6f 62]                             (key="bob", 3 bytes)
  [0x00 0x00 0x00 0x05]                  (value_len=5)
  [61 6c 69 63 65]                       (value="alice", 5 bytes)

Total frame size: 15 (header) + 57 (payload) = 72 bytes
```

**Example 2: Notice SUBSCRIBE (MessageType=501)**

Wire format specification:
```
[u32 BE]   route_pattern_len
[bytes]    route_pattern
```

Actual bytes on wire:
```
[0x01 0xF5]                              (type=501, Notice SUBSCRIBE)
[0x00]                                   (flags=0)
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x02]  (correlation=2)
[0x00 0x00 0x00 0x18]                    (length=24 bytes)
  [0x00 0x00 0x00 0x14]                  (route_pattern_len=20)
  [6e 6f 74 69 63 65 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 2a]  (pattern="notice://prod/app/*", 20 bytes)

Total frame size: 15 (header) + 24 (payload) = 39 bytes
```

**Example 3: KV BEGIN (MessageType=100)**

BEGIN selects scope and transaction mode only:
```
[u32 BE]  route_len
[bytes]   route
[u8]      mode (0=ReadOnly, 1=ReadWrite)
```

For `kv://prod/app/users` (19 UTF-8 bytes), the message is:
```
[0x00 0x64]                              (type=100, KV BEGIN)
[0x00]                                   (flags=0)
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x03]  (correlation=3)
[0x00 0x00 0x00 0x18]                    (length=24 bytes)
  [0x00 0x00 0x00 0x13]                   (route_len=19)
  [6b 76 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 75 73 65 72 73]
  [0x01]                                 (ReadWrite)
```

Total frame size: 15 (header) + 24 (payload) = 39 bytes.
KV COMMIT requires `[u64 tx_id][u32 route_len][route][u8 persistence]`.
Stream COMMIT requires `[u64 session_id][u8 persistence]`. Both use
`0=Buffered` and `1=Sync`; omitted, invalid, or trailing fields are rejected.

**Example 4: RPC REQUEST (MessageType=302)**

Wire format specification:
```
[16 bytes] correlation_id (UUID)
[u32 BE]   route_len
[bytes]    route
[u32 BE]   body_len
[bytes]    body
```

Actual bytes on wire:
```
[0x01 0x2E]                              (type=302, RPC REQUEST)
[0x00]                                   (flags=0)
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x04]  (correlation=4)
[0x00 0x00 0x00 0x30]                    (length=48 bytes)
  [12 34 56 78 9a bc de f0 12 34 56 78 9a bc de f0]  (correlation_id, 16 bytes UUID)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [72 70 63 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 77 6f 72 6b 65 72]  (route="rpc://prod/app/worker", 21 bytes)
  [0x00 0x00 0x00 0x03]                  (body_len=3)
  [66 6f 6f]                             (body="foo", 3 bytes)

Total frame size: 15 (header) + 48 (payload) = 63 bytes
```

#### Transport Layer Framing

**WebSocket:**
```
One binary WebSocket frame contains one message:
[header 15 bytes][value]

Example: Send KV PUT
WebSocket binary frame body = 72 bytes (from Example 1 above)
```

**TCP (with length-prefixed framing):**
```
[frame_length (u32 BE)][header 15 bytes][value]
│                       │
│                       └─ one message
└─ header + value byte count (excludes the 4-byte prefix)

Example: Send KV PUT (72-byte message)
[0x00 0x00 0x00 0x48]  (frame_length=72)
[0x00 0x68]            (type=104)
[0x00]                 (flags=0)
[...8 bytes correlation...]
[0x00 0x00 0x00 0x39]  (length=57)
[...57 bytes payload...]
```

#### Reference Decoder Pseudocode

```python
def decode_frame(frame_bytes):
    """Decode one frame into a domain message."""
    # Parse the fixed 15-byte header
    message_type = read_u16_be(frame_bytes[0:2])
    flags = frame_bytes[2]
    correlation = read_u64_be(frame_bytes[3:11])
    length = read_u32_be(frame_bytes[11:15])
    payload = frame_bytes[15:]

    if flags & 0xFE:
        raise ProtocolError("Reserved flag bit set")
    if len(payload) != length:
        raise ProtocolError("Payload length mismatch")
    
    # Route to domain decoder based on MessageType
    if 100 <= message_type <= 199:
        return decode_kv_message(message_type, payload)
    elif 200 <= message_type <= 299:
        return decode_queue_message(message_type, payload)
    elif 300 <= message_type <= 399:
        return decode_rpc_message(message_type, payload)
    elif 400 <= message_type <= 499:
        return decode_lease_message(message_type, payload)
    elif 500 <= message_type <= 599:
        return decode_notice_message(message_type, payload)
    elif 600 <= message_type <= 699:
        return decode_stream_message(message_type, payload)
    elif 700 <= message_type <= 799:
        return decode_schedule_message(message_type, payload)
    else:
        raise UnknownMessageType(message_type)

def decode_kv_message(message_type, payload):
    """Decode KV domain message based on MessageType."""
    offset = 0
    
    if message_type == 100:  # BEGIN
        route_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        route = payload[offset:offset+route_len].decode('utf-8')
        offset += route_len
        mode = payload[offset]
        offset += 1
        if mode not in (0, 1):
            raise ProtocolError("Invalid transaction mode")
        
        if offset != len(payload):
            raise ProtocolError("Trailing BEGIN data")
        return KvBegin(route=route, mode=mode)
    
    elif message_type == 104:  # PUT
        tx_id = read_u64_be(payload[offset:offset+8])
        offset += 8
        
        route_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        route = payload[offset:offset+route_len].decode('utf-8')
        offset += route_len
        
        key_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        key = payload[offset:offset+key_len]
        offset += key_len
        
        value_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        value = payload[offset:offset+value_len]
        offset += value_len
        
        # Verify all payload consumed
        if offset != len(payload):
            raise ProtocolError("Trailing data in PUT payload")
        
        return KvPut(tx_id=tx_id, route=route, key=key, value=value)
    
    # ... other KV verbs

def decode_notice_message(message_type, payload):
    """Decode Notice domain message based on MessageType."""
    offset = 0
    
    if message_type == 501:  # SUBSCRIBE
        pattern_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        pattern = payload[offset:offset+pattern_len].decode('utf-8')
        offset += pattern_len
        
        if offset != len(payload):
            raise ProtocolError("Trailing data in SUBSCRIBE payload")
        
        return NoticeSubscribe(pattern=pattern)
    
    # ... other Notice verbs
```

**Key Insights for Implementers:**

1. **One message per frame:** the 15-byte header is followed by the whole value; nothing else is in the frame
2. **type = Verb:** the header `type` field IS the verb wire code (100-799) or a control id (1-4)
3. **Payload = Concatenated Fields:** Just concatenate domain fields in order (no internal record structure)
4. **Length Validation:** Always verify `offset == len(payload)` after decoding (detects trailing data)
5. **Transport Agnostic:** Same header and value for WebSocket and TCP (TCP adds the outer length prefix)

### Primitive Encodings

All fields use **big-endian** byte order.
| Type | Encoding |
| ------------- | ------------------------------- |
| `u8` | single byte |
| `u16` | 2 bytes, big-endian |
| `u32` | 4 bytes, big-endian |
| `u64` | 8 bytes, big-endian |
| `String` | `[u32 BE len][UTF-8 bytes]` |
| `Bytes` | `[u32 BE len][raw bytes]` |
| `Optional<T>` | `[u8 present]` + T if present=1 |
| `UUID` | 16 raw bytes (no hyphens) |

### Encoding Invariants

Clients MUST:

1. Encode all integers in big-endian byte order
2. Consume all bytes in request payloads; error if trailing data remains
3. Encode responses with exact length prefixes
4. Encode `type` as a plain `u16 BE`; there is no variable-width type encoding
5. Send exactly one message per transport frame, with reserved flag bits zero, and reject a frame whose `length` does not match the bytes present
6. Bound every message by the `max_message_bytes` the broker states in `HELLO`. Larger payloads are split across operations (Stream APPEND records, Queue messages, RPC response chunks), never across frames

### Response Format Convention

**All domain responses follow this standard structure:**

1. **Status byte** (u8): 0=success, 1=error
2. **If success (status=0):** Domain-specific success payload
3. **If error (status=1):** the common error envelope
   ```
   [u16 BE] code       the specific condition, see error-codes.md
   [u8]     class      0 domain-specific, 1..10 the shared condition
   [u8]     effect     0 NOT_APPLIED, 1 INDETERMINATE, 2 APPLIED
   [u32 BE] error_len
   [bytes]  error_msg (UTF-8, diagnostic only; clients MUST NOT branch on it)
   ```

**Clients MUST check status byte first before parsing payload.** Retry only
when `effect` is NOT_APPLIED, or when the operation is idempotent by the
domain's own rules.

**Exception: RPC Domain**
RPC responses include a `correlation_id` field (16-byte UUID) to match responses to requests across multiple in-flight operations. See [RPC Domain](queue-rpc.md) for details on how RPC enables per-request correlation.

**Example (KV GET success):**
```
[0x00]                    (status=0, success)
[0x00 0x00 0x00 0x05]     (value_len=5)
[0x61 0x6c 0x69 0x63 0x65] ("alice")
```

**Example (KV GET error):**
```
[0x01]                    (status=1, error)
[0x04 0x1c]               (code=1052, KEY_NOT_FOUND)
[0x09]                    (class=9, NOT_FOUND)
[0x00]                    (effect=0, NOT_APPLIED)
[0x00 0x00 0x00 0x0d]     (error_len=13)
[0x4b 0x65 0x79...]       ("Key not found")
```

**Rationale:** Standardized error format across all domains simplifies client
error handling and ensures consistent debugging experience. The header
correlation provides per-request matching for every request/response verb.
RPC additionally retains its own 16-byte domain identifier for end-to-end
caller/worker routing.

### Message Size Limits

**Default maximum message size: 1 MiB (1,048,576 bytes), stated in `HELLO`**

**Rules:**
- The broker states `max_message_bytes` in `HELLO`; it bounds header plus value
- Client MUST NOT send a message larger than `max_message_bytes`
- Broker closes the connection on a larger message (transport error)
- Deployments set the limit with `FITZ_MAX_MESSAGE_BYTES`; clients read it from `HELLO` rather than assuming it
- Clients SHOULD still expose a local ceiling so an application cannot build a message the broker will refuse

**Handling large payloads:**
- Split across multiple operations (Stream APPEND records, Queue messages)
- RPC responses stream in chunks
- Application-level chunking (not protocol-level)
- For Queue/Notice/RPC: keep the payload under the limit or use external blob storage with a reference

## Connection Lifecycle

### 1. Open Transport

- **WebSocket:** `wss://broker:port/` (TLS recommended)
- **TCP:** `tcp://broker:port` (TLS recommended)
- Broker address and credentials must be configured before opening

### Client State Machine

Clients SHOULD implement a simple connection state machine to keep behavior predictable and testable.
States:

- DISCONNECTED → CONNECTING → AUTHENTICATED → CLOSED
  Transitions:
- DISCONNECTED: initial state
- CONNECTING: transport open; send CONNECT
- AUTHENTICATED: CONNECT accepted (no close); ready for domain requests
- CLOSED: transport closed or unrecoverable error
  ASCII diagram:

```
DISCONNECTED --(open transport)--> CONNECTING --(CONNECT & accepted)--> AUTHENTICATED
     ^                                            |
     |                                            v
     +---------(close / unrecoverable)----------- CLOSED
```

Notes:

- Clients MUST handle transport failures and implement exponential backoff on reconnect.
- **Multiplexing Support**: Clients MAY keep any number of requests in flight,
  of any message types, each with a unique non-zero correlation. `HELLO`
  states `max_in_flight`, the admission limit beyond which the broker answers
  `BUSY` (class 5, effect NOT_APPLIED).

### 2. Send CONNECT (FIRST MESSAGE)

Clients MUST send `CONNECT` (type 1, flags 0, correlation 0) as the first
message:

```
[u16 BE] min_version      lowest wire version the client speaks (2)
[u16 BE] max_version      highest wire version the client speaks (2)
[u32 BE] client_caps      capability bits the client implements (0)
[string] jwt              compact JWT; empty in anonymous mode
[string] service_name     client-reported service name, may be empty
[string] client_build     SDK name and version for diagnostics, may be empty
```

`string` is `[u32 BE len][UTF-8 bytes]`. `service_name` is display metadata
shown in admin surfaces, never identity; clients SHOULD send it trimmed, at
most 128 bytes, with no control characters.

**Example (Anonymous Mode, no service name):**

```
[0x00 0x01]                              (type=1, CONNECT)
[0x00]                                   (flags=0)
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00]  (correlation=0)
[0x00 0x00 0x00 0x14]                    (length=20)
  [0x00 0x02]                            (min_version=2)
  [0x00 0x02]                            (max_version=2)
  [0x00 0x00 0x00 0x00]                  (client_caps=0)
  [0x00 0x00 0x00 0x00]                  (jwt: empty)
  [0x00 0x00 0x00 0x00]                  (service_name: empty)
  [0x00 0x00 0x00 0x00]                  (client_build: empty)
```

**Constraints:**

- CONNECT MUST be the first message; a domain message before it closes the connection
- **Authenticated mode (`FITZ_AUTH_REQUIRED=true`):** JWT required
- **Anonymous mode (`FITZ_AUTH_REQUIRED=false`):** JWT may be empty
- Clients SHOULD implement a CONNECT timeout (5–10 seconds) awaiting the answer

### 3. Await HELLO or CONNECT_REJECTED

The broker answers every `CONNECT` with exactly one of:

- **`HELLO`** (type 2, correlation 0): the session is established.
  ```
  [u16 BE] version            the wire version in force (2)
  [u32 BE] broker_caps        capability bits the broker implements
  [u32 BE] max_message_bytes  largest message the broker accepts, header included
  [u32 BE] max_in_flight      requests the broker admits before answering BUSY
  [u64 BE] session_id         diagnostic identity shown in admin surfaces
  ```
  Readers MUST ignore trailing bytes after `session_id`, so a later version
  can append limits without a new message type. `HELLO` is the only message
  with that rule. The version in force is the highest value in both ranges.
- **`CONNECT_REJECTED`** (type 3, correlation 0), after which the broker closes:
  ```
  [u16 BE] code     protocol block of error-codes.md: 0001 UNAUTHORIZED,
                    0050 VERSION_MISMATCH, 0051 FAMILY_NOT_PROVISIONED
  [string] message
  ```
- **No `CONNECT` within 10 seconds:** the broker closes the connection.

Clients MUST:

- Read `HELLO` before relying on `max_message_bytes` or `max_in_flight`. A
  client MAY pipeline domain requests behind `CONNECT`; the broker serves
  them only after a successful `CONNECT`, and discards them with the
  connection on `CONNECT_REJECTED`
- Surface `CONNECT_REJECTED` with its code and message
- Do NOT retry with the same JWT after `0001`
- Treat a close with neither `HELLO` nor `CONNECT_REJECTED` as a transport
  failure: reconnect with backoff

**Recommended client pattern:**

```
1. Open transport (WebSocket/TCP)
2. Send CONNECT
3. Read the first message:
   HELLO            → record version, caps, limits, session_id; the session is live
   CONNECT_REJECTED → surface code and message; do not retry the same JWT on 0001
4. Send domain requests, each with a unique non-zero correlation
5. If neither answer arrives within the CONNECT timeout → close, retry with backoff
```
  **Session State After Successful CONNECT:**
  On successful CONNECT, broker creates session and MUST:
- Assign a unique session ID (returned in `HELLO` for diagnostics; not a credential)
- Extract JWT identity context and resolve route family through server configuration
- Establish normalized route-shaped permissions for all subsequent requests
- Track active subscriptions, transactions, and resources
  **Session Cleanup On Disconnect:**
  When client disconnects:
- All active subscriptions are dropped
- All active transactions (KV) are rolled back
- All active stream sessions are aborted
- All held leases are released
- All RPC worker registrations are cleared
- All pending RPC requests are discarded
- Queued notifications are discarded
  **State NOT Restored On Reconnect:**
  On reconnect with new CONNECT:
- New session ID issued (previous session ID is invalid)
- Previous subscriptions, transactions, and worker registrations are NOT recovered
- Previous pending RPC requests are NOT durably recovered or replayed
- Client MUST explicitly re-subscribe, re-begin, or re-register if needed

### 4. Send Domain Requests

After successful CONNECT, client may send domain-specific requests.

**Request Multiplexing:**

- **Any mix of message types**: every request carries a unique non-zero correlation, so any number may be in flight up to `max_in_flight` from `HELLO`.
- **RPC has two distinct identifiers**: RPC REQUEST retains its explicit 16-byte UUID for end-to-end caller/worker routing. `RESPONSE` frames carry correlation 0 and are matched by that UUID.
- **RPC registrations are session-scoped**: A worker reconnecting after disconnect or broker restart MUST send `Subscribe` again before it will receive new requests.
- **Out-of-band messages**: Notifications carry correlation 0 and clients MUST route them by message type and subscription identifier.
- **Order guarantees**: The broker does not guarantee response order, including between requests with the same message type.

### 5. Receive Responses

Each request receives at least one response frame carrying its correlation. Response format is domain-specific (see domain specs); every error uses the common envelope.

### Broker-initiated GOAWAY

At any time after `HELLO` the broker may send `GOAWAY` (type 4, correlation 0):

```
[u16 BE] code       protocol block of error-codes.md: 0054 DRAINING, 0055 FORCED_CLOSE, 0056 LIMIT_CHANGED
[u32 BE] grace_ms   how long the broker keeps serving before it closes; 0 means now
[string] reason     diagnostic only
```

`DRAINING` reaches every live session the moment a planned drain begins; the
broker keeps answering requests for `grace_ms` and then closes. Clients
SHOULD finish in-flight work and reconnect within the grace. `GOAWAY` is not
an error on any pending request: those are answered or lost with the
connection as usual.

### 6. Close Connection

Clients SHOULD:

- Send WebSocket close frame or TCP FIN gracefully
- Clean up resources
- Discard pending requests on abrupt close
  Clients MUST:
- Assume connection is closed if transport layer signals close
- Reconnect and explicitly rebuild any required session-scoped state

## Authentication & Security

### Authentication Modes

Fitz brokers support two authentication modes controlled by server configuration:
**1. Authenticated Mode** (`FITZ_AUTH_REQUIRED=true`):

- JWT authentication is **required** for all connections
- CONNECT frame MUST include valid JWT
- Broker validates JWT signature and claims
- Missing or invalid JWT, including missing or unmapped route-family identity context, causes immediate connection close
  **2. Anonymous Mode** (`FITZ_AUTH_REQUIRED=false`):
- JWT authentication is **optional**
- CONNECT frame MAY include empty JWT or placeholder value
- Broker assigns default permissions (typically full access to all realms/areas)
- Broker always uses internal route family `1`
- Useful for development, testing, or trusted internal networks

### JWT (Authentication Mechanism)

**When authentication is required,** clients MUST:

1. Obtain a JWT from an external authentication service
2. Pass the compact JWT string in the CONNECT record
3. Treat JWT as opaque (do not parse or validate server-side)
4. Resend JWT on reconnect
   **When authentication is optional (anonymous mode),** clients MAY:

- Send empty JWT (zero-length payload)
- Send placeholder JWT (e.g., "anonymous")
- Omit JWT field (broker accepts connection without authentication)
  Clients MUST NOT:
- Generate or sign JWTs
- Validate JWT signatures
- Cache or reuse JWTs across sessions
- Attempt to decode JWT claims

### Authorization

Authorization is **always server-side**:

- **Authenticated mode:** Broker validates JWT claims against route permissions
- **Anonymous mode:** Broker uses default permission set (no JWT validation)
- If client sends unauthorized request, broker returns error
- Clients MUST NOT attempt local permission checking

### TLS on Untrusted Networks

When traffic crosses an untrusted network, clients MUST:
Clients MUST:

- Use `wss://` for WebSocket (never plain `ws://`)
- Use TLS for TCP (never plain TCP on untrusted networks)
- Validate server certificate chain against system CA roots
- Perform hostname verification (certificate CN or SAN must match broker hostname)
- Reject expired certificates
- Reject revoked certificates (if OCSP stapling available)
- Reject self-signed certificates (unless explicitly in trust store via deployment config)

Browser WebSocket deployments must also send a normal HTTP `Origin` header
during the upgrade. Fitz accepts only exact configured browser origins, such as
`https://app.example.com`, when an origin allowlist is configured. Configured
origins must not include paths, query strings, fragments, wildcards, or trailing
slashes.

  **Development/Testing (MAY Skip with Explicit Flag):**
  Clients MAY accept self-signed or invalid certificates ONLY if:
- Explicitly enabled via configuration flag (e.g., `insecure_skip_verify=true`)
- User acknowledges security risk in documentation
- Never default to insecure; require explicit opt-in
  Clients MUST NOT:
- Skip certificate validation to "work around" deployment issues
- Accept expired or revoked certificates without explicit flag
- Disable hostname verification
- Accept any certificate on an untrusted network

## Flow Control & Backpressure

Clients MUST implement queueing, backoff, and bounded concurrency:

- Implement configurable write queue with maximum size.
- Enforce a configurable per-connection maximum in-flight work limit.
- When the queue or concurrency limit is reached, surface a retryable backpressure error or wait for capacity before admitting more work. Do NOT silently drop or spawn unbounded work.
- Implement exponential backoff for retries.

**Server backpressure signaling:**

Brokers signal backpressure through **domain error responses**. There is no separate backpressure frame. When a domain's internal queue is full, the broker returns a domain error in the standard response format:

- RPC: `6005 = ERR_BUSY`
- Queue: `4005 = ERR_QUEUE_FULL`
- Other domains: connection close if internal buffers overflow

**Client behavior on backpressure errors:**

1. Pause sending to the affected domain
2. Apply exponential backoff (starting at 100ms, max 30s)
3. Retry the failed operation after backoff
4. If backpressure persists, surface error to caller

**Notice domain exception:** Since PUBLISH is fire-and-forget with no response, the broker silently drops notifications under backpressure. Clients have no visibility into dropped notices — this is by design (best-effort semantics).

## Routing

Routes are **opaque URI-like strings** that address domain resources. The
message type identifies the operation; some domains accept operation suffixes
for live protocol ergonomics and canonicalize them to the resource identity for
authorization.

### Route Format

```
{scheme}://{realm}/{area}/{resource}
```

**Components:**
| Component | Type | Example | Rules |
| ----------- | ------ | ------------------------- | ----------------------------------------------------- |
| `scheme` | string | `kv`, `queue`, `notice` | Identifies domain; MUST match known domain list |
| `realm` | string | `prod`, `tenant-123` | Opaque to client; case-sensitive |
| `area` | string | `app`, `system` | Opaque to client; case-sensitive |
| `resource` | string | `users`, `orders` | Opaque to client; may be omitted for admin operations |

No route segment may contain a control character (Unicode `Cc`, including
`0x00`). The broker rejects such a route with the operation's existing
invalid-route or invalid-pattern error. Clients need not pre-validate this.

**Route Examples:**

```
kv://prod/app/users              # KV resource
queue://prod/app/orders/send     # Queue enqueue route, authorized as queue://prod/app/orders
notice://prod/app/events         # Live fanout route
```

## HTTP-Like Design Principle

Fitz follows an **HTTP-like model** where every operation is explicitly
addressed on the wire, while domains may still maintain live session-scoped
state when their contract requires it:

### Core Analogy

**HTTP:**

```
POST /api/users HTTP/1.1
Host: example.com
Content-Type: application/json

{"name": "alice"}
```

**Fitz:**

```
Verb: PUT (MessageType=104)
Route: kv://prod/app/users
Payload: [tx_id][key][value]
```

### Key Principles

1. **Every operation is explicitly scoped**
   - Route-scoped example: KV PUT = `[tx_id][route][key][value]`
   - Route-scoped example: Queue ENQUEUE = `[route][body][delay]`
   - Session-scoped example: Stream APPEND = `[session_id][body]` after a prior BEGIN bound the session to one resource

2. **Operations are explicitly addressable**
   - Most operations carry a route on the wire
   - Session-based operations may carry an opaque handle instead of repeating the route
   - Connection loss does not require reconstructing hidden broker topology, though session-scoped domain state may still be cleaned up

3. **Verbs determine action** (like HTTP GET/POST/PUT/DELETE)
   - MessageType selects operation
   - Wire codes are stable ABI
   - Domain+Verb fully specifies behavior

4. **A fixed header plus value is the wire format** (like HTTP has headers+body)
   - 15-byte header: type, flags, correlation, length
   - Binary efficient, not text-based
   - Extensible without version negotiation

### Why This Matters

**For implementers:**

- Simple mental model: "It's like HTTP but binary and over WebSocket/TCP"
- Familiar patterns: routes, verbs, self-contained requests
- Easy to reason about: no hidden state machines

**For operations:**

- Debuggable: every message carries explicit addressing and can be inspected without hidden route context
- Explicitly addressed: the wire format does not depend on per-connection route defaults
- Session-scoped domains still require re-establishing live state after reconnect when the domain contract says that state is ephemeral

### Comparison

| Aspect         | HTTP                            | Fitz                                   |
| -------------- | ------------------------------- | -------------------------------------- |
| **Addressing** | URL path                        | Route (kv://realm/area/resource)       |
| **Verb**       | GET, POST, PUT, DELETE          | MessageType (100=BEGIN, 104=PUT, etc.) |
| **Transport**  | TCP + TLS                       | WebSocket or TCP + TLS                 |
| **Format**     | Text (headers + body)           | Binary (fixed header + value)          |
| **State**      | Stateless (cookies for session) | Explicit routing; some domains keep live session state |
| **Operations** | Self-contained requests         | Self-contained requests                |

## Route Acceptance Criteria (Authoritative)

Moved to [wire-route-shapes.md](wire-route-shapes.md): route acceptance criteria, global route rules, route shapes by domain, and the lock-in rule.

## Verbs

Verbs are the **primary behavior selector**. They determine what action a request performs.

### Verb Requirements

Clients MUST:

1. **Expose verbs as constants or enums** in the client's native language
   - Python: `class KvVerb: GET = "GET"; PUT = "PUT"`
   - Rust: `enum KvVerb { Get, Put, ... }`
   - JavaScript: `const KvVerb = { Get: "get", Put: "put" }`
2. **Never expose wire codes** in public API
3. **Map verbs to i16 wire codes internally**
4. **Treat wire codes as ABI-stable** (never reused, append-only)

### Verb Set (All Domains)

| Domain   | Verb               | Wire Code | Plane         | Notes                   |
| -------- | ------------------ | --------: | ------------- | ----------------------- |
| KV       | BEGIN              |       100 | Data          | Start transaction       |
| KV       | COMMIT             |       101 | Data          | Finalize transaction    |
| KV       | ROLLBACK           |       102 | Data          | Abort transaction       |
| KV       | GET                |       103 | Data          | Read key                |
| KV       | PUT                |       104 | Data          | Write key               |
| KV       | INSERT             |       105 | Data          | Insert (fail if exists) |
| KV       | DELETE             |       106 | Data          | Delete key              |
| KV       | DELETE_RANGE       |       107 | Data          | Delete key range        |
| KV       | SCAN               |       108 | Data          | Scan keys in range      |
| KV       | SUBSCRIBE          |       109 | Data          | Watch committed changes |
| KV       | UNSUBSCRIBE        |       110 | Data          | Stop watching changes   |
| KV       | NOTIFY             |       111 | Notification  | Committed change event  |
| Queue    | ENQUEUE            |       200 | Data          | Add message             |
| Queue    | RESERVE            |       202 | Data          | Lease message(s)        |
| Queue    | EXTEND             |       203 | Data          | Extend lease            |
| Queue    | COMPLETE           |       204 | Data          | Mark complete           |
| Queue    | SUBSCRIBE          |       207 | Data          | Subscribe to pattern    |
| Queue    | UNSUBSCRIBE        |       208 | Data          | Unsubscribe pattern     |
| Queue    | NOTIFY             |       209 | Notification  | Availability event      |
| RPC      | SUBSCRIBE_WORKER   |       300 | Data          | Register worker         |
| RPC      | UNSUBSCRIBE_WORKER |       301 | Data          | Unregister worker       |
| RPC      | REQUEST            |       302 | Data          | Send request            |
| RPC      | RESPONSE           |       303 | Data          | Send response           |
| Lease    | ACQUIRE            |       400 | Data          | Acquire lease           |
| Lease    | RENEW              |       401 | Data          | Extend lease            |
| Lease    | RELEASE            |       402 | Data          | Release lease           |
| Lease    | QUERY              |       403 | Data          | Query lease status      |
| Lease    | SUBSCRIBE          |       407 | Data          | Subscribe to changes    |
| Lease    | UNSUBSCRIBE        |       408 | Data          | Unsubscribe             |
| Lease    | NOTIFY             |       409 | Server→Client | Lease change event      |
| Notice   | PUBLISH            |       500 | Data          | Publish message         |
| Notice   | SUBSCRIBE          |       501 | Data          | Subscribe to pattern    |
| Notice   | UNSUBSCRIBE        |       502 | Data          | Unsubscribe             |
| Notice   | UNSUBSCRIBE_ALL    |       503 | Data          | Clear all subscriptions |
| Notice   | NOTIFY             |       504 | Server→Client | Delivery                |
| Stream   | BEGIN              |       600 | Data          | Start session           |
| Stream   | APPEND             |       601 | Data          | Append record           |
| Stream   | COMMIT             |       602 | Data          | Finalize session        |
| Stream   | ROLLBACK           |       603 | Data          | Abort session           |
| Stream   | READ               |       604 | Data          | Read range              |
| Stream   | LAST               |       605 | Data          | Get last record         |
| Stream   | GET_METADATA       |       606 | Data          | Get metadata            |
| Stream   | SUBSCRIBE          |       607 | Data          | Subscribe to changes    |
| Stream   | UNSUBSCRIBE        |       608 | Data          | Unsubscribe             |
| Stream   | NOTIFY             |       609 | Server→Client | Change notification     |
| Schedule | CREATE             |       700 | Data          | Create schedule         |
| Schedule | CANCEL             |       701 | Data          | Cancel schedule         |
| Schedule | LIST               |       702 | Data          | List schedules          |
| Schedule | SUBSCRIBE          |       703 | Data          | Subscribe to fires      |
| Schedule | UNSUBSCRIBE        |       704 | Data          | Unsubscribe             |
| Schedule | NOTIFY             |       705 | Server→Client | Fire notification       |
| Schedule | CREATE_BATCH       |       706 | Data          | Broker batch extension  |

### MessageType Ranges Are Non-Overlapping

Each domain occupies an exclusive 100-code block. The broker's mux layer routes by numeric range — **no overlap, no disambiguation needed**.
**Clients MUST use the wire codes from the Constants & Message Registry section.**
